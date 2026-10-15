"""Automatizacion de infraestructura y ciclo de vida del pipeline ETL.

Decision de diseno:
    - Python (este script): automatiza el entorno local, la verificacion de
      conectividad con MongoDB Atlas (sample_mflix), las zonas del Data Lake y
      la ejecucion del ETL. Es la herramienta recomendada para este proyecto
      (mismo lenguaje, sin dependencias de nube, reproducible academicamente).
    - Terraform (infra/terraform/): gestiona SOLO el aprovisionamiento del
      cluster en MongoDB Atlas como codigo (ver su README); no ejecuta el ETL
      ni administra el Data Lake local.

Modos de ejecucion:
    python scripts/automatizacion.py --check      verificacion del entorno completo
    python scripts/automatizacion.py --setup      crear .env (plantilla) + zonas
    python scripts/automatizacion.py --pipeline   ejecutar el ETL (main.py)
    python scripts/automatizacion.py --all        check + setup + pipeline
    python scripts/automatizacion.py --all --with-tests   (incluye pytest)
    python scripts/automatizacion.py --full       cadena completa: OLAP -> ETL ->
                                                 DW -> benchmark -> Spark ->
                                                 dashboard (streaming aparte)

Compatible con la base 'sample_mflix' y sus colecciones:
    users, movies, comments, theaters
"""

import argparse
import importlib.util
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

from src.config import DEFAULT_COLLECTIONS, load_settings, mask_uri  # noqa: E402
from src.data_lake import DataLake  # noqa: E402
from src.extractor import list_collections  # noqa: E402
from src.logger import get_logger  # noqa: E402
from src.mongo_client import MongoConnection  # noqa: E402

logger = get_logger("automatizacion")

ENV_FILE = ROOT / ".env"
ENV_TEMPLATE = ROOT / ".env.example"
REQUIREMENTS = ROOT / "requirements.txt"
MAIN_PY = ROOT / "main.py"

REQUIRED_DEPS = {
    "pymongo": "PyMongo",
    "pandas": "pandas",
    "pyarrow": "pyarrow",
    "dotenv": "python-dotenv",
}


def _exit_ok(msg: str):
    print(f"[OK]   {msg}")


def _exit_warn(msg: str):
    print(f"[AVISO] {msg}")


def _exit_err(msg: str):
    print(f"[ERROR] {msg}")
    return False


def check_dependencies() -> bool:
    """Verifica que las dependencias requeridas esten instaladas."""
    ok = True
    for module, name in REQUIRED_DEPS.items():
        found = importlib.util.find_spec(module) is not None
        ver = ""
        if found:
            try:
                from importlib.metadata import version

                ver = version(name)
            except Exception:
                ver = "?"
        if found:
            _exit_ok(f"dependencia {name} ({ver})")
        else:
            _exit_err(f"dependencia {name} NO instalada; ejecute: pip install -r requirements.txt")
            ok = False
    return ok


def check_env_file() -> bool:
    """Verifica la existencia de .env y de las variables obligatorias."""
    if not ENV_FILE.exists():
        return _exit_err(
            f"falta '.env'. Copie de la plantilla: Copy-Item .env.example .env (y complete credenciales)"
        )
    load_dotenv(dotenv_path=str(ENV_FILE))
    for var in ("MONGODB_URI", "MONGODB_DATABASE"):
        if not (__import__("os").environ.get(var) or __import__("os").environ.get("MONGO_URI")):
            _exit_err(f"variable '{var}' no definida en .env")
            return False
    _exit_ok("archivo .env presente con variables obligatorias")
    return True


def check_data_lake_zones() -> bool:
    """Verifica que existan las zonas RAW, PROCESSED y CURATED."""
    root = Path(__import__("os").environ.get("DATA_LAKE_ROOT", "data_lake"))
    zones = DataLake(root).zones
    missing = [z for z in zones if not (root / z).is_dir()]
    if missing:
        return _exit_err(f"faltan zonas del Data Lake: {missing}; ejecute --setup")
    _exit_ok(f"zonas del Data Lake '{root}': {', '.join(zones)}")
    return True


def check_mongodb() -> bool:
    """Conecta a MongoDB Atlas, lista colecciones y valida las configuradas."""
    try:
        settings = load_settings(env_path=str(ENV_FILE))
    except Exception as exc:  # noqa: BLE001
        return _exit_err(f"configuracion invalida: {exc}")
    conn = MongoConnection(settings.mongodb_uri, settings.mongodb_database, retries=2)
    try:
        _, db = conn.connect()
        _exit_ok(f"conexion a '{settings.mongodb_database}' (uri: {mask_uri(settings.mongodb_uri)})")
    except Exception as exc:  # noqa: BLE001
        return _exit_err(f"no fue posible conectar a MongoDB Atlas: {exc}")

    try:
        existing = list_collections(db)
    finally:
        conn.close()

    _exit_ok(f"colecciones en '{settings.mongodb_database}': {', '.join(existing)}")
    target = settings.mongodb_collections or list(DEFAULT_COLLECTIONS)
    missing = [c for c in target if c not in existing]
    if missing:
        return _exit_err(
            f"colecciones configuradas no encontradas: {missing}. "
            "Revisar MONGODB_COLLECTIONS en .env (o usar las por defecto)."
        )
    _exit_ok(f"colecciones objetivo validadas: {', '.join(target)}")
    return True


def setup_env_file() -> bool:
    """Crea .env desde la plantilla sin sobrescribir si ya existe."""
    if ENV_FILE.exists():
        _exit_warn("'.env' ya existe; no se sobrescribe (complete/editelo si falta algo)")
        return True
    if not ENV_TEMPLATE.exists():
        return _exit_err("no existe '.env.example' en la raiz del proyecto")
    shutil.copyfile(ENV_TEMPLATE, ENV_FILE)
    _exit_ok(f"'.env' creado desde plantilla: {ENV_FILE}")
    _exit_warn("EDITE '.env' y reemplace <usuario>:<password> por sus credenciales reales de Atlas")
    return True


def setup_zones() -> bool:
    root = Path(__import__("os").environ.get("DATA_LAKE_ROOT", "data_lake"))
    DataLake(root).create_zones()
    return True


def run_tests() -> bool:
    cmd = [sys.executable, "-m", "pytest", "-q", str(ROOT / "tests")]
    print(f"\n-- Ejecutando pruebas: {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=str(ROOT)).returncode == 0


def run_pipeline() -> bool:
    print("\n-- Ejecutando el pipeline ETL completo (puede tardar varios minutos)")
    return subprocess.run([sys.executable, str(MAIN_PY)], cwd=str(ROOT)).returncode == 0

FULL_STEPS = [
    {"key": "estructura",
     "desc": "Estructura OLTP + indices (crear_estructura.py)",
     "cmd": [sys.executable, "scripts/crear_estructura.py"],
     "timeout": 300},
    {"key": "olap",
     "desc": "OLTP -> OLAP interacciones_analiticas (transformar_oltp_a_olap.py)",
     "cmd": [sys.executable, "scripts/transformar_oltp_a_olap.py"],
     "timeout": 600},
    {"key": "etl",
     "desc": "ETL Mongo -> Data Lake -> Parquet (main.py)",
     "cmd": [sys.executable, "main.py", "--collections",
             "users,movies,comments,theaters"],
     "timeout": 1200},
    {"key": "dw",
     "desc": "Data Warehouse estrella (data_warehouse.py)",
     "cmd": [sys.executable, "data_warehouse.py", "--db", "sample_mflix"],
     "timeout": 600},
    {"key": "benchmark",
     "desc": "Benchmark 10 consultas (benchmarking_consultas.py)",
     "cmd": [sys.executable, "scripts/benchmarking_consultas.py"],
     "timeout": 600},
    {"key": "spark",
     "desc": "Job Spark INTERACCIONES_CATEGORIA (spark_jobs.py)",
     "cmd": [sys.executable, "spark_jobs.py", "--master", "local[*]",
             "--single-file"],
     "timeout": 600},
    {"key": "dashboard",
     "desc": "Dashboard de monitoreo PNG (dashboard_monitoreo.py)",
     "cmd": [sys.executable, "scripts/dashboard_monitoreo.py",
             "--db", "sample_mflix", "--benchmark-n", "10"],
     "timeout": 600},
]


def run_step(step: dict) -> tuple:
    """Ejecuta un paso con timeout; retorna (ok, segundos)."""
    print("\n" + "-" * 70)
    print(f"PASO [{step['key']}] {step['desc']}")
    print(f"$ {' '.join(step['cmd'])}")
    print("-" * 70)
    t0 = time.perf_counter()
    try:
        rc = subprocess.run(step["cmd"], cwd=str(ROOT),
                            timeout=step["timeout"]).returncode
    except subprocess.TimeoutExpired:
        print(f"[ERROR] paso '{step['key']}' excedio {step['timeout']} s")
        return False, round(time.perf_counter() - t0, 1)
    secs = round(time.perf_counter() - t0, 1)
    print(f"Paso '{step['key']}': {'OK' if rc == 0 else 'FALLO'} ({secs} s)")
    return rc == 0, secs


def cmd_full(with_tests: bool = False) -> bool:
    """Ejecuta la cadena completa con parada ante el primer fallo."""
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = []
    ok_all = True

    if with_tests and not run_tests():
        return False

    for step in FULL_STEPS:
        ok, secs = run_step(step)
        results.append({"step": step["key"], "ok": ok, "seconds": secs})
        if not ok:
            ok_all = False
            print(f"\n[ERROR] cadena detenida en el paso '{step['key']}'")
            break

    total = round(sum(r["seconds"] for r in results), 1)
    audit = {"mode": "full", "started": stamp, "steps": results,
             "total_seconds": total, "ok": ok_all}
    try:
        out_dir = ROOT / "data_lake" / "audit"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"automatizacion_{stamp}.json").write_text(
            json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001 - el reporte no debe tumbar el run
        print(f"[AVISO] no se pudo escribir la auditoria: {exc}")

    print("\n" + "=" * 70)
    print("RESUMEN DE LA CADENA COMPLETA")
    print("=" * 70)
    for r in results:
        print(f"  {r['step']:<12} {'OK' if r['ok'] else 'FALLO':<6} {r['seconds']:>8} s")
    print("-" * 70)
    print(f"  {'TOTAL':<12} {'':<6} {total:>8} s")
    print("=" * 70)
    print("Estado:", "CADENA COMPLETADA OK" if ok_all else "CADENA DETENIDA CON ERRORES")
    return ok_all


def cmd_check() -> bool:
    print("=" * 70)
    print(" 1. DEPENDENCIAS")
    print("=" * 70)
    r1 = check_dependencies()
    print("\n" + "=" * 70)
    print(" 2. CONFIGURACION (.env)")
    print("=" * 70)
    r2 = check_env_file()
    print("\n" + "=" * 70)
    print(" 3. ZONAS DEL DATA LAKE")
    print("=" * 70)
    r3 = check_data_lake_zones()
    print("\n" + "=" * 70)
    print(" 4. MONGODB ATLAS (sample_mflix)")
    print("=" * 70)
    r4 = check_mongodb()
    ok = all([r1, r2, r3, r4])
    print("\n" + "=" * 70)
    print("  RESULTADO:", "ENTORNO PREPARADO" if ok else "HUBO PROBLEMAS (revise los ERROR arriba)")
    print("=" * 70)
    return ok


def cmd_setup(with_tests: bool = False) -> bool:
    ok = True
    ok &= setup_env_file()
    ok &= setup_zones()
    if with_tests:
        ok &= run_tests()
    return ok


def main(argv=None):
    parser = argparse.ArgumentParser(description="Automatizacion de entorno y ETL")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="verificacion del entorno completo")
    group.add_argument("--setup", action="store_true", help="crear .env (plantilla) y zonas del Data Lake")
    group.add_argument("--pipeline", action="store_true", help="ejecutar el ETL (main.py)")
    group.add_argument("--all", action="store_true", help="check + setup + pipeline")
    group.add_argument("--full", action="store_true",
                       help="cadena completa: estructura, OLAP, ETL, DW, benchmark, Spark, dashboard")
    parser.add_argument("--with-tests", action="store_true", help="incluir pytest en --setup/--all/--full")
    args = parser.parse_args(argv)

    if args.check:
        sys.exit(0 if cmd_check() else 1)
    if args.setup:
        sys.exit(0 if cmd_setup(args.with_tests) else 1)
    if args.pipeline:
        sys.exit(0 if run_pipeline() else 1)
    if args.full:
        sys.exit(0 if cmd_full(args.with_tests) else 1)
    if args.all:
        ok = cmd_check()
        ok = cmd_setup(args.with_tests) and ok
        ok = run_pipeline() and ok
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()