"""iac_setup.py - Orquestador de Entorno y Configuration as Code.

Objetivo pedagogico (ABP "Infraestructura como Codigo"):
    Elimina el "en mi maquina si funciona": quien clona el repositorio
    ejecuta UN solo comando y obtiene el entorno reproducible completo.

    python iac_setup.py

Pasos (bootstrap reproducible):
    1. Valida la version correcta de Python.
    2. Crea el entorno virtual (.venv) en la raiz del proyecto.
    3. Re-ejecuta este script DENTRO del venv (activacion automatica).
    4. Instala las dependencias de requirements.txt si faltan.
    5. Provisiona la estructura local del Data Lake (raw, processed, curated).
    6. Health check: valida que '.env' exista y hace ping a MongoDB Atlas.

Alcance (decision de diseno):
    - Este script gestiona el ENTORNO LOCAL y la configuracion como codigo.
    - La infraestructura en la nube (cluster MongoDB Atlas) se declara con
      Terraform en infra/terraform/ (ver su README): es la otra mitad de IaC.

Compatibilidad: Windows (PowerShell) y Linux/macOS (bash). Probado con la
base 'sample_mflix' de MongoDB Atlas.
"""

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
from importlib.metadata import version as pkg_version
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

VENV_DIR = ROOT / ".venv"
ENV_FILE = ROOT / ".env"
ENV_TEMPLATE = ROOT / ".env.example"
REQUIREMENTS = ROOT / "requirements.txt"
REQUIRED_PYTHON = (3, 9)

# (modulo importable, nombre del paquete en PyPI)
REQUIRED_DEPS = [
    ("pymongo", "pymongo"),
    ("pandas", "pandas"),
    ("pyarrow", "pyarrow"),
    ("dotenv", "python-dotenv"),
    ("pytest", "pytest"),
    ("pyspark", "pyspark"),
]


def _ok(msg: str) -> None:
    print(f"[OK]     {msg}")


def _warn(msg: str) -> None:
    print(f"[AVISO]  {msg}")


def _err(msg: str) -> None:
    print(f"[ERROR]  {msg}")


def _venv_python() -> Path:
    if os.name == "nt":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def _is_running_in_venv() -> bool:
    try:
        return Path(sys.prefix).resolve() == VENV_DIR.resolve()
    except OSError:
        return False


def validar_python() -> bool:
    current = (sys.version_info[0], sys.version_info[1])
    if current < REQUIRED_PYTHON:
        _err(
            f"version de Python {'.'.join(map(str, current))} NO soportada; "
            f"se requiere {'.'.join(map(str, REQUIRED_PYTHON))} o superior."
        )
        return False
    _ok(f"version de Python {'.'.join(map(str, current))} (>= {'.'.join(map(str, REQUIRED_PYTHON))})")
    return True


def crear_venv() -> Path:
    target = _venv_python()
    if target.exists():
        _ok(f"entorno virtual ya listo: {VENV_DIR}")
        return target

    _warn(f"no existe '{VENV_DIR}'; creando entorno virtual...")
    base = Path(sys.executable) if Path(sys.executable).exists() else Path("python")
    rc = subprocess.call([str(base), "-m", "venv", str(VENV_DIR)])
    if rc != 0 or not target.exists():
        _err(f"fallo al crear el entorno virtual con '{base}'")
        raise SystemExit(2)
    _ok(f"entorno virtual creado en {VENV_DIR}")
    return target


def _missing_dependencies() -> list:
    missing = []
    for module, package in REQUIRED_DEPS:
        if importlib.util.find_spec(module) is None:
            missing.append(package)
    return missing


def instalar_dependencias() -> bool:
    try:
        installed = [
            f"{package}=={pkg_version(package)}"
            for _, package in REQUIRED_DEPS
        ]
        _ok(f"dependencias ya instaladas en '{sys.prefix}': {', '.join(installed)}")
        return True
    except Exception:  # noqa: BLE001 - falta alguna dependencia -> se instala
        pass

    missing = _missing_dependencies()
    _warn(
        f"instalando dependencias de '{REQUIREMENTS.name}' "
        f"(faltan {len(missing)}: {', '.join(missing)})..."
    )
    cmd = [
        sys.executable, "-m", "pip", "install",
        "--disable-pip-version-check", "-r", str(REQUIREMENTS),
    ]
    rc = subprocess.call(cmd)
    if rc != 0:
        _err("no se pudieron instalar las dependencias; revise el error de pip")
        return False
    _ok(f"dependencias instaladas: {', '.join(REQUIREMENTS.read_text(encoding='utf-8').splitlines())}")
    return True


def crear_zonas() -> bool:
    from src.data_lake import DataLake

    DataLake(ROOT / "data_lake").create_zones()
    _ok("zonas del Data Lake creadas: raw, processed, curated")
    return True


def check_env() -> bool:
    if not ENV_FILE.exists():
        if ENV_TEMPLATE.exists():
            shutil.copyfile(ENV_TEMPLATE, ENV_FILE)
            _warn("'.env' no existia: se copio desde '.env.example'")
        else:
            _err("falta '.env' y no existe '.env.example' en la raiz del proyecto")
            return False

    content = ENV_FILE.read_text(encoding="utf-8", errors="replace")
    if any(tag in content for tag in ("<usuario>", "<password>", "SU_MONGODB_URI", "TU_URI")):
        _warn("'.env' conserva credenciales de ejemplo; reemplacelas por las reales de Atlas")
    _ok(f"archivo '.env' presente: {ENV_FILE}")
    return True


def check_mongodb() -> bool:
    from src.config import load_settings, mask_uri
    from src.mongo_client import MongoConnection

    try:
        settings = load_settings(env_path=str(ENV_FILE))
    except Exception as exc:  # noqa: BLE001 - configuracion invalida concreta
        _err(f"configuracion invalida: {exc}")
        return False

    conn = MongoConnection(settings.mongodb_uri, settings.mongodb_database,
                           timeout_ms=6000, retries=2)
    try:
        client, db = conn.connect()
        client.admin.command("ping")
        collections = sorted(db.list_collection_names())
        _ok(
            f"ping OK a MongoDB Atlas (db='{settings.mongodb_database}', "
            f"uri={mask_uri(settings.mongodb_uri)})"
        )
        _ok(f"colecciones disponibles: {', '.join(collections) or '(base vacia)'}")
        return True
    except Exception as exc:  # noqa: BLE001 - el ping decide la salud
        _err(f"no fue posible hacer ping a MongoDB Atlas: {exc}")
        return False
    finally:
        conn.close()


def health_check(do_ping: bool = True) -> bool:
    ok = check_env()
    if ok and do_ping:
        ok = check_mongodb()
    return ok


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Configuracion como Codigo: prepara el entorno reproducibile del pipeline."
    )
    parser.add_argument("--skip-install", action="store_true",
                        help="no (re)instalar dependencias de requirements.txt")
    parser.add_argument("--skip-ping", action="store_true",
                        help="no probar la conexion a MongoDB Atlas")
    args = parser.parse_args(argv)

    print("=" * 72)
    print(" IAC SETUP | Configuracion como Codigo (entorno reproducible)")
    print("=" * 72)

    ok = True

    print("\n[1/6] Validando version de Python")
    ok &= validar_python()

    print("\n[2/6] Creando/verificando entorno virtual (.venv)")
    crear_venv()

    # Bootstrap: re-ejecutar DENTRO del venv para que pip/imports queden aislados.
    if not _is_running_in_venv():
        print(f"\n[activacion] re-ejecutando dentro de {_venv_python()}",
              flush=True)
        code = subprocess.call([str(_venv_python()), str(__file__)] + (argv or []))
        return code

    print("\n[3/6] Gestionando dependencias")
    if args.skip_install:
        _warn("--skip-install: se omite la instalacion de dependencias")
    else:
        ok &= instalar_dependencias()

    print("\n[4/6] Provisionando estructura del Data Lake")
    ok &= crear_zonas()

    print("\n[5/6] Health check: archivo .env")
    ok &= check_env()

    print("\n[6/6] Health check: ping a MongoDB Atlas")
    if args.skip_ping:
        _warn("--skip-ping: se omite la conexion a MongoDB Atlas")
    else:
        ok &= check_mongodb()

    print("\n" + "=" * 72)
    print("  RESULTADO:",
          "ENTORNO PREPARADO (listo para ejecutar main.py)"
          if ok else "HUBO PROBLEMAS (revise los [ERROR] anteriores)")
    print("=" * 72)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))