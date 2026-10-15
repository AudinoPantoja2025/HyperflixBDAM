"""Pruebas del orquestador iac_setup (sin ejecutar el bootstrap real)."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import iac_setup as iac  # noqa: E402


def test_requisitos_minimos_python():
    assert iac.REQUIRED_PYTHON >= (3, 9)


def test_python_version_valida(monkeypatch):
    monkeypatch.setattr(iac.sys, "version_info", (3, 12, 0, "final", 0))
    assert iac.validar_python() is True


def test_python_version_antigua_rechazada(monkeypatch):
    monkeypatch.setattr(iac.sys, "version_info", (3, 7, 9, "final", 0))
    assert iac.validar_python() is False


def test_ruta_python_del_venv_windows(monkeypatch):
    monkeypatch.setattr(iac.os, "name", "nt")
    assert iac._venv_python().as_posix().endswith(".venv/Scripts/python.exe")


def test_ruta_python_del_venv_unix(monkeypatch):
    monkeypatch.setattr(iac.os, "name", "posix")
    assert iac._venv_python().as_posix().endswith(".venv/bin/python")


def test_dependencias_requeridas_estan_declaradas(monkeypatch):
    for module, package in iac.REQUIRED_DEPS:
        assert isinstance(package, str) and isinstance(module, str) and package
    reqs = iac.REQUIREMENTS.read_text(encoding="utf-8")
    for _, package in iac.REQUIRED_DEPS:
        assert package.split(">")[0].split("=")[0] in reqs


def test_crear_zonas_construye_estructura(monkeypatch, tmp_path):
    monkeypatch.setattr(iac, "ROOT", tmp_path)
    assert iac.crear_zonas() is True
    data_lake = tmp_path / "data_lake"
    assert (data_lake / "raw").is_dir()
    assert (data_lake / "processed").is_dir()
    assert (data_lake / "curated").is_dir()


def test_check_env_faltante_copia_plantilla(monkeypatch, tmp_path):
    (tmp_path / ".env.example").write_text(
        "MONGODB_URI=mongodb+srv://<usuario>:<password>@cluster0.x.mongodb.net/\n"
        "MONGODB_DATABASE=sample_mflix\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(iac, "ENV_FILE", tmp_path / ".env")
    monkeypatch.setattr(iac, "ENV_TEMPLATE", tmp_path / ".env.example")
    assert iac.check_env() is True
    assert (tmp_path / ".env").exists()


def test_check_env_sin_env_ni_plantilla(monkeypatch, tmp_path):
    monkeypatch.setattr(iac, "ENV_FILE", tmp_path / ".env")
    monkeypatch.setattr(iac, "ENV_TEMPLATE", tmp_path / ".env.example")
    assert iac.check_env() is False


def test_check_env_presente(monkeypatch, tmp_path):
    env = tmp_path / ".env"
    env.write_text(
        "MONGODB_URI=mongodb+srv://cluster0.abc.mongodb.net/\n"
        "MONGODB_DATABASE=sample_mflix\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(iac, "ENV_FILE", env)
    monkeypatch.setattr(iac, "ENV_TEMPLATE", tmp_path / ".env.example")
    assert iac.check_env() is True