"""Pruebas de configuracion y variables de entorno."""

import pytest

from src import config
from src.config import ConfigError, Settings, load_settings, mask_uri


def test_carga_correcta_de_variables(env_file):
    s = load_settings(env_path=env_file)
    assert s.mongodb_database == "sample_mflix"
    assert s.batch_size == 500
    assert s.mongodb_collections == ["comments", "movies"]
    assert s.mongodb_uri.startswith("mongodb+srv://")
    assert isinstance(s, Settings)


def test_configuracion_incompleta_lanza_error(empty_env_file):
    with pytest.raises(ConfigError) as exc:
        load_settings(env_path=empty_env_file)
    assert "MONGODB_URI" in str(exc.value)


def test_colecciones_por_defecto_si_no_se_configuran(tmp_path):
    path = tmp_path / ".env.min"
    path.write_text(
        "MONGODB_URI=mongodb+srv://u:p@cli.test.mongodb.net/\n"
        "MONGODB_DATABASE=sample_mflix\n",
        encoding="utf-8",
    )
    s = load_settings(env_path=str(path))
    assert s.mongodb_collections == config.DEFAULT_COLLECTIONS
    assert "users" in s.mongodb_collections


def test_mask_uri_no_expone_credenciales():
    uri = "mongodb+srv://usuario:miclave@cluster0.abcd.mongodb.net"
    masked = mask_uri(uri)
    assert "miclave" not in masked
    assert "usuario" not in masked
    assert "cluster0.abcd.mongodb.net" in masked