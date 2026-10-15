"""Pruebas de MongoConnection sin conexion real (mocks de PyMongo).

El objetivo es verificar el contrato: ping obligatorio, reintentos, error
tipificado al agotarse y cierre garantizado (manejador de contexto).
"""

import pytest

from src.mongo_client import MongoConnection, MongoConnectionError


class FakeClientOK:
    """Cliente que responde el ping y devuelve una base falsa."""

    def __init__(self, uri, serverSelectionTimeoutMS):
        self.uri = uri
        self.closed = False

    @property
    def admin(self):
        return self

    def command(self, cmd):
        assert cmd == "ping"
        return {"ok": 1}

    def __getitem__(self, name):
        return FakeDB(name)

    def close(self):
        self.closed = True


class FakeDB:
    def __init__(self, name):
        self.name = name


class FakeClientFail:
    """Cliente que siempre falla el ping (servidor/credenciales invalidas)."""

    def __init__(self, uri, serverSelectionTimeoutMS):
        self.uri = uri
        self.closed = False

    @property
    def admin(self):
        return self

    def command(self, cmd):
        from pymongo.errors import ConnectionFailure

        raise ConnectionFailure("fallo de red simulado")

    def close(self):
        self.closed = True


@pytest.fixture
def mock_ok(monkeypatch):
    import src.mongo_client as mc

    monkeypatch.setattr(mc, "MongoClient", FakeClientOK)


@pytest.fixture
def mock_fail(monkeypatch):
    import src.mongo_client as mc

    monkeypatch.setattr(mc, "MongoClient", FakeClientFail)


def test_conexion_y_ping_con_mock(mock_ok):
    conn = MongoConnection("mongodb://fake", "sample_mflix", retries=1)
    client, db = conn.connect()
    assert db.name == "sample_mflix"
    conn.close()
    assert client.closed is True


def test_error_de_conexion_agota_reintentos(mock_fail):
    conn = MongoConnection("mongodb://fake", "sample_mflix", retries=2, retry_delay=0)
    with pytest.raises(MongoConnectionError):
        conn.connect()


def test_context_manager_cierra_en_salida(mock_ok):
    conn = MongoConnection("mongodb://fake", "sample_mflix", retries=1)
    with conn as c:
        assert c._db is not None
        assert c._db.name == "sample_mflix"
    assert conn._client is None  # cierre garantizado al salir del contexto