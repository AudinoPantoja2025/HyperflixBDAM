"""Fixtures compartidos para las pruebas del pipeline."""

import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import load_settings  # noqa: E402
from src.data_lake import DataLake  # noqa: E402


@pytest.fixture
def env_file(tmp_path):
    """Archivo .env de prueba (sin credenciales reales)."""
    path = tmp_path / ".env.test"
    path.write_text(
        "\n".join(
            [
                "MONGODB_URI=mongodb+srv://testuser:mypass@cluster0.test.mongodb.net/",
                "MONGODB_DATABASE=sample_mflix",
                "MONGODB_COLLECTIONS=comments,movies",
                "MONGO_BATCH_SIZE=500",
                "DATA_LAKE_ROOT=data_lake_test",
            ]
        ),
        encoding="utf-8",
    )
    return str(path)


@pytest.fixture
def settings(env_file):
    return load_settings(env_path=env_file)


@pytest.fixture
def empty_env_file(tmp_path):
    """Archivo .env vacio para probar configuracion incompleta."""
    path = tmp_path / ".env.vacio"
    path.write_text("", encoding="utf-8")
    return str(path)


@pytest.fixture
def sample_comments_docs():
    """Documentos de la coleccion comments de muestra (fechas y texto realista)."""
    return [
        {
            "_id": "5a9427648b0beebeb69579c7",
            "name": "Mercedes Tyler",
            "email": "mercedes_tyler@fakegmail.com",
            "movie_id": "573a1390f29313caabcd446f",
            "text": "Great movie!",
            "date": datetime(1994, 1, 4, 10, 27, 5),
        },
        {
            "_id": "5a9427648b0beebeb69579c8",
            "name": "Roberto Salazar",
            "email": "roberto@fakegmail.com",
            "movie_id": "573a1390f29313caabcd446f",
            "text": "Nice plot twist.",
            "date": datetime(2005, 7, 20, 3, 15, 0),
        },
    ]


@pytest.fixture
def sample_user_docs():
    return [
        {
            "_id": "59b99db4cfa9a34dcd7885b0",
            "name": "Mercedes Tyler",
            "email": "mercedes_tyler@fakegmail.com",
            "password": "s3cr3t-password",
        }
    ]


@pytest.fixture
def sample_movie_docs():
    return [
        {
            "_id": "573a1390f29313caabcd446f",
            "title": "The Great Train Robbery",
            "year": 1903,
            "runtime": 12,
            "rated": "TV-G",
            "genres": ["Short", "Western"],
            "directors": ["Edwin S. Porter"],
            "released": datetime(1903, 12, 1),
            "imdb": {"rating": 7.4, "votes": 9847, "id": "tt0438488"},
            "tomatoes": {"viewer": {"rating": 3.7}},
        }
    ]


@pytest.fixture
def sample_theater_docs():
    return [
        {
            "_id": "59a47286cfa9a3a73e51e72c",
            "theaterId": 1001,
            "location": {
                "address": {
                    "street1": "400 West 43rd Street",
                    "city": "New York",
                    "state": "NY",
                    "zipcode": "10036",
                },
                "geo": {"type": "Point", "coordinates": [-73.992983, 40.759808]},
            },
        }
    ]


@pytest.fixture
def data_lake(tmp_path):
    """Data Lake aislado en un directorio temporal."""
    dl = DataLake(tmp_path / "data_lake")
    dl.create_zones()
    return dl