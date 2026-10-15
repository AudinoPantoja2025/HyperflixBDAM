"""Pruebas del modulo de extraccion (con un MongoDB falso, sin red)."""

import pytest

from src.extractor import ExtractionError, collection_exists, extract_collection, extract_collection_stream


class FakeCollection:
    """Coleccion falsa: expone find() con batch_size() igual que PyMongo."""

    def __init__(self, docs):
        self.docs = docs

    def find(self, *args, **kwargs):
        return self

    def batch_size(self, n):
        return self

    def __iter__(self):
        return iter(self.docs)

    def count_documents(self, _=None):
        return len(self.docs)


class FakeDB:
    def __init__(self, collections):
        self._collections = collections

    def list_collection_names(self):
        return list(self._collections)

    def __getitem__(self, name):
        return self._collections[name]

    @property
    def name(self):
        return "fake_db"


@pytest.fixture
def fake_db(sample_comments_docs):
    return FakeDB({"comments": FakeCollection(sample_comments_docs), "empty": FakeCollection([])})


def test_extraccion_ok(fake_db, sample_comments_docs):
    docs = extract_collection(fake_db, "comments")
    assert len(docs) == 2
    assert docs[0]["_id"] == sample_comments_docs[0]["_id"]
    assert docs[0]["movie_id"] == sample_comments_docs[0]["movie_id"]


def test_extraccion_stream_ok(fake_db, sample_comments_docs):
    stream = list(extract_collection_stream(fake_db, "comments"))
    assert stream == sample_comments_docs


def test_coleccion_vacia(fake_db):
    assert extract_collection(fake_db, "empty") == []


def test_coleccion_inexistente_error(fake_db):
    with pytest.raises(ExtractionError):
        extract_collection(fake_db, "no_existe")


def test_collection_exists(fake_db):
    assert collection_exists(fake_db, "comments") is True
    assert collection_exists(fake_db, "xyz") is False