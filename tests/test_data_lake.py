"""Pruebas del Data Lake (zonas RAW, PROCESSED, CURATED y trazabilidad)."""

from datetime import datetime

from bson import json_util

from src.data_lake import DataLake


def test_creacion_de_las_tres_zonas(data_lake):
    for zone in ("raw", "processed", "curated"):
        assert (data_lake.root / zone).is_dir()


def test_escritura_y_relectura_raw(data_lake, sample_comments_docs):
    meta = data_lake.write_raw_jsonl("comments", sample_comments_docs, "RUN-1", "2026-09-15")
    assert meta["count"] == 2
    assert meta["zone"] == "raw"

    reread = data_lake.read_raw_jsonl("comments", "2026-09-15", run_id="RUN-1")
    assert len(reread) == 2
    # El _id se preserva exactamente (trazabilidad por identificador).
    assert reread[0]["_id"] == sample_comments_docs[0]["_id"]
    assert reread[0]["date"].year == 1994


def test_raw_no_sobrescribe_ejecuciones_previas(data_lake, sample_comments_docs):
    data_lake.write_raw_jsonl("comments", sample_comments_docs, "RUN-1", "2026-09-15")
    data_lake.write_raw_jsonl("comments", sample_comments_docs, "RUN-2", "2026-09-15")
    files = list(data_lake.raw_partition_dir("comments", "2026-09-15").glob("run_*.jsonl"))
    assert len(files) == 2
    docs = data_lake.read_raw_jsonl("comments", "2026-09-15")
    assert len(docs) == 4  # ambas corridas conviven, no se pisan


def test_metadata_sidecar(data_lake, sample_comments_docs):
    data_lake.write_raw_jsonl("comments", sample_comments_docs, "RUN-1", "2026-09-15")
    meta_path = data_lake.raw_partition_dir("comments", "2026-09-15") / "run_RUN-1.meta.json"
    assert meta_path.exists()
    import json

    with meta_path.open(encoding="utf-8") as fh:
        meta = json.load(fh)
    assert meta["count"] == 2
    assert meta["format"] == "jsonl"


def test_particiones_por_coleccion_y_fecha(data_lake):
    d = data_lake.raw_partition_dir("movies", "2026-09-15")
    assert d.name == "ingestion_date=2026-09-15"
    assert d.parent.name == "movies"
    assert d.parent.parent.name == "raw"


def test_formato_raw_es_json_util_valido(data_lake, sample_comments_docs):
    data_lake.write_raw_jsonl("comments", sample_comments_docs, "RUN-1", "2026-09-15")
    file = data_lake.raw_partition_dir("comments", "2026-09-15") / "run_RUN-1.jsonl"
    with file.open(encoding="utf-8") as fh:
        raw = fh.readline()
    doc = json_util.loads(raw)
    assert doc["_id"] == sample_comments_docs[0]["_id"]
    assert isinstance(doc["date"], datetime)


def test_write_rejected_persiste_duplicados(data_lake):
    data = [{"_id": "a", "x": 1}, {"_id": "a", "x": 2}, {"_id": "b", "x": 3}]
    import pandas as pd

    df = pd.DataFrame(data)
    from src import transformations as tf

    kept, rejected = tf.split_duplicates(df, subset=["_id"])
    assert len(rejected) == 1

    path = data_lake.write_rejected("comments", rejected, "RUN-1", "2026-09-15")
    assert path.exists()
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    assert "_id" in lines[0]