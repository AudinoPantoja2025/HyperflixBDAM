"""Pruebas unitarias del consumidor CDC, sin MongoDB Atlas real."""

from src.streaming import ChangeStreamConsumer


class FakeStream:
    def __init__(self, changes):
        self.changes = iter(changes)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def try_next(self):
        try:
            return next(self.changes)
        except StopIteration:
            return None


class FakeDB:
    def __init__(self, changes):
        self.changes = changes
        self.pipeline = None
        self.options = None

    def watch(self, pipeline, **options):
        self.pipeline = pipeline
        self.options = options
        return FakeStream(self.changes)


def _change(token, collection="comments", operation="insert"):
    return {
        "_id": {"_data": token},
        "operationType": operation,
        "ns": {"db": "sample_mflix", "coll": collection},
        "documentKey": {"_id": token},
        "fullDocument": {"_id": token, "text": "ejemplo"},
    }


def test_cdc_persiste_micro_lote_y_checkpoint(data_lake):
    db = FakeDB([_change("a"), _change("b", operation="update")])
    consumer = ChangeStreamConsumer(
        db, data_lake, ["comments"], "RUN-CDC", "2026-09-16", microbatch_size=2
    )
    result = consumer.run(duration_seconds=0.02)

    assert result.events == 2
    assert result.microbatches == 1
    assert result.operations == {"insert": 1, "update": 1}
    files = list(data_lake.cdc_partition_dir("comments", "2026-09-16").glob("*.jsonl"))
    assert len(files) == 1
    assert len(files[0].read_text(encoding="utf-8").splitlines()) == 2
    assert data_lake.read_cdc_checkpoint("_database")["resume_token"] == {"_data": "b"}


def test_cdc_reanuda_desde_checkpoint_global(data_lake):
    data_lake.write_cdc_checkpoint("_database", {"resume_token": {"_data": "previo"}})
    db = FakeDB([])
    consumer = ChangeStreamConsumer(
        db, data_lake, ["comments"], "RUN-CDC", "2026-09-16", max_await_time_ms=1
    )
    consumer.run(duration_seconds=0.01)

    assert db.options["resume_after"] == {"_data": "previo"}
    assert db.pipeline == [{"$match": {"ns.coll": {"$in": ["comments"]}}}]
