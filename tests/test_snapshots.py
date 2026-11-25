"""Tests for authoritative snapshot import and gap recovery."""

import base64
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MIRROR_DB_PATH", str(tmp_path / "mirror.db"))
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    from app import main

    with TestClient(main.app, raise_server_exceptions=False) as test_client:
        yield test_client


def send_events(client, events):
    manifest = []
    files = {}
    for index, event in enumerate(events):
        item = {k: v for k, v in event.items() if k != "body"}
        if "body" in event:
            field = f"f{index}"
            item["file"] = field
            files[field] = (item["path"].rsplit("/", 1)[-1], event["body"])
        manifest.append(item)
    files["manifest"] = (None, json.dumps(manifest))
    return client.post("/events", files=files)


def publish(seq, path, body=b"x"):
    return {"seq": seq, "path": path, "operation": "publish", "body": body}


def delete(seq, path):
    return {"seq": seq, "path": path, "operation": "delete"}


def send_snapshot(client, baseline, entries, path="/snapshots", baseline_in="manifest"):
    """Send a multipart snapshot.

    Entries may carry ``body`` bytes, which are attached as numbered files.
    """

    manifest_entries = []
    files = {}
    for index, entry in enumerate(entries):
        item = {k: v for k, v in entry.items() if k != "body"}
        if "body" in entry:
            field = f"s{index}"
            item["file"] = field
            files[field] = (entry["path"].rsplit("/", 1)[-1], entry["body"])
        manifest_entries.append(item)

    if baseline_in == "manifest":
        manifest = {"baseline_seq": baseline, "entries": manifest_entries}
        files["manifest"] = (None, json.dumps(manifest))
        return client.post(path, files=files)

    # baseline as a separate form field, manifest is a bare array
    files["manifest"] = (None, json.dumps(manifest_entries))
    return client.post(path, data={"baseline_seq": str(baseline)}, files=files)


# ---------------------------------------------------------------------------
# Core merge semantics
# ---------------------------------------------------------------------------


def test_snapshot_replaces_baseline_governed_paths(client):
    send_events(
        client,
        [
            publish(1, "pkg/a.tgz", b"a-v1"),
            publish(2, "pkg/c.tgz", b"c-v1"),
        ],
    )

    r = send_snapshot(
        client,
        10,
        [
            {"path": "pkg/a.tgz", "operation": "publish", "body": b"a-authoritative"},
            {"path": "pkg/c.tgz", "operation": "publish", "body": b"c-authoritative"},
        ],
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["baseline_seq"] == 10
    assert data["watermark_before"] == 0
    assert data["watermark_after"] == 10

    assert client.get("/artifacts/pkg/a.tgz").content == b"a-authoritative"
    assert client.get("/artifacts/pkg/c.tgz").content == b"c-authoritative"
    assert client.get("/artifacts/pkg/a.tgz").headers["x-sequence"] == "10"

    status = client.get("/status").json()
    assert status["watermark"] == 10
    assert status["last_snapshot_baseline"] == 10


def test_paths_missing_from_snapshot_are_marked_deleted(client):
    send_events(
        client,
        [
            publish(1, "keep-listed.tgz", b"k"),
            publish(2, "ghost.tgz", b"g"),
            delete(4, "already-gone.tgz"),
        ],
    )

    r = send_snapshot(
        client,
        10,
        [{"path": "keep-listed.tgz", "operation": "publish", "body": b"k2"}],
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["missing_paths"] == ["already-gone.tgz", "ghost.tgz"]

    assert client.get("/artifacts/ghost.tgz").status_code == 404
    assert client.get("/artifacts/already-gone.tgz").status_code == 404
    # Tombstones are visible in the catalog with include_deleted.
    catalog = {
        a["path"]: a
        for a in client.get("/artifacts", params={"include_deleted": "true"}).json()[
            "artifacts"
        ]
    }
    assert catalog["ghost.tgz"]["operation"] == "delete"
    assert catalog["ghost.tgz"]["latest_seq"] == 10
    assert catalog["already-gone.tgz"]["latest_seq"] == 10


def test_higher_sequence_state_is_preserved(client):
    send_events(
        client,
        [
            publish(1, "old.tgz", b"old"),
            publish(5, "future-publish.tgz", b"future"),
            publish(4, "future-delete.tgz", b"fd"),
            delete(6, "future-delete.tgz"),
        ],
    )

    # Snapshot baseline 3: future-publish (seq 5) and future-delete (seq 6)
    # are ahead. The snapshot even carries a stale publish for the deleted
    # path, which must be ignored.
    r = send_snapshot(
        client,
        3,
        [
            {"path": "old.tgz", "operation": "publish", "body": b"old-fixed"},
            {"path": "future-publish.tgz", "operation": "delete"},
            {"path": "future-delete.tgz", "operation": "publish", "body": b"stale"},
        ],
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert set(data["preserved_paths"]) == {
        "future-publish.tgz",
        "future-delete.tgz",
    }
    assert data["skipped_paths"] == ["future-delete.tgz", "future-publish.tgz"]

    assert client.get("/artifacts/old.tgz").content == b"old-fixed"
    # Newer publish wins over the snapshot delete marker.
    fp = client.get("/artifacts/future-publish.tgz")
    assert fp.status_code == 200
    assert fp.content == b"future"
    assert fp.headers["x-sequence"] == "5"
    # Newer delete wins over the stale snapshot publish.
    assert client.get("/artifacts/future-delete.tgz").status_code == 404


def test_paths_ahead_and_absent_from_snapshot_are_not_deleted(client):
    send_events(client, [publish(8, "ahead-only.tgz", b"ahead")])
    r = send_snapshot(client, 5, [])
    assert r.status_code == 200, r.text
    assert r.json()["preserved_paths"] == ["ahead-only.tgz"]
    assert client.get("/artifacts/ahead-only.tgz").content == b"ahead"


def test_stale_baseline_is_conflict_and_changes_nothing(client):
    send_events(client, [publish(1, "a.tgz", b"a")])
    assert client.post("/gc", json={"limit": 10}).status_code == 200
    assert client.get("/status").json()["watermark"] == 1

    # Baseline equal to watermark is still allowed (idempotent re-import)...
    r = send_snapshot(client, 1, [{"path": "a.tgz", "operation": "delete"}])
    assert r.status_code == 200, r.text

    # ...but a lower baseline is refused.
    r = send_snapshot(
        client, 0, [{"path": "a.tgz", "operation": "publish", "body": b"zombie"}]
    )
    assert r.status_code == 409
    body = r.json()
    assert "watermark 1" in body["detail"]
    assert body["error"] == "baseline_below_watermark"
    assert body["baseline_seq"] == 0
    assert body["watermark"] == 1
    # The refused import must not resurrect the path or move anything.
    assert client.get("/artifacts/a.tgz").status_code == 404
    assert client.get("/status").json()["watermark"] == 1


def test_late_events_at_or_before_baseline_are_ack_only(client):
    # seq 3 is the permanently missing log number (the gap).
    send_events(
        client,
        [
            publish(1, "a.tgz", b"a-v1"),
            publish(2, "e.tgz", b"e-v1"),
            publish(4, "c.tgz", b"c-v1"),
        ],
    )
    r = send_snapshot(
        client,
        10,
        [
            {"path": "a.tgz", "operation": "publish", "body": b"a-v2"},
            {"path": "c.tgz", "operation": "publish", "body": b"c-v2"},
        ],
    )
    assert r.status_code == 200, r.text

    # The missing seq 3 shows up late, plus a resend of seq 1 with old bytes.
    r = send_events(
        client,
        [
            publish(3, "c.tgz", b"LATE-GAP-FILLER"),
            publish(1, "a.tgz", b"OLD-RESEND"),
        ],
    )
    assert r.status_code == 200, r.text
    for result in r.json()["results"]:
        assert result["status"] == "already_cleared"
        assert result["acknowledged"] is True
        assert result["applied"] is False

    assert client.get("/artifacts/c.tgz").content == b"c-v2"
    assert client.get("/artifacts/a.tgz").content == b"a-v2"
    # The omitted path stays deleted despite any late traffic.
    assert client.get("/artifacts/e.tgz").status_code == 404


def test_snapshot_clears_receipts_up_to_baseline(client):
    send_events(client, [publish(1, "a"), publish(2, "b"), publish(5, "e")])
    r = send_snapshot(client, 4, [{"path": "a", "operation": "delete"}])
    assert r.status_code == 200, r.text
    assert r.json()["receipts_cleared"] == 2
    status = client.get("/status").json()
    assert status["receipt_count"] == 1
    assert [e["seq"] for e in client.get("/events").json()["events"]] == [5]


def test_snapshot_state_survives_restart(client, tmp_path):
    send_snapshot(
        client,
        7,
        [
            {"path": "a.tgz", "operation": "publish", "body": b"persisted"},
            {"path": "gone.tgz", "operation": "delete"},
        ],
    )

    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    from app import main

    with TestClient(main.app) as restarted:
        assert restarted.get("/status").json()["watermark"] == 7
        resp = restarted.get("/artifacts/a.tgz")
        assert resp.status_code == 200
        assert resp.content == b"persisted"
        assert restarted.get("/artifacts/gone.tgz").status_code == 404

        # Watermark enforcement survives the restart too.
        r = send_events(restarted, [publish(2, "a.tgz", b"zombie")])
        assert r.json()["results"][0]["status"] == "already_cleared"
        assert restarted.get("/artifacts/a.tgz").content == b"persisted"


def test_failed_write_leaves_no_half_snapshot(client, monkeypatch):
    send_events(client, [publish(1, "a.tgz", b"before"), publish(2, "b.tgz", b"b")])
    from app import db

    def failing_set(conn, value):
        raise RuntimeError("simulated watermark write failure")

    monkeypatch.setattr(db, "_set_watermark", failing_set)
    r = send_snapshot(
        client,
        9,
        [
            {"path": "a.tgz", "operation": "publish", "body": b"half"},
            {"path": "new.tgz", "operation": "publish", "body": b"new"},
        ],
    )
    assert r.status_code == 500
    monkeypatch.undo()

    # Entire transaction rolled back: old bytes, old watermark, no new path.
    assert client.get("/status").json()["watermark"] == 0
    assert client.get("/artifacts/a.tgz").content == b"before"
    assert client.get("/artifacts/b.tgz").content == b"b"
    assert client.get("/artifacts/new.tgz").status_code == 404
    assert client.get("/status").json()["receipt_count"] == 2

    # Retry succeeds from the same starting state.
    r = send_snapshot(client, 9, [{"path": "a.tgz", "operation": "delete"}])
    assert r.status_code == 200, r.text
    assert client.get("/status").json()["watermark"] == 9


# ---------------------------------------------------------------------------
# Request formats and validation
# ---------------------------------------------------------------------------


def test_json_snapshot_with_base64_content(client):
    payload = {
        "baseline_seq": 3,
        "entries": [
            {
                "path": "bin/blob.bin",
                "operation": "publish",
                "content_b64": base64.b64encode(bytes(range(256))).decode(),
                "media_type": "application/octet-stream",
            },
            {"path": "gone", "operation": "delete"},
        ],
    }
    r = client.post("/snapshots", json=payload)
    assert r.status_code == 200, r.text
    assert client.get("/artifacts/bin/blob.bin").content == bytes(range(256))
    assert client.get("/artifacts/gone").status_code == 404


def test_json_snapshot_path_map_shape(client):
    payload = {
        "baseline": 4,
        "paths": {
            "plain.txt": "hello snapshot",
            "gone.txt": None,
        },
    }
    r = client.post("/snapshots", json=payload)
    assert r.status_code == 200, r.text
    assert client.get("/artifacts/plain.txt").content == b"hello snapshot"
    assert client.get("/artifacts/gone.txt").status_code == 404
    assert client.get("/status").json()["watermark"] == 4


def test_multipart_baseline_as_separate_form_field(client):
    r = send_snapshot(
        client, 6, [{"path": "a", "operation": "delete"}], baseline_in="form"
    )
    assert r.status_code == 200, r.text
    assert r.json()["baseline_seq"] == 6
    assert client.get("/status").json()["watermark"] == 6


def test_alternative_routes_all_work(client):
    for path in ("/snapshot", "/snapshots/import", "/import-snapshot"):
        r = client.post(
            path,
            json={"baseline_seq": 1, "entries": [{"path": f"p-{path.strip('/').replace('/', '-')}", "operation": "delete"}]},
        )
        assert r.status_code == 200, (path, r.status_code, r.text)
        assert r.json()["watermark_after"] == 1


def test_duplicate_paths_rejected_without_writes(client):
    r = client.post(
        "/snapshots",
        json={
            "baseline_seq": 2,
            "entries": [
                {"path": "a", "operation": "delete"},
                {"path": "a", "operation": "delete"},
            ],
        },
    )
    assert r.status_code == 422
    assert client.get("/status").json()["watermark"] == 0


def test_bad_path_rejected(client):
    r = client.post(
        "/snapshots",
        json={"baseline_seq": 2, "entries": [{"path": "../evil", "operation": "delete"}]},
    )
    assert r.status_code == 422
    assert client.get("/status").json()["watermark"] == 0


def test_missing_baseline_rejected(client):
    r = client.post("/snapshots", json={"entries": []})
    assert r.status_code == 422


def test_negative_baseline_rejected(client):
    r = client.post("/snapshots", json={"baseline_seq": -1, "entries": []})
    assert r.status_code == 422


def test_empty_snapshot_tombstones_every_governed_path(client):
    send_events(client, [publish(1, "a"), publish(20, "future")])
    r = client.post("/snapshots", json={"baseline_seq": 5, "entries": []})
    assert r.status_code == 200, r.text
    assert r.json()["missing_paths"] == ["a"]
    assert client.get("/artifacts/a").status_code == 404
    assert client.get("/artifacts/future").content == b"x"


def test_snapshot_idempotent_at_same_baseline(client):
    payload = {
        "baseline_seq": 5,
        "entries": [{"path": "a", "operation": "publish", "content": "v"}],
    }
    first = client.post("/snapshots", json=payload)
    second = client.post("/snapshots", json=payload)
    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["watermark_before"] == 5
    assert client.get("/artifacts/a").content == b"v"
    assert client.get("/status").json()["watermark"] == 5


# ---------------------------------------------------------------------------
# db-layer contract
# ---------------------------------------------------------------------------


def test_db_import_snapshot_accepts_object_signature(tmp_path):
    from app import db

    path = tmp_path / "mirror.db"
    db.init_db(path)
    result = db.import_snapshot(
        path,
        {"baseline_seq": 3, "entries": [{"path": "a", "operation": "delete"}]},
    )
    assert result["watermark_after"] == 3
    assert db.gc_status(path)["watermark"] == 3


def test_db_stale_baseline_exception(tmp_path):
    from app import db

    path = tmp_path / "mirror.db"
    db.init_db(path)
    db.import_snapshot(path, 5, [])
    with pytest.raises(db.BaselineBelowWatermark) as exc:
        db.import_snapshot(path, 4, [])
    assert exc.value.watermark == 5
    assert exc.value.baseline_seq == 4


def test_baseline_as_query_param_with_bare_array(client):
    r = client.post(
        "/snapshots?baseline_seq=8",
        json=[{"path": "q.tgz", "operation": "publish", "content": "q"}],
    )
    assert r.status_code == 200, r.text
    assert r.json()["baseline_seq"] == 8
    assert client.get("/artifacts/q.tgz").content == b"q"


def test_put_and_trailing_slash_work(client):
    r = client.put(
        "/snapshots",
        json={"baseline_seq": 2, "entries": [{"path": "p", "operation": "delete"}]},
    )
    assert r.status_code == 200, r.text
    r = client.post(
        "/snapshots/",
        json={"baseline_seq": 3, "entries": [{"path": "p2", "operation": "delete"}]},
    )
    assert r.status_code == 200, r.text
    assert client.get("/status").json()["watermark"] == 3


def test_baseline_as_path_segment(client):
    r = client.post(
        "/snapshots/6/import",
        json=[{"path": "p", "operation": "delete"}],
    )
    assert r.status_code == 200, r.text
    assert r.json()["baseline_seq"] == 6
    # The static .../import spelling is not captured as a baseline value.
    r = client.post(
        "/snapshots/import",
        json={"baseline_seq": 7, "entries": []},
    )
    assert r.status_code == 200, r.text
    assert r.json()["baseline_seq"] == 7
    assert client.get("/status").json()["watermark"] == 7


def test_form_urlencoded_snapshot(client):
    manifest = json.dumps(
        {"baseline_seq": 4, "entries": [{"path": "f", "operation": "delete"}]}
    )
    r = client.post(
        "/snapshots",
        data={"manifest": manifest},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert r.status_code == 200, r.text
    assert client.get("/status").json()["watermark"] == 4


def test_db_lenient_entry_shapes(tmp_path):
    from app import db

    path = tmp_path / "mirror.db"
    db.init_db(path)
    # No operation: bytes imply publish, deleted flag implies delete.
    result = db.import_snapshot(
        path,
        2,
        [
            {"path": "a", "content": b"hello"},
            {"path": "b", "deleted": True},
            {"path": "c"},
        ],
    )
    assert result["published_paths"] == ["a"]
    assert set(result["deleted_paths"]) == {"b", "c"}
    conn = db.connect(path)
    try:
        rows = {r["path"]: r for r in conn.execute("SELECT * FROM artifacts").fetchall()}
        assert bytes(rows["a"]["content"]) == b"hello"
        assert rows["b"]["operation"] == "delete"
        assert rows["c"]["operation"] == "delete"
    finally:
        conn.close()


def test_concurrent_snapshot_and_ingest_keep_consistency(tmp_path):
    import threading

    from app import db

    path = tmp_path / "mirror.db"
    db.init_db(path)
    errors = []

    def snap():
        try:
            db.import_snapshot(
                path, 10, [{"path": "s", "operation": "publish", "content": b"s"}]
            )
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    def ingest():
        try:
            db.ingest_events(
                path,
                [
                    {"seq": 11, "path": "n", "operation": "publish",
                     "content_bytes": b"n", "media_type": "text/plain"}
                ],
            )
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=snap), threading.Thread(target=ingest)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    status = db.gc_status(path)
    assert status["watermark"] == 10
    assert status["receipt_count"] == 1

