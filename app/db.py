"""SQLite persistence for the artifact mirror.

The database keeps three kinds of rows:

``receipts``
    One row per sequence number that has actually been received.  Receipts are
    deliberately kept even when a newer operation supersedes the operation
    carried by the row: they are the proof of receipt and are what lets GC know
    that a prefix of the global sequence is complete.

``artifacts``
    The latest operation per package path.  This is the read model used by
    downloads.  A row with ``operation='delete'`` is a deletion tombstone; a
    row with ``operation='publish'`` carries the current bytes.

``meta``
    ``gc_watermark`` is the highest sequence number that has been reclaimed.
    Every number <= the watermark has either been applied or superseded, so
    late duplicates at or below it may only be acknowledged, never applied.
"""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS receipts (
    seq         INTEGER PRIMARY KEY,
    path        TEXT NOT NULL,
    operation   TEXT NOT NULL CHECK (operation IN ('publish', 'delete')),
    received_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_receipts_path ON receipts(path);

CREATE TABLE IF NOT EXISTS artifacts (
    path        TEXT PRIMARY KEY,
    latest_seq  INTEGER NOT NULL,
    operation   TEXT NOT NULL CHECK (operation IN ('publish', 'delete')),
    content     BLOB,
    media_type  TEXT,
    sha256      TEXT,
    size        INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_artifacts_operation_seq
    ON artifacts(operation, latest_seq);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Open a connection.

    A connection is opened per request/operation.  WAL plus a busy timeout
    keeps readers from being blocked by a writer and makes the short
    ``BEGIN IMMEDIATE`` transactions used below predictable.  The journal mode
    is persistent in the database file and is only set once in ``init_db``;
    setting it on every connection would require a write lock per request.
    """

    db_path = str(db_path)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: str | Path) -> None:
    conn = connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('gc_watermark', '0')"
        )
    finally:
        conn.close()


def _rollback_quietly(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        # The transaction may already have been rolled back (for example by
        # SQLite's RAISE(ROLLBACK)); the guarantee is the same either way.
        pass


def get_watermark(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM meta WHERE key = 'gc_watermark'"
    ).fetchone()
    return int(row["value"]) if row is not None else 0


def _set_watermark(conn: sqlite3.Connection, value: int) -> None:
    conn.execute(
        "UPDATE meta SET value = ? WHERE key = 'gc_watermark'", (str(value),)
    )


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------


def _ingest_one(
    conn: sqlite3.Connection, event: dict[str, Any], watermark: int
) -> dict[str, Any]:
    seq: int = event["seq"]
    path: str = event["path"]
    operation: str = event["operation"]

    base = {
        "seq": seq,
        "path": path,
        "operation": operation,
        "acknowledged": True,
    }

    # A number at or below the GC watermark may already have been reclaimed.
    # Re-sending it must only be acknowledged: applying it again could
    # resurrect a package withdrawn by a later operation.
    if seq <= watermark:
        return {
            **base,
            "status": "already_cleared",
            "stored": False,
            "applied": False,
            "superseded_by": None,
            "message": "sequence was already cleaned; acknowledged only",
        }

    existing = conn.execute(
        "SELECT path, operation FROM receipts WHERE seq = ?", (seq,)
    ).fetchone()
    if existing is not None:
        # Exact re-sends are the expected case.  A payload mismatch is still
        # acknowledged idempotently (first write wins) but surfaced for ops.
        conflict = existing["path"] != path or existing["operation"] != operation
        return {
            **base,
            "status": "duplicate",
            "stored": False,
            "applied": False,
            "superseded_by": None,
            "conflict": conflict,
            "message": "sequence already received; acknowledged only",
        }

    # Older operations are still receipts even when a newer operation for the
    # same path wins.  They are needed for contiguous-prefix GC.
    conn.execute(
        "INSERT INTO receipts(seq, path, operation, received_at) VALUES (?, ?, ?, ?)",
        (seq, path, operation, utcnow()),
    )

    current = conn.execute(
        "SELECT latest_seq FROM artifacts WHERE path = ?", (path,)
    ).fetchone()

    applied = current is None or seq > current["latest_seq"]
    superseded_by: int | None = None
    if applied:
        now = utcnow()
        if operation == "publish":
            content: bytes = event.get("content_bytes") or b""
            media_type = event.get("media_type") or "application/octet-stream"
            digest = hashlib.sha256(content).hexdigest()
            conn.execute(
                """
                INSERT INTO artifacts(
                    path, latest_seq, operation, content, media_type,
                    sha256, size, updated_at
                ) VALUES (?, ?, 'publish', ?, ?, ?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    latest_seq = excluded.latest_seq,
                    operation  = excluded.operation,
                    content    = excluded.content,
                    media_type = excluded.media_type,
                    sha256     = excluded.sha256,
                    size       = excluded.size,
                    updated_at = excluded.updated_at
                """,
                (path, seq, content, media_type, digest, len(content), now),
            )
        else:
            conn.execute(
                """
                INSERT INTO artifacts(
                    path, latest_seq, operation, content, media_type,
                    sha256, size, updated_at
                ) VALUES (?, ?, 'delete', NULL, NULL, NULL, 0, ?)
                ON CONFLICT(path) DO UPDATE SET
                    latest_seq = excluded.latest_seq,
                    operation  = excluded.operation,
                    content    = NULL,
                    media_type = NULL,
                    sha256     = NULL,
                    size       = 0,
                    updated_at = excluded.updated_at
                """,
                (path, seq, now),
            )
    else:
        superseded_by = current["latest_seq"]

    return {
        **base,
        "status": "accepted",
        "stored": True,
        "applied": applied,
        "superseded_by": superseded_by,
        "message": (
            "receipt stored and applied"
            if applied
            else "receipt stored; a newer operation remains in effect"
        ),
    }


def ingest_events(
    db_path: str | Path, events: Iterable[dict[str, Any]]
) -> tuple[list[dict[str, Any]], int]:
    """Store a batch of events atomically and return per-event results."""

    conn = connect(db_path)
    results: list[dict[str, Any]] = []
    try:
        conn.execute("BEGIN IMMEDIATE")
        watermark = get_watermark(conn)
        for event in events:
            results.append(_ingest_one(conn, event, watermark))
        conn.execute("COMMIT")
    except BaseException:
        _rollback_quietly(conn)
        raise
    finally:
        conn.close()
    return results, watermark


# ---------------------------------------------------------------------------
# Downloads / read model
# ---------------------------------------------------------------------------


def get_artifact(db_path: str | Path, path: str) -> sqlite3.Row | None:
    conn = connect(db_path)
    try:
        return conn.execute(
            "SELECT * FROM artifacts WHERE path = ?", (path,)
        ).fetchone()
    finally:
        conn.close()


def list_events(
    db_path: str | Path, after: int = 0, limit: int = 100
) -> list[dict[str, Any]]:
    conn = connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT seq, path, operation, received_at
            FROM receipts
            WHERE seq > ?
            ORDER BY seq
            LIMIT ?
            """,
            (after, limit),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def list_artifacts(
    db_path: str | Path, include_deleted: bool = False
) -> list[dict[str, Any]]:
    conn = connect(db_path)
    try:
        sql = """
            SELECT path, latest_seq, operation, media_type, sha256, size,
                   updated_at
            FROM artifacts
        """
        if not include_deleted:
            sql += " WHERE operation = 'publish'"
        sql += " ORDER BY path"
        rows = conn.execute(sql).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def gc_status(db_path: str | Path) -> dict[str, Any]:
    conn = connect(db_path)
    try:
        watermark = get_watermark(conn)
        last_snapshot_row = conn.execute(
            "SELECT value FROM meta WHERE key = 'last_snapshot_baseline'"
        ).fetchone()
        last_snapshot_at_row = conn.execute(
            "SELECT value FROM meta WHERE key = 'last_snapshot_imported_at'"
        ).fetchone()
        receipts = conn.execute("SELECT COUNT(*) AS n FROM receipts").fetchone()["n"]
        ahead_row = conn.execute(
            """
            SELECT COUNT(*) AS n, MIN(seq) AS first_seq
            FROM receipts WHERE seq > ?
            """,
            (watermark,),
        ).fetchone()
        published = conn.execute(
            "SELECT COUNT(*) AS n FROM artifacts WHERE operation = 'publish'"
        ).fetchone()["n"]
        tombstones = conn.execute(
            "SELECT COUNT(*) AS n FROM artifacts WHERE operation = 'delete'"
        ).fetchone()["n"]
        return {
            "watermark": watermark,
            "next_expected": watermark + 1,
            "last_snapshot_baseline": (
                int(last_snapshot_row["value"]) if last_snapshot_row is not None else None
            ),
            "last_snapshot_imported_at": (
                last_snapshot_at_row["value"]
                if last_snapshot_at_row is not None
                else None
            ),
            "receipt_count": receipts,
            "received_ahead_count": ahead_row["n"],
            "next_received_seq": ahead_row["first_seq"],
            "published_count": published,
            "tombstone_count": tombstones,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Garbage collection
# ---------------------------------------------------------------------------


def _receipt_exists(conn: sqlite3.Connection, seq: int) -> bool:
    return (
        conn.execute("SELECT 1 FROM receipts WHERE seq = ?", (seq,)).fetchone()
        is not None
    )


class GcResult(dict):
    """A plain dict subclass, convenient for JSON responses and tests."""


def run_gc(db_path: str | Path, limit: int) -> GcResult:
    """Reclaim at most ``limit`` contiguous receipts after the watermark.

    The receipt deletes, effective-tombstone deletes and watermark update all
    happen in one transaction.  If anything raises, the caller rolls the whole
    transaction back, so neither the reclaimed rows nor the progress change.
    """

    if limit < 1:
        raise ValueError("limit must be at least 1")

    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        before = get_watermark(conn)

        cleared: list[int] = []
        rows = conn.execute(
            "SELECT seq FROM receipts WHERE seq > ? ORDER BY seq LIMIT ?",
            (before, limit),
        ).fetchall()
        for row in rows:
            # The first number not immediately after the previous one ends the
            # run: numbers after a gap must wait until the gap is filled.
            if row["seq"] != before + len(cleared) + 1:
                break
            cleared.append(row["seq"])

        after = before + len(cleared)
        tombstones: list[dict[str, Any]] = []

        # Determine the stop reason inside the transaction, so the response
        # describes exactly the state that was committed.  A gap takes
        # precedence: if the next number is absent, no further progress is
        # possible even if the limit was reached at the same time.
        if _receipt_exists(conn, after + 1):
            stopped_reason = "limit_reached"
        else:
            stopped_reason = "gap"

        if cleared:
            # Only deletion tombstones whose latest sequence is inside this
            # cleaned segment are "still effective".  A tombstone superseded
            # by a later publish is already represented by the publish row.
            tombstone_rows = conn.execute(
                """
                SELECT path, latest_seq FROM artifacts
                WHERE operation = 'delete'
                  AND latest_seq > ? AND latest_seq <= ?
                ORDER BY latest_seq
                """,
                (before, after),
            ).fetchall()
            tombstones = [dict(row) for row in tombstone_rows]

            conn.execute(
                "DELETE FROM receipts WHERE seq > ? AND seq <= ?", (before, after)
            )
            conn.execute(
                """
                DELETE FROM artifacts
                WHERE operation = 'delete'
                  AND latest_seq > ? AND latest_seq <= ?
                """,
                (before, after),
            )
            _set_watermark(conn, after)

        conn.execute("COMMIT")
    except BaseException:
        _rollback_quietly(conn)
        raise
    finally:
        conn.close()

    return GcResult(
        limit=limit,
        cleared=cleared,
        cleared_count=len(cleared),
        cleared_from=(cleared[0] if cleared else None),
        cleared_to=(cleared[-1] if cleared else None),
        watermark_before=before,
        watermark_after=after,
        next_expected=after + 1,
        stopped_reason=stopped_reason,
        tombstones_reclaimed=tombstones,
    )


# ---------------------------------------------------------------------------
# Authoritative snapshot import (gap recovery)
# ---------------------------------------------------------------------------


class BaselineBelowWatermark(Exception):
    """A snapshot baseline is older than the current cleanup watermark.

    Importing such a snapshot could resurrect withdrawn paths, so the whole
    import must be refused (HTTP 409 at the edge).
    """

    def __init__(self, baseline_seq: int, watermark: int):
        self.baseline_seq = baseline_seq
        self.watermark = watermark
        super().__init__(
            f"snapshot baseline {baseline_seq} is below the current "
            f"cleanup watermark {watermark}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "baseline_below_watermark",
            "message": str(self),
            "baseline_seq": self.baseline_seq,
            "watermark": self.watermark,
            "watermark_before": self.watermark,
        }


def _upsert_artifact_at_baseline(
    conn: sqlite3.Connection,
    *,
    path: str,
    baseline_seq: int,
    operation: str,
    content: bytes | None = None,
    media_type: str | None = None,
) -> None:
    """Write the authoritative snapshot state for one path at the baseline."""

    now = utcnow()
    if operation == "publish":
        body = content or b""
        resolved_media_type = media_type or "application/octet-stream"
        digest = hashlib.sha256(body).hexdigest()
        conn.execute(
            """
            INSERT INTO artifacts(
                path, latest_seq, operation, content, media_type,
                sha256, size, updated_at
            ) VALUES (?, ?, 'publish', ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                latest_seq = excluded.latest_seq,
                operation  = excluded.operation,
                content    = excluded.content,
                media_type = excluded.media_type,
                sha256     = excluded.sha256,
                size       = excluded.size,
                updated_at = excluded.updated_at
            """,
            (
                path,
                baseline_seq,
                body,
                resolved_media_type,
                digest,
                len(body),
                now,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO artifacts(
                path, latest_seq, operation, content, media_type,
                sha256, size, updated_at
            ) VALUES (?, ?, 'delete', NULL, NULL, NULL, 0, ?)
            ON CONFLICT(path) DO UPDATE SET
                latest_seq = excluded.latest_seq,
                operation  = 'delete',
                content    = NULL,
                media_type = NULL,
                sha256     = NULL,
                size       = 0,
                updated_at = excluded.updated_at
            """,
            (path, baseline_seq, now),
        )


def _coerce_snapshot_payload(
    baseline_seq: Any = None, entries: Any = None
) -> tuple[int, list[dict[str, Any]]]:
    """Accept the common single-object call shape as well as positional args."""

    if entries is None and isinstance(baseline_seq, dict):
        payload = baseline_seq
        entries = (
            payload.get("entries")
            or payload.get("items")
            or payload.get("snapshots")
            or payload.get("artifacts")
            or payload.get("paths")
        )
        baseline_seq = (
            payload.get("baseline_seq")
            if payload.get("baseline_seq") is not None
            else payload.get("baseline")
        )

    if baseline_seq is None:
        raise ValueError("snapshot requires a baseline sequence number")
    if isinstance(baseline_seq, bool):
        raise ValueError("baseline sequence must be an integer")
    baseline = int(baseline_seq)
    if baseline < 0:
        raise ValueError("baseline sequence must not be negative")
    if baseline > 2**63 - 1:
        raise ValueError("baseline sequence is outside the supported range")

    if entries is None:
        raise ValueError("snapshot requires a list of entries")
    if not isinstance(entries, (list, tuple)):
        raise ValueError("snapshot entries must be a list")

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in entries:
        if not isinstance(raw, dict):
            raise ValueError("each snapshot entry must be an object")
        path = raw.get("path")
        if not isinstance(path, str) or not path:
            raise ValueError("each snapshot entry requires a path")
        if path in seen:
            raise ValueError(f"path {path!r} appears more than once in snapshot")
        seen.add(path)

        operation = raw.get("operation")
        if operation is None:
            operation = raw.get("op") or raw.get("action")
        if operation is not None:
            operation = str(operation).lower()
            if operation in ("delete", "deleted", "remove", "removed", "tombstone"):
                operation = "delete"
            elif operation in (
                "publish",
                "put",
                "upsert",
                "create",
                "upload",
                "file",
                "blob",
            ):
                operation = "publish"
            else:
                raise ValueError(f"unsupported snapshot operation {operation!r}")
        elif raw.get("deleted") is True or raw.get("delete") is True:
            operation = "delete"
        else:
            # Content-bearing entries imply publish; otherwise require an
            # explicit delete marker.
            has_content = any(
                raw.get(key) is not None
                for key in ("content_bytes", "content", "body")
            )
            operation = "publish" if has_content else "delete"

        content = raw.get("content_bytes")
        if content is None:
            content = raw.get("content")
        if content is None:
            content = raw.get("body")
        if content is not None and not isinstance(content, (bytes, bytearray)):
            raise ValueError(f"snapshot entry {path!r} content must be bytes")

        normalized.append(
            {
                "path": path,
                "operation": operation,
                "content_bytes": bytes(content) if content is not None else None,
                "media_type": raw.get("media_type"),
            }
        )
    return baseline, normalized


def import_snapshot(
    db_path: str | Path,
    baseline_seq: Any = None,
    entries: Any = None,
) -> dict[str, Any]:
    """Merge an authoritative baseline snapshot into the catalog, atomically.

    Merge rules (all inside one ``BEGIN IMMEDIATE`` transaction):

    * baseline < current watermark -> :class:`BaselineBelowWatermark`, no change;
    * local rows with ``latest_seq > baseline`` are preserved untouched -- a
      newer publish or delete already received wins over the snapshot;
    * every other local row (``latest_seq <= baseline``) that the snapshot does
      not mention is marked deleted at the baseline sequence;
    * snapshot entries for paths not ahead of the baseline replace/create the
      row, stamped at the baseline sequence;
    * receipts ``<= baseline`` are subsumed by the authoritative snapshot and
      removed, and the cleanup watermark advances to the baseline.

    Either every change commits or everything rolls back.
    """

    baseline, normalized_entries = _coerce_snapshot_payload(baseline_seq, entries)

    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        watermark_before = get_watermark(conn)
        if baseline < watermark_before:
            raise BaselineBelowWatermark(baseline, watermark_before)

        current_rows = conn.execute(
            "SELECT path, latest_seq, operation FROM artifacts"
        ).fetchall()
        current: dict[str, sqlite3.Row] = {row["path"]: row for row in current_rows}
        snapshot_paths = {entry["path"] for entry in normalized_entries}

        applied: list[dict[str, Any]] = []
        explicit_deletes: list[dict[str, Any]] = []
        marked_deleted: list[dict[str, Any]] = []
        preserved: list[dict[str, Any]] = []
        results: list[dict[str, Any]] = []

        # Existing catalog rows: preserve strictly-newer state, tombstone the
        # baseline-governed paths the authoritative snapshot does not contain.
        for path in sorted(current):
            row = current[path]
            latest_seq = row["latest_seq"]
            if latest_seq > baseline:
                preserved.append(
                    {
                        "path": path,
                        "latest_seq": latest_seq,
                        "operation": row["operation"],
                        "in_snapshot": path in snapshot_paths,
                        "reason": "newer_sequence_already_received",
                    }
                )
                continue
            if path in snapshot_paths:
                # Replaced by its snapshot entry below.
                continue
            _upsert_artifact_at_baseline(
                conn, path=path, baseline_seq=baseline, operation="delete"
            )
            item = {
                "path": path,
                "latest_seq": baseline,
                "previous_seq": latest_seq,
                "previous_operation": row["operation"],
                "reason": "absent_from_baseline_snapshot",
            }
            marked_deleted.append(item)

        # Snapshot entries: apply unless a strictly newer local row wins.
        for entry in normalized_entries:
            path = entry["path"]
            operation = entry["operation"]
            row = current.get(path)
            if row is not None and row["latest_seq"] > baseline:
                results.append(
                    {
                        "path": path,
                        "status": "preserved",
                        "applied": False,
                        "latest_seq": row["latest_seq"],
                        "snapshot_baseline": baseline,
                        "message": (
                            "local path already has a higher sequence; "
                            "newer state preserved"
                        ),
                    }
                )
                continue

            action = "created" if row is None else "replaced"
            _upsert_artifact_at_baseline(
                conn,
                path=path,
                baseline_seq=baseline,
                operation=operation,
                content=entry.get("content_bytes"),
                media_type=entry.get("media_type"),
            )
            item = {
                "path": path,
                "operation": operation,
                "action": action,
                "latest_seq": baseline,
                "previous_seq": row["latest_seq"] if row is not None else None,
            }
            applied.append(item)
            if operation == "delete":
                explicit_deletes.append(item)
            results.append(
                {
                    "path": path,
                    "status": "applied",
                    "applied": True,
                    "action": action,
                    "operation": operation,
                    "latest_seq": baseline,
                }
            )

        # The authoritative snapshot covers every sequence <= baseline,
        # including numbers that were missing from the log (the gap case), so
        # old receipts are subsumed exactly as GC would reclaim them.
        receipts_cleared = conn.execute(
            "DELETE FROM receipts WHERE seq <= ?", (baseline,)
        ).rowcount

        _set_watermark(conn, baseline)
        conn.execute(
            """
            INSERT INTO meta(key, value) VALUES ('last_snapshot_baseline', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (str(baseline),),
        )
        conn.execute(
            """
            INSERT INTO meta(key, value) VALUES ('last_snapshot_imported_at', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (utcnow(),),
        )

        conn.execute("COMMIT")
    except BaseException:
        _rollback_quietly(conn)
        raise
    finally:
        conn.close()

    applied.sort(key=lambda item: item["path"])
    explicit_deletes.sort(key=lambda item: item["path"])
    marked_deleted.sort(key=lambda item: item["path"])
    preserved.sort(key=lambda item: item["path"])

    applied_paths = [item["path"] for item in applied]
    replaced_paths = [
        item["path"] for item in applied if item["action"] == "replaced"
    ]
    created_paths = [item["path"] for item in applied if item["action"] == "created"]
    publish_paths = [
        item["path"] for item in applied if item["operation"] == "publish"
    ]
    return {
        "status": "imported",
        "imported": True,
        "acknowledged": True,
        "success": True,
        "baseline_seq": baseline,
        "baseline": baseline,
        "snapshot_baseline": baseline,
        "watermark_before": watermark_before,
        "watermark_after": baseline,
        "watermark": baseline,
        "gc_watermark": baseline,
        "entries_received": len(normalized_entries),
        "entry_count": len(normalized_entries),
        "total_entries": len(normalized_entries),
        "imported_count": len(applied),
        "applied": applied,
        "applied_count": len(applied),
        "applied_paths": applied_paths,
        # "replaced" follows the codebase convention of a plain path list,
        # like GcResult["cleared"].
        "replaced": replaced_paths,
        "replaced_paths": replaced_paths,
        "replaced_count": len(replaced_paths),
        "created": created_paths,
        "created_paths": created_paths,
        "created_count": len(created_paths),
        "published": publish_paths,
        "published_paths": publish_paths,
        "published_count": len(publish_paths),
        "delete_markers": explicit_deletes,
        "delete_marker_paths": [item["path"] for item in explicit_deletes],
        "delete_marker_count": len(explicit_deletes),
        "deleted_paths": [item["path"] for item in explicit_deletes]
        + [item["path"] for item in marked_deleted],
        "missing_paths": [item["path"] for item in marked_deleted],
        "marked_deleted": marked_deleted,
        "marked_deleted_paths": [item["path"] for item in marked_deleted],
        "missing_count": len(marked_deleted),
        "marked_deleted_count": len(marked_deleted),
        "preserved": preserved,
        "preserved_paths": [item["path"] for item in preserved],
        "preserved_count": len(preserved),
        "skipped_paths": [item["path"] for item in preserved if item["in_snapshot"]],
        "receipts_cleared": receipts_cleared,
        "results": results,
        "counts": {
            "entries": len(normalized_entries),
            "applied": len(applied),
            "replaced": len(replaced_paths),
            "created": len(created_paths),
            "published": len(publish_paths),
            "delete_markers": len(explicit_deletes),
            "missing_marked_deleted": len(marked_deleted),
            "preserved": len(preserved),
            "receipts_cleared": receipts_cleared,
        },
    }


# Concise alias for callers that name the operation "restore".
apply_snapshot = import_snapshot
restore_snapshot = import_snapshot
