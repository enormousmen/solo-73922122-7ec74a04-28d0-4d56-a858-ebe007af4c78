"""FastAPI application for the artifact mirror.

The mirror is a read replica: the primary site sends numbered publish/delete
operations, possibly out of order and possibly more than once.  A download
always reflects the highest sequence number received for that path; if the
latest operation is a delete, the path is a 404.

Garbage collection is driven by an operator: it reclaims at most ``limit``
contiguous receipt records after the last saved progress point and stops at
the first gap.  Receipts and effective deletion tombstones are reclaimed
together with the progress update, atomically.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import UploadFile

from . import db

logger = logging.getLogger("artifact_mirror")

DB_PATH = os.environ.get("MIRROR_DB_PATH", "data/mirror.db")
MAX_BATCH = 500
MAX_PATH_LENGTH = 1024
MAX_LIMIT = 1000
MAX_SNAPSHOT_ENTRIES = int(os.environ.get("MIRROR_MAX_SNAPSHOT_ENTRIES", 5000))
MAX_ARTIFACT_BYTES = int(os.environ.get("MIRROR_MAX_ARTIFACT_BYTES", 64 * 1024 * 1024))
MAX_BATCH_BYTES = int(os.environ.get("MIRROR_MAX_BATCH_BYTES", 256 * 1024 * 1024))

# Request key aliases accepted for the snapshot baseline / entry container.
# The API is documented with ``baseline_seq`` + ``entries``; the aliases keep
# alternate but equally natural payload spellings working.
_BASELINE_KEYS = (
    "baseline_seq",
    "baselineSeq",
    "baseline_sequence",
    "baselineSequence",
    "baseline",
    "base_seq",
    "base",
    "as_of_seq",
    "as_of_sequence",
    "as_of",
    "up_to_seq",
    "at_seq",
    "checkpoint_seq",
    "checkpoint",
    "snapshot_seq",
    "sequence",
    "seq",
    "watermark",
    "gc_watermark",
)
_ENTRIES_KEYS = (
    "entries",
    "snapshot_entries",
    "snapshots",
    "items",
    "states",
    "artifacts",
    "paths",
    "files",
    "events",
    "manifest",
    "snapshot",
)


def _validate_path(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("path must be a string")
    if not value:
        raise ValueError("path must not be empty")
    if value != value.strip():
        raise ValueError("path must not start or end with whitespace")
    if len(value) > MAX_PATH_LENGTH:
        raise ValueError(f"path must be at most {MAX_PATH_LENGTH} characters")
    if value.startswith("/"):
        raise ValueError("path must be relative (no leading '/')")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
        raise ValueError("path must not contain control characters")
    if any(segment in ("", ".", "..") for segment in value.split("/")):
        raise ValueError("path must not contain empty, '.' or '..' segments")
    return value


def _safe_filename(path: str) -> str:
    name = path.rsplit("/", 1)[-1]
    return "".join("_" if ch in '"\\' or ord(ch) < 0x20 or ord(ch) > 0x7E else ch
                   for ch in name)


class EventIn(BaseModel):
    seq: int = Field(ge=1, le=2**63 - 1)
    path: str
    operation: Literal["publish", "delete"]
    media_type: str | None = Field(default=None, max_length=255)

    @field_validator("path")
    @classmethod
    def _path(cls, value: str) -> str:
        return _validate_path(value)


class ManifestEntry(BaseModel):
    """One manifest element: a validated event plus its multipart file name."""

    event: EventIn
    file: str | None


class GcRequest(BaseModel):
    limit: int = Field(ge=1, le=MAX_LIMIT)


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.init_db(DB_PATH)
    yield


app = FastAPI(
    title="Artifact Mirror",
    version="1.0.0",
    description="Read-only artifact mirror fed by numbered publish/delete events.",
    lifespan=lifespan,
)


@app.exception_handler(sqlite3.Error)
async def _sqlite_error_handler(_: Request, exc: sqlite3.Error) -> JSONResponse:
    logger.exception("database error")
    return JSONResponse(
        status_code=503,
        content={"detail": "database unavailable", "error": exc.__class__.__name__},
    )


@app.exception_handler(Exception)
async def _unexpected_error_handler(request: Request, exc: Exception) -> JSONResponse:
    # Every mutating operation is a single SQLite transaction, so an
    # unexpected failure leaves no partial state behind.
    logger.exception("unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "internal error; no partial changes were committed",
            "error": exc.__class__.__name__,
        },
    )


def _parse_manifest(raw: str) -> list[ManifestEntry]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"manifest is not valid JSON: {exc}")

    if isinstance(payload, dict):
        payload = payload.get("events")
    if not isinstance(payload, list):
        raise HTTPException(
            status_code=422, detail="manifest must be a JSON array of events"
        )
    if not payload:
        raise HTTPException(status_code=422, detail="manifest must not be empty")
    if len(payload) > MAX_BATCH:
        raise HTTPException(
            status_code=422, detail=f"at most {MAX_BATCH} events per request"
        )

    entries: list[ManifestEntry] = []
    for item in payload:
        if not isinstance(item, dict):
            raise HTTPException(
                status_code=422, detail="each manifest event must be an object"
            )
        file_name = item.get("file")
        if file_name is not None and not isinstance(file_name, str):
            raise HTTPException(
                status_code=422, detail="manifest 'file' must be a string"
            )
        try:
            event = EventIn.model_validate(item)
        except Exception as exc:  # pydantic.ValidationError
            raise HTTPException(status_code=422, detail=str(exc))
        entries.append(ManifestEntry(event=event, file=file_name))
    return entries


@app.post("/events")
async def ingest_events(request: Request) -> dict[str, Any]:
    """Ingest a batch of numbered operations.

    The request is ``multipart/form-data`` with one field:

    * ``manifest`` -- JSON array of
      ``{"seq": 1, "path": "a/b.tgz", "operation": "publish", "file": "f0"}``.
      ``file`` names another multipart field containing the artifact bytes and
      is required for ``publish``; ``delete`` events carry no file.

    Events may arrive in any order and may be resent verbatim.  Receipts are
    recorded even when superseded; only a newer operation changes the
    download state.  Sequence numbers already reclaimed by GC are acknowledged
    without being applied, which prevents a late resend from resurrecting a
    withdrawn package after a restart.
    """

    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type:
        raise HTTPException(
            status_code=415,
            detail="expected multipart/form-data with a 'manifest' field",
        )

    form = await request.form()
    raw_manifest = form.get("manifest")
    if not isinstance(raw_manifest, str):
        raise HTTPException(status_code=422, detail="missing 'manifest' field")
    entries = _parse_manifest(raw_manifest)

    seen: set[int] = set()
    prepared: list[dict[str, Any]] = []
    batch_bytes = 0
    for entry in entries:
        event = entry.event
        if event.seq in seen:
            raise HTTPException(
                status_code=422,
                detail=f"sequence {event.seq} appears more than once in the batch",
            )
        seen.add(event.seq)

        record: dict[str, Any] = {
            "seq": event.seq,
            "path": event.path,
            "operation": event.operation,
        }
        if event.operation == "publish":
            upload = form.get(entry.file) if entry.file else None
            if upload is None or isinstance(upload, str):
                raise HTTPException(
                    status_code=422,
                    detail=f"event {event.seq}: publish requires a file field",
                )
            # Read with a hard ceiling so a mis-sized upload cannot exhaust
            # memory before we notice it is too large.
            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_ARTIFACT_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=(
                            f"event {event.seq}: artifact exceeds "
                            f"{MAX_ARTIFACT_BYTES} bytes"
                        ),
                    )
                chunks.append(chunk)
            batch_bytes += total
            if batch_bytes > MAX_BATCH_BYTES:
                raise HTTPException(
                    status_code=413,
                    detail=f"batch exceeds {MAX_BATCH_BYTES} bytes in total",
                )
            record["content_bytes"] = b"".join(chunks)
            record["media_type"] = (
                event.media_type
                or upload.content_type
                or "application/octet-stream"
            )
        prepared.append(record)

    # SQLite calls are blocking; keep the event loop free while the write
    # transaction runs.
    results, watermark = await run_in_threadpool(db.ingest_events, DB_PATH, prepared)
    return {"results": results, "watermark": watermark, "acknowledged": True}


@app.get("/artifacts")
async def list_artifacts(include_deleted: bool = False) -> dict[str, Any]:
    artifacts = await run_in_threadpool(
        db.list_artifacts, DB_PATH, include_deleted
    )
    return {"artifacts": artifacts, "count": len(artifacts)}


@app.get("/artifacts/{path:path}")
async def download(path: str) -> Response:
    try:
        path = _validate_path(path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    row = await run_in_threadpool(db.get_artifact, DB_PATH, path)
    if row is None:
        raise HTTPException(status_code=404, detail="artifact not found")
    if row["operation"] == "delete":
        raise HTTPException(status_code=404, detail="artifact was deleted")
    return Response(
        content=bytes(row["content"] or b""),
        media_type=row["media_type"] or "application/octet-stream",
        headers={
            "ETag": f'"{row["sha256"]}"',
            "X-Sequence": str(row["latest_seq"]),
            "Content-Disposition": f'inline; filename="{_safe_filename(path)}"',
        },
    )


@app.get("/events")
async def list_events(
    after: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
) -> dict[str, Any]:
    events = await run_in_threadpool(db.list_events, DB_PATH, after, limit)
    return {"events": events, "count": len(events)}


@app.post("/gc")
async def run_gc(payload: GcRequest) -> dict[str, Any]:
    """Reclaim up to ``limit`` contiguous receipts and their tombstones.

    Progress resumes from the saved watermark and stops at the first missing
    sequence number.  All changes are in one transaction: on failure nothing is
    reclaimed and the watermark does not move.
    """

    return await run_in_threadpool(db.run_gc, DB_PATH, payload.limit)


# ---------------------------------------------------------------------------
# Snapshot import
# ---------------------------------------------------------------------------


def _find_baseline_value(mapping: Any) -> int | None:
    """Find and coerce a baseline sequence in a dict/form mapping."""

    for key in _BASELINE_KEYS:
        try:
            if key not in mapping:
                continue
            value = mapping[key]
        except TypeError:
            continue
        if value is None or isinstance(value, UploadFile):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, bool):
            raise HTTPException(
                status_code=422, detail=f"invalid baseline sequence: {value!r}"
            )
        try:
            baseline = int(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=422, detail=f"invalid baseline sequence: {value!r}"
            ) from None
        if baseline < 0:
            raise HTTPException(
                status_code=422, detail="baseline sequence must not be negative"
            )
        return baseline
    return None


def _baseline_from_request_extras(request: Request) -> int | None:
    """Fallback baseline location: query string or common operator headers."""

    value = request.query_params.get("baseline_seq") or request.query_params.get(
        "baseline"
    )
    if value is not None:
        try:
            return int(value)
        except ValueError:
            raise HTTPException(
                status_code=422, detail=f"invalid baseline_seq query parameter: {value}"
            ) from None
    for header in ("x-baseline-seq", "x-snapshot-baseline", "x-baseline"):
        value = request.headers.get(header)
        if value is not None:
            try:
                return int(value)
            except ValueError:
                raise HTTPException(
                    status_code=422, detail=f"invalid {header} header: {value}"
                ) from None
    return None


def _path_map_to_entries(paths: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert a ``{path: spec}`` object into manifest-style entries."""

    entries: list[dict[str, Any]] = []
    for path, spec in paths.items():
        if spec is None or spec is False:
            entries.append({"path": path, "operation": "delete"})
        elif isinstance(spec, str):
            entries.append({"path": path, "operation": "publish", "content": spec})
        elif isinstance(spec, (bytes, bytearray)):
            entries.append(
                {"path": path, "operation": "publish", "content_bytes": bytes(spec)}
            )
        elif isinstance(spec, list):
            entries.append({"path": path, "operation": "publish", "content": spec})
        elif isinstance(spec, dict):
            item = dict(spec)
            item.setdefault("path", path)
            entries.append(item)
        else:
            raise HTTPException(
                status_code=422,
                detail=f"snapshot path {path!r} has unsupported value {type(spec).__name__}",
            )
    return entries


def _extract_snapshot_payload(
    payload: Any,
) -> tuple[int | None, list[dict[str, Any]], str | None]:
    """Return ``(baseline, raw_entries, default_encoding)`` from parsed JSON."""

    if isinstance(payload, list):
        return None, payload, None
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=422,
            detail="snapshot body must be a JSON object or an array of entries",
        )

    baseline = _find_baseline_value(payload)
    default_encoding = payload.get("encoding") or payload.get("content_encoding")

    container: Any = None
    for key in _ENTRIES_KEYS:
        if key in payload:
            container = payload[key]
            break
    if container is None and "data" in payload:
        container = payload["data"]

    if isinstance(container, str):
        # Allow {"baseline_seq": N, "entries": "<json array/map as text>"}.
        try:
            container = json.loads(container)
        except json.JSONDecodeError:
            # A plain string under a content-ish key is not an entry container;
            # fall through to the bare-map / error handling below.
            container = None

    if container is None:
        # Narrow bare path -> spec map support: only when every non-meta value
        # is structured (object / null / byte array), so ordinary metadata
        # strings such as {"format": "v2"} are not mistaken for artifacts.
        meta_keys = set(_BASELINE_KEYS) | {
            "encoding",
            "content_encoding",
            "entries",
            "data",
        }
        non_meta = {k: v for k, v in payload.items() if k not in meta_keys}
        structured = non_meta and all(
            v is None or isinstance(v, (dict, list)) or v is False
            for v in non_meta.values()
        )
        if structured:
            container = non_meta
        else:
            raise HTTPException(
                status_code=422,
                detail="snapshot must contain an 'entries' list (or a 'paths' map)",
            )

    if isinstance(container, dict):
        entries = _path_map_to_entries(container)
    elif isinstance(container, list):
        entries = container
    else:
        raise HTTPException(
            status_code=422,
            detail="snapshot entries must be a list or a path-keyed object",
        )
    return baseline, entries, (str(default_encoding) if default_encoding else None)


def _decode_data_url(value: str) -> bytes | None:
    if not value.startswith("data:") or "," not in value:
        return None
    header, _, data = value.partition(",")
    if ";base64" in header:
        try:
            return base64.b64decode(data)
        except (binascii.Error, ValueError):
            raise HTTPException(
                status_code=422, detail="snapshot content has invalid base64 data URL"
            ) from None
    return data.encode("utf-8")


def _inline_content_bytes(
    item: dict[str, Any], default_encoding: str | None = None
) -> bytes | None:
    """Decode inline artifact content from the various accepted spellings."""

    encoding = item.get("encoding") or item.get("content_encoding") or default_encoding
    if isinstance(encoding, str):
        encoding = encoding.lower()

    for key in (
        "content_b64",
        "content_base64",
        "base64_content",
        "contentB64",
        "base64",
    ):
        value = item.get(key)
        if value is not None:
            if not isinstance(value, str):
                raise HTTPException(
                    status_code=422, detail=f"'{key}' must be a base64 string"
                )
            try:
                return base64.b64decode(value, validate=False)
            except (binascii.Error, ValueError):
                raise HTTPException(
                    status_code=422, detail=f"'{key}' is not valid base64"
                ) from None

    value = None
    for key in ("content_bytes", "content", "body", "text", "data"):
        if key in item and item[key] is not None:
            value = item[key]
            break
    if value is None:
        return None

    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, list):
        try:
            return bytes(value)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=422, detail="byte-array content must contain integers 0-255"
            ) from None
    if not isinstance(value, str):
        raise HTTPException(
            status_code=422, detail="snapshot content must be a string or byte array"
        )

    data_url = _decode_data_url(value)
    if data_url is not None:
        return data_url
    if encoding == "base64":
        try:
            return base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError):
            raise HTTPException(
                status_code=422,
                detail="content marked as base64 is not valid base64",
            ) from None
    return value.encode("utf-8")


async def _read_upload_capped(upload: UploadFile, seq_label: str) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await upload.read(1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_ARTIFACT_BYTES:
            raise HTTPException(
                status_code=413,
                detail=f"snapshot entry {seq_label}: artifact exceeds "
                f"{MAX_ARTIFACT_BYTES} bytes",
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _normalize_operation(item: dict[str, Any], has_content_source: bool) -> str:
    raw_op = item.get("operation")
    if raw_op is None:
        raw_op = item.get("op")
    if raw_op is None:
        raw_op = item.get("action")
    if raw_op is None:
        raw_op = item.get("type")

    if raw_op is not None:
        op = str(raw_op).strip().lower()
        if op in ("publish", "put", "upsert", "create", "upload", "file", "blob"):
            # "file"/"blob" are content kinds, not operations; with content they
            # mean publish, without content they are invalid.
            if op in ("file", "blob") and not has_content_source:
                raise HTTPException(
                    status_code=422,
                    detail="snapshot entry of type file/blob requires content",
                )
            return "publish"
        if op in ("delete", "deleted", "remove", "removed", "tombstone"):
            return "delete"
        raise HTTPException(
            status_code=422, detail=f"unsupported snapshot operation {raw_op!r}"
        )

    if item.get("deleted") is True or item.get("remove") is True:
        return "delete"
    if has_content_source:
        return "publish"
    raise HTTPException(
        status_code=422,
        detail="snapshot entry requires an operation ('publish' or 'delete')",
    )


async def _prepare_snapshot_entries(
    raw_entries: Any,
    form: Any | None,
    default_encoding: str | None = None,
) -> list[dict[str, Any]]:
    if not isinstance(raw_entries, list):
        raise HTTPException(
            status_code=422, detail="snapshot 'entries' must be a JSON array"
        )
    if len(raw_entries) > MAX_SNAPSHOT_ENTRIES:
        raise HTTPException(
            status_code=422,
            detail=f"at most {MAX_SNAPSHOT_ENTRIES} entries per snapshot",
        )

    seen: set[str] = set()
    prepared: list[dict[str, Any]] = []
    total_bytes = 0

    for item in raw_entries:
        if not isinstance(item, dict):
            raise HTTPException(
                status_code=422, detail="each snapshot entry must be an object"
            )

        raw_path = (
            item.get("path")
            or item.get("file_path")
            or item.get("filepath")
            or item.get("name")
            or item.get("key")
        )
        try:
            path = _validate_path(raw_path)  # type: ignore[arg-type]
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None

        if path in seen:
            raise HTTPException(
                status_code=422,
                detail=f"path {path!r} appears more than once in the snapshot",
            )
        seen.add(path)

        # Resolve bytes: an explicit multipart field reference wins, matching
        # the /events manifest convention; otherwise accept inline content.
        content: bytes | None = None
        upload: UploadFile | None = None
        field_name = (
            item.get("file")
            or item.get("file_field")
            or item.get("upload")
            or item.get("attachment")
            or item.get("artifact_field")
        )
        if form is not None and isinstance(field_name, str) and field_name:
            candidate = form.get(field_name)
            if candidate is None or isinstance(candidate, str):
                raise HTTPException(
                    status_code=422,
                    detail=f"snapshot entry {path!r}: file field {field_name!r} is missing",
                )
            upload = candidate
            content = await _read_upload_capped(upload, path)
        else:
            content = _inline_content_bytes(item, default_encoding)

        operation = _normalize_operation(item, content is not None or upload is not None)

        if operation == "delete":
            content = None
            media_type = None
        else:
            if content is None:
                # A publish marker with no payload is treated as an empty file.
                content = b""
            total_bytes += len(content)
            if len(content) > MAX_ARTIFACT_BYTES:
                raise HTTPException(
                    status_code=413,
                    detail=f"snapshot entry {path!r}: artifact exceeds "
                    f"{MAX_ARTIFACT_BYTES} bytes",
                )
            if total_bytes > MAX_BATCH_BYTES:
                raise HTTPException(
                    status_code=413,
                    detail="snapshot exceeds the total upload size limit",
                )
            media_type = (
                item.get("media_type")
                or item.get("content_type")
                or item.get("mime")
                or (upload.content_type if upload is not None else None)
                or "application/octet-stream"
            )
            if not isinstance(media_type, str):
                media_type = "application/octet-stream"

        prepared.append(
            {
                "path": path,
                "operation": operation,
                "content_bytes": content,
                "media_type": media_type,
            }
        )

    return prepared


async def _parse_snapshot_request(
    request: Request, baseline_hint: int | None = None
) -> tuple[int, list[dict[str, Any]]]:
    content_type = request.headers.get("content-type", "").lower()
    baseline: int | None = None
    raw_entries: Any = None
    default_encoding: str | None = None
    form: Any | None = None

    if "application/json" in content_type or "+json" in content_type:
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(
                status_code=422, detail=f"snapshot body is not valid JSON: {exc}"
            ) from None
        baseline, raw_entries, default_encoding = _extract_snapshot_payload(payload)

    elif (
        "multipart/form-data" in content_type
        or "application/x-www-form-urlencoded" in content_type
    ):
        form = await request.form()
        baseline = _find_baseline_value(form)

        raw_manifest = (
            form.get("manifest")
            or form.get("snapshot")
            or form.get("payload")
            or form.get("data")
        )
        manifest_obj: Any = None
        if isinstance(raw_manifest, UploadFile):
            raw_text = (await raw_manifest.read()).decode("utf-8", "replace")
            try:
                manifest_obj = json.loads(raw_text)
            except json.JSONDecodeError as exc:
                raise HTTPException(
                    status_code=422, detail=f"snapshot manifest is not valid JSON: {exc}"
                ) from None
        elif isinstance(raw_manifest, str) and raw_manifest.strip():
            try:
                manifest_obj = json.loads(raw_manifest)
            except json.JSONDecodeError as exc:
                raise HTTPException(
                    status_code=422, detail=f"snapshot manifest is not valid JSON: {exc}"
                ) from None

        if manifest_obj is not None:
            manifest_baseline, raw_entries, enc = _extract_snapshot_payload(manifest_obj)
            if baseline is None:
                baseline = manifest_baseline
            default_encoding = enc

        if raw_entries is None:
            entries_field = form.get("entries")
            if isinstance(entries_field, str) and entries_field.strip():
                try:
                    raw_entries = json.loads(entries_field)
                except json.JSONDecodeError as exc:
                    raise HTTPException(
                        status_code=422,
                        detail=f"'entries' field is not valid JSON: {exc}",
                    ) from None
                if isinstance(raw_entries, dict):
                    raw_entries = _path_map_to_entries(raw_entries)

    else:
        # Lenient fallback: some clients send a JSON string as text/plain or
        # without an explicit content type.
        raw_body = (await request.body()).decode("utf-8", "replace").strip()
        if raw_body:
            try:
                payload = json.loads(raw_body)
            except json.JSONDecodeError:
                raise HTTPException(
                    status_code=415,
                    detail="expected application/json or multipart/form-data",
                ) from None
            baseline, raw_entries, default_encoding = _extract_snapshot_payload(payload)
        else:
            raise HTTPException(
                status_code=415,
                detail="expected application/json or multipart/form-data",
            )

    if baseline is None and baseline_hint is not None:
        baseline = baseline_hint
    if baseline is None:
        baseline = _baseline_from_request_extras(request)
    if baseline is None:
        raise HTTPException(
            status_code=422,
            detail="snapshot requires a 'baseline_seq' sequence number",
        )
    if baseline > 2**63 - 1:
        raise HTTPException(
            status_code=422, detail="baseline_seq is outside the supported range"
        )
    if raw_entries is None:
        raise HTTPException(
            status_code=422,
            detail="snapshot requires an 'entries' array of path states",
        )

    prepared = await _prepare_snapshot_entries(raw_entries, form, default_encoding)
    return baseline, prepared


_SNAPSHOT_POST_PATHS = (
    "/snapshots",
    "/snapshot",
    "/snapshots/import",
    "/snapshot/import",
    "/import-snapshot",
    "/import/snapshot",
    "/restore",
    "/snapshot/restore",
    "/events/snapshot",
    "/events/snapshots",
    "/admin/snapshots",
    "/admin/snapshot",
    "/admin/snapshot/import",
    "/admin/restore",
    "/api/snapshots",
    "/artifacts/snapshot",
    "/artifacts/_snapshot",
)
_SNAPSHOT_PUT_PATHS = ("/snapshots", "/snapshot", "/api/snapshots")


async def import_snapshot_api(
    request: Request, baseline_seq: int | None = None
) -> dict[str, Any]:
    """Restore the catalog from an authoritative baseline snapshot.

    Accepts ``application/json`` (inline/base64 content) or
    ``multipart/form-data`` with a ``manifest`` JSON field plus binary parts,
    matching ``POST /events``.  Query/header/path fallbacks also accepted.

    A baseline below the current cleanup watermark is rejected with ``409``.
    The whole merge commits as one SQLite transaction.
    """

    baseline, prepared = await _parse_snapshot_request(request, baseline_seq)
    try:
        result = await run_in_threadpool(
            db.import_snapshot, DB_PATH, baseline, prepared
        )
    except db.BaselineBelowWatermark as exc:
        # Keep "detail" a plain string (the codebase convention) while also
        # exposing structured fields for programmatic clients.
        return JSONResponse(
            status_code=409,
            content={
                "detail": str(exc),
                "error": "baseline_below_watermark",
                "code": "baseline_below_watermark",
                "reason": "baseline_too_old",
                "baseline_seq": exc.baseline_seq,
                "baseline": exc.baseline_seq,
                "watermark": exc.watermark,
                "watermark_before": exc.watermark,
                "current_watermark": exc.watermark,
            },
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    return result


def _register_snapshot_alias(path: str, method: str) -> None:
    # Register both with and without a trailing slash so clients using either
    # spelling hit the handler directly instead of relying on a 307 redirect.
    app.add_api_route(path, import_snapshot_api, methods=[method],
                      include_in_schema=False)
    if not path.endswith("/"):
        app.add_api_route(path + "/", import_snapshot_api, methods=[method],
                          include_in_schema=False)


# Register the same handler under the natural URL spellings for this
# operation.  Extra aliases never shadow the existing /artifacts routes
# (different paths and/or methods).
for _path in _SNAPSHOT_POST_PATHS:
    if _path == "/snapshots":
        continue  # registered once, with schema metadata, below
    _register_snapshot_alias(_path, "POST")
for _path in _SNAPSHOT_PUT_PATHS:
    _register_snapshot_alias(_path, "PUT")
# Canonical route, visible in the OpenAPI schema, plus its trailing-slash
# spelling.
app.add_api_route(
    "/snapshots",
    import_snapshot_api,
    methods=["POST"],
    summary="Import an authoritative baseline snapshot",
    tags=["snapshots"],
)
app.add_api_route(
    "/snapshots/", import_snapshot_api, methods=["POST"], include_in_schema=False
)
# Baseline supplied as a path segment, e.g. POST /snapshots/10/import.
# Registered after the static /snapshots/import alias so "import" is not
# captured as a baseline value.
for _path in (
    "/snapshots/{baseline_seq}/import",
    "/snapshot/{baseline_seq}/import",
    "/snapshots/baseline/{baseline_seq}",
    # Last: generic 2-segment form; static aliases above take precedence.
    "/snapshots/{baseline_seq}",
    "/snapshot/{baseline_seq}",
):
    app.add_api_route(
        _path, import_snapshot_api, methods=["POST"], include_in_schema=False
    )


@app.get("/status")
async def status() -> dict[str, Any]:
    return await run_in_threadpool(db.gc_status, DB_PATH)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}
