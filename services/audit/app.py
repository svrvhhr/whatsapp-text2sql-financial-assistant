import os
import time
import json
import logging
import hashlib
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import Json, RealDictCursor
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field
import secrets


# =========================================================
# ENV
# =========================================================
POSTGRES_DB = os.getenv("POSTGRES_DB", "orionis")
POSTGRES_USER = os.getenv("POSTGRES_USER", "orionis")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "orionis")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "orionis-postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

ENV = os.getenv("ENV", "dev")

AUDIT_TABLE = os.getenv("AUDIT_TABLE", "audit_service_event")
MAX_LIST_EVENTS = int(os.getenv("MAX_LIST_EVENTS", "500"))
DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "5"))

AUDIT_AUTO_MIGRATE = os.getenv("AUDIT_AUTO_MIGRATE", "1") == "1"

# Basic auth (admin UI)
AUDIT_ADMIN_USER = os.getenv("AUDIT_ADMIN_USER", "admin")
AUDIT_ADMIN_PASS = os.getenv("AUDIT_ADMIN_PASS", "admin")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(message)s")
logger = logging.getLogger("audit_service")

security = HTTPBasic()


# =========================================================
# Models
# =========================================================
class AuditEventIn(BaseModel):
    request_id: str
    status: str  # executed|refused|error
    operation: str = "UNKNOWN"

    # SQL info (can be omitted by caller, but we support it)
    sql: Optional[str] = None
    params: List[Any] = Field(default_factory=list)
    reasons: List[str] = Field(default_factory=list)

    row_count: Optional[int] = None
    affected_rows: Optional[int] = None
    duration_ms: Optional[int] = None

    context: Dict[str, Any] = Field(default_factory=dict)

    # IMPORTANT: these are super useful for UI filters
    source: Optional[str] = None      # e.g. "api-gateway" | "text2sql" | "sql-guard" | "sql-executor" | "response-writer"
    stage: Optional[str] = None       # e.g. "convert" | "guard" | "execute" | "write"
    http_status: Optional[int] = None
    error_code: Optional[str] = None

    timestamp_ms: Optional[int] = None


class AuditEventOut(BaseModel):
    id: int
    created_at: str
    request_id: str
    status: str
    operation: str
    row_count: Optional[int] = None
    affected_rows: Optional[int] = None
    duration_ms: Optional[int] = None
    actor_id: Optional[str] = None
    role: Optional[str] = None
    entreprise_id: Optional[int] = None
    projet_id: Optional[int] = None

    # New columns
    source: Optional[str] = None
    stage: Optional[str] = None
    sql_preview: Optional[str] = None
    sql_hash: Optional[str] = None
    http_status: Optional[int] = None
    error_code: Optional[str] = None

    payload: Dict[str, Any]


class StatsOut(BaseModel):
    range_hours: int
    total: int
    executed: int
    refused: int
    error: int
    top_operations: List[Dict[str, Any]]
    top_actors: List[Dict[str, Any]]
    top_sources: List[Dict[str, Any]]
    top_stages: List[Dict[str, Any]]


# =========================================================
# DB
# =========================================================
def _pg_connect():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        connect_timeout=DB_CONNECT_TIMEOUT,
    )


def _sql_preview(sql: Optional[str], max_len: int = 220) -> Optional[str]:
    if not sql:
        return None
    s = " ".join((sql or "").split())
    return s if len(s) <= max_len else s[:max_len] + "..."


def _sql_hash(sql: Optional[str]) -> Optional[str]:
    if not sql:
        return None
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def ensure_table_exists() -> None:
    """
    Create table if missing, and add missing columns safely.
    """
    if not AUDIT_AUTO_MIGRATE:
        return

    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS public.{AUDIT_TABLE} (
                  id SERIAL PRIMARY KEY,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  request_id TEXT NOT NULL,
                  status TEXT NOT NULL,
                  operation TEXT NOT NULL,

                  row_count INT,
                  affected_rows INT,
                  duration_ms INT,

                  actor_id TEXT,
                  role TEXT,
                  entreprise_id INT,
                  projet_id INT,

                  -- new useful indexed fields
                  source TEXT,
                  stage TEXT,
                  sql_preview TEXT,
                  sql_hash TEXT,
                  http_status INT,
                  error_code TEXT,

                  payload JSONB NOT NULL DEFAULT '{{}}'::jsonb
                );
                """
            )

            # Backward-compatible ALTER (safe if columns already exist)
            cur.execute(f"ALTER TABLE public.{AUDIT_TABLE} ADD COLUMN IF NOT EXISTS entreprise_id INT;")
            cur.execute(f"ALTER TABLE public.{AUDIT_TABLE} ADD COLUMN IF NOT EXISTS projet_id INT;")
            cur.execute(f"ALTER TABLE public.{AUDIT_TABLE} ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{{}}'::jsonb;")

            cur.execute(f"ALTER TABLE public.{AUDIT_TABLE} ADD COLUMN IF NOT EXISTS source TEXT;")
            cur.execute(f"ALTER TABLE public.{AUDIT_TABLE} ADD COLUMN IF NOT EXISTS stage TEXT;")
            cur.execute(f"ALTER TABLE public.{AUDIT_TABLE} ADD COLUMN IF NOT EXISTS sql_preview TEXT;")
            cur.execute(f"ALTER TABLE public.{AUDIT_TABLE} ADD COLUMN IF NOT EXISTS sql_hash TEXT;")
            cur.execute(f"ALTER TABLE public.{AUDIT_TABLE} ADD COLUMN IF NOT EXISTS http_status INT;")
            cur.execute(f"ALTER TABLE public.{AUDIT_TABLE} ADD COLUMN IF NOT EXISTS error_code TEXT;")

            # Indexes
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{AUDIT_TABLE}_request_id ON public.{AUDIT_TABLE}(request_id);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{AUDIT_TABLE}_created_at ON public.{AUDIT_TABLE}(created_at DESC);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{AUDIT_TABLE}_status ON public.{AUDIT_TABLE}(status);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{AUDIT_TABLE}_operation ON public.{AUDIT_TABLE}(operation);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{AUDIT_TABLE}_actor_id ON public.{AUDIT_TABLE}(actor_id);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{AUDIT_TABLE}_source ON public.{AUDIT_TABLE}(source);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{AUDIT_TABLE}_stage ON public.{AUDIT_TABLE}(stage);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{AUDIT_TABLE}_sql_hash ON public.{AUDIT_TABLE}(sql_hash);")

            conn.commit()
    finally:
        conn.close()


def insert_event(ev: AuditEventIn) -> int:
    conn = _pg_connect()
    try:
        ctx = ev.context or {}
        actor_id = ctx.get("actor_id")
        role = ctx.get("role")
        entreprise_id = ctx.get("entreprise_id")
        projet_id = ctx.get("projet_id")

        payload = ev.model_dump()

        sp = _sql_preview(ev.sql)
        sh = _sql_hash(ev.sql)

        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO public.{AUDIT_TABLE}
                  (request_id, status, operation,
                   row_count, affected_rows, duration_ms,
                   actor_id, role, entreprise_id, projet_id,
                   source, stage, sql_preview, sql_hash, http_status, error_code,
                   payload)
                VALUES
                  (%s,%s,%s,
                   %s,%s,%s,
                   %s,%s,%s,%s,
                   %s,%s,%s,%s,%s,%s,
                   %s::jsonb)
                RETURNING id;
                """,
                (
                    ev.request_id, ev.status, ev.operation,
                    ev.row_count, ev.affected_rows, ev.duration_ms,
                    actor_id, role, entreprise_id, projet_id,
                    ev.source, ev.stage, sp, sh, ev.http_status, ev.error_code,
                    Json(payload),
                ),
            )
            new_id = cur.fetchone()[0]
            conn.commit()
            return int(new_id)
    finally:
        conn.close()


def _auth(credentials: HTTPBasicCredentials = Depends(security)):
    ok_user = secrets.compare_digest(credentials.username, AUDIT_ADMIN_USER)
    ok_pass = secrets.compare_digest(credentials.password, AUDIT_ADMIN_PASS)
    if not (ok_user and ok_pass):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True


def list_events(
    limit: int = 50,
    offset: int = 0,
    status: Optional[str] = None,
    operation: Optional[str] = None,
    actor_id: Optional[str] = None,
    source: Optional[str] = None,
    stage: Optional[str] = None,
    q: Optional[str] = None,
) -> List[Dict[str, Any]]:
    limit = max(1, min(limit, MAX_LIST_EVENTS))
    offset = max(0, offset)

    wheres = []
    params: List[Any] = []

    if status:
        wheres.append("status = %s"); params.append(status)
    if operation:
        wheres.append("operation = %s"); params.append(operation)
    if actor_id:
        wheres.append("actor_id = %s"); params.append(actor_id)
    if source:
        wheres.append("source = %s"); params.append(source)
    if stage:
        wheres.append("stage = %s"); params.append(stage)
    if q:
        wheres.append("(request_id ILIKE %s OR payload::text ILIKE %s OR sql_preview ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])

    where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""

    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, created_at, request_id, status, operation,
                       row_count, affected_rows, duration_ms,
                       actor_id, role, entreprise_id, projet_id,
                       source, stage, sql_preview, sql_hash, http_status, error_code,
                       payload
                FROM public.{AUDIT_TABLE}
                {where_sql}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s;
                """,
                tuple(params + [limit, offset]),
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_event_by_id(event_id: int) -> Optional[Dict[str, Any]]:
    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, created_at, request_id, status, operation,
                       row_count, affected_rows, duration_ms,
                       actor_id, role, entreprise_id, projet_id,
                       source, stage, sql_preview, sql_hash, http_status, error_code,
                       payload
                FROM public.{AUDIT_TABLE}
                WHERE id = %s;
                """,
                (event_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def stats(range_hours: int = 24) -> Dict[str, Any]:
    range_hours = max(1, min(range_hours, 24 * 30))

    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                  COUNT(*) AS total,
                  SUM(CASE WHEN status='executed' THEN 1 ELSE 0 END) AS executed,
                  SUM(CASE WHEN status='refused' THEN 1 ELSE 0 END) AS refused,
                  SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS error
                FROM public.{AUDIT_TABLE}
                WHERE created_at >= NOW() - (%s || ' hours')::interval;
                """,
                (range_hours,),
            )
            totals = cur.fetchone() or {}

            def top_by(col: str):
                cur.execute(
                    f"""
                    SELECT {col} AS key, COUNT(*) AS cnt
                    FROM public.{AUDIT_TABLE}
                    WHERE created_at >= NOW() - (%s || ' hours')::interval
                    GROUP BY {col}
                    ORDER BY cnt DESC
                    LIMIT 10;
                    """,
                    (range_hours,),
                )
                return cur.fetchall()

            top_ops = top_by("operation")
            top_actors = top_by("actor_id")
            top_sources = top_by("source")
            top_stages = top_by("stage")

        return {
            "range_hours": range_hours,
            "total": int(totals.get("total") or 0),
            "executed": int(totals.get("executed") or 0),
            "refused": int(totals.get("refused") or 0),
            "error": int(totals.get("error") or 0),
            "top_operations": top_ops,
            "top_actors": top_actors,
            "top_sources": top_sources,
            "top_stages": top_stages,
        }
    finally:
        conn.close()


# =========================================================
# FastAPI
# =========================================================
app = FastAPI(title="Audit & Tracability Service", version="2.2.0")


@app.on_event("startup")
def startup():
    ensure_table_exists()
    logger.info(json.dumps({"event": "startup", "env": ENV, "table": AUDIT_TABLE}, ensure_ascii=False))


@app.get("/health")
def health():
    ok = True
    err = None
    try:
        conn = _pg_connect()
        conn.close()
    except Exception as e:
        ok = False
        err = str(e)

    return {
        "status": "ok" if ok else "error",
        "env": ENV,
        "db_host": POSTGRES_HOST,
        "table": AUDIT_TABLE,
        "auto_migrate": AUDIT_AUTO_MIGRATE,
        "db_error": err,
    }


@app.post("/event")
def ingest_event(ev: AuditEventIn):
    try:
        new_id = insert_event(ev)
        return {"status": "stored", "id": new_id, "request_id": ev.request_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Audit store error: {e}")


# --------- ADMIN endpoints (protected) ---------
@app.get("/events", response_model=List[AuditEventOut])
def get_events(
    _=Depends(_auth),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    status: Optional[str] = None,
    operation: Optional[str] = None,
    actor_id: Optional[str] = None,
    source: Optional[str] = None,
    stage: Optional[str] = None,
    q: Optional[str] = None,
):
    rows = list_events(limit=limit, offset=offset, status=status, operation=operation,
                       actor_id=actor_id, source=source, stage=stage, q=q)
    return [
        AuditEventOut(
            id=r["id"],
            created_at=r["created_at"].isoformat(),
            request_id=r["request_id"],
            status=r["status"],
            operation=r["operation"],
            row_count=r["row_count"],
            affected_rows=r["affected_rows"],
            duration_ms=r["duration_ms"],
            actor_id=r["actor_id"],
            role=r["role"],
            entreprise_id=r.get("entreprise_id"),
            projet_id=r.get("projet_id"),
            source=r.get("source"),
            stage=r.get("stage"),
            sql_preview=r.get("sql_preview"),
            sql_hash=r.get("sql_hash"),
            http_status=r.get("http_status"),
            error_code=r.get("error_code"),
            payload=r.get("payload") or {},
        )
        for r in rows
    ]


@app.get("/events/id/{event_id}", response_model=AuditEventOut)
def get_event(event_id: int, _=Depends(_auth)):
    r = get_event_by_id(event_id)
    if not r:
        raise HTTPException(404, "Not found")
    return AuditEventOut(
        id=r["id"],
        created_at=r["created_at"].isoformat(),
        request_id=r["request_id"],
        status=r["status"],
        operation=r["operation"],
        row_count=r["row_count"],
        affected_rows=r["affected_rows"],
        duration_ms=r["duration_ms"],
        actor_id=r["actor_id"],
        role=r["role"],
        entreprise_id=r.get("entreprise_id"),
        projet_id=r.get("projet_id"),
        source=r.get("source"),
        stage=r.get("stage"),
        sql_preview=r.get("sql_preview"),
        sql_hash=r.get("sql_hash"),
        http_status=r.get("http_status"),
        error_code=r.get("error_code"),
        payload=r.get("payload") or {},
    )


@app.get("/stats", response_model=StatsOut)
def get_stats(range_hours: int = 24, _=Depends(_auth)):
    s = stats(range_hours=range_hours)
    return StatsOut(**s)
