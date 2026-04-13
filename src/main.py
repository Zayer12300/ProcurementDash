# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
ProDash  —  Backend API  v3.3
==============================
Changes from v3.2:
  - /api/search        : new global search route prioritising customer_po
                         (45-series) and customer_pr (50-series)
  - /api/dashboard/dataset: search clause already covers customer_po +
                         customer_pr (was in v3.2, preserved)
  - /api/analytics/customer-references : new — fill-rate + sample values
                         for both reference series
  - ENTITY_SELECT      : already contains both fields (preserved from v3.2)

Run:
    py src/main.py
"""

import json
import logging
import re
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import numpy as np
import pandas as pd
import requests
import urllib3
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from requests.auth import HTTPBasicAuth
from starlette.middleware.gzip import GZipMiddleware

from transformations import run_transformations

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ─────────────────────────────────────────────────────────────────────────────
# Paths & config
# ─────────────────────────────────────────────────────────────────────────────
_SRC_DIR      = Path(__file__).resolve().parent
_PROJECT_ROOT = _SRC_DIR.parent

DB_PATH          = _PROJECT_ROOT / "data"   / "prodash.duckdb"
HTML_PATH        = _SRC_DIR               / "index.html"
CREDENTIALS_PATH = _PROJECT_ROOT / "config" / "Credentials.json"

APP_HOST    = "127.0.0.1"
APP_PORT    = 8094
PAGE_SIZE   = 5000
TIMEOUT     = 180
MAX_WORKERS = 8


# ─────────────────────────────────────────────────────────────────────────────
# OData $select
# ─────────────────────────────────────────────────────────────────────────────
ENTITY_SELECT: Dict[str, str] = {
    "I_PurchaseOrder": (
        "PurchaseOrder,Supplier,CreationDate,DocumentCurrency,"
        "PurchaseOrderType,PurchasingOrganization,PurchasingGroup,"
        "CompanyCode,CorrespncExternalReference,"          # ← 45-series
        "PurchasingProcessingStatus,PurgReleaseSequenceStatus,"
        "PurchasingDocumentDeletionCode,PaymentTerms,CreatedByUser"
    ),
    "I_PurchaseOrderItem": (
        "PurchaseOrder,PurchaseOrderItem,Material,MaterialGroup,"
        "MaterialType,Plant,StorageLocation,PurchaseOrderItemText,"
        "OrderQuantity,PurchaseOrderQuantityUnit,NetPriceAmount,"
        "NetAmount,AccountAssignmentCategory,CostCenter,GLAccount,"
        "WBSElementInternalID,ProfitCenter,RequisitionerName,"
        "PlannedDeliveryDurationInDays,IsCompletelyDelivered,"
        "IsFinallyInvoiced,GoodsReceiptIsExpected,InvoiceIsExpected,"
        "PurchaseRequisition,PurchaseRequisitionItem,"          # ← 50-series
        "PurchasingDocumentDeletionCode"
    ),
    "C_PurchaseOrderGoodsReceipt": (
        "PurchaseOrder,PurchaseOrderItem,QuantityInBaseUnit,"
        "PostingDate,GoodsMovementType,GoodsMovementIsCancelled"
    ),
    "C_PurOrdSuplrConfDisplay": (
        "PurchaseOrder,PurchaseOrderItem,ConfirmedQuantity,DeliveryDate"
    ),
    "C_PurReqItemByPurOrder": (
        "PurchaseOrder,PurchaseRequisition,PurchaseRequisitionItem,"
        "DeliveryDate,MaterialGroup_Text"
    ),
    "I_Supplier": "Supplier,SupplierName,OrganizationBPName1",
    "I_Material": "Material,MaterialName",
    "I_Plant":    "Plant,PlantName",
}


# ─────────────────────────────────────────────────────────────────────────────
# OData entity routing
# ─────────────────────────────────────────────────────────────────────────────
ENTITY_ROUTING: Dict[str, List[str]] = {
    "PurchaseOrderService": [
        "I_PurchaseOrder",
        "I_PurchaseOrderItem",
        "C_PurchaseOrderGoodsReceipt",
        "C_PurOrdSuplrConfDisplay",
        "C_PurReqItemByPurOrder",
        "I_Supplier",
        "I_Material",
        "I_Plant",
    ]
}


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ProDash")


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="ProDash API",
    version="3.3.0",
    description="Procurement Dashboard — SAP OData → DuckDB → Dashboard",
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/style.css", include_in_schema=False)
def serve_css():
    css_path = _SRC_DIR / "style.css"
    if css_path.exists():
        return FileResponse(str(css_path))
    raise HTTPException(404, "style.css not found")


@app.get("/script.js", include_in_schema=False)
def serve_js():
    js_path = _SRC_DIR / "script.js"
    if js_path.exists():
        return FileResponse(str(js_path))
    raise HTTPException(404, "script.js not found")


# ── Locks ─────────────────────────────────────────────────────────────────────
_sync_lock   = threading.Lock()
_cache_lock  = threading.Lock()
_schema_lock = threading.Lock()

# ── Dashboard response cache (60 s TTL) ───────────────────────────────────────
_dashboard_cache: Dict[str, Any] = {}
_CACHE_TTL = 60

# ── Schema existence cache ────────────────────────────────────────────────────
_schema_cache: Dict[str, bool] = {}


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_conn() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def table_exists(db: duckdb.DuckDBPyConnection, name: str) -> bool:
    key = f"t:{name.lower()}"
    with _schema_lock:
        if key in _schema_cache:
            return _schema_cache[key]
    result = bool(db.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE lower(table_name) = lower(?)", [name],
    ).fetchone()[0])
    with _schema_lock:
        _schema_cache[key] = result
    return result


def view_exists(db: duckdb.DuckDBPyConnection, name: str) -> bool:
    key = f"v:{name.lower()}"
    with _schema_lock:
        if key in _schema_cache:
            return _schema_cache[key]
    result = bool(db.execute(
        "SELECT COUNT(*) FROM information_schema.views "
        "WHERE lower(table_name) = lower(?)", [name],
    ).fetchone()[0])
    with _schema_lock:
        _schema_cache[key] = result
    return result


def _col_exists(db: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    """Check if a specific column exists in a table."""
    try:
        cols = {
            r[1].lower()
            for r in db.execute(f"PRAGMA table_info('{table}')").fetchall()
        }
        return col.lower() in cols
    except Exception:
        return False


def _clear_schema_cache() -> None:
    with _schema_lock:
        _schema_cache.clear()


def safe_scalar(db, sql, params=None, default=None):
    try:
        row = db.execute(sql, params or []).fetchone()
        return row[0] if row else default
    except Exception:
        return default


# ─────────────────────────────────────────────────────────────────────────────
# App status
# ─────────────────────────────────────────────────────────────────────────────

def init_db() -> None:
    with get_conn() as db:
        db.execute("DROP TABLE IF EXISTS app_status")
        db.execute("""
            CREATE TABLE app_status (
                key   VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """)
        db.execute("""
            INSERT INTO app_status VALUES
                ('backend_ready',    'false'),
                ('last_sync_time',   ''),
                ('last_sync_status', 'never'),
                ('last_error',       '')
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS sync_runs (
                run_id      VARCHAR,
                started_at  TIMESTAMP,
                finished_at TIMESTAMP,
                status      VARCHAR,
                message     VARCHAR
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS app_po_item_overrides (
                purchase_order      VARCHAR,
                purchase_order_item VARCHAR,
                status_override     VARCHAR,
                owner               VARCHAR,
                priority            VARCHAR,
                note                VARCHAR,
                updated_at          TIMESTAMP
            )
        """)


def set_status(key: str, value: str) -> None:
    with get_conn() as db:
        db.execute("DELETE FROM app_status WHERE key = ?", [key])
        db.execute(
            "INSERT INTO app_status (key, value) VALUES (?, ?)",
            [key, value],
        )


def get_status() -> Dict[str, str]:
    with get_conn() as db:
        return {k: v for k, v in db.execute(
            "SELECT key, value FROM app_status"
        ).fetchall()}


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ─────────────────────────────────────────────────────────────────────────────
# Credentials
# ─────────────────────────────────────────────────────────────────────────────

def load_credentials() -> Dict[str, Dict[str, str]]:
    if not CREDENTIALS_PATH.exists():
        raise FileNotFoundError(f"Credentials not found: {CREDENTIALS_PATH}")
    with open(CREDENTIALS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    conns = {c["name"]: c for c in data.get("sap_connections", [])}
    if not conns:
        raise ValueError("No sap_connections found in Credentials.json")
    return conns


# ─────────────────────────────────────────────────────────────────────────────
# OData normalisation
# ─────────────────────────────────────────────────────────────────────────────
_DATE_PAT = re.compile(r"/Date\((\d+)([+-]\d{4})?\)/")


def _sanitize(name: str) -> str:
    name = re.sub(r"[^A-Za-z0-9_]", "_", str(name))
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(
                lambda x: json.dumps(x, ensure_ascii=False)
                if isinstance(x, (dict, list)) else x
            )
    if "__metadata" in df.columns:
        df = df.drop(columns=["__metadata"])
    df.columns = [_sanitize(c) for c in df.columns]
    for col in df.columns:
        if df[col].dtype == object:
            s  = df[col].astype(str)
            ms = s.str.extract(_DATE_PAT, expand=False)[0]
            if ms.notna().any():
                ms_num = pd.to_numeric(ms, errors="coerce")
                dt     = pd.to_datetime(ms_num, unit="ms", errors="coerce")
                df[col] = np.where(
                    dt.notna(), dt.dt.strftime("%Y-%m-%d"), df[col]
                )
    return df.where(pd.notnull(df), None)


# ─────────────────────────────────────────────────────────────────────────────
# OData fetch
# ─────────────────────────────────────────────────────────────────────────────

def fetch_entity(
    entity: str,
    conn_name: str,
    conns: Dict[str, Dict[str, str]],
) -> pd.DataFrame:
    cfg        = conns[conn_name]
    url        = f"{cfg['base_url'].rstrip('/')}/{cfg['service'].strip('/')}/{entity}"
    auth       = HTTPBasicAuth(cfg["username"], cfg["password"])
    verify_ssl = cfg.get("verify_ssl", False)
    rows: List[dict] = []
    skip = 0

    base_params: Dict[str, Any] = {"$format": "json", "$top": PAGE_SIZE}
    if entity in ENTITY_SELECT:
        base_params["$select"] = ENTITY_SELECT[entity]

    while True:
        resp = requests.get(
            url,
            params={**base_params, "$skip": skip},
            auth=auth,
            verify=verify_ssl,
            timeout=TIMEOUT,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"{entity} → HTTP {resp.status_code}: {resp.text[:400]}"
            )
        batch = resp.json().get("d", {}).get("results", [])
        if not batch:
            break
        rows.extend(batch)
        skip += PAGE_SIZE
        if len(batch) < PAGE_SIZE:
            break

    logger.info("  Fetched %-40s  %6d rows", entity, len(rows))
    return normalize_df(pd.DataFrame(rows))


# ─────────────────────────────────────────────────────────────────────────────
# DuckDB raw writer
# ─────────────────────────────────────────────────────────────────────────────

def store_raw(
    db: duckdb.DuckDBPyConnection, entity: str, df: pd.DataFrame
) -> int:
    table    = f"raw_{entity}"
    tmp_name = f"_tmp_{entity}"
    if df is None or df.empty:
        db.execute(
            f"CREATE OR REPLACE TABLE {table} AS "
            f"SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0"
        )
        return 0
    db.register(tmp_name, df)
    db.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {tmp_name}")
    db.unregister(tmp_name)
    return int(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


# ─────────────────────────────────────────────────────────────────────────────
# Chart helper
# ─────────────────────────────────────────────────────────────────────────────

def _chart(
    df: pd.DataFrame,
    label_col: str = "label",
    value_col: str = "value",
    extras: list = None,
) -> dict:
    result = {
        "labels": df[label_col].tolist() if not df.empty else [],
        "data":   df[value_col].fillna(0).tolist() if not df.empty else [],
    }
    for col in (extras or []):
        if col in df.columns:
            result[col] = df[col].fillna(0).tolist()
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Routes — home
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def home():
    if HTML_PATH.exists():
        return FileResponse(str(HTML_PATH))
    return JSONResponse({"message": "ProDash API", "docs": "/docs"})


# ─────────────────────────────────────────────────────────────────────────────
# Routes — system
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/health", tags=["System"])
def api_health():
    try:
        with get_conn() as db:
            db.execute("SELECT 1").fetchone()
        return {"status": "healthy", "server_time": now_utc(), "db": str(DB_PATH)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/status", tags=["System"])
def api_status():
    st = get_status()
    return {
        "status":           "online",
        "server_time":      now_utc(),
        "db":               str(DB_PATH),
        "backend_ready":    st.get("backend_ready") == "true",
        "last_sync_time":   st.get("last_sync_time", ""),
        "last_sync_status": st.get("last_sync_status", ""),
        "last_error":       st.get("last_error", ""),
    }


@app.get("/api/ready", tags=["System"])
def api_ready():
    st = get_status()
    return {
        "ready":            st.get("backend_ready") == "true",
        "last_sync_time":   st.get("last_sync_time", ""),
        "last_sync_status": st.get("last_sync_status", ""),
    }


@app.get("/api/dashboard/bootstrap", tags=["System"])
def api_bootstrap():
    st = get_status()
    with get_conn() as db:
        rows = (
            safe_scalar(db, "SELECT COUNT(*) FROM mart_dashboard_po_item", default=0)
            if table_exists(db, "mart_dashboard_po_item") else 0
        )
    return {
        "status": "success",
        "backend": {
            "ready":            st.get("backend_ready") == "true",
            "last_sync_time":   st.get("last_sync_time", ""),
            "last_sync_status": st.get("last_sync_status", ""),
            "last_error":       st.get("last_error", ""),
            "row_count":        int(rows or 0),
        },
        "endpoints": {
            "sync":                  "POST /api/sync",
            "status":                "GET  /api/status",
            "search":                "GET  /api/search?q=",           # ← new
            "dashboard":             "GET  /api/analytics/dashboard",
            "dataset":               "GET  /api/dashboard/dataset",
            "suppliers":             "GET  /api/analytics/suppliers",
            "supplier_risk":         "GET  /api/analytics/supplier-risk",
            "trend":                 "GET  /api/analytics/trend",
            "material_groups":       "GET  /api/analytics/material-groups",
            "overdue":               "GET  /api/analytics/overdue",
            "delivery":              "GET  /api/analytics/delivery-performance",
            "by_plant":              "GET  /api/analytics/kpis/by-plant",
            "p2p_pipeline":          "GET  /api/analytics/p2p-pipeline",
            "customer_references":   "GET  /api/analytics/customer-references",  # ← new
            "sync_history":          "GET  /api/sync/history",
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Routes — POST /api/sync
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/api/sync", tags=["Sync"])
def api_sync():
    if not _sync_lock.acquire(blocking=False):
        raise HTTPException(429, "Sync already in progress — try again shortly.")

    run_id = f"run_{int(datetime.now(timezone.utc).timestamp())}"

    try:
        set_status("backend_ready",    "false")
        set_status("last_sync_status", "running")
        set_status("last_error",       "")

        with _cache_lock:
            _dashboard_cache.clear()
        _clear_schema_cache()

        with get_conn() as db:
            db.execute(
                "INSERT INTO sync_runs VALUES "
                "(?, current_timestamp, NULL, 'running', '')",
                [run_id],
            )

        conns = load_credentials()

        jobs: List[tuple] = [
            (entity, conn_name)
            for conn_name, entities in ENTITY_ROUTING.items()
            for entity in entities
            if conn_name in conns
        ]

        # ── Parallel OData fetch ──────────────────────────────────────────────
        t0 = time.time()
        logger.info("Sync %s — fetching %d entities …", run_id, len(jobs))
        fetched: Dict[str, pd.DataFrame] = {}
        fetch_errors: Dict[str, str]     = {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(fetch_entity, entity, conn_name, conns): entity
                for entity, conn_name in jobs
            }
            for future in as_completed(futures):
                entity = futures[future]
                try:
                    fetched[entity] = future.result()
                except Exception as exc:
                    fetch_errors[entity] = str(exc)
                    logger.error("  FAILED  %s — %s", entity, exc)

        fetch_secs = round(time.time() - t0, 1)
        if fetch_errors:
            logger.warning("Non-fatal fetch errors: %s", fetch_errors)

        # ── Single-transaction DuckDB write + transformations ─────────────────
        t1 = time.time()
        logger.info("Writing %d raw tables → %s …", len(fetched), DB_PATH.name)
        raw_counts: Dict[str, int] = {}

        with get_conn() as db:
            db.execute("BEGIN TRANSACTION")
            try:
                for entity, df in fetched.items():
                    raw_counts[entity] = store_raw(db, entity, df)
                    logger.info(
                        "  Stored  raw_%-35s  %6d rows",
                        entity, raw_counts[entity],
                    )
                logger.info("Running transformations …")
                transform_counts = run_transformations(db)
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise

        write_secs = round(time.time() - t1, 1)
        _clear_schema_cache()

        # ── Mark success ──────────────────────────────────────────────────────
        set_status("backend_ready",    "true")
        set_status("last_sync_status", "success")
        set_status("last_sync_time",   now_utc())
        set_status("last_error",       "")

        summary = {**raw_counts, **transform_counts}
        with get_conn() as db:
            db.execute(
                "UPDATE sync_runs SET finished_at=current_timestamp, "
                "status='success', message=? WHERE run_id=?",
                [json.dumps(summary), run_id],
            )

        total_secs = round(time.time() - t0, 1)
        logger.info(
            "Sync %s complete in %.1fs (fetch=%.1fs, write=%.1fs)",
            run_id, total_secs, fetch_secs, write_secs,
        )

        return {
            "status":        "success",
            "run_id":        run_id,
            "db":            str(DB_PATH),
            "raw_tables":    raw_counts,
            "output_tables": transform_counts,
            "fetch_errors":  fetch_errors,
            "completed_at":  now_utc(),
            "timing": {
                "fetch_seconds": fetch_secs,
                "write_seconds": write_secs,
                "total_seconds": total_secs,
            },
            "message": (
                f"SAP OData → {DB_PATH.name} refreshed in {total_secs}s — "
                f"mart_dashboard_po_item: "
                f"{transform_counts.get('mart_dashboard_po_item', 0):,} rows"
            ),
        }

    except Exception as exc:
        err = str(exc)
        logger.error("Sync %s FAILED\n%s", run_id, traceback.format_exc())
        set_status("backend_ready",    "false")
        set_status("last_sync_status", "failed")
        set_status("last_sync_time",   now_utc())
        set_status("last_error",       err)
        with get_conn() as db:
            db.execute(
                "UPDATE sync_runs SET finished_at=current_timestamp, "
                "status='failed', message=? WHERE run_id=?",
                [err, run_id],
            )
        raise HTTPException(500, err)

    finally:
        _sync_lock.release()


# ─────────────────────────────────────────────────────────────────────────────
# Routes — sync history
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/sync/history", tags=["Sync"])
def api_sync_history():
    with get_conn() as db:
        df = db.execute("""
            SELECT run_id, started_at, finished_at, status, message
            FROM sync_runs ORDER BY started_at DESC LIMIT 20
        """).df()
    return {"status": "success",
            "data": df.replace({np.nan: None}).to_dict("records")}


# ─────────────────────────────────────────────────────────────────────────────
# Routes — NEW: Global search  /api/search
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/search", tags=["Data"])
def api_search(
    q:     str = Query("", description="Search term — prefix 45 → customer_po, prefix 50 → customer_pr"),
    limit: int = Query(default=100, ge=1, le=1000),
):
    """
    Smart global search across all key fields.
    Priority routing:
      • Starts with '45' → searches customer_po first (CorrespncExternalReference)
      • Starts with '50' → searches customer_pr first (PurchaseRequisition)
      • Anything else    → searches all text fields (purchase_order, supplier,
                           material, customer_po, customer_pr, requisitioner)
    Returns matched rows from mart_dashboard_po_item.
    """
    q = q.strip()
    if not q:
        return {"status": "ok", "count": 0, "results": []}

    try:
        with get_conn() as db:
            if not table_exists(db, "mart_dashboard_po_item"):
                return {"status": "not_ready", "count": 0, "results": [],
                        "message": "Run POST /api/sync first"}

            # ── Detect which columns actually exist in the mart ───────────────
            avail = {
                r[1].lower()
                for r in db.execute(
                    "PRAGMA table_info('mart_dashboard_po_item')"
                ).fetchall()
            }

            like = f"%{q}%"

            # ── Build WHERE clause based on prefix ────────────────────────────
            if q.startswith("45"):
                # Customer PO (45-series) — prioritised first, fallback to PO#
                conditions = []
                params     = []
                if "customer_po" in avail:
                    conditions.append('customer_po ILIKE ?')
                    params.append(like)
                conditions.append('purchase_order ILIKE ?')
                params.append(like)

            elif q.startswith("50"):
                # Customer PR (50-series) — prioritised first
                conditions = []
                params     = []
                if "customer_pr" in avail:
                    conditions.append('customer_pr ILIKE ?')
                    params.append(like)
                if "purchase_requisition" in avail:
                    conditions.append('purchase_requisition ILIKE ?')
                    params.append(like)
                conditions.append('purchase_order ILIKE ?')
                params.append(like)

            else:
                # Generic — search all text columns
                SEARCH_MAP = [
                    ("purchase_order",        like),
                    ("customer_po",           like),   # 45-series
                    ("customer_pr",           like),   # 50-series
                    ("supplier",              like),
                    ("supplier_name",         like),
                    ("material",              like),
                    ("material_name",         like),
                    ("requisitioner",         like),
                    ("purchase_order_item_text", like),
                ]
                conditions = []
                params     = []
                for col, val in SEARCH_MAP:
                    if col in avail:
                        conditions.append(f'CAST("{col}" AS VARCHAR) ILIKE ?')
                        params.append(val)

            if not conditions:
                return {"status": "ok", "count": 0, "results": []}

            where = " OR ".join(conditions)

            # ── SELECT — always return both reference fields ──────────────────
            select_cols = [
                "purchase_order",
                "purchase_order_item",
            ]
            for col in ("customer_po", "customer_pr", "customer_pr_item"):
                if col in avail:
                    select_cols.append(col)

            select_cols += [
                "supplier",
                "supplier_name",
                "material",
                "material_name",
                "plant",
                "creation_date",
                "dashboard_status",
                "order_qty",
                "order_qty_unit",
                "spend",
                "document_currency",
                "purchasing_org",
            ]
            # Only include columns that exist
            safe_cols = [c for c in select_cols if c in avail]
            select_str = ", ".join(f'"{c}"' for c in safe_cols)

            sql = f"""
                SELECT {select_str}
                FROM mart_dashboard_po_item
                WHERE {where}
                ORDER BY creation_date DESC
                LIMIT ?
            """
            params.append(limit)

            df = db.execute(sql, params).df()

        return {
            "status":  "success",
            "count":   len(df),
            "query":   q,
            "results": df.replace({np.nan: None}).to_dict("records"),
        }

    except Exception as exc:
        raise HTTPException(500, str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# Routes — NEW: Customer reference fill-rate  /api/analytics/customer-references
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/analytics/customer-references", tags=["Analytics"])
def api_customer_references(samples: int = Query(default=5, ge=1, le=20)):
    """
    Returns fill-rate statistics and sample values for:
      • customer_po  (CorrespncExternalReference — 45-series)
      • customer_pr  (PurchaseRequisition        — 50-series)
    Useful for validating data quality after a sync.
    """
    try:
        with get_conn() as db:
            if not table_exists(db, "mart_dashboard_po_item"):
                return {"status": "not_ready",
                        "message": "Run POST /api/sync first"}

            result: Dict[str, Any] = {"status": "success", "fields": {}}

            for col, label, series in [
                ("customer_po", "CorrespncExternalReference", "45-series"),
                ("customer_pr", "PurchaseRequisition",        "50-series"),
            ]:
                if not _col_exists(db, "mart_dashboard_po_item", col):
                    result["fields"][col] = {
                        "label":    label,
                        "series":   series,
                        "present":  False,
                        "message":  f"Column '{col}' not found — run sync",
                    }
                    continue

                stats = db.execute(f"""
                    SELECT
                        COUNT(*)                                                    AS total,
                        COUNT("{col}")                                              AS non_null,
                        COUNT(DISTINCT "{col}")                                     AS distinct_vals,
                        ROUND(
                            100.0 * COUNT("{col}") / NULLIF(COUNT(*), 0), 1
                        )                                                           AS fill_pct
                    FROM mart_dashboard_po_item
                """).fetchone()

                sample_rows = db.execute(f"""
                    SELECT DISTINCT "{col}"
                    FROM mart_dashboard_po_item
                    WHERE "{col}" IS NOT NULL
                      AND TRIM(CAST("{col}" AS VARCHAR)) <> ''
                    ORDER BY "{col}"
                    LIMIT ?
                """, [samples]).df()

                # POs that have this reference set
                po_count = db.execute(f"""
                    SELECT COUNT(DISTINCT purchase_order)
                    FROM mart_dashboard_po_item
                    WHERE "{col}" IS NOT NULL
                      AND TRIM(CAST("{col}" AS VARCHAR)) <> ''
                """).fetchone()[0]

                result["fields"][col] = {
                    "label":         label,
                    "series":        series,
                    "present":       True,
                    "total_rows":    int(stats[0]),
                    "non_null":      int(stats[1]),
                    "distinct_vals": int(stats[2]),
                    "fill_pct":      float(stats[3] or 0),
                    "pos_with_ref":  int(po_count),
                    "sample_values": sample_rows[col].tolist()
                                     if not sample_rows.empty else [],
                }

        return result

    except Exception as exc:
        raise HTTPException(500, str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# Routes — GET /api/analytics/dashboard  (60 s cache)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/analytics/dashboard", tags=["Analytics"])
def api_analytics_dashboard():
    now_ts = time.time()

    with _cache_lock:
        if _dashboard_cache.get("ts", 0) + _CACHE_TTL > now_ts:
            return _dashboard_cache["data"]

    try:
        with get_conn() as db:
            if not table_exists(db, "mart_dashboard_po_item"):
                return {"status": "not_ready",
                        "message": "Run POST /api/sync first",
                        "kpis": {}, "charts": {}}

            kpi_row = db.execute("SELECT * FROM vw_dashboard_kpis").df()
            kpi = (
                {k: (None if pd.isna(v) else v)
                 for k, v in kpi_row.iloc[0].to_dict().items()}
                if not kpi_row.empty else {}
            )

            status_df = db.execute("""
                SELECT dashboard_status AS label, COUNT(*) AS value
                FROM mart_dashboard_po_item
                GROUP BY dashboard_status ORDER BY value DESC
            """).df()

            trend_df = db.execute("""
                SELECT year_month AS label, spend AS value, pos
                FROM vw_spend_by_month ORDER BY label ASC LIMIT 18
            """).df()

            sup_df = db.execute("""
                SELECT COALESCE(
                           CAST(supplier_name AS VARCHAR),
                           CAST(supplier      AS VARCHAR)
                       ) AS label,
                       total_spend     AS value,
                       supplier,
                       spend_share_pct,
                       delivered_lines,
                       open_lines,
                       avg_days_to_asn
                FROM vw_spend_by_supplier LIMIT 10
            """).df()

            mat_df = db.execute("""
                SELECT COALESCE(
                           CAST(material_group_text AS VARCHAR),
                           CAST(material_group      AS VARCHAR)
                       ) AS label,
                       total_spend AS value,
                       lines,
                       supplier_count
                FROM vw_spend_by_material_group LIMIT 10
            """).df()

            risk_df = db.execute("""
                SELECT COALESCE(
                           CAST(supplier_name AS VARCHAR),
                           CAST(supplier      AS VARCHAR)
                       ) AS label,
                       risk_score AS value,
                       risk_tier,
                       open_lines,
                       avg_days_to_asn,
                       delivery_rate_pct
                FROM vw_supplier_risk_tiers LIMIT 15
            """).df()

            result = {
                "status": "success",
                "kpis":   kpi,
                "charts": {
                    "status_distribution":     _chart(status_df),
                    "spend_timeline":          _chart(trend_df, extras=["pos"]),
                    "top_suppliers":           _chart(
                                                sup_df,
                                                extras=["spend_share_pct",
                                                        "delivered_lines",
                                                        "open_lines",
                                                        "avg_days_to_asn"]),
                    "spend_by_material_group": _chart(
                                                mat_df,
                                                extras=["lines",
                                                        "supplier_count"]),
                    "supplier_risk":           _chart(
                                                risk_df,
                                                extras=["risk_tier",
                                                        "open_lines",
                                                        "avg_days_to_asn",
                                                        "delivery_rate_pct"]),
                },
            }

        with _cache_lock:
            _dashboard_cache["ts"]   = now_ts
            _dashboard_cache["data"] = result

        return result

    except Exception as exc:
        raise HTTPException(500, str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# Routes — individual analytics
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/analytics/suppliers", tags=["Analytics"])
def api_suppliers(limit: int = Query(default=50, ge=1, le=200)):
    with get_conn() as db:
        if not view_exists(db, "vw_spend_by_supplier"):
            return {"status": "not_ready", "data": []}
        df = db.execute(
            "SELECT * FROM vw_spend_by_supplier LIMIT ?", [limit]
        ).df()
    return {"status": "success", "count": len(df),
            "data": df.replace({np.nan: None}).to_dict("records")}


@app.get("/api/analytics/supplier-risk", tags=["Analytics"])
def api_supplier_risk(limit: int = Query(default=100, ge=1, le=500)):
    with get_conn() as db:
        if not view_exists(db, "vw_supplier_risk_tiers"):
            return {"status": "not_ready", "data": []}
        df = db.execute(
            "SELECT * FROM vw_supplier_risk_tiers LIMIT ?", [limit]
        ).df()
    return {"status": "success", "count": len(df),
            "data": df.replace({np.nan: None}).to_dict("records")}


@app.get("/api/analytics/trend", tags=["Analytics"])
def api_trend(months: int = Query(default=12, ge=1, le=60)):
    with get_conn() as db:
        if not view_exists(db, "vw_spend_by_month"):
            return {"status": "not_ready", "data": []}
        df = db.execute(
            "SELECT * FROM vw_spend_by_month "
            "ORDER BY year_month DESC LIMIT ?", [months]
        ).df()
    return {"status": "success", "count": len(df),
            "data": df.replace({np.nan: None}).to_dict("records")}


@app.get("/api/analytics/material-groups", tags=["Analytics"])
def api_material_groups(limit: int = Query(default=20, ge=1, le=100)):
    with get_conn() as db:
        if not view_exists(db, "vw_spend_by_material_group"):
            return {"status": "not_ready", "data": []}
        df = db.execute(
            "SELECT * FROM vw_spend_by_material_group LIMIT ?", [limit]
        ).df()
    return {"status": "success", "count": len(df),
            "data": df.replace({np.nan: None}).to_dict("records")}


@app.get("/api/analytics/overdue", tags=["Analytics"])
def api_overdue(limit: int = Query(default=500, ge=1, le=2000)):
    with get_conn() as db:
        if not view_exists(db, "vw_overdue_pos"):
            return {"status": "not_ready", "data": []}
        df = db.execute(
            "SELECT * FROM vw_overdue_pos LIMIT ?", [limit]
        ).df()
    return {"status": "success", "count": len(df),
            "data": df.replace({np.nan: None}).to_dict("records")}


@app.get("/api/analytics/delivery-performance", tags=["Analytics"])
def api_delivery_performance(limit: int = Query(default=100, ge=1, le=500)):
    with get_conn() as db:
        if not view_exists(db, "vw_delivery_performance"):
            return {"status": "not_ready", "data": []}
        df = db.execute(
            "SELECT * FROM vw_delivery_performance LIMIT ?", [limit]
        ).df()
    return {"status": "success", "count": len(df),
            "data": df.replace({np.nan: None}).to_dict("records")}


@app.get("/api/analytics/kpis/by-plant", tags=["Analytics"])
def api_kpis_by_plant():
    with get_conn() as db:
        if not view_exists(db, "vw_spend_by_plant"):
            return {"status": "not_ready", "data": []}
        df = db.execute("SELECT * FROM vw_spend_by_plant").df()
    return {"status": "success", "count": len(df),
            "data": df.replace({np.nan: None}).to_dict("records")}


# ─────────────────────────────────────────────────────────────────────────────
# Routes — P2P Pipeline
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/analytics/p2p-pipeline", tags=["Analytics"])
def api_p2p_pipeline():
    with get_conn() as db:
        if not table_exists(db, "mart_dashboard_po_item"):
            return {"status": "not_ready", "stages": {}, "total_lines": 0}

        df = db.execute("""
            SELECT
                dashboard_status,
                COUNT(*)                                    AS lines,
                COALESCE(SUM(CAST(spend AS DOUBLE)), 0.0)  AS total_spend
            FROM mart_dashboard_po_item
            GROUP BY dashboard_status
        """).df()

        total = int(df["lines"].sum()) or 1

        stages: Dict[str, Any] = {}
        for _, row in df.iterrows():
            key = str(row["dashboard_status"]) if row["dashboard_status"] else "Unknown"
            stages[key] = {
                "lines":     int(row["lines"]),
                "spend":     float(row["total_spend"] or 0),
                "share_pct": round(float(row["lines"]) / total * 100, 1),
            }

        kpi = db.execute("""
            SELECT
                COUNT(*) FILTER (WHERE dashboard_status = 'PO Created')             AS po_created,
                COUNT(*) FILTER (WHERE dashboard_status IN
                    ('Shipped (Full)','Shipped (Partial)'))                          AS shipped,
                COUNT(*) FILTER (WHERE dashboard_status = 'Partially Delivered')     AS partially_delivered,
                COUNT(*) FILTER (WHERE dashboard_status = 'Fully Delivered')         AS fully_delivered,
                COUNT(*) FILTER (WHERE dashboard_status = 'Cancelled')               AS cancelled,
                COUNT(*)                                                              AS total
            FROM mart_dashboard_po_item
        """).df()

        kpi_dict = (
            {k: int(v) for k, v in kpi.iloc[0].to_dict().items()}
            if not kpi.empty else {}
        )

    return {
        "status":      "success",
        "total_lines": total,
        "stages":      stages,
        "kpis":        kpi_dict,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Routes — filterable dataset
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/dashboard/dataset", tags=["Data"])
def api_dataset(
    search:         str = "",
    status:         str = "",
    supplier:       str = "",
    material:       str = "",
    material_group: str = "",
    plant:          str = "",
    po:             str = "",
    customer_pr:    str = "",
    customer_po:    str = "",           # ← dedicated 45-series filter
    purchasing_org: str = "",
    date_from:      str = "",
    date_to:        str = "",
    limit: int = Query(default=500, ge=1, le=5000),
):
    try:
        with get_conn() as db:
            if not table_exists(db, "mart_dashboard_po_item"):
                return {"status": "not_ready", "count": 0, "data": []}

            sql = """
            SELECT
                purchase_order, purchase_order_item,
                customer_pr, customer_pr_item,
                customer_po,
                po_type, purchasing_org, purchasing_group, company_code,
                supplier, supplier_name,
                creation_date, document_currency,
                material, material_name,
                material_group, material_group_text, material_type,
                plant, storage_location,
                account_assignment_cat, cost_center, gl_account,
                wbs_element, profit_center, requisitioner,
                order_qty, order_qty_unit,
                net_price, net_amount,
                spend               AS estimated_spend,
                asn_qty, gr_qty,
                earliest_asn_delivery_date, last_gr_date,
                planned_delivery_days, days_po_to_asn,
                is_completely_delivered, is_finally_invoiced,
                dashboard_status, system_status,
                owner, priority, note
            FROM mart_dashboard_po_item
            WHERE 1=1
            """
            params: List[Any] = []

            if search:
                like = f"%{search}%"
                sql += """
                AND (purchase_order      ILIKE ?
                  OR customer_pr         ILIKE ?
                  OR customer_po         ILIKE ?
                  OR supplier            ILIKE ?
                  OR supplier_name       ILIKE ?
                  OR material            ILIKE ?
                  OR material_name       ILIKE ?
                  OR requisitioner       ILIKE ?)"""
                params.extend([like] * 8)

            if status:
                sql += " AND dashboard_status = ?"
                params.append(status)
            if supplier:
                sql += " AND (supplier ILIKE ? OR supplier_name ILIKE ?)"
                params.extend([f"%{supplier}%", f"%{supplier}%"])
            if material:
                sql += " AND (material ILIKE ? OR material_name ILIKE ?)"
                params.extend([f"%{material}%", f"%{material}%"])
            if material_group:
                sql += " AND material_group = ?"
                params.append(material_group)
            if plant:
                sql += " AND plant = ?"
                params.append(plant)
            if po:
                sql += " AND (purchase_order ILIKE ? OR customer_po ILIKE ?)"
                params.extend([f"%{po}%", f"%{po}%"])
            if customer_pr:
                sql += " AND customer_pr ILIKE ?"
                params.append(f"%{customer_pr}%")
            if customer_po:                                    # ← new filter
                sql += " AND customer_po ILIKE ?"
                params.append(f"%{customer_po}%")
            if purchasing_org:
                sql += " AND purchasing_org = ?"
                params.append(purchasing_org)
            if date_from:
                sql += " AND creation_date >= ?"
                params.append(date_from)
            if date_to:
                sql += " AND creation_date <= ?"
                params.append(date_to)

            sql += """
            ORDER BY creation_date DESC, purchase_order DESC,
                     purchase_order_item ASC LIMIT ?"""
            params.append(limit)

            df = db.execute(sql, params).df()
            return {"status": "success", "count": int(len(df)),
                    "data": df.replace({np.nan: None}).to_dict("records")}

    except Exception as exc:
        raise HTTPException(500, str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# Routes — manual overrides
# ─────────────────────────────────────────────────────────────────────────────

@app.patch("/api/dashboard/override", tags=["Data"])
def api_override(payload: Dict[str, Any]):
    po   = payload.get("purchase_order")
    item = payload.get("purchase_order_item")
    if not po or not item:
        raise HTTPException(400, "purchase_order and purchase_order_item required")
    with get_conn() as db:
        db.execute(
            "DELETE FROM app_po_item_overrides "
            "WHERE purchase_order=? AND purchase_order_item=?",
            [po, item],
        )
        db.execute(
            "INSERT INTO app_po_item_overrides "
            "(purchase_order, purchase_order_item, status_override, "
            " owner, priority, note, updated_at) "
            "VALUES (?,?,?,?,?,?,current_timestamp)",
            [po, item,
             payload.get("status_override"),
             payload.get("owner"),
             payload.get("priority"),
             payload.get("note")],
        )
    with _cache_lock:
        _dashboard_cache.clear()
    return {"status": "success",
            "updated": {"purchase_order": po, "item": item}}


@app.delete(
    "/api/dashboard/override/{purchase_order}/{purchase_order_item}",
    tags=["Data"],
)
def api_override_delete(purchase_order: str, purchase_order_item: str):
    with get_conn() as db:
        db.execute(
            "DELETE FROM app_po_item_overrides "
            "WHERE purchase_order=? AND purchase_order_item=?",
            [purchase_order, purchase_order_item],
        )
    with _cache_lock:
        _dashboard_cache.clear()
    return {"status": "success",
            "deleted": {"purchase_order": purchase_order,
                        "item": purchase_order_item}}


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    init_db()

    print("\n" + "=" * 60)
    print("  ProDash API  v3.3  —  RUNNING")
    print(f"  Dashboard  :  http://{APP_HOST}:{APP_PORT}")
    print(f"  API Docs   :  http://{APP_HOST}:{APP_PORT}/docs")
    print(f"  Database   :  {DB_PATH}")
    print("=" * 60 + "\n")

    uvicorn.run(
        app,
        host=APP_HOST,
        port=APP_PORT,
        log_level="warning",
        timeout_graceful_shutdown=5,
    )