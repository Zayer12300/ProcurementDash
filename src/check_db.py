# -*- coding: utf-8 -*-
#!/usr/bin/env python3

"""
check_db.py  —  ProDash Database & SAP OData Inspector

Connects to prodash.db and the SAP OData service, then prints:
  1. SAP service metadata (all entity sets + key fields + column count)
  2. DuckDB table inventory (row counts, schema)
  3. Sample data from core tables
  4. Sync run history
  5. KPI snapshot
  6. CustomerPRNumber field probe (new)
"""

import json
import warnings
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd
import requests
import urllib3
from requests.auth import HTTPBasicAuth

warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
BASE_DIR         = Path(__file__).resolve().parent
PROJECT_ROOT     = BASE_DIR.parent
DB_PATH          = PROJECT_ROOT / "data" / "prodash.db"
CREDENTIALS_PATH = PROJECT_ROOT / "config" / "Credentials.json"

SEPARATOR = "─" * 80
SECTION   = "═" * 80

# Fields to highlight wherever they appear in any entity or table
TRACKED_FIELDS = {
    "CustomerPRNumber",
    "PurchaseRequisition",
    "PurchaseRequisitionItem",
}

# ─────────────────────────────────────────────
# Credentials
# ─────────────────────────────────────────────
def load_credentials() -> Dict:
    if not CREDENTIALS_PATH.exists():
        raise FileNotFoundError(f"Credentials not found: {CREDENTIALS_PATH}")
    with open(CREDENTIALS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    conns = {c["name"]: c for c in data.get("sap_connections", [])}
    if not conns:
        raise ValueError("No sap_connections found in Credentials.json")
    return conns


# ─────────────────────────────────────────────
# SAP OData Metadata Inspector
# ─────────────────────────────────────────────
def inspect_sap_metadata(conn_cfg: Dict) -> None:
    base_url = conn_cfg["base_url"].rstrip("/")
    service  = conn_cfg["service"].strip("/")
    meta_url = f"{base_url}/{service}/$metadata"
    auth     = HTTPBasicAuth(conn_cfg["username"], conn_cfg["password"])

    print(f"\n{SECTION}")
    print(f"  SAP OData Service Metadata")
    print(f"  Endpoint : {base_url}/{service}")
    print(f"  User     : {conn_cfg['username']}")
    print(SECTION)

    try:
        resp = requests.get(meta_url, auth=auth, verify=False, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [ERROR] Could not fetch $metadata: {e}")
        return

    try:
        import xml.etree.ElementTree as ET

        ns   = {"edm": "http://schemas.microsoft.com/ado/2009/11/edm"}
        root = ET.fromstring(resp.text)

        # Build entity type → properties map
        entity_types: Dict[str, List[str]] = {}
        key_fields:   Dict[str, List[str]] = {}

        for et in root.iter("{http://schemas.microsoft.com/ado/2009/11/edm}EntityType"):
            name  = et.get("Name", "")
            props = [p.get("Name") for p in et.iter("{http://schemas.microsoft.com/ado/2009/11/edm}Property")]
            keys  = [
                pr.get("Name")
                for k  in et.iter("{http://schemas.microsoft.com/ado/2009/11/edm}Key")
                for pr in k.iter("{http://schemas.microsoft.com/ado/2009/11/edm}PropertyRef")
            ]
            entity_types[name] = props
            key_fields[name]   = keys

        # Entity sets
        entity_sets: List[Dict] = []
        for es in root.iter("{http://schemas.microsoft.com/ado/2009/11/edm}EntitySet"):
            set_name  = es.get("Name", "")
            type_name = es.get("EntityType", "").split(".")[-1]
            cols      = entity_types.get(type_name, [])
            keys      = key_fields.get(type_name, [])
            entity_sets.append({
                "EntitySet"  : set_name,
                "EntityType" : type_name,
                "Keys"       : ", ".join(keys) if keys else "—",
                "Columns"    : len(cols),
                "_props"     : cols,   # kept for tracked-field detection, not printed in table
            })

        df_meta = pd.DataFrame(entity_sets)
        print(f"\n  Total entity sets: {len(df_meta)}\n")

        PRODASH_ENTITIES = {
            "I_PurchaseOrder", "I_PurchaseOrderItem",
            "C_PurchaseOrderGoodsReceipt", "C_PurOrdSuplrConfDisplay",
            "C_PurReqItemByPurOrder",
        }
        RECOMMENDED_EXTRAS = {
            "I_PurchaseOrderScheduleLine"     : "Delivery schedule lines (promised dates)",
            "C_PurOrdItemEnh"                 : "Enhanced item fields (plant, storage loc, acct assignment)",
            "C_POScheduleLineFactSheet"        : "Schedule line fact sheet",
            "C_POAccountAssignmentFactSheet"   : "Cost center / WBS / GL account per item",
            "C_SuplInvPurOrdRef"               : "Supplier invoice references (for invoice matching)",
            "I_PurchasingDocumentStatus"       : "Status codes and descriptions",
            "I_Supplier"                       : "Supplier master data (name, country, etc.)",
            "I_PurchasingOrganization"         : "Purchasing org master data",
            "I_Plant"                          : "Plant master data",
            "I_MaterialGroup"                  : "Material group master",
            "I_PurchaseOrderEnhanced"          : "Enhanced PO header fields",
        }

        print(f"  {'EntitySet':<45} {'EntityType':<45} {'Keys':<35} {'Cols':>4}  {'ProDash':<20}  TrackedFields")
        print(f"  {'-'*45} {'-'*45} {'-'*35} {'----':>4}  {'-'*20}  {'-'*40}")

        for _, row in df_meta.sort_values("EntitySet").iterrows():
            tag = ""
            if row["EntitySet"] in PRODASH_ENTITIES:
                tag = "✔ USED"
            elif row["EntitySet"] in RECOMMENDED_EXTRAS:
                tag = "★ RECOMMENDED"

            # Detect tracked fields present in this entity's properties
            present_tracked = [f for f in TRACKED_FIELDS if f in (row["_props"] or [])]
            tracked_str = ", ".join(present_tracked) if present_tracked else ""

            print(
                f"  {row['EntitySet']:<45} {row['EntityType']:<45} "
                f"{row['Keys']:<35} {row['Columns']:>4}  {tag:<20}  {tracked_str}"
            )

        # ── CustomerPRNumber SAP-side summary ───────────────
        print(f"\n  ── Entities containing CustomerPRNumber (SAP side) ──")
        found_any = False
        for _, row in df_meta.iterrows():
            if "CustomerPRNumber" in (row["_props"] or []):
                found_any = True
                print(f"    ✔  {row['EntitySet']}  ({row['EntityType']})")
        if not found_any:
            print("    ✘  CustomerPRNumber not found in any entity set's properties")

        print(f"\n  ── Recommended entities to add to ENTITY_ROUTING ──")
        for ename, reason in RECOMMENDED_EXTRAS.items():
            exists = any(df_meta["EntitySet"] == ename)
            status = "✔ available" if exists else "✘ not found"
            print(f"  [{status}]  {ename:<45}  → {reason}")

    except Exception as e:
        print(f"  [ERROR] Metadata parse failed: {e}")
        print(f"  Raw metadata (first 500 chars):\n{resp.text[:500]}")


# ─────────────────────────────────────────────
# DuckDB Inspector
# ─────────────────────────────────────────────
def inspect_duckdb() -> None:
    if not DB_PATH.exists():
        print(f"\n[WARNING] DuckDB file not found: {DB_PATH}")
        print("  Run POST /api/sync first to populate the database.")
        return

    print(f"\n{SECTION}")
    print(f"  DuckDB Database: {DB_PATH}")
    print(SECTION)

    with duckdb.connect(str(DB_PATH), read_only=True) as db:

        # ── Tables ─────────────────────────────────────────
        tables = db.execute(
            """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_type, table_name
            """
        ).df()

        print(f"\n  {'Table / View':<45} {'Type':<10} {'Rows':>10}  {'Columns':>8}")
        print(f"  {'-'*45} {'-'*10} {'-'*10}  {'-'*8}")

        for _, row in tables.iterrows():
            tname = row["table_name"]
            ttype = row["table_type"]
            try:
                row_count = db.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
                col_count = len(db.execute(f'DESCRIBE "{tname}"').df())
            except Exception:
                row_count = "ERR"
                col_count = "ERR"
            print(f"  {tname:<45} {ttype:<10} {row_count:>10,}  {col_count:>8}")

        # ── Schema for core tables ──────────────────────────
        CORE_TABLES = [
            "raw_I_PurchaseOrder",
            "raw_I_PurchaseOrderItem",
            "raw_C_PurchaseOrderGoodsReceipt",
            "raw_C_PurOrdSuplrConfDisplay",
            "raw_C_PurReqItemByPurOrder",
            "fact_po_item",
            "mart_dashboard_po_item",
        ]

        # Lowercase aliases to match against (SAP raw vs snake_case transformed)
        HIGHLIGHT_COLS = {
            "customerPRNumber",
            "customer_pr_number",
            "purchaserequisition",
            "purchase_requisition",
            "purchaserequisitionitem",
            "purchase_requisition_item",
        }
        HIGHLIGHT_LOWER = {c.lower() for c in HIGHLIGHT_COLS}

        for tname in CORE_TABLES:
            if not _table_exists(db, tname):
                print(f"\n  [SKIP] {tname} — not yet created")
                continue

            schema_df = db.execute(f'DESCRIBE "{tname}"').df()
            row_count = db.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
            null_pct  = _null_summary(db, tname, schema_df["column_name"].tolist())

            print(f"\n  {SEPARATOR}")
            print(f"  TABLE: {tname}   ({row_count:,} rows, {len(schema_df)} columns)")
            print(f"  {SEPARATOR}")
            print(f"  {'Column':<40} {'Type':<20} {'Nulls%':>8}  Note")
            print(f"  {'-'*40} {'-'*20} {'-'*8}  {'-'*25}")

            for _, col in schema_df.iterrows():
                cname = col["column_name"]
                ctype = col["column_type"]
                npct  = null_pct.get(cname, "?")
                note  = "◄ CustomerPR field" if cname.lower() in HIGHLIGHT_LOWER else ""
                print(f"  {cname:<40} {ctype:<20} {npct:>8}  {note}")

            # Sample rows
            sample = db.execute(f'SELECT * FROM "{tname}" LIMIT 3').df()
            if not sample.empty:
                print(f"\n  ── Sample (3 rows) ──")
                print(sample.to_string(index=False, max_colwidth=30))

        # ── CustomerPRNumber Probe ───────────────────────────
        _probe_customer_pr_number(db)

        # ── KPI Snapshot ────────────────────────────────────
        print(f"\n{SECTION}")
        print(f"  KPI Snapshot")
        print(SECTION)

        if _view_exists(db, "vw_dashboard_kpis"):
            kpis = db.execute("SELECT * FROM vw_dashboard_kpis").df()
            for col in kpis.columns:
                val = kpis[col].iloc[0]
                if isinstance(val, float):
                    print(f"  {col:<40} {val:>20,.2f}")
                else:
                    print(f"  {col:<40} {val:>20,}")
        else:
            print("  vw_dashboard_kpis not found — run a sync first.")

        # ── Status distribution ─────────────────────────────
        if _table_exists(db, "mart_dashboard_po_item"):
            print(f"\n  ── Status Distribution (mart_dashboard_po_item) ──")
            status_df = db.execute(
                """
                SELECT
                    dashboard_status,
                    COUNT(*) AS lines,
                    COUNT(DISTINCT purchase_order) AS pos,
                    ROUND(SUM(COALESCE(spend, 0)), 2) AS spend,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
                FROM mart_dashboard_po_item
                GROUP BY dashboard_status
                ORDER BY lines DESC
                """
            ).df()
            print(status_df.to_string(index=False))

            # Top 10 suppliers by spend
            print(f"\n  ── Top 10 Suppliers by Spend ──")
            top_sup = db.execute(
                """
                SELECT
                    supplier,
                    COUNT(DISTINCT purchase_order) AS pos,
                    COUNT(*) AS lines,
                    ROUND(SUM(COALESCE(spend, 0)), 2) AS total_spend
                FROM mart_dashboard_po_item
                WHERE supplier IS NOT NULL
                GROUP BY supplier
                ORDER BY total_spend DESC
                LIMIT 10
                """
            ).df()
            print(top_sup.to_string(index=False))

            # Monthly spend trend
            print(f"\n  ── Monthly Spend Trend ──")
            trend = db.execute(
                """
                SELECT
                    SUBSTR(CAST(creation_date AS VARCHAR), 1, 7) AS month,
                    COUNT(DISTINCT purchase_order) AS pos,
                    ROUND(SUM(COALESCE(spend, 0)), 2) AS spend
                FROM mart_dashboard_po_item
                WHERE creation_date IS NOT NULL AND creation_date <> ''
                GROUP BY month
                ORDER BY month DESC
                LIMIT 12
                """
            ).df()
            print(trend.to_string(index=False))

        # ── Sync history ────────────────────────────────────
        if _table_exists(db, "sync_runs"):
            print(f"\n{SECTION}")
            print(f"  Sync Run History (last 10)")
            print(SECTION)
            hist = db.execute(
                """
                SELECT
                    run_id,
                    started_at,
                    finished_at,
                    status,
                    CASE
                        WHEN status = 'success'
                        THEN SUBSTRING(message, 1, 120)
                        ELSE message
                    END AS summary
                FROM sync_runs
                ORDER BY started_at DESC
                LIMIT 10
                """
            ).df()
            print(hist.to_string(index=False, max_colwidth=60))

        # ── App status ──────────────────────────────────────
        if _table_exists(db, "app_status"):
            print(f"\n  ── App Status ──")
            app_st = db.execute("SELECT key, value FROM app_status").df()
            for _, r in app_st.iterrows():
                print(f"  {r['key']:<30} {r['value']}")


# ─────────────────────────────────────────────
# CustomerPRNumber Probe  (new)
# ─────────────────────────────────────────────
def _probe_customer_pr_number(db: duckdb.DuckDBPyConnection) -> None:
    """
    Checks every core table and mart for CustomerPRNumber / customer_pr_number,
    prints fill-rate stats and sample distinct values.
    """
    print(f"\n{SECTION}")
    print(f"  CustomerPRNumber Field Probe")
    print(SECTION)

    # (table_name, candidate_column_names_to_try)
    PR_TABLES = [
        ("raw_C_PurReqItemByPurOrder", ["CustomerPRNumber"]),
        ("raw_I_PurchaseOrderItem",    ["CustomerPRNumber"]),
        ("raw_I_PurchaseOrder",        ["CustomerPRNumber"]),
        ("fact_po_item",               ["customer_pr_number", "CustomerPRNumber"]),
        ("mart_dashboard_po_item",     ["customer_pr_number", "CustomerPRNumber"]),
    ]

    any_found = False

    for tname, candidates in PR_TABLES:
        if not _table_exists(db, tname):
            print(f"  [SKIP] {tname:<45} — table not found in DB")
            continue

        schema_cols_lower = (
            db.execute(f'DESCRIBE "{tname}"').df()["column_name"].str.lower().tolist()
        )

        # Find first matching candidate column
        matched_col: Optional[str] = None
        for c in candidates:
            if c.lower() in schema_cols_lower:
                matched_col = c
                break

        if matched_col is None:
            # Show all candidate names that were tried
            tried = ", ".join(f"'{c}'" for c in candidates)
            print(f"  [MISS] {tname:<45} — column not present (tried: {tried})")
            continue

        any_found = True

        # Fill-rate statistics
        stats = db.execute(f"""
            SELECT
                COUNT(*)                                                        AS total_rows,
                COUNT("{matched_col}")                                          AS non_null,
                COUNT(DISTINCT "{matched_col}")                                 AS distinct_vals,
                ROUND(100.0 * COUNT("{matched_col}") / NULLIF(COUNT(*), 0), 1) AS fill_pct
            FROM "{tname}"
        """).fetchone()

        total, non_null, distinct, fill = stats
        status_icon = "✔" if fill and fill > 0 else "⚠"

        print(
            f"  {status_icon} {tname:<45}  col='{matched_col}'"
            f"  total={total:>8,}  non_null={non_null:>8,}"
            f"  distinct={distinct:>6,}  fill={fill}%"
        )

        # Sample distinct non-null values
        samples = db.execute(f"""
            SELECT DISTINCT "{matched_col}"
            FROM "{tname}"
            WHERE "{matched_col}" IS NOT NULL
              AND TRIM(CAST("{matched_col}" AS VARCHAR)) <> ''
            ORDER BY "{matched_col}"
            LIMIT 8
        """).df()

        if not samples.empty:
            vals = "  |  ".join(str(v) for v in samples[matched_col].tolist())
            print(f"    Sample values : {vals}")
        else:
            print(f"    Sample values : (none — all NULL or empty)")

        # Cross-check: how many PO items have a CustomerPRNumber set
        if tname in ("raw_C_PurReqItemByPurOrder", "fact_po_item", "mart_dashboard_po_item"):
            po_col = _find_col(db, tname, ["purchase_order", "PurchaseOrder"])
            if po_col:
                cross = db.execute(f"""
                    SELECT
                        COUNT(DISTINCT "{po_col}") AS pos_with_pr
                    FROM "{tname}"
                    WHERE "{matched_col}" IS NOT NULL
                      AND TRIM(CAST("{matched_col}" AS VARCHAR)) <> ''
                """).fetchone()[0]
                print(f"    POs with CustomerPRNumber set: {cross:,}")

    if not any_found:
        print(
            "\n  ⚠  CustomerPRNumber was NOT found in any table.\n"
            "     Possible reasons:\n"
            "     • The SAP entity C_PurReqItemByPurOrder does not expose this field\n"
            "     • The ETL pipeline drops or renames it — check your field mappings\n"
            "     • A sync has not been run yet (tables are empty / missing)\n"
        )
    else:
        print(
            f"\n  Tip: If fill_pct is low, verify that CustomerPRNumber is populated\n"
            f"  in SAP for the relevant PO items, or check ETL field mapping in\n"
            f"  your sync / transformation layer."
        )


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def _table_exists(db: duckdb.DuckDBPyConnection, name: str) -> bool:
    n = db.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE lower(table_name)=lower(?)",
        [name],
    ).fetchone()[0]
    return bool(n)


def _view_exists(db: duckdb.DuckDBPyConnection, name: str) -> bool:
    n = db.execute(
        "SELECT COUNT(*) FROM information_schema.views WHERE lower(table_name)=lower(?)",
        [name],
    ).fetchone()[0]
    return bool(n)


def _find_col(
    db: duckdb.DuckDBPyConnection,
    table: str,
    candidates: List[str],
) -> Optional[str]:
    """Return the first candidate column name that exists in `table`, or None."""
    existing = db.execute(f'DESCRIBE "{table}"').df()["column_name"].str.lower().tolist()
    for c in candidates:
        if c.lower() in existing:
            return c
    return None


def _null_summary(
    db: duckdb.DuckDBPyConnection, table: str, columns: List[str]
) -> Dict[str, str]:
    """Return null % per column as a dict."""
    exprs = ", ".join(
        f'ROUND(100.0 * COUNT(*) FILTER (WHERE "{c}" IS NULL) / NULLIF(COUNT(*),0), 1) AS "{c}"'
        for c in columns
    )
    try:
        row = db.execute(f'SELECT {exprs} FROM "{table}"').fetchone()
        return {c: f"{v}%" if v is not None else "?" for c, v in zip(columns, row)}
    except Exception:
        return {}


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
def main() -> None:
    print(f"\n{SECTION}")
    print(f"  ProDash Inspector  —  {Path(DB_PATH).name}")
    print(SECTION)

    # 1. SAP metadata
    try:
        conns      = load_credentials()
        first_conn = next(iter(conns.values()))
        inspect_sap_metadata(first_conn)
    except Exception as e:
        print(f"\n[WARNING] SAP inspection skipped: {e}")

    # 2. DuckDB
    inspect_duckdb()

    print(f"\n{SECTION}")
    print(f"  Inspection complete.")
    print(SECTION)


if __name__ == "__main__":
    main()