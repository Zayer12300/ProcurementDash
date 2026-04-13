# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
transformations.py  —  ProDash ETL Transform Layer v2.3
=========================================================
Changes from v2.2:
  - customer_po  ← CorrespncExternalReference (45-series, PO Header)
  - customer_pr  ← PurchaseRequisition        (50-series, PO Item)
  - Both fields indexed in mart_dashboard_po_item for fast search
  - vw_overdue_pos exposes customer_po alongside customer_pr
  - All COALESCE / numeric expressions use TRY_CAST / explicit CAST
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import duckdb

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────
_SRC_DIR      = Path(__file__).resolve().parent
_PROJECT_ROOT = _SRC_DIR.parent
DB_PATH       = _PROJECT_ROOT / "data" / "prodash.duckdb"

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ProDash.Transforms")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _exists(db: duckdb.DuckDBPyConnection, name: str, kind: str = "table") -> bool:
    schema = "views" if kind == "view" else "tables"
    return bool(db.execute(
        f"SELECT COUNT(*) FROM information_schema.{schema} "
        f"WHERE lower(table_name) = lower(?)", [name],
    ).fetchone()[0])


def _cols(db: duckdb.DuckDBPyConnection, table: str) -> set:
    if not _exists(db, table):
        return set()
    return {r[1] for r in db.execute(f"PRAGMA table_info('{table}')").fetchall()}


def _pick(cols: set, candidates: list) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


def _col_expr(col: Optional[str], alias: str, prefix: str = "", cast: str = "") -> str:
    if col is None:
        return f"NULL AS {alias}"
    ref = f"{prefix}.{col}" if prefix else col
    if cast:
        return f"TRY_CAST({ref} AS {cast}) AS {alias}"
    return f"{ref} AS {alias}"


def _assert_required(db: duckdb.DuckDBPyConnection) -> None:
    required = ["raw_I_PurchaseOrder", "raw_I_PurchaseOrderItem"]
    tables   = {r[0] for r in db.execute("SHOW TABLES").fetchall()}
    missing  = [t for t in required if t not in tables]
    if missing:
        raise RuntimeError(
            "Required raw tables not found:\n"
            + "\n".join(f"  ✘ {t}" for t in missing)
            + "\n\nRun POST /api/sync first."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_transformations(db: duckdb.DuckDBPyConnection) -> dict:
    try:
        db.execute("PRAGMA threads=4")
        db.execute("PRAGMA memory_limit='1GB'")
    except Exception:
        pass

    _assert_required(db)

    logger.info("Step 1/3 — Building fact_po_item …")
    _build_fact_po_item(db)
    fact_rows = db.execute("SELECT COUNT(*) FROM fact_po_item").fetchone()[0]
    logger.info("  fact_po_item: %s rows", f"{fact_rows:,}")

    logger.info("Step 2/3 — Building mart_dashboard_po_item …")
    _build_mart(db)
    mart_rows = db.execute("SELECT COUNT(*) FROM mart_dashboard_po_item").fetchone()[0]
    logger.info("  mart_dashboard_po_item: %s rows", f"{mart_rows:,}")

    logger.info("Step 3/3 — Building analytical views …")
    _build_views(db)
    logger.info(
        "  Views: vw_dashboard_kpis, vw_spend_by_supplier, "
        "vw_spend_by_month, vw_spend_by_material_group, "
        "vw_delivery_performance, vw_overdue_pos, "
        "vw_spend_by_plant, vw_supplier_risk_tiers"
    )

    return {
        "fact_po_item":           fact_rows,
        "mart_dashboard_po_item": mart_rows,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — fact_po_item
# ─────────────────────────────────────────────────────────────────────────────

def _build_fact_po_item(db: duckdb.DuckDBPyConnection) -> None:

    po_cols   = _cols(db, "raw_I_PurchaseOrder")
    item_cols = _cols(db, "raw_I_PurchaseOrderItem")
    gr_cols   = _cols(db, "raw_C_PurchaseOrderGoodsReceipt")
    asn_cols  = _cols(db, "raw_C_PurOrdSuplrConfDisplay")
    pr_cols   = _cols(db, "raw_C_PurReqItemByPurOrder")

    # ── PO header ────────────────────────────────────────────────────────────
    po_supplier      = _pick(po_cols, ["Supplier"])
    po_creation_date = _pick(po_cols, ["CreationDate", "PurchaseOrderDate"])
    po_currency      = _pick(po_cols, ["DocumentCurrency"])
    po_customer_po   = _pick(po_cols, ["CorrespncExternalReference"])   # 45-series
    po_status        = _pick(po_cols, ["PurchasingProcessingStatus"])
    po_type          = _pick(po_cols, ["PurchaseOrderType"])
    po_purch_org     = _pick(po_cols, ["PurchasingOrganization"])
    po_purch_grp     = _pick(po_cols, ["PurchasingGroup"])
    po_company_code  = _pick(po_cols, ["CompanyCode"])
    po_rel_status    = _pick(po_cols, ["PurgReleaseSequenceStatus"])
    po_del_code      = _pick(po_cols, ["PurchasingDocumentDeletionCode"])
    po_payment_terms = _pick(po_cols, ["PaymentTerms"])
    po_created_by    = _pick(po_cols, ["CreatedByUser"])

    if po_customer_po:
        logger.info("  ✔ customer_po field found: %s", po_customer_po)
    else:
        logger.warning(
            "  ⚠ CorrespncExternalReference not found in raw_I_PurchaseOrder "
            "— customer_po will be NULL. Check ENTITY_SELECT in main.py."
        )

    # ── PO item ──────────────────────────────────────────────────────────────
    item_text        = _pick(item_cols, ["PurchaseOrderItemText"])
    item_material    = _pick(item_cols, ["Material"])
    item_mat_grp     = _pick(item_cols, ["MaterialGroup"])
    item_mat_type    = _pick(item_cols, ["MaterialType"])
    item_qty         = _pick(item_cols, ["OrderQuantity"])
    item_price       = _pick(item_cols, ["NetPriceAmount"])
    item_qty_unit    = _pick(item_cols, ["PurchaseOrderQuantityUnit", "BaseUnit"])
    item_plant       = _pick(item_cols, ["Plant"])
    item_sloc        = _pick(item_cols, ["StorageLocation"])
    item_acct_cat    = _pick(item_cols, ["AccountAssignmentCategory"])
    item_cost_ctr    = _pick(item_cols, ["CostCenter"])
    item_gl_acct     = _pick(item_cols, ["GLAccount"])
    item_wbs         = _pick(item_cols, ["WBSElementInternalID"])
    item_profit_ctr  = _pick(item_cols, ["ProfitCenter"])
    item_reqr_name   = _pick(item_cols, ["RequisitionerName"])
    item_pld_days    = _pick(item_cols, ["PlannedDeliveryDurationInDays"])
    item_final_inv   = _pick(item_cols, ["IsFinallyInvoiced"])
    item_completely  = _pick(item_cols, ["IsCompletelyDelivered"])
    item_gr_exp      = _pick(item_cols, ["GoodsReceiptIsExpected"])
    item_inv_exp     = _pick(item_cols, ["InvoiceIsExpected"])
    item_category    = _pick(item_cols, ["PurchaseOrderItemCategory"])
    item_net_amt     = _pick(item_cols, ["NetAmount"])
    item_purch_pr    = _pick(item_cols, ["PurchaseRequisition"])        # 50-series
    item_purch_pr_i  = _pick(item_cols, ["PurchaseRequisitionItem"])
    item_del_code    = _pick(item_cols, ["PurchasingDocumentDeletionCode"])

    if item_purch_pr:
        logger.info("  ✔ customer_pr field found: %s", item_purch_pr)
    else:
        logger.warning(
            "  ⚠ PurchaseRequisition not found in raw_I_PurchaseOrderItem "
            "— customer_pr will be NULL. Check ENTITY_SELECT in main.py."
        )

    # ── GR ───────────────────────────────────────────────────────────────────
    gr_po_col    = _pick(gr_cols, ["PurchaseOrder"])
    gr_item_col  = _pick(gr_cols, ["PurchaseOrderItem"])
    gr_qty_col   = _pick(gr_cols, ["QuantityInBaseUnit", "QuantityInEntryUnit"])
    gr_post_dt   = _pick(gr_cols, ["PostingDate"])
    gr_mov_type  = _pick(gr_cols, ["GoodsMovementType"])
    gr_cancelled = _pick(gr_cols, ["GoodsMovementIsCancelled"])

    # ── ASN ──────────────────────────────────────────────────────────────────
    asn_po_col   = _pick(asn_cols, ["PurchaseOrder"])
    asn_item_col = _pick(asn_cols, ["PurchaseOrderItem"])
    asn_qty_col  = _pick(asn_cols, ["ConfirmedQuantity"])
    asn_delv_dt  = _pick(asn_cols, ["DeliveryDate"])

    # ── PR entity ─────────────────────────────────────────────────────────────
    pr_po_col      = _pick(pr_cols, ["PurchaseOrder"])
    pr_pr_col      = _pick(pr_cols, ["PurchaseRequisition"])
    pr_pri_col     = _pick(pr_cols, ["PurchaseRequisitionItem"])
    pr_del_col     = _pick(pr_cols, ["DeliveryDate"])
    pr_mat_grp_txt = _pick(pr_cols, ["MaterialGroup_Text"])

    # ── Supplier name ─────────────────────────────────────────────────────────
    sup_join      = ""
    sup_name_expr = "NULL AS supplier_name"
    if _exists(db, "raw_I_Supplier") and po_supplier:
        sup_cols     = _cols(db, "raw_I_Supplier")
        sup_name_col = _pick(sup_cols, ["SupplierName", "OrganizationBPName1"])
        if sup_name_col:
            sup_join      = (
                f"LEFT JOIN raw_I_Supplier ds "
                f"ON h.{po_supplier} = ds.Supplier"
            )
            sup_name_expr = f"ds.{sup_name_col} AS supplier_name"

    # ── Material name ─────────────────────────────────────────────────────────
    if item_text and item_material:
        mat_name_expr = (
            f"COALESCE(i.{item_text}, i.{item_material}) AS material_name"
        )
    elif item_material:
        mat_name_expr = f"i.{item_material} AS material_name"
    else:
        mat_name_expr = "NULL AS material_name"

    mat_join = ""
    if _exists(db, "raw_I_Material") and item_material:
        mat_cols     = _cols(db, "raw_I_Material")
        mat_desc_col = _pick(mat_cols, ["MaterialName"])
        if mat_desc_col:
            mat_join = f"""
        LEFT JOIN (
            SELECT Material, {mat_desc_col} AS mat_desc
            FROM raw_I_Material
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY Material ORDER BY {mat_desc_col}
            ) = 1
        ) dm ON i.{item_material} = dm.Material"""
            src = (
                f"COALESCE(dm.mat_desc, i.{item_text})"
                if item_text else "dm.mat_desc"
            )
            mat_name_expr = f"{src} AS material_name"

    # ── GR subquery ───────────────────────────────────────────────────────────
    gr_join      = ""
    gr_qty_expr  = "CAST(0.0 AS DOUBLE) AS gr_qty"
    gr_date_expr = "NULL AS last_gr_date"

    if (all([gr_po_col, gr_item_col, gr_qty_col])
            and _exists(db, "raw_C_PurchaseOrderGoodsReceipt")):
        cancelled_clause = (
            f"AND (g.{gr_cancelled} IS NULL OR g.{gr_cancelled} = FALSE)"
            if gr_cancelled else ""
        )
        mov_filter   = (
            f"AND g.{gr_mov_type} NOT IN ('102','122')"
            if gr_mov_type else ""
        )
        post_dt_expr = f"MAX(g.{gr_post_dt})" if gr_post_dt else "NULL"
        gr_join = f"""
        LEFT JOIN (
            SELECT
                g.{gr_po_col}   AS _gr_po,
                g.{gr_item_col} AS _gr_po_item,
                SUM(TRY_CAST(g.{gr_qty_col} AS DOUBLE)) AS _gr_qty,
                {post_dt_expr}                           AS _last_gr_date
            FROM raw_C_PurchaseOrderGoodsReceipt g
            WHERE 1=1
            {mov_filter}
            {cancelled_clause}
            GROUP BY g.{gr_po_col}, g.{gr_item_col}
        ) gr ON i.PurchaseOrder      = gr._gr_po
             AND i.PurchaseOrderItem  = gr._gr_po_item"""
        gr_qty_expr  = "COALESCE(gr._gr_qty, CAST(0.0 AS DOUBLE)) AS gr_qty"
        gr_date_expr = "gr._last_gr_date AS last_gr_date"

    # ── ASN subquery ──────────────────────────────────────────────────────────
    asn_join      = ""
    asn_qty_expr  = "CAST(0.0 AS DOUBLE) AS asn_qty"
    asn_date_expr = "NULL AS earliest_asn_delivery_date"

    if (all([asn_po_col, asn_item_col, asn_qty_col])
            and _exists(db, "raw_C_PurOrdSuplrConfDisplay")):
        delv_expr = f"MIN(a.{asn_delv_dt})" if asn_delv_dt else "NULL"
        asn_join = f"""
        LEFT JOIN (
            SELECT
                a.{asn_po_col}   AS _asn_po,
                a.{asn_item_col} AS _asn_po_item,
                SUM(TRY_CAST(a.{asn_qty_col} AS DOUBLE)) AS _asn_qty,
                {delv_expr}                               AS _asn_delivery_date
            FROM raw_C_PurOrdSuplrConfDisplay a
            GROUP BY a.{asn_po_col}, a.{asn_item_col}
        ) asn ON i.PurchaseOrder      = asn._asn_po
              AND i.PurchaseOrderItem  = asn._asn_po_item"""
        asn_qty_expr  = "COALESCE(asn._asn_qty, CAST(0.0 AS DOUBLE)) AS asn_qty"
        asn_date_expr = "asn._asn_delivery_date AS earliest_asn_delivery_date"

    # ── PR subquery ───────────────────────────────────────────────────────────
    pr_join          = ""
    pr_pr_expr       = (
        "i.PurchaseRequisition      AS customer_pr"
        if item_purch_pr else "NULL AS customer_pr"
    )
    pr_pri_expr      = (
        "i.PurchaseRequisitionItem  AS customer_pr_item"
        if item_purch_pr_i else "NULL AS customer_pr_item"
    )
    pr_del_date_expr = "NULL AS pr_requested_delivery_date"
    pr_mat_grp_expr  = "NULL AS material_group_text"

    if (all([pr_po_col, pr_pr_col, pr_pri_col])
            and _exists(db, "raw_C_PurReqItemByPurOrder")):
        del_col_expr = f"MIN(pr.{pr_del_col})"     if pr_del_col     else "NULL"
        grp_txt_expr = f"MIN(pr.{pr_mat_grp_txt})" if pr_mat_grp_txt else "NULL"
        pr_join = f"""
        LEFT JOIN (
            SELECT
                pr.{pr_po_col}  AS _pr_po,
                pr.{pr_pr_col}  AS _pr_pr,
                pr.{pr_pri_col} AS _pr_pri,
                {del_col_expr}  AS _pr_del_date,
                {grp_txt_expr}  AS _pr_mat_grp_txt
            FROM raw_C_PurReqItemByPurOrder pr
            GROUP BY pr.{pr_po_col}, pr.{pr_pr_col}, pr.{pr_pri_col}
        ) pr
          ON i.PurchaseOrder           = pr._pr_po
         AND i.PurchaseRequisition     = pr._pr_pr
         AND i.PurchaseRequisitionItem = pr._pr_pri"""
        pr_pr_expr = (
            "COALESCE(i.PurchaseRequisition,     pr._pr_pr)  AS customer_pr"
            if item_purch_pr else "pr._pr_pr  AS customer_pr"
        )
        pr_pri_expr = (
            "COALESCE(i.PurchaseRequisitionItem, pr._pr_pri) AS customer_pr_item"
            if item_purch_pr_i else "pr._pr_pri AS customer_pr_item"
        )
        pr_del_date_expr = "pr._pr_del_date    AS pr_requested_delivery_date"
        pr_mat_grp_expr  = "pr._pr_mat_grp_txt AS material_group_text"

    # ── Derived expressions ───────────────────────────────────────────────────
    qty_ref    = f"TRY_CAST(i.{item_qty}   AS DOUBLE)" if item_qty   else "NULL"
    price_ref  = f"TRY_CAST(i.{item_price} AS DOUBLE)" if item_price else "NULL"
    spend_expr = (
        f"({qty_ref} * {price_ref})"
        if item_qty and item_price else "CAST(NULL AS DOUBLE)"
    )
    del_code_h = (
        f"COALESCE(CAST(h.{po_del_code} AS VARCHAR), '')"
        if po_del_code else "''"
    )
    del_code_i = (
        f"COALESCE(CAST(i.{item_del_code} AS VARCHAR), '')"
        if item_del_code else "''"
    )
    completely = (
        f"i.{item_completely} = TRUE" if item_completely else "FALSE"
    )
    qty_expr  = qty_ref if item_qty else "CAST(0 AS DOUBLE)"
    days_expr = (
        f"DATE_DIFF('day', TRY_CAST(h.{po_creation_date} AS DATE), "
        f"TRY_CAST(asn._asn_delivery_date AS DATE))"
        if po_creation_date and asn_delv_dt else "CAST(NULL AS INTEGER)"
    )

    # ── BUILD FACT TABLE ──────────────────────────────────────────────────────
    db.execute("DROP TABLE IF EXISTS fact_po_item")
    db.execute(f"""
    CREATE TABLE fact_po_item AS
    SELECT
        -- ── Keys ──────────────────────────────────────────────────────────
        i.PurchaseOrder                                                 AS purchase_order,
        i.PurchaseOrderItem                                             AS purchase_order_item,

        -- ── Customer Reference Fields ──────────────────────────────────────
        {_col_expr(po_customer_po,   'customer_po',            'h')},  -- 45-series (Header)
        {pr_pr_expr},                                                   -- 50-series (Item)
        {pr_pri_expr},
        {pr_del_date_expr},
        {pr_mat_grp_expr},

        -- ── PO Header ─────────────────────────────────────────────────────
        {_col_expr(po_type,          'po_type',                'h')},
        {_col_expr(po_purch_org,     'purchasing_org',         'h')},
        {_col_expr(po_purch_grp,     'purchasing_group',       'h')},
        {_col_expr(po_company_code,  'company_code',           'h')},
        {_col_expr(po_supplier,      'supplier',               'h')},
        {sup_name_expr},
        {_col_expr(po_creation_date, 'creation_date',          'h')},
        {_col_expr(po_currency,      'document_currency',      'h')},
        {_col_expr(po_status,        'po_processing_status',   'h')},
        {_col_expr(po_rel_status,    'release_status',         'h')},
        {_col_expr(po_del_code,      'po_deletion_code',       'h')},
        {_col_expr(po_payment_terms, 'payment_terms',          'h')},
        {_col_expr(po_created_by,    'created_by',             'h')},

        -- ── PO Item ───────────────────────────────────────────────────────
        {_col_expr(item_material,    'material',               'i')},
        {mat_name_expr},
        {_col_expr(item_mat_grp,     'material_group',         'i')},
        {_col_expr(item_mat_type,    'material_type',          'i')},
        {_col_expr(item_plant,       'plant',                  'i')},
        {_col_expr(item_sloc,        'storage_location',       'i')},
        {_col_expr(item_acct_cat,    'account_assignment_cat', 'i')},
        {_col_expr(item_cost_ctr,    'cost_center',            'i')},
        {_col_expr(item_gl_acct,     'gl_account',             'i')},
        {_col_expr(item_wbs,         'wbs_element',            'i')},
        {_col_expr(item_profit_ctr,  'profit_center',          'i')},
        {_col_expr(item_reqr_name,   'requisitioner',          'i')},
        {_col_expr(item_category,    'item_category',          'i')},
        {_col_expr(item_purch_pr,    'purchase_requisition',   'i')},
        {_col_expr(item_purch_pr_i,  'purchase_requisition_item', 'i')},
        {_col_expr(item_del_code,    'item_deletion_code',     'i')},

        -- ── Quantities & Pricing ──────────────────────────────────────────
        {_col_expr(item_qty,         'order_qty',              'i', 'DOUBLE')},
        {_col_expr(item_qty_unit,    'order_qty_unit',         'i')},
        {_col_expr(item_price,       'net_price',              'i', 'DOUBLE')},
        {_col_expr(item_net_amt,     'net_amount',             'i', 'DOUBLE')},
        {spend_expr}                                                    AS spend,

        -- ── Delivery Flags ────────────────────────────────────────────────
        {_col_expr(item_completely,  'is_completely_delivered','i')},
        {_col_expr(item_final_inv,   'is_finally_invoiced',   'i')},
        {_col_expr(item_gr_exp,      'gr_is_expected',         'i')},
        {_col_expr(item_inv_exp,     'invoice_is_expected',    'i')},
        {_col_expr(item_pld_days,    'planned_delivery_days',  'i', 'INTEGER')},

        -- ── ASN / GR ──────────────────────────────────────────────────────
        {asn_qty_expr},
        {asn_date_expr},
        {gr_qty_expr},
        {gr_date_expr},

        -- ── System Status ─────────────────────────────────────────────────
        CASE
            WHEN {del_code_h} <> '' OR {del_code_i} <> ''
                THEN 'Cancelled'
            WHEN {completely}
              OR (COALESCE(gr._gr_qty, CAST(0.0 AS DOUBLE)) >= {qty_expr}
                  AND {qty_expr} > 0)
                THEN 'Fully Delivered'
            WHEN COALESCE(gr._gr_qty, CAST(0.0 AS DOUBLE)) > 0
                THEN 'Partially Delivered'
            WHEN COALESCE(asn._asn_qty, CAST(0.0 AS DOUBLE)) >= {qty_expr}
                 AND {qty_expr} > 0
                THEN 'Shipped (Full)'
            WHEN COALESCE(asn._asn_qty, CAST(0.0 AS DOUBLE)) > 0
                THEN 'Shipped (Partial)'
            ELSE 'PO Created'
        END                                                             AS system_status,

        {days_expr}                                                     AS days_po_to_asn

    FROM raw_I_PurchaseOrderItem i
    LEFT JOIN raw_I_PurchaseOrder h ON i.PurchaseOrder = h.PurchaseOrder
    {pr_join}
    {asn_join}
    {gr_join}
    {sup_join}
    {mat_join}
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — mart_dashboard_po_item
# ─────────────────────────────────────────────────────────────────────────────

def _build_mart(db: duckdb.DuckDBPyConnection) -> None:
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

    db.execute("DROP TABLE IF EXISTS mart_dashboard_po_item")
    db.execute("""
    CREATE TABLE mart_dashboard_po_item AS
    SELECT
        f.*,
        COALESCE(o.status_override, f.system_status) AS dashboard_status,
        o.owner,
        o.priority,
        o.note,
        o.updated_at AS override_updated_at
    FROM fact_po_item f
    LEFT JOIN app_po_item_overrides o
      ON f.purchase_order      = o.purchase_order
     AND f.purchase_order_item = o.purchase_order_item
    """)

    # ── Indexes — includes customer_po and customer_pr for fast search ────────
    for idx in [
        "CREATE INDEX IF NOT EXISTS idx_mart_status      ON mart_dashboard_po_item(dashboard_status)",
        "CREATE INDEX IF NOT EXISTS idx_mart_po          ON mart_dashboard_po_item(purchase_order)",
        "CREATE INDEX IF NOT EXISTS idx_mart_supplier    ON mart_dashboard_po_item(supplier)",
        "CREATE INDEX IF NOT EXISTS idx_mart_plant       ON mart_dashboard_po_item(plant)",
        "CREATE INDEX IF NOT EXISTS idx_mart_date        ON mart_dashboard_po_item(creation_date)",
        "CREATE INDEX IF NOT EXISTS idx_mart_matgrp      ON mart_dashboard_po_item(material_group)",
        "CREATE INDEX IF NOT EXISTS idx_mart_purch_org   ON mart_dashboard_po_item(purchasing_org)",
        "CREATE INDEX IF NOT EXISTS idx_mart_customer_po ON mart_dashboard_po_item(customer_po)",   # 45-series
        "CREATE INDEX IF NOT EXISTS idx_mart_customer_pr ON mart_dashboard_po_item(customer_pr)",   # 50-series
    ]:
        try:
            db.execute(idx)
        except Exception:
            pass

    # ── Post-build fill-rate log for both reference fields ───────────────────
    for col, label in [("customer_po", "45-series"), ("customer_pr", "50-series")]:
        try:
            stats = db.execute(f"""
                SELECT
                    COUNT(*)                                                        AS total,
                    COUNT("{col}")                                                  AS non_null,
                    ROUND(100.0 * COUNT("{col}") / NULLIF(COUNT(*), 0), 1)         AS fill_pct
                FROM mart_dashboard_po_item
            """).fetchone()
            logger.info(
                "  %s (%s): %s / %s rows populated (%.1f%%)",
                col, label, f"{stats[1]:,}", f"{stats[0]:,}", stats[2] or 0.0,
            )
        except Exception:
            logger.warning("  Could not compute fill-rate for %s", col)


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Analytical views
# ─────────────────────────────────────────────────────────────────────────────

def _build_views(db: duckdb.DuckDBPyConnection) -> None:

    # ── KPIs ──────────────────────────────────────────────────────────────────
    db.execute("DROP VIEW IF EXISTS vw_dashboard_kpis")
    db.execute("""
    CREATE VIEW vw_dashboard_kpis AS
    SELECT
        COUNT(*)                                                                AS total_lines,
        COUNT(DISTINCT purchase_order)                                          AS total_pos,
        COUNT(DISTINCT supplier)                                                AS active_suppliers,
        COUNT(DISTINCT plant)                                                   AS active_plants,
        ROUND(SUM(COALESCE(TRY_CAST(spend   AS DOUBLE), 0.0)), 2)               AS total_estimated_spend,
        COUNT(*) FILTER (WHERE dashboard_status = 'Fully Delivered')            AS fully_delivered_lines,
        COUNT(*) FILTER (WHERE dashboard_status = 'Partially Delivered')        AS partially_delivered_lines,
        COUNT(*) FILTER (WHERE dashboard_status = 'Shipped (Full)')             AS shipped_full_lines,
        COUNT(*) FILTER (WHERE dashboard_status = 'Shipped (Partial)')          AS shipped_partial_lines,
        COUNT(*) FILTER (WHERE dashboard_status = 'PO Created')                 AS open_lines,
        COUNT(*) FILTER (WHERE dashboard_status = 'Cancelled')                  AS cancelled_lines,
        ROUND(
            100.0
            * COUNT(*) FILTER (WHERE dashboard_status = 'Fully Delivered')
            / NULLIF(COUNT(*) FILTER (WHERE dashboard_status <> 'Cancelled'), 0),
        1)                                                                      AS fulfillment_rate_pct,
        ROUND(SUM(COALESCE(TRY_CAST(gr_qty  AS DOUBLE), 0.0)), 2)               AS total_gr_qty,
        ROUND(SUM(COALESCE(TRY_CAST(asn_qty AS DOUBLE), 0.0)), 2)               AS total_asn_qty,
        -- Customer reference coverage KPIs
        COUNT(customer_po)                                                      AS pos_with_customer_po,
        COUNT(customer_pr)                                                      AS lines_with_customer_pr,
        ROUND(100.0 * COUNT(customer_po) / NULLIF(COUNT(*), 0), 1)              AS customer_po_fill_pct,
        ROUND(100.0 * COUNT(customer_pr) / NULLIF(COUNT(*), 0), 1)              AS customer_pr_fill_pct
    FROM mart_dashboard_po_item
    WHERE COALESCE(CAST(po_deletion_code AS VARCHAR), '') = ''
    """)

    # ── Spend by supplier ─────────────────────────────────────────────────────
    db.execute("DROP VIEW IF EXISTS vw_spend_by_supplier")
    db.execute("""
    CREATE VIEW vw_spend_by_supplier AS
    SELECT
        supplier,
        supplier_name,
        purchasing_org,
        COUNT(DISTINCT purchase_order)                                          AS pos,
        COUNT(*)                                                                AS lines,
        ROUND(SUM(COALESCE(TRY_CAST(spend AS DOUBLE), 0.0)), 2)                 AS total_spend,
        ROUND(
            100.0 * SUM(COALESCE(TRY_CAST(spend AS DOUBLE), 0.0))
            / NULLIF(SUM(SUM(COALESCE(TRY_CAST(spend AS DOUBLE), 0.0))) OVER (), 0.0),
        2)                                                                      AS spend_share_pct,
        COUNT(*) FILTER (WHERE dashboard_status = 'Fully Delivered')            AS delivered_lines,
        COUNT(*) FILTER (WHERE dashboard_status = 'PO Created')                 AS open_lines,
        ROUND(AVG(TRY_CAST(days_po_to_asn AS DOUBLE)), 1)                       AS avg_days_to_asn
    FROM mart_dashboard_po_item
    WHERE COALESCE(CAST(po_deletion_code AS VARCHAR), '') = ''
    GROUP BY supplier, supplier_name, purchasing_org
    ORDER BY total_spend DESC
    """)

    # ── Spend by month ────────────────────────────────────────────────────────
    db.execute("DROP VIEW IF EXISTS vw_spend_by_month")
    db.execute("""
    CREATE VIEW vw_spend_by_month AS
    SELECT
        SUBSTR(CAST(creation_date AS VARCHAR), 1, 7)                            AS year_month,
        purchasing_org,
        COUNT(DISTINCT purchase_order)                                          AS pos,
        COUNT(*)                                                                AS lines,
        ROUND(SUM(COALESCE(TRY_CAST(spend  AS DOUBLE), 0.0)), 2)                AS spend,
        ROUND(SUM(COALESCE(TRY_CAST(gr_qty AS DOUBLE), 0.0)), 2)                AS gr_qty,
        COUNT(*) FILTER (WHERE dashboard_status = 'PO Created')                 AS open_lines
    FROM mart_dashboard_po_item
    WHERE creation_date IS NOT NULL
      AND CAST(creation_date AS VARCHAR) <> ''
      AND COALESCE(CAST(po_deletion_code AS VARCHAR), '') = ''
    GROUP BY year_month, purchasing_org
    ORDER BY year_month DESC
    """)

    # ── Spend by material group ───────────────────────────────────────────────
    db.execute("DROP VIEW IF EXISTS vw_spend_by_material_group")
    db.execute("""
    CREATE VIEW vw_spend_by_material_group AS
    SELECT
        material_group,
        material_group_text,
        COUNT(DISTINCT purchase_order)                                          AS pos,
        COUNT(*)                                                                AS lines,
        ROUND(SUM(COALESCE(TRY_CAST(spend AS DOUBLE), 0.0)), 2)                 AS total_spend,
        COUNT(DISTINCT supplier)                                                AS supplier_count,
        COUNT(DISTINCT plant)                                                   AS plant_count
    FROM mart_dashboard_po_item
    WHERE COALESCE(CAST(po_deletion_code AS VARCHAR), '') = ''
    GROUP BY material_group, material_group_text
    ORDER BY total_spend DESC
    """)

    # ── Delivery performance ──────────────────────────────────────────────────
    db.execute("DROP VIEW IF EXISTS vw_delivery_performance")
    db.execute("""
    CREATE VIEW vw_delivery_performance AS
    SELECT
        supplier,
        supplier_name,
        plant,
        COUNT(*)                                                                AS total_lines,
        COUNT(*) FILTER (WHERE dashboard_status = 'Fully Delivered')            AS delivered_lines,
        COUNT(*) FILTER (WHERE dashboard_status = 'PO Created')                 AS pending_lines,
        ROUND(
            100.0
            * COUNT(*) FILTER (WHERE dashboard_status = 'Fully Delivered')
            / NULLIF(COUNT(*), 0),
        1)                                                                      AS delivery_rate_pct,
        ROUND(AVG(TRY_CAST(days_po_to_asn        AS DOUBLE)), 1)                AS avg_days_to_asn,
        ROUND(AVG(TRY_CAST(planned_delivery_days AS DOUBLE)), 1)                AS avg_planned_days,
        ROUND(
            AVG(TRY_CAST(days_po_to_asn        AS DOUBLE))
          - AVG(TRY_CAST(planned_delivery_days AS DOUBLE)),
        1)                                                                      AS avg_delay_days
    FROM mart_dashboard_po_item
    WHERE COALESCE(CAST(po_deletion_code AS VARCHAR), '') = ''
    GROUP BY supplier, supplier_name, plant
    HAVING COUNT(*) >= 5
    ORDER BY avg_delay_days DESC NULLS LAST
    """)

    # ── Overdue open POs — now includes customer_po and customer_pr ───────────
    db.execute("DROP VIEW IF EXISTS vw_overdue_pos")
    db.execute("""
    CREATE VIEW vw_overdue_pos AS
    SELECT
        purchase_order,
        purchase_order_item,
        customer_po,                                                            -- 45-series
        customer_pr,                                                            -- 50-series
        customer_pr_item,
        supplier,
        supplier_name,
        plant,
        material,
        material_name,
        creation_date,
        TRY_CAST(order_qty             AS DOUBLE)                               AS order_qty,
        order_qty_unit,
        TRY_CAST(asn_qty               AS DOUBLE)                               AS asn_qty,
        TRY_CAST(gr_qty                AS DOUBLE)                               AS gr_qty,
        dashboard_status,
        earliest_asn_delivery_date,
        TRY_CAST(planned_delivery_days AS INTEGER)                              AS planned_delivery_days,
        DATEDIFF('day', TRY_CAST(creation_date AS DATE), CURRENT_DATE)          AS age_days,
        owner,
        priority,
        note
    FROM mart_dashboard_po_item
    WHERE dashboard_status NOT IN ('Fully Delivered', 'Cancelled')
      AND COALESCE(CAST(po_deletion_code   AS VARCHAR), '') = ''
      AND COALESCE(CAST(item_deletion_code AS VARCHAR), '') = ''
      AND DATEDIFF('day', TRY_CAST(creation_date AS DATE), CURRENT_DATE)
          > COALESCE(TRY_CAST(planned_delivery_days AS INTEGER), 30)
    ORDER BY age_days DESC
    """)

    # ── Spend by plant ────────────────────────────────────────────────────────
    db.execute("DROP VIEW IF EXISTS vw_spend_by_plant")
    db.execute("""
    CREATE VIEW vw_spend_by_plant AS
    SELECT
        plant,
        purchasing_org,
        COUNT(DISTINCT purchase_order)                                          AS pos,
        COUNT(*)                                                                AS lines,
        ROUND(SUM(COALESCE(TRY_CAST(spend AS DOUBLE), 0.0)), 2)                 AS total_spend,
        COUNT(DISTINCT supplier)                                                AS supplier_count,
        COUNT(*) FILTER (WHERE dashboard_status = 'PO Created')                 AS open_lines
    FROM mart_dashboard_po_item
    WHERE COALESCE(CAST(po_deletion_code AS VARCHAR), '') = ''
    GROUP BY plant, purchasing_org
    ORDER BY total_spend DESC
    """)

    # ── Supplier risk tiers ───────────────────────────────────────────────────
    db.execute("DROP VIEW IF EXISTS vw_supplier_risk_tiers")
    db.execute("""
    CREATE VIEW vw_supplier_risk_tiers AS
    SELECT
        supplier,
        supplier_name,
        purchasing_org,
        total_spend,
        lines,
        delivered_lines,
        open_lines,
        avg_days_to_asn,
        ROUND(
            100.0 * CAST(delivered_lines AS DOUBLE)
            / NULLIF(CAST(lines AS DOUBLE), 0.0),
        1)                                                                      AS delivery_rate_pct,
        ROUND(
            CAST(open_lines AS DOUBLE)
            * COALESCE(CAST(avg_days_to_asn AS DOUBLE), 0.0),
        0)                                                                      AS risk_score,
        CASE
            WHEN avg_days_to_asn > 120
              OR (open_lines > 100 AND avg_days_to_asn > 60)                   THEN 'High Risk'
            WHEN avg_days_to_asn BETWEEN 45 AND 120
              OR open_lines > 30                                                THEN 'Medium Risk'
            ELSE                                                                     'Low Risk'
        END                                                                     AS risk_tier
    FROM vw_spend_by_supplier
    WHERE lines >= 5
    ORDER BY risk_score DESC
    """)

    # ── Customer Reference Search view (new) ──────────────────────────────────
    db.execute("DROP VIEW IF EXISTS vw_customer_reference_search")
    db.execute("""
    CREATE VIEW vw_customer_reference_search AS
    SELECT
        purchase_order,
        purchase_order_item,
        customer_po,                                                            -- 45-series
        customer_pr,                                                            -- 50-series
        customer_pr_item,
        supplier,
        supplier_name,
        material,
        material_name,
        plant,
        creation_date,
        dashboard_status,
        ROUND(TRY_CAST(spend AS DOUBLE), 2)                                     AS spend,
        document_currency
    FROM mart_dashboard_po_item
    WHERE (customer_po IS NOT NULL OR customer_pr IS NOT NULL)
      AND COALESCE(CAST(po_deletion_code AS VARCHAR), '') = ''
    ORDER BY creation_date DESC
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Standalone runner
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("DB path : %s", DB_PATH)

    if not DB_PATH.exists():
        logger.error("Database not found: %s", DB_PATH)
        logger.error("Run POST /api/sync first.")
        sys.exit(1)

    with duckdb.connect(str(DB_PATH)) as db:
        tables = sorted(r[0] for r in db.execute("SHOW TABLES").fetchall())
        logger.info("Tables in DB (%d): %s", len(tables), tables)
        counts = run_transformations(db)

    logger.info("Done.")
    for name, n in counts.items():
        logger.info("  %-35s %s rows", name, f"{n:,}")


if __name__ == "__main__":
    main()