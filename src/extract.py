import json
from pathlib import Path
import pandas as pd
import numpy as np


BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = BASE_DIR / "exports_json"   # where your JSONs actually are
REPORT_PATH = EXPORT_DIR / "full_analysis_report.json"

FILES = {
    "pr": "pr_po_summary.json",
    "gr": "stg_C_PurchaseOrderGoodsReceipt.json",
    "asn": "stg_C_PurOrdSuplrConfDisplay.json",
    "bridge": "stg_C_PurReqItemByPurOrder.json",
    "po": "stg_I_PurchaseOrder.json",
    "item": "stg_I_PurchaseOrderItem.json",
    "supplier": "stg_I_Supplier.json",
}

def find_file(filename: str) -> Path:
    """
    Find a file either in exports_json/ or next to extract.py.
    """
    p1 = EXPORT_DIR / filename
    if p1.exists():
        return p1
    p2 = BASE_DIR / filename
    if p2.exists():
        return p2
    raise FileNotFoundError(f"Missing file: {filename}\nChecked:\n - {p1}\n - {p2}")

def load_json_df(filename: str) -> pd.DataFrame:
    p = find_file(filename)
    with open(p, "r", encoding="utf-8") as f:
        return pd.DataFrame(json.load(f))

def norm_str(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str)

def safe_null_rate(df: pd.DataFrame, col: str) -> float:
    if col not in df.columns:
        return float("nan")
    return float(df[col].isna().mean())

def main():
    print(f"📁 Script folder : {BASE_DIR}")
    print(f"📁 Exports folder: {EXPORT_DIR}")

    # Load all JSONs
    dfs = {}
    for k, fn in FILES.items():
        p = find_file(fn)
        df = load_json_df(fn)
        dfs[k] = df
        print(f"✅ Loaded {fn}: {len(df):,} rows, {df.shape[1]} cols  ({p})")

    pr = dfs["pr"]
    gr = dfs["gr"]
    asn = dfs["asn"]
    bridge = dfs["bridge"]
    po = dfs["po"]
    item = dfs["item"]
    supplier = dfs["supplier"]

    # Normalize common keys
    for df in [pr, gr, asn, bridge, po, item, supplier]:
        norm_str(df, ["PurchaseOrder", "PurchaseOrderItem", "Supplier", "SupplierID"])

    report = {
        "paths": {
            "script_dir": str(BASE_DIR),
            "exports_dir": str(EXPORT_DIR),
        },
        "row_counts": {k: int(len(dfs[k])) for k in dfs},
        "duplicates": {},
        "missingness": {},
        "bridge_explosion": {},
        "status_validation": {},
        "supplier_coverage": {},
        "spend_validation": {},
        "notes": [],
    }

    # ----------------------------
    # 1) Duplicate checks
    # ----------------------------
    if "PurchaseOrder" in po.columns:
        report["duplicates"]["po_header_purchaseorder"] = int(po["PurchaseOrder"].duplicated().sum())

    if {"PurchaseOrder", "PurchaseOrderItem"} <= set(item.columns):
        k_item = item["PurchaseOrder"] + "-" + item["PurchaseOrderItem"]
        report["duplicates"]["po_item_po_item"] = int(k_item.duplicated().sum())
        report["duplicates"]["po_item_unique_keys"] = int(k_item.nunique())

    if {"PurchaseOrder", "PurchaseOrderItem"} <= set(pr.columns):
        k_pr = pr["PurchaseOrder"] + "-" + pr["PurchaseOrderItem"]
        report["duplicates"]["pr_po_summary_po_item"] = int(k_pr.duplicated().sum())
        report["duplicates"]["pr_po_summary_unique_keys"] = int(k_pr.nunique())

    # ----------------------------
    # 2) Missingness / completeness
    # ----------------------------
    for name, df in [("po", po), ("item", item), ("pr", pr), ("supplier", supplier)]:
        report["missingness"][name] = {
            "CreationDate_null_rate": safe_null_rate(df, "CreationDate"),
        }

    # ----------------------------
    # 3) Bridge explosion analysis
    # ----------------------------
    if "PurchaseOrder" in bridge.columns:
        pr_per_po = bridge["PurchaseOrder"].value_counts()
        report["bridge_explosion"]["top_pr_items_per_po"] = pr_per_po.head(10).to_dict()

        if "PurchaseOrder" in item.columns:
            items_per_po = item["PurchaseOrder"].value_counts()
            est_rows = (items_per_po * pr_per_po.reindex(items_per_po.index).fillna(0)).sum()
            report["bridge_explosion"]["estimated_rows_if_join_on_po_only"] = int(est_rows)

            # Also capture worst-case multiplier POs
            joined = pd.DataFrame({
                "items_per_po": items_per_po,
                "pr_items_per_po": pr_per_po.reindex(items_per_po.index).fillna(0).astype(int)
            })
            joined["multiplier"] = joined["items_per_po"] * joined["pr_items_per_po"]
            worst = joined.sort_values("multiplier", ascending=False).head(20)
            report["bridge_explosion"]["worst_join_multipliers"] = worst.reset_index().rename(columns={"index":"PurchaseOrder"}).to_dict("records")

    # ----------------------------
    # 4) Status validation (PO-level vs PO-ITEM-level)
    # ----------------------------
    if {"PurchaseOrder", "PurchaseOrderItem", "CurrentStatus"} <= set(pr.columns) and \
       {"PurchaseOrder", "PurchaseOrderItem"} <= set(gr.columns) and \
       {"PurchaseOrder", "PurchaseOrderItem"} <= set(asn.columns):

        po_gr = set(gr["PurchaseOrder"].dropna().unique())
        po_asn = set(asn["PurchaseOrder"].dropna().unique())

        poi_gr = set(zip(gr["PurchaseOrder"], gr["PurchaseOrderItem"]))
        poi_asn = set(zip(asn["PurchaseOrder"], asn["PurchaseOrderItem"]))

        def expected_po(po_):
            if po_ in po_gr:
                return "Fulfilled"
            if po_ in po_asn:
                return "Shipped"
            return "PO Created"

        def expected_item(po_, it_):
            if (po_, it_) in poi_gr:
                return "Fulfilled"
            if (po_, it_) in poi_asn:
                return "Shipped"
            return "PO Created"

        pr["expected_po"] = pr["PurchaseOrder"].map(expected_po)
        pr["expected_item"] = [expected_item(p, i) for p, i in zip(pr["PurchaseOrder"], pr["PurchaseOrderItem"])]

        mismatch_po = int((pr["CurrentStatus"] != pr["expected_po"]).sum())
        mismatch_item = int((pr["CurrentStatus"] != pr["expected_item"]).sum())
        fulfilled_but_no_gr_item = int(((pr["CurrentStatus"] == "Fulfilled") & (pr["expected_item"] != "Fulfilled")).sum())

        report["status_validation"] = {
            "mismatch_vs_po_level_logic": mismatch_po,
            "mismatch_vs_item_level_logic": mismatch_item,
            "fulfilled_but_no_gr_item": fulfilled_but_no_gr_item,
        }

        # Provide a small sample of the problematic lines
        sample = pr.loc[
            (pr["CurrentStatus"] == "Fulfilled") & (pr["expected_item"] != "Fulfilled"),
            ["PurchaseOrder", "PurchaseOrderItem", "CurrentStatus", "expected_item"]
        ].head(25)
        report["status_validation"]["sample_fulfilled_but_no_gr_item"] = sample.to_dict("records")

    else:
        report["notes"].append("Status validation skipped: required columns not found in pr/gr/asn.")

    # ----------------------------
    # 5) Supplier coverage (missing supplier master)
    # ----------------------------
    if "Supplier" in supplier.columns:
        known = set(supplier["Supplier"].dropna().unique())

        # pr view may have SupplierID
        if "SupplierID" in pr.columns:
            missing = pr.loc[~pr["SupplierID"].isin(known), "SupplierID"].value_counts().head(50)
            report["supplier_coverage"]["missing_supplier_ids_in_pr_top50"] = missing.to_dict()
            report["supplier_coverage"]["missing_supplier_id_count_distinct"] = int(pr.loc[~pr["SupplierID"].isin(known), "SupplierID"].nunique())
        else:
            report["notes"].append("Supplier coverage: pr_po_summary missing SupplierID column.")

    # ----------------------------
    # 6) Spend validation (where possible)
    # ----------------------------
    if {"OrderQuantity", "NetPriceAmount", "TotalEstimatedSpend"} <= set(pr.columns):
        oq = pd.to_numeric(pr["OrderQuantity"], errors="coerce")
        npv = pd.to_numeric(pr["NetPriceAmount"], errors="coerce")
        tev = pd.to_numeric(pr["TotalEstimatedSpend"], errors="coerce")
        calc = oq * npv
        diff = (calc - tev).abs()
        report["spend_validation"] = {
            "exact_match_pct": float((diff.fillna(0) < 1e-6).mean()),
            "rows_diff_gt_0_01": int((diff > 0.01).sum()),
            "rows_with_nan_any": int(((oq.isna()) | (npv.isna()) | (tev.isna())).sum()),
        }

    # Write report
    EXPORT_DIR.mkdir(exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n📝 Wrote full report: {REPORT_PATH}")

    # Print the most important findings in console
    print("\n===== QUICK SUMMARY =====")
    print("Rows:", report["row_counts"])
    print("Duplicates:", report["duplicates"])
    if report["status_validation"]:
        print("Status:", report["status_validation"])
    if report["bridge_explosion"]:
        print("Bridge explosion estimate:", report["bridge_explosion"].get("estimated_rows_if_join_on_po_only"))
    print("=========================")


if __name__ == "__main__":
    main()