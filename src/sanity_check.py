import duckdb
from pathlib import Path

DB = Path(r'C:\temp\Procurement Dashboard\V2\V2\data\prodash.db')
with duckdb.connect(str(DB), read_only=True) as db:

    print('=== PR Linkage Fix ===')
    db.execute('''
        SELECT
            COUNT(*)                                        AS total_lines,
            COUNT(customer_pr)                              AS lines_with_pr,
            ROUND(100.0 * COUNT(customer_pr) / COUNT(*), 1) AS pr_fill_pct
        FROM fact_po_item
    ''').df().pipe(print)

    print()
    print('=== Status Distribution ===')
    db.execute('''
        SELECT system_status, COUNT(*) AS lines,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct
        FROM fact_po_item
        GROUP BY system_status ORDER BY lines DESC
    ''').df().pipe(print)

    print()
    print('=== New Column Fill Rates ===')
    db.execute('''
        SELECT
            COUNT(plant)             AS has_plant,
            COUNT(cost_center)       AS has_cost_center,
            COUNT(gl_account)        AS has_gl_account,
            COUNT(wbs_element)       AS has_wbs,
            COUNT(profit_center)     AS has_profit_center,
            COUNT(requisitioner)     AS has_requisitioner,
            COUNT(supplier_name)     AS has_supplier_name,
            COUNT(material_group_text) AS has_mat_grp_text
        FROM fact_po_item
    ''').df().pipe(print)

    print()
    print('=== KPI Snapshot ===')
    db.execute('SELECT * FROM vw_dashboard_kpis').df().T.pipe(print)

    print()
    print('=== Overdue POs (sample 5) ===')
    db.execute('''
        SELECT purchase_order, supplier, material_name,
               creation_date, age_days, planned_delivery_days, dashboard_status
        FROM vw_overdue_pos LIMIT 5
    ''').df().pipe(print)