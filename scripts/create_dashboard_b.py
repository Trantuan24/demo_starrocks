#!/usr/bin/env python3
"""
create_dashboard_b.py  (Phương án B)
Tạo Superset dashboard trên native serving mart serving.ads_revenue_daily.

Chạy sau: python scripts/setup_superset_b.py
    python scripts/create_dashboard_b.py
"""

import io
import json
import sys
import urllib.error
import urllib.request
from typing import Any

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

SUPERSET_URL = "http://localhost:8088"
ADMIN_USER = "admin"
ADMIN_PASS = "admin"
DB_NAME = "StarRocks Serving (B)"
DATASET_SCHEMA = "serving"
DATASET_TABLE = "ads_revenue_daily"
DASHBOARD_TITLE = "StarRocks Iceberg Serving Mart (B)"
DASHBOARD_SLUG = "starrocks-iceberg-mart"

CHART_NAMES = [
    "Total Revenue",
    "Total Orders (mart)",
    "Revenue by Day",
    "Revenue by Province (mart)",
    "Top Products (mart)",
    "Revenue Mart Detail",
]


def request_json(path, method="GET", data=None, headers=None):
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    body = json.dumps(data).encode("utf-8") if data is not None else None
    req = urllib.request.Request(f"{SUPERSET_URL}{path}", data=body, headers=req_headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def login():
    data = request_json(
        "/api/v1/security/login", "POST",
        {"username": ADMIN_USER, "password": ADMIN_PASS, "provider": "db", "refresh": True},
    )
    return {"Authorization": f"Bearer {data['access_token']}"}


def list_items(path, headers):
    return request_json(path, headers=headers).get("result", [])


def delete_item(path, item_id, headers):
    try:
        request_json(f"{path}/{item_id}", "DELETE", headers=headers)
    except urllib.error.HTTPError as e:
        if e.code not in (404, 422):
            raise


def find_database(headers):
    for db in list_items("/api/v1/database/", headers):
        if db.get("database_name") == DB_NAME:
            return db["id"]
    raise RuntimeError(f"Connection '{DB_NAME}' not found. Run: python scripts/setup_superset_b.py")


def ensure_dataset(database_id, headers):
    for ds in list_items("/api/v1/dataset/", headers):
        if (ds.get("database", {}).get("id") == database_id
                and ds.get("schema") == DATASET_SCHEMA
                and ds.get("table_name") == DATASET_TABLE):
            return ds["id"]
    data = request_json(
        "/api/v1/dataset/", "POST",
        {"database": database_id, "schema": DATASET_SCHEMA, "table_name": DATASET_TABLE},
        headers,
    )
    return data["id"]


def cleanup_existing(headers):
    for d in list_items("/api/v1/dashboard/", headers):
        if d.get("slug") == DASHBOARD_SLUG or d.get("dashboard_title") == DASHBOARD_TITLE:
            print(f"Deleting existing dashboard: {d['dashboard_title']} (id={d['id']})")
            delete_item("/api/v1/dashboard", d["id"], headers)
    for c in list_items("/api/v1/chart/", headers):
        if c.get("slice_name") in CHART_NAMES:
            delete_item("/api/v1/chart", c["id"], headers)


def metric_sql(label, expression):
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "column": None,
        "aggregate": None,
        "hasCustomLabel": True,
        "label": label,
        "optionName": f"metric_{label.lower().replace(' ', '_')}",
    }


def create_chart(name, viz_type, dataset_id, dashboard_id, form_data, headers):
    base = {
        "datasource": f"{dataset_id}__table",
        "viz_type": viz_type,
        "slice_id": 0,
        "adhoc_filters": [],
        "row_limit": 10000,
        "time_range": "No filter",
    }
    base.update(form_data)
    cols = base.get("all_columns") or []
    if not cols:
        x = base.get("x_axis")
        cols = [x] if x else base.get("groupby") or []
    metrics = []
    if "metric" in base:
        metrics.append(base["metric"])
    metrics.extend(base.get("metrics", []))
    orderby = [[metrics[0], False]] if metrics else ([[cols[0], False]] if cols else [])
    query = {
        "filters": [], "extras": {"having": "", "where": ""}, "applied_time_extras": {},
        "columns": cols, "metrics": metrics, "orderby": orderby, "annotation_layers": [],
        "row_limit": base.get("row_limit", 10000), "series_limit": 0,
        "order_desc": base.get("order_desc", True), "url_params": {},
        "custom_params": {}, "custom_form_data": {},
        "time_range": base.get("time_range", "No filter"),
    }
    payload = {
        "slice_name": name, "viz_type": viz_type, "datasource_id": dataset_id,
        "datasource_type": "table", "params": json.dumps(base),
        "query_context": json.dumps({
            "datasource": {"id": dataset_id, "type": "table"}, "queries": [query],
            "form_data": base, "result_format": "json", "result_type": "full",
        }),
        "dashboards": [dashboard_id],
    }
    data = request_json("/api/v1/chart/", "POST", payload, headers)
    print(f"Created chart: {name} (id={data['id']})")
    return data["id"]


def create_dashboard(headers):
    data = request_json(
        "/api/v1/dashboard/", "POST",
        {
            "dashboard_title": DASHBOARD_TITLE, "slug": DASHBOARD_SLUG, "published": True,
            "json_metadata": json.dumps({"refresh_frequency": 0}),
        },
        headers,
    )
    print(f"Created dashboard: {DASHBOARD_TITLE} (id={data['id']})")
    return data["id"]


def update_layout(dashboard_id, chart_ids, headers):
    rows = [
        ["Total Revenue", "Total Orders (mart)"],
        ["Revenue by Day"],
        ["Revenue by Province (mart)", "Top Products (mart)"],
        ["Revenue Mart Detail"],
    ]
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": []},
    }
    for ri, row in enumerate(rows):
        rid = f"ROW-{ri}"
        position["GRID_ID"]["children"].append(rid)
        position[rid] = {"type": "ROW", "id": rid, "children": [],
                         "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        width = 12 // len(row)
        for name in row:
            cid = chart_ids[name]
            key = f"CHART-{cid}"
            height = 24 if "Detail" in name else (20 if "by Day" in name else 18)
            position[rid]["children"].append(key)
            position[key] = {"type": "CHART", "id": key, "children": [],
                             "meta": {"chartId": cid, "height": height, "width": width, "uuid": None}}
    request_json(
        f"/api/v1/dashboard/{dashboard_id}", "PUT",
        {"position_json": json.dumps(position),
         "json_metadata": json.dumps({"refresh_frequency": 0}), "published": True},
        headers,
    )


def main():
    print("Creating Superset dashboard for StarRocks Demo B (Iceberg serving mart)...")
    headers = login()
    database_id = find_database(headers)
    cleanup_existing(headers)
    dataset_id = ensure_dataset(database_id, headers)
    dashboard_id = create_dashboard(headers)

    revenue = metric_sql("Revenue", "SUM(revenue)")
    orders = metric_sql("Orders", "SUM(order_count)")

    ids = {}
    ids["Total Revenue"] = create_chart(
        "Total Revenue", "big_number_total", dataset_id, dashboard_id,
        {"metric": revenue}, headers)
    ids["Total Orders (mart)"] = create_chart(
        "Total Orders (mart)", "big_number_total", dataset_id, dashboard_id,
        {"metric": orders}, headers)
    ids["Revenue by Day"] = create_chart(
        "Revenue by Day", "echarts_timeseries_bar", dataset_id, dashboard_id,
        {"x_axis": "report_date", "groupby": [], "metrics": [revenue],
         "order_desc": False, "row_limit": 120, "orientation": "vertical",
         "x_axis_sort": "report_date", "x_axis_sort_asc": True, "show_legend": True},
        headers)
    ids["Revenue by Province (mart)"] = create_chart(
        "Revenue by Province (mart)", "echarts_timeseries_bar", dataset_id, dashboard_id,
        {"x_axis": "province", "groupby": [], "metrics": [revenue],
         "order_desc": True, "orientation": "vertical", "row_limit": 12},
        headers)
    ids["Top Products (mart)"] = create_chart(
        "Top Products (mart)", "echarts_timeseries_bar", dataset_id, dashboard_id,
        {"x_axis": "product", "groupby": [], "metrics": [revenue],
         "order_desc": True, "orientation": "horizontal", "row_limit": 10},
        headers)
    ids["Revenue Mart Detail"] = create_chart(
        "Revenue Mart Detail", "table", dataset_id, dashboard_id,
        {"all_columns": ["report_date", "province", "product", "revenue", "order_count", "total_qty"],
         "order_by_cols": ["[\"report_date\", false]"], "row_limit": 50},
        headers)

    update_layout(dashboard_id, ids, headers)
    print()
    print("Dashboard created.")
    print(f"URL: {SUPERSET_URL}/superset/dashboard/{DASHBOARD_SLUG}/")


if __name__ == "__main__":
    main()
