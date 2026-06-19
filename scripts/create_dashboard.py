#!/usr/bin/env python3
"""
Create a Superset dashboard for the StarRocks realtime sales demo.

Run after services are up and StarRocks connection is registered:
    python scripts/create_dashboard.py
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
DB_NAME = "StarRocks Demo"
DATASET_SCHEMA = "demo"
DATASET_TABLE = "rt_sales_events"
DASHBOARD_TITLE = "StarRocks Realtime Sales"
DASHBOARD_SLUG = "starrocks-realtime-sales"

CHART_NAMES = [
    "Total Orders",
    "Realtime Revenue by Minute",
    "Orders by Minute",
    "Revenue by Province",
    "Top Products",
    "Payment Method Split",
    "Latest Sales Events",
]


def request_json(path: str, method: str = "GET", data: dict[str, Any] | None = None,
                 headers: dict[str, str] | None = None) -> dict[str, Any]:
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    body = json.dumps(data).encode("utf-8") if data is not None else None
    req = urllib.request.Request(
        f"{SUPERSET_URL}{path}",
        data=body,
        headers=req_headers,
        method=method,
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def login() -> dict[str, str]:
    data = request_json(
        "/api/v1/security/login",
        "POST",
        {
            "username": ADMIN_USER,
            "password": ADMIN_PASS,
            "provider": "db",
            "refresh": True,
        },
    )
    return {"Authorization": f"Bearer {data['access_token']}"}


def list_items(path: str, headers: dict[str, str]) -> list[dict[str, Any]]:
    return request_json(path, headers=headers).get("result", [])


def delete_item(path: str, item_id: int, headers: dict[str, str]) -> None:
    try:
        request_json(f"{path}/{item_id}", "DELETE", headers=headers)
    except urllib.error.HTTPError as e:
        if e.code not in (404, 422):
            raise


def find_database(headers: dict[str, str]) -> int:
    for db in list_items("/api/v1/database/", headers):
        if db.get("database_name") == DB_NAME:
            return db["id"]
    raise RuntimeError(f"Database connection '{DB_NAME}' not found. Run: python scripts/setup_superset.py")


def ensure_dataset(database_id: int, headers: dict[str, str]) -> int:
    for dataset in list_items("/api/v1/dataset/", headers):
        if (
            dataset.get("database", {}).get("id") == database_id
            and dataset.get("schema") == DATASET_SCHEMA
            and dataset.get("table_name") == DATASET_TABLE
        ):
            return dataset["id"]

    data = request_json(
        "/api/v1/dataset/",
        "POST",
        {
            "database": database_id,
            "schema": DATASET_SCHEMA,
            "table_name": DATASET_TABLE,
        },
        headers,
    )
    return data["id"]


def cleanup_existing(headers: dict[str, str]) -> None:
    for dashboard in list_items("/api/v1/dashboard/", headers):
        if dashboard.get("slug") == DASHBOARD_SLUG or dashboard.get("dashboard_title") == DASHBOARD_TITLE:
            print(f"Deleting existing dashboard: {dashboard['dashboard_title']} (id={dashboard['id']})")
            delete_item("/api/v1/dashboard", dashboard["id"], headers)

    for chart in list_items("/api/v1/chart/", headers):
        if chart.get("slice_name") in CHART_NAMES:
            print(f"Deleting existing chart: {chart['slice_name']} (id={chart['id']})")
            delete_item("/api/v1/chart", chart["id"], headers)


def metric_sql(label: str, expression: str) -> dict[str, Any]:
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "column": None,
        "aggregate": None,
        "datasourceWarning": False,
        "hasCustomLabel": True,
        "label": label,
        "optionName": f"metric_{label.lower().replace(' ', '_').replace('(', '').replace(')', '')}",
    }


def create_chart(
    name: str,
    viz_type: str,
    dataset_id: int,
    dashboard_id: int,
    form_data: dict[str, Any],
    headers: dict[str, str],
) -> int:
    base_form_data = {
        "datasource": f"{dataset_id}__table",
        "viz_type": viz_type,
        "slice_id": 0,
        "adhoc_filters": [],
        "row_limit": 10000,
        "time_range": "No filter",
    }
    base_form_data.update(form_data)
    query_columns = base_form_data.get("all_columns") or []
    if not query_columns:
        x_axis = base_form_data.get("x_axis")
        query_columns = [x_axis] if x_axis else base_form_data.get("groupby") or []
    query_metrics = []
    if "metric" in base_form_data:
        query_metrics.append(base_form_data["metric"])
    query_metrics.extend(base_form_data.get("metrics", []))
    orderby = []
    if query_metrics:
        orderby = [[query_metrics[0], False]]
    elif query_columns:
        orderby = [[query_columns[0], False]]
    query = {
        "filters": [],
        "extras": {"having": "", "where": ""},
        "applied_time_extras": {},
        "columns": query_columns,
        "metrics": query_metrics,
        "orderby": orderby,
        "annotation_layers": [],
        "row_limit": base_form_data.get("row_limit", 10000),
        "series_limit": 0,
        "order_desc": base_form_data.get("order_desc", True),
        "url_params": {},
        "custom_params": {},
        "custom_form_data": {},
        "time_range": base_form_data.get("time_range", "No filter"),
    }

    payload = {
        "slice_name": name,
        "viz_type": viz_type,
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "params": json.dumps(base_form_data),
        "query_context": json.dumps(
            {
                "datasource": {"id": dataset_id, "type": "table"},
                "queries": [query],
                "form_data": base_form_data,
                "result_format": "json",
                "result_type": "full",
            }
        ),
        "dashboards": [dashboard_id],
    }
    data = request_json("/api/v1/chart/", "POST", payload, headers)
    chart_id = data["id"]
    print(f"Created chart: {name} (id={chart_id})")
    return chart_id


def create_dashboard(headers: dict[str, str]) -> int:
    data = request_json(
        "/api/v1/dashboard/",
        "POST",
        {
            "dashboard_title": DASHBOARD_TITLE,
            "slug": DASHBOARD_SLUG,
            "published": True,
            "json_metadata": json.dumps(
                {
                    "refresh_frequency": 30,
                    "timed_refresh_immune_slices": [],
                    "expanded_slices": {},
                }
            ),
        },
        headers,
    )
    dashboard_id = data["id"]
    print(f"Created dashboard: {DASHBOARD_TITLE} (id={dashboard_id})")
    return dashboard_id


def update_dashboard_layout(dashboard_id: int, chart_ids: dict[str, int], headers: dict[str, str]) -> None:
    rows = [
        ["Total Orders"],
        ["Realtime Revenue by Minute", "Orders by Minute"],
        ["Revenue by Province", "Top Products"],
        ["Payment Method Split", "Latest Sales Events"],
    ]

    position: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": []},
    }

    for row_index, row_charts in enumerate(rows):
        row_id = f"ROW-{row_index}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        width = 12 // len(row_charts)
        for name in row_charts:
            chart_id = chart_ids[name]
            chart_key = f"CHART-{chart_id}"
            height = 18 if "Latest" not in name else 24
            position[row_id]["children"].append(chart_key)
            position[chart_key] = {
                "type": "CHART",
                "id": chart_key,
                "children": [],
                "meta": {
                    "chartId": chart_id,
                    "height": height,
                    "width": width,
                    "uuid": None,
                },
            }

    request_json(
        f"/api/v1/dashboard/{dashboard_id}",
        "PUT",
        {
            "position_json": json.dumps(position),
            "json_metadata": json.dumps({"refresh_frequency": 30}),
            "published": True,
        },
        headers,
    )


def main() -> None:
    print("Creating Superset dashboard for StarRocks demo...")
    headers = login()
    database_id = find_database(headers)
    cleanup_existing(headers)
    dataset_id = ensure_dataset(database_id, headers)
    dashboard_id = create_dashboard(headers)

    revenue = metric_sql("Revenue", "SUM(amount)")
    orders = metric_sql("Orders", "COUNT(*)")

    chart_ids: dict[str, int] = {}
    chart_ids["Total Orders"] = create_chart(
        "Total Orders",
        "big_number_total",
        dataset_id,
        dashboard_id,
        {"metric": orders},
        headers,
    )
    chart_ids["Realtime Revenue by Minute"] = create_chart(
        "Realtime Revenue by Minute",
        "echarts_timeseries_bar",
        dataset_id,
        dashboard_id,
        {
            "x_axis": {
                "expressionType": "SQL",
                "sqlExpression": "date_trunc('minute', event_time)",
                "label": "minute_time",
            },
            "groupby": [],
            "metrics": [revenue],
            "order_desc": False,
            "row_limit": 100,
            "orientation": "vertical",
            "x_axis_sort": {
                "expressionType": "SQL",
                "sqlExpression": "date_trunc('minute', event_time)",
                "label": "minute_time",
            },
            "x_axis_sort_asc": True,
            "show_legend": True,
        },
        headers,
    )
    chart_ids["Orders by Minute"] = create_chart(
        "Orders by Minute",
        "echarts_timeseries_bar",
        dataset_id,
        dashboard_id,
        {
            "x_axis": {
                "expressionType": "SQL",
                "sqlExpression": "date_trunc('minute', event_time)",
                "label": "minute_time",
            },
            "groupby": [],
            "metrics": [orders],
            "order_desc": False,
            "row_limit": 100,
            "orientation": "vertical",
            "x_axis_sort": {
                "expressionType": "SQL",
                "sqlExpression": "date_trunc('minute', event_time)",
                "label": "minute_time",
            },
            "x_axis_sort_asc": True,
            "show_legend": False,
        },
        headers,
    )
    chart_ids["Revenue by Province"] = create_chart(
        "Revenue by Province",
        "echarts_timeseries_bar",
        dataset_id,
        dashboard_id,
        {
            "x_axis": "province",
            "groupby": [],
            "metrics": [revenue],
            "order_desc": True,
            "orientation": "vertical",
            "row_limit": 12,
        },
        headers,
    )
    chart_ids["Top Products"] = create_chart(
        "Top Products",
        "echarts_timeseries_bar",
        dataset_id,
        dashboard_id,
        {
            "x_axis": "product",
            "groupby": [],
            "metrics": [revenue],
            "order_desc": True,
            "orientation": "horizontal",
            "row_limit": 10,
        },
        headers,
    )
    chart_ids["Payment Method Split"] = create_chart(
        "Payment Method Split",
        "pie",
        dataset_id,
        dashboard_id,
        {
            "groupby": ["payment_method"],
            "metric": orders,
            "row_limit": 10,
        },
        headers,
    )
    chart_ids["Latest Sales Events"] = create_chart(
        "Latest Sales Events",
        "table",
        dataset_id,
        dashboard_id,
        {
            "all_columns": [
                "event_time",
                "order_id",
                "province",
                "product",
                "amount",
                "payment_method",
                "ingest_time",
            ],
            "order_by_cols": ["[\"event_time\", false]"],
            "row_limit": 20,
        },
        headers,
    )

    update_dashboard_layout(dashboard_id, chart_ids, headers)

    print()
    print("Dashboard created.")
    print(f"URL: {SUPERSET_URL}/superset/dashboard/{DASHBOARD_SLUG}/")


if __name__ == "__main__":
    main()
