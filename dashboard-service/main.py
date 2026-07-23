from __future__ import annotations

import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as ds
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from minio import Minio

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="Airbnb Dashboard Service")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@dataclass
class TableLoadResult:
    df: pd.DataFrame
    warning: str | None = None


class GoldParquetRepository:
    def __init__(self) -> None:
        endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        if endpoint.startswith("http://"):
            endpoint = endpoint.replace("http://", "", 1)
            secure = False
        elif endpoint.startswith("https://"):
            endpoint = endpoint.replace("https://", "", 1)
            secure = True
        else:
            secure = False

        self.bucket = os.getenv("DASHBOARD_MINIO_BUCKET", "gold-bucket")
        self.cache_seconds = int(os.getenv("DASHBOARD_CACHE_SECONDS", "60"))
        self.base_dir = Path(os.getenv("DASHBOARD_DATA_DIR", "/tmp/dashboard-data"))
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self.client = Minio(
            endpoint,
            access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "password"),
            secure=secure,
        )

        self._cache: dict[str, tuple[float, pd.DataFrame]] = {}

    def load_table(self, table_name: str) -> TableLoadResult:
        now = time.time()
        cached = self._cache.get(table_name)
        if cached and (now - cached[0] <= self.cache_seconds):
            return TableLoadResult(df=cached[1].copy())

        local_dir = self.base_dir / table_name
        if local_dir.exists():
            shutil.rmtree(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)

        prefix = f"{table_name}.parquet/"

        try:
            objects = list(
                self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
            )
            if not objects:
                # Fallback when the parquet is stored as a single object without nested part files.
                prefix = f"{table_name}.parquet"
                objects = list(
                    self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
                )
        except Exception as exc:  # noqa: BLE001
            return TableLoadResult(
                df=pd.DataFrame(),
                warning=f"Failed to connect to MinIO while reading {table_name}: {exc}",
            )

        if not objects:
            return TableLoadResult(
                df=pd.DataFrame(),
                warning=(
                    f"No parquet files found yet for '{table_name}' in bucket '{self.bucket}'."
                ),
            )

        for obj in objects:
            if not obj.object_name or obj.object_name.endswith("/"):
                continue
            relative_key = obj.object_name.removeprefix(prefix).lstrip("/")
            if not relative_key:
                relative_key = Path(obj.object_name).name
            destination = local_dir / relative_key
            destination.parent.mkdir(parents=True, exist_ok=True)
            self.client.fget_object(self.bucket, obj.object_name, str(destination))

        parquet_files = list(local_dir.rglob("*.parquet"))
        if not parquet_files:
            return TableLoadResult(
                df=pd.DataFrame(),
                warning=f"{table_name} exists in MinIO but has no readable parquet part files yet.",
            )

        try:
            dataset = ds.dataset(str(local_dir), format="parquet")
            df = dataset.to_table().to_pandas()
        except Exception as exc:  # noqa: BLE001
            return TableLoadResult(
                df=pd.DataFrame(),
                warning=f"Failed to parse parquet for {table_name}: {exc}",
            )

        self._cache[table_name] = (now, df)
        return TableLoadResult(df=df.copy())


repo = GoldParquetRepository()


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:  # noqa: BLE001
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if pd.isna(v):
            return default
        return int(float(v))
    except Exception:  # noqa: BLE001
        return default


def _fmt_int(v: int | float) -> str:
    return f"{int(round(v)):,}"


def _fmt_money(v: int | float) -> str:
    return f"${float(v):,.0f}"


def _fmt_pct_from_fraction(v: int | float) -> str:
    return f"{(float(v) * 100):.2f}%"


@app.get("/", response_class=HTMLResponse)
@app.get("/index.html", response_class=HTMLResponse)
def index_page(request: Request) -> HTMLResponse:
    invest_result = repo.load_table("AggNeighbourhoodInvest")
    warnings: list[str] = []
    if invest_result.warning:
        warnings.append(invest_result.warning)

    df = invest_result.df.copy()

    cards: list[dict[str, Any]] = []
    revenue_labels: list[str] = []
    revenue_data: list[float] = []
    bookings_data: list[int] = []

    if not df.empty:
        expected_cols = {
            "city_name",
            "neighbourhood_name",
            "avg_revenue_l365d",
            "neighbourhood_most_bookings",
            "neighbourhood_most_views",
            "avg_order_price",
            "avg_response_rate",
            "median_availability",
            "neighbourhood_needed_boost",
        }
        missing = expected_cols.difference(df.columns)
        if missing:
            warnings.append(
                "AggNeighbourhoodInvest is missing expected columns: "
                + ", ".join(sorted(missing))
            )
            df = pd.DataFrame()

    if not df.empty:
        top = df.sort_values("avg_revenue_l365d", ascending=False).head(6)

        for _, row in top.iterrows():
            boost_val = row.get("neighbourhood_needed_boost", [])
            boost_txt = "-"
            if isinstance(boost_val, (list, tuple)) and boost_val:
                boost_txt = ", ".join(str(x) for x in boost_val)
            elif boost_val is not None:
                boost_txt = str(boost_val)

            cards.append(
                {
                    "city": str(row.get("city_name", "-")).title(),
                    "neighbourhood": str(row.get("neighbourhood_name", "-")),
                    "revenue": _fmt_money(_to_float(row.get("avg_revenue_l365d", 0))),
                    "bookings": _fmt_int(
                        _to_int(row.get("neighbourhood_most_bookings", 0))
                    ),
                    "views": _fmt_int(_to_int(row.get("neighbourhood_most_views", 0))),
                    "avg_price": _fmt_money(_to_float(row.get("avg_order_price", 0))),
                    "response_rate": f"{_to_float(row.get('avg_response_rate', 0)):.1f}%",
                    "availability": _fmt_int(
                        _to_int(row.get("median_availability", 0))
                    ),
                    "boost": boost_txt,
                }
            )

        chart_df = df.sort_values("avg_revenue_l365d", ascending=False).head(10)
        revenue_labels = [
            f"{str(c).title()} · {str(n)}"
            for c, n in zip(
                chart_df["city_name"],
                chart_df["neighbourhood_name"],
                strict=False,
            )
        ]
        revenue_data = [
            round(_to_float(v), 2) for v in chart_df["avg_revenue_l365d"].tolist()
        ]
        bookings_data = [
            _to_int(v) for v in chart_df["neighbourhood_most_bookings"].tolist()
        ]

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "warnings": warnings,
            "cards": cards,
            "has_data": bool(cards),
            "revenue_labels": revenue_labels,
            "revenue_data": revenue_data,
            "bookings_data": bookings_data,
        },
    )


@app.get("/map", response_class=HTMLResponse)
@app.get("/map.html", response_class=HTMLResponse)
def map_page(
    request: Request,
    city: str | None = Query(default=None, description="Filter selected city"),
) -> HTMLResponse:
    invest_result = repo.load_table("AggNeighbourhoodInvest")
    location_result = repo.load_table("dimLocation")
    listing_result = repo.load_table("factListing")

    warnings: list[str] = []
    for result in (invest_result, location_result, listing_result):
        if result.warning:
            warnings.append(result.warning)

    invest_df = invest_result.df.copy()
    location_df = location_result.df.copy()
    listing_df = listing_result.df.copy()

    cities: list[str] = []
    selected_city = ""

    kpis = {
        "active_listings": "-",
        "bookings": "-",
        "avg_price": "-",
        "total_revenue": "-",
        "avg_response": "-",
        "median_availability": "-",
    }

    views_labels: list[str] = []
    views_data: list[int] = []
    bookings_labels: list[str] = []
    bookings_data: list[int] = []
    markers: list[dict[str, Any]] = []

    if not invest_df.empty and "city_name" in invest_df.columns:
        cities = sorted(
            {
                str(c).strip()
                for c in invest_df["city_name"].dropna().tolist()
                if str(c).strip()
            }
        )

        if cities:
            selected_city = city.strip().lower() if city else cities[0]
            if selected_city not in cities:
                selected_city = cities[0]

            city_df = invest_df[invest_df["city_name"] == selected_city].copy()
            if not city_df.empty:
                active_listings = city_df["neighbourhood_most_views"].fillna(0).sum()
                bookings_sum = city_df["neighbourhood_most_bookings"].fillna(0).sum()
                avg_price = city_df["avg_order_price"].fillna(0).mean()
                avg_response = city_df["avg_response_rate"].fillna(0).mean()
                median_availability = city_df["median_availability"].fillna(0).median()
                total_revenue = (
                    city_df["avg_revenue_l365d"].fillna(0)
                    * city_df["neighbourhood_most_views"].fillna(0)
                ).sum()

                kpis = {
                    "active_listings": _fmt_int(active_listings),
                    "bookings": _fmt_int(bookings_sum),
                    "avg_price": _fmt_money(avg_price),
                    "total_revenue": _fmt_money(total_revenue),
                    "avg_response": f"{avg_response:.2f}%",
                    "median_availability": _fmt_int(median_availability),
                }

                chart_df = city_df.sort_values(
                    "neighbourhood_most_bookings", ascending=False
                ).head(12)
                views_labels = chart_df["neighbourhood_name"].fillna("unknown").tolist()
                views_data = [
                    _to_int(v) for v in chart_df["neighbourhood_most_views"].tolist()
                ]
                bookings_labels = views_labels
                bookings_data = [
                    _to_int(v) for v in chart_df["neighbourhood_most_bookings"].tolist()
                ]

    if (
        selected_city
        and not location_df.empty
        and not listing_df.empty
        and {"location_key", "city", "latitude", "longitude"}.issubset(
            location_df.columns
        )
        and {
            "location_key",
            "id",
            "estimated_revenue_l365d",
            "estimated_occupancy_l365d",
        }.issubset(listing_df.columns)
    ):
        city_loc = location_df[
            location_df["city"].fillna("").str.strip().str.lower() == selected_city
        ][["location_key", "city", "neighbourhood", "latitude", "longitude"]].copy()

        city_fact = listing_df[
            [
                "location_key",
                "id",
                "estimated_revenue_l365d",
                "estimated_occupancy_l365d",
                "price_per_night",
            ]
        ].copy()

        merged = city_loc.merge(city_fact, on="location_key", how="inner")
        if not merged.empty:
            rev_q = merged["estimated_revenue_l365d"].fillna(0).quantile(0.25)
            occ_q = merged["estimated_occupancy_l365d"].fillna(0).quantile(0.25)

            low_perf = merged[
                (merged["estimated_revenue_l365d"].fillna(0) <= rev_q)
                | (merged["estimated_occupancy_l365d"].fillna(0) <= occ_q)
            ].dropna(subset=["latitude", "longitude"])

            for _, row in low_perf.head(200).iterrows():
                markers.append(
                    {
                        "lat": round(_to_float(row.get("latitude")), 6),
                        "lng": round(_to_float(row.get("longitude")), 6),
                        "listing_id": _to_int(row.get("id")),
                        "neighbourhood": str(row.get("neighbourhood", "unknown")),
                        "revenue": _fmt_money(
                            _to_float(row.get("estimated_revenue_l365d", 0))
                        ),
                        "occupancy": _fmt_int(
                            _to_int(row.get("estimated_occupancy_l365d", 0))
                        ),
                        "price": _fmt_money(_to_float(row.get("price_per_night", 0))),
                    }
                )

    return templates.TemplateResponse(
        request=request,
        name="map.html",
        context={
            "warnings": warnings,
            "cities": cities,
            "selected_city": selected_city,
            "selected_city_title": selected_city.title() if selected_city else "-",
            "kpis": kpis,
            "views_labels": views_labels,
            "views_data": views_data,
            "bookings_labels": bookings_labels,
            "bookings_data": bookings_data,
            "markers": markers,
            "has_data": bool(cities),
        },
    )


@app.get("/promotion", response_class=HTMLResponse)
@app.get("/promotion.html", response_class=HTMLResponse)
def promotion_page(request: Request) -> HTMLResponse:
    ads_result = repo.load_table("AggAds")

    warnings: list[str] = []
    if ads_result.warning:
        warnings.append(ads_result.warning)

    ads_df = ads_result.df.copy()

    kpis = {
        "avg_conversion": "-",
        "total_spend": "-",
        "total_customers": "-",
        "provider_count": "-",
    }
    cards: list[dict[str, Any]] = []
    table_rows: list[dict[str, Any]] = []

    conversion_labels: list[str] = []
    conversion_data: list[float] = []
    cost_labels: list[str] = []
    cost_data: list[float] = []
    customers_labels: list[str] = []
    customers_data: list[int] = []
    posts_labels: list[str] = []
    posts_data: list[int] = []

    if not ads_df.empty:
        expected_cols = {
            "provider_name",
            "provider_conversion_rate",
            "most_clicked_provider",
            "customer_gained",
            "avg_campaign_cost",
            "post_amount",
        }
        missing = expected_cols.difference(ads_df.columns)
        if missing:
            warnings.append(
                "AggAds is missing expected columns: " + ", ".join(sorted(missing))
            )
            ads_df = pd.DataFrame()

    if not ads_df.empty:
        kpis = {
            "avg_conversion": _fmt_pct_from_fraction(
                ads_df["provider_conversion_rate"].fillna(0).mean()
            ),
            "total_spend": _fmt_money(ads_df["avg_campaign_cost"].fillna(0).sum()),
            "total_customers": _fmt_int(ads_df["customer_gained"].fillna(0).sum()),
            "provider_count": _fmt_int(len(ads_df.index)),
        }

        ranked = ads_df.sort_values("provider_conversion_rate", ascending=False)
        for _, row in ranked.head(3).iterrows():
            cards.append(
                {
                    "name": str(row.get("provider_name", "-")),
                    "conversion": _fmt_pct_from_fraction(
                        _to_float(row.get("provider_conversion_rate", 0))
                    ),
                    "customers": _fmt_int(_to_int(row.get("customer_gained", 0))),
                    "avg_cost": _fmt_money(_to_float(row.get("avg_campaign_cost", 0))),
                    "posts": _fmt_int(_to_int(row.get("post_amount", 0))),
                }
            )

        chart_df = ranked.head(12)
        conversion_labels = chart_df["provider_name"].fillna("unknown").tolist()
        conversion_data = [
            round(_to_float(v) * 100, 2)
            for v in chart_df["provider_conversion_rate"].tolist()
        ]

        cost_labels = chart_df["provider_name"].fillna("unknown").tolist()
        cost_data = [
            round(_to_float(v), 2) for v in chart_df["avg_campaign_cost"].tolist()
        ]

        customers_labels = chart_df["provider_name"].fillna("unknown").tolist()
        customers_data = [_to_int(v) for v in chart_df["customer_gained"].tolist()]

        posts_labels = chart_df["provider_name"].fillna("unknown").tolist()
        posts_data = [_to_int(v) for v in chart_df["post_amount"].tolist()]

        for _, row in ranked.head(20).iterrows():
            table_rows.append(
                {
                    "name": str(row.get("provider_name", "-")),
                    "conversion": _fmt_pct_from_fraction(
                        _to_float(row.get("provider_conversion_rate", 0))
                    ),
                    "most_clicked": f"{_to_float(row.get('most_clicked_provider', 0)):.4f}",
                    "customers": _fmt_int(_to_int(row.get("customer_gained", 0))),
                    "cost": _fmt_money(_to_float(row.get("avg_campaign_cost", 0))),
                    "posts": _fmt_int(_to_int(row.get("post_amount", 0))),
                }
            )

    return templates.TemplateResponse(
        request=request,
        name="promotion.html",
        context={
            "warnings": warnings,
            "kpis": kpis,
            "cards": cards,
            "table_rows": table_rows,
            "conversion_labels": conversion_labels,
            "conversion_data": conversion_data,
            "cost_labels": cost_labels,
            "cost_data": cost_data,
            "customers_labels": customers_labels,
            "customers_data": customers_data,
            "posts_labels": posts_labels,
            "posts_data": posts_data,
            "has_data": bool(table_rows),
        },
    )
