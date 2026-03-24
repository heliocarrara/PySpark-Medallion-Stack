from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


def lake_root() -> Path:
    env_value = os.environ.get("LAKE_ROOT")
    if env_value:
        return Path(env_value).expanduser().resolve()

    script_dir = Path(__file__).resolve().parent
    return (script_dir.parent / "data_lake").resolve()


def parquet_table_path(table_name: str) -> Path:
    return lake_root() / "gold" / "parquet" / table_name


@st.cache_data(show_spinner=False)
def load_parquet_table(table_name: str) -> pd.DataFrame:
    path = parquet_table_path(table_name)
    if not path.exists():
        raise FileNotFoundError(f"Não encontrei a tabela '{table_name}' em: {path.as_posix()}")
    if path.is_file() and path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)

    if not path.is_dir():
        raise FileNotFoundError(f"Esperava um diretório ou arquivo .parquet, mas encontrei: {path.as_posix()}")

    parquet_files: list[Path] = []
    for p in path.rglob("*.parquet"):
        if any(part.startswith("_") for part in p.parts):
            continue
        parquet_files.append(p)

    if not parquet_files:
        raise FileNotFoundError(f"Nenhum arquivo .parquet encontrado em: {path.as_posix()}")

    return pd.read_parquet(parquet_files)


def as_datetime(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([], dtype="datetime64[ns]")
    return pd.to_datetime(df[col], errors="coerce")


def main() -> None:
    st.set_page_config(page_title="XLM Lakehouse Dashboard", layout="wide")
    st.title("XLM Lakehouse Dashboard")

    st.caption(f"Lake root: {lake_root().as_posix()}")

    try:
        daily = load_parquet_table("xlm_daily").copy()
        hourly = load_parquet_table("xlm_hourly").copy()
        by_country = load_parquet_table("xlm_by_country_daily").copy()
    except Exception as e:
        st.error(str(e))
        st.info("Rode primeiro: python 04_analytics_gold/transform_gold.py para gerar os Parquets em data_lake/gold/parquet/.")
        return

    daily["day_ts"] = as_datetime(daily, "day_ts")
    hourly["hour_ts"] = as_datetime(hourly, "hour_ts")
    by_country["day_ts"] = as_datetime(by_country, "day_ts")

    daily = daily.dropna(subset=["day_ts"]).sort_values("day_ts")
    hourly = hourly.dropna(subset=["hour_ts"]).sort_values("hour_ts")
    by_country = by_country.dropna(subset=["day_ts"]).sort_values(["day_ts", "country_code"])

    min_day = daily["day_ts"].min()
    max_day = daily["day_ts"].max()

    with st.sidebar:
        st.header("Filtros")
        if pd.isna(min_day) or pd.isna(max_day):
            date_range = None
        else:
            date_range = st.date_input(
                "Intervalo de datas (diário)",
                value=(min_day.date(), max_day.date()),
                min_value=min_day.date(),
                max_value=max_day.date(),
            )

        top_n = st.slider("Top N países", min_value=5, max_value=30, value=10, step=1)

    if date_range and isinstance(date_range, tuple) and len(date_range) == 2:
        start_day, end_day = date_range
        daily_f = daily[(daily["day_ts"].dt.date >= start_day) & (daily["day_ts"].dt.date <= end_day)]
        by_country_f = by_country[(by_country["day_ts"].dt.date >= start_day) & (by_country["day_ts"].dt.date <= end_day)]
        hourly_f = hourly
    else:
        daily_f = daily
        by_country_f = by_country
        hourly_f = hourly

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        st.metric("Tx count (período)", int(daily_f["tx_count"].sum()) if "tx_count" in daily_f.columns else 0)
    with kpi2:
        st.metric(
            "Total volume XLM (período)",
            float(daily_f["total_volume_xlm"].sum()) if "total_volume_xlm" in daily_f.columns else 0.0,
        )
    with kpi3:
        st.metric(
            "Total notional USD (período)",
            float(daily_f["total_notional_usd"].sum()) if "total_notional_usd" in daily_f.columns else 0.0,
        )
    with kpi4:
        st.metric(
            "Preço médio USD (período)",
            float(daily_f["avg_price_usd"].mean()) if "avg_price_usd" in daily_f.columns else 0.0,
        )

    c1, c2 = st.columns(2)
    with c1:
        fig_price = px.line(
            daily_f,
            x="day_ts",
            y="avg_price_usd",
            title="Preço médio (USD) por dia",
            markers=True,
        )
        st.plotly_chart(fig_price, use_container_width=True)

    with c2:
        fig_volume = px.bar(
            daily_f,
            x="day_ts",
            y="total_volume_xlm",
            title="Volume total (XLM) por dia",
        )
        st.plotly_chart(fig_volume, use_container_width=True)

    st.subheader("Análise por país (diária)")
    if "country_code" in by_country_f.columns and "total_notional_usd" in by_country_f.columns:
        by_country_agg = (
            by_country_f.groupby("country_code", as_index=False)
            .agg(
                total_notional_usd=("total_notional_usd", "sum"),
                total_volume_xlm=("total_volume_xlm", "sum"),
                tx_count=("tx_count", "sum"),
            )
            .sort_values("total_notional_usd", ascending=False)
            .head(top_n)
        )
        fig_country = px.bar(
            by_country_agg,
            x="country_code",
            y="total_notional_usd",
            title=f"Top {top_n} países por notional (USD)",
        )
        st.plotly_chart(fig_country, use_container_width=True)

    st.subheader("Tabelas (amostra)")
    t1, t2, t3 = st.columns(3)
    with t1:
        st.write("xlm_daily")
        st.dataframe(daily_f.tail(25), use_container_width=True, hide_index=True)
    with t2:
        st.write("xlm_hourly")
        st.dataframe(hourly_f.tail(25), use_container_width=True, hide_index=True)
    with t3:
        st.write("xlm_by_country_daily")
        st.dataframe(by_country_f.tail(25), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
