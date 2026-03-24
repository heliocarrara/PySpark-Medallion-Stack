from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components


def lake_root() -> Path:
    env_value = os.environ.get("LAKE_ROOT")
    if env_value:
        return Path(env_value).expanduser().resolve()

    script_dir = Path(__file__).resolve().parent
    return (script_dir.parent / "data_lake").resolve()


def parquet_table_path(table_name: str) -> Path:
    root = lake_root() / "gold" / "parquet"
    direct = root / table_name
    if direct.exists():
        return direct
    candidates = sorted(root.glob(f"{table_name}__*"), reverse=True)
    if candidates:
        return candidates[0]
    return direct


def find_parquet_files(path: Path) -> list[Path]:
    parquet_files: list[Path] = []
    for p in path.rglob("*.parquet"):
        if any(part.startswith("_") for part in p.parts):
            continue
        parquet_files.append(p)
    return parquet_files

def silver_parquet_table_path(table_name: str) -> Path:
    root = lake_root() / "silver" / "parquet"
    direct = root / table_name
    if direct.exists():
        return direct
    candidates = sorted(root.glob(f"{table_name}__*"), reverse=True)
    if candidates:
        return candidates[0]
    return direct


@st.cache_data(show_spinner=False, ttl=60)
def load_parquet_table(table_name: str) -> pd.DataFrame:
    root = lake_root() / "gold" / "parquet"
    path = parquet_table_path(table_name)
    if not path.exists():
        raise FileNotFoundError(f"Não encontrei a tabela '{table_name}' em: {path.as_posix()}")
    if path.is_file() and path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)

    if not path.is_dir():
        raise FileNotFoundError(f"Esperava um diretório ou arquivo .parquet, mas encontrei: {path.as_posix()}")

    parquet_files = find_parquet_files(path)
    if not parquet_files:
        candidates = sorted(root.glob(f"{table_name}__*"), reverse=True)
        for c in candidates:
            if not c.is_dir():
                continue
            candidate_files = find_parquet_files(c)
            if candidate_files:
                return pd.read_parquet(candidate_files)
        raise FileNotFoundError(f"Nenhum arquivo .parquet encontrado em: {path.as_posix()}")

    return pd.read_parquet(parquet_files)

@st.cache_data(show_spinner=False, ttl=15)
def load_silver_parquet_ticks() -> pd.DataFrame:
    path = silver_parquet_table_path("xlm_price_ticks")
    if not path.exists():
        return pd.DataFrame()
    if path.is_file() and path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if not path.is_dir():
        return pd.DataFrame()
    files = find_parquet_files(path)
    if not files:
        # tentar versões com run_id
        candidates = sorted((lake_root() / "silver" / "parquet").glob("xlm_price_ticks__*"), reverse=True)
        for c in candidates:
            cf = find_parquet_files(c)
            if cf:
                return pd.read_parquet(cf)
        return pd.DataFrame()
    return pd.read_parquet(files)


def as_datetime(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([], dtype="datetime64[ns]")
    return pd.to_datetime(df[col], errors="coerce")


def main() -> None:
    st.set_page_config(page_title="XLM Lakehouse Dashboard", layout="wide")
    st.title("XLM Lakehouse Dashboard")

    st.caption(f"Lake root: {lake_root().as_posix()}")

    with st.sidebar:
        st.header("Atualização")
        refresh_sec = st.number_input("Auto-refresh (segundos)", min_value=5, max_value=60, value=10, step=1)
        try:
            from streamlit_autorefresh import st_autorefresh  # type: ignore
            st_autorefresh(interval=refresh_sec * 1000, key="auto-refresh-prices")
        except Exception:
            components.html(f"<script>setTimeout(function(){{window.location.reload();}}, {int(refresh_sec)*1000});</script>", height=0)
        st.caption("Ative auto-refresh para ver os preços atualizarem.")

    try:
        daily = load_parquet_table("xlm_daily").copy()
        hourly = load_parquet_table("xlm_hourly").copy()
        by_country = load_parquet_table("xlm_by_country_daily").copy()
    except Exception as e:
        st.error(str(e))
        st.info("Rode primeiro: python 04_analytics_gold/transform_gold.py para gerar os Parquets em data_lake/gold/parquet/.")
        return
    try:
        price_hourly_by_exchange = load_parquet_table("xlm_price_hourly_by_exchange").copy()
        price_daily_by_exchange = load_parquet_table("xlm_price_daily_by_exchange").copy()
        prices_available = True
    except Exception:
        price_hourly_by_exchange = pd.DataFrame()
        price_daily_by_exchange = pd.DataFrame()
        prices_available = False

    ticks = load_silver_parquet_ticks().copy()
    ticks["event_ts"] = as_datetime(ticks, "event_ts")
    ticks = ticks.dropna(subset=["event_ts"]).sort_values(["event_ts", "exchange"])
    # limitar à janela recente para gráfico mais útil
    try:
        latest_ts = ticks["event_ts"].max()
        if pd.notna(latest_ts):
            recent_mask = ticks["event_ts"] >= (latest_ts - pd.Timedelta(hours=2))
            ticks_recent = ticks[recent_mask]
        else:
            ticks_recent = ticks
    except Exception:
        ticks_recent = ticks

    daily["day_ts"] = as_datetime(daily, "day_ts")
    hourly["hour_ts"] = as_datetime(hourly, "hour_ts")
    by_country["day_ts"] = as_datetime(by_country, "day_ts")
    if prices_available:
        price_hourly_by_exchange["hour_ts"] = as_datetime(price_hourly_by_exchange, "hour_ts")
        price_daily_by_exchange["day_ts"] = as_datetime(price_daily_by_exchange, "day_ts")

    daily = daily.dropna(subset=["day_ts"]).sort_values("day_ts")
    hourly = hourly.dropna(subset=["hour_ts"]).sort_values("hour_ts")
    by_country = by_country.dropna(subset=["day_ts"]).sort_values(["day_ts", "country_code"])
    if prices_available:
        price_hourly_by_exchange = price_hourly_by_exchange.dropna(subset=["hour_ts"]).sort_values(["hour_ts", "exchange"])
        price_daily_by_exchange = price_daily_by_exchange.dropna(subset=["day_ts"]).sort_values(["day_ts", "exchange"])

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
        if prices_available and "exchange" in price_daily_by_exchange.columns:
            exchanges = sorted([x for x in price_daily_by_exchange["exchange"].dropna().unique().tolist() if isinstance(x, str)])
            selected_exchanges = st.multiselect("Exchanges (preços)", options=exchanges, default=exchanges)
        else:
            selected_exchanges = None

    # seção principal focada em preços reais
    st.subheader("Preços em tempo quase real (APIs a cada ~10s)")
    if ticks_recent.empty:
        st.info("Sem ticks de preço em Silver. Verifique producer (10s) e ingestão Bronze (1min), depois rode Silver.")
    else:
        if selected_exchanges:
            ticks_recent = ticks_recent[ticks_recent["exchange"].isin(selected_exchanges)]
        fig_ticks = px.line(
            ticks_recent,
            x="event_ts",
            y="last_price",
            color="exchange" if "exchange" in ticks_recent.columns else None,
            title="Últimos preços (ticks) por exchange",
        )
        st.plotly_chart(fig_ticks, use_container_width=True)
        # KPIs de último preço por exchange
        latest_rows = (
            ticks_recent.sort_values("event_ts")
            .groupby("exchange", as_index=False)
            .tail(1)
            .sort_values("exchange")
        )
        kpis = st.columns(max(1, len(latest_rows)))
        for i, (_, row) in enumerate(latest_rows.iterrows()):
            with kpis[i]:
                st.metric(f"{row['exchange']} (último)", float(row["last_price"]))

    if date_range and isinstance(date_range, tuple) and len(date_range) == 2:
        start_day, end_day = date_range
        daily_f = daily[(daily["day_ts"].dt.date >= start_day) & (daily["day_ts"].dt.date <= end_day)]
        by_country_f = by_country[(by_country["day_ts"].dt.date >= start_day) & (by_country["day_ts"].dt.date <= end_day)]
        hourly_f = hourly
        if prices_available:
            price_daily_f = price_daily_by_exchange[
                (price_daily_by_exchange["day_ts"].dt.date >= start_day) & (price_daily_by_exchange["day_ts"].dt.date <= end_day)
            ]
            price_hourly_f = price_hourly_by_exchange[
                (price_hourly_by_exchange["hour_ts"].dt.date >= start_day) & (price_hourly_by_exchange["hour_ts"].dt.date <= end_day)
            ]
        else:
            price_daily_f = pd.DataFrame()
            price_hourly_f = pd.DataFrame()
    else:
        daily_f = daily
        by_country_f = by_country
        hourly_f = hourly
        price_daily_f = price_daily_by_exchange if prices_available else pd.DataFrame()
        price_hourly_f = price_hourly_by_exchange if prices_available else pd.DataFrame()

    if prices_available and selected_exchanges is not None and "exchange" in price_daily_f.columns:
        price_daily_f = price_daily_f[price_daily_f["exchange"].isin(selected_exchanges)]
    if prices_available and selected_exchanges is not None and "exchange" in price_hourly_f.columns:
        price_hourly_f = price_hourly_f[price_hourly_f["exchange"].isin(selected_exchanges)]

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

    st.subheader("Preços por exchange (APIs)")
    if not prices_available:
        st.info(
            "Tabelas de preços não encontradas. Rode: python 03_processing_silver/transform_silver.py e depois python 04_analytics_gold/transform_gold.py."
        )
    else:
        p1, p2 = st.columns(2)
        with p1:
            fig_api_daily = px.line(
                price_daily_f,
                x="day_ts",
                y="avg_price",
                color="exchange" if "exchange" in price_daily_f.columns else None,
                title="Preço médio por dia (por exchange)",
                markers=True,
            )
            st.plotly_chart(fig_api_daily, use_container_width=True)
        with p2:
            fig_api_hourly = px.line(
                price_hourly_f,
                x="hour_ts",
                y="avg_price",
                color="exchange" if "exchange" in price_hourly_f.columns else None,
                title="Preço médio por hora (por exchange)",
            )
            st.plotly_chart(fig_api_hourly, use_container_width=True)

    with st.expander("Simulação de transações (Silver/Gold)", expanded=False):
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
    t1, t2, t3, t4, t5 = st.columns(5)
    with t1:
        st.write("xlm_daily")
        st.dataframe(daily_f.tail(25), use_container_width=True, hide_index=True)
    with t2:
        st.write("xlm_hourly")
        st.dataframe(hourly_f.tail(25), use_container_width=True, hide_index=True)
    with t3:
        st.write("xlm_by_country_daily")
        st.dataframe(by_country_f.tail(25), use_container_width=True, hide_index=True)
    with t4:
        st.write("xlm_price_daily_by_exchange")
        if prices_available:
            st.dataframe(price_daily_f.tail(25), use_container_width=True, hide_index=True)
        else:
            st.dataframe(pd.DataFrame(), use_container_width=True, hide_index=True)
    with t5:
        st.write("xlm_price_ticks (Silver)")
        st.dataframe(ticks_recent.tail(25), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
