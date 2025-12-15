import io
import requests
import pandas as pd
import streamlit as st
import pyarrow.parquet as pq
from urllib.parse import urlparse, urlunparse

# ===== CONFIG =====
NAMENODE_HTTP = "http://localhost:9870"
HDFS_USER = "spark"
REFRESH_SECONDS = 10

BASE = "/tmp/ecommerce/metrics"
PATHS = {
    "sales": f"{BASE}/streaming_sales_per_minute",
    "cart": f"{BASE}/cart_per_minute",
    "top_products": f"{BASE}/top_products_per_minute",
    "conversion": f"{BASE}/conversion",
}

st.set_page_config(page_title="E-Commerce Realtime Dashboard", layout="wide")
st.title("📊 E-Commerce Realtime Dashboard (Kafka → Spark → HDFS)")
st.caption("Todo en un solo dashboard. Se actualiza automáticamente cada 10s leyendo Parquet desde HDFS (WebHDFS).")

# Auto-refresh
st.markdown(
    f"""
    <script>
    setTimeout(() => window.location.reload(), {REFRESH_SECONDS * 1000});
    </script>
    """,
    unsafe_allow_html=True
)

# ===== WebHDFS helpers =====
def webhdfs_url(path, op, **params):
    params["user.name"] = HDFS_USER
    q = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{NAMENODE_HTTP}/webhdfs/v1{path}?op={op}&{q}"

def list_status(hdfs_path: str):
    url = webhdfs_url(hdfs_path, "LISTSTATUS")
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()["FileStatuses"]["FileStatus"]

def list_part_parquets(hdfs_dir: str, limit: int = 30):
    """
    Lista solo part-*.parquet (ignora spark_metadata, etc).
    """
    files = []
    for s in list_status(hdfs_dir):
        name = s.get("pathSuffix", "")
        if s.get("type") == "FILE" and name.startswith("part-") and name.endswith(".parquet"):
            files.append(f"{hdfs_dir}/{name}")
    return sorted(files)[-limit:]

def rewrite_datanode_location(location_url: str) -> str:
    """
    Location viene como http://<container-id>:9864/...
    En el host, debes usar localhost:9864 (porque está publicado).
    """
    u = urlparse(location_url)
    port = u.port or 9864
    fixed = u._replace(netloc=f"localhost:{port}")
    return urlunparse(fixed)

def read_parquet_from_webhdfs_file(hdfs_file: str) -> pd.DataFrame:
    """
    OPEN con noredirect=true devuelve JSON con Location.
    Luego descargamos desde localhost:9864.
    """
    url = webhdfs_url(hdfs_file, "OPEN", noredirect="true")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    j = r.json()
    if "Location" not in j:
        raise RuntimeError(f"Respuesta inesperada WebHDFS: {j}")

    dn_url = rewrite_datanode_location(j["Location"])
    data = requests.get(dn_url, timeout=60)
    data.raise_for_status()

    table = pq.read_table(io.BytesIO(data.content))
    return table.to_pandas()

def load_metric_dir(hdfs_dir: str, read_last_n_files: int = 10) -> pd.DataFrame | None:
    """
    Carga los últimos N part-*.parquet del directorio. Si no existe / no hay data, retorna None.
    """
    try:
        files = list_part_parquets(hdfs_dir, limit=50)
        if not files:
            return None
        dfs = []
        for f in files[-read_last_n_files:]:
            try:
                dfs.append(read_parquet_from_webhdfs_file(f))
            except Exception:
                # puede fallar si un archivo está a medio escribir; seguimos
                pass
        if not dfs:
            return None
        return pd.concat(dfs, ignore_index=True)
    except Exception:
        return None

# ===== LOAD DATA =====
df_sales = load_metric_dir(PATHS["sales"], read_last_n_files=15)
df_cart = load_metric_dir(PATHS["cart"], read_last_n_files=15)
df_top = load_metric_dir(PATHS["top_products"], read_last_n_files=15)
df_conv = load_metric_dir(PATHS["conversion"], read_last_n_files=15)

# ===== KPIs =====
k1, k2, k3, k4, k5 = st.columns(5)

if df_sales is not None and {"total_sales","orders_count"}.issubset(df_sales.columns):
    total_sales = float(df_sales["total_sales"].fillna(0).sum())
    total_orders = int(df_sales["orders_count"].fillna(0).sum())
    k1.metric("💰 Ventas (sum)", f"${total_sales:,.2f}")
    k2.metric("🧾 Órdenes (sum)", f"{total_orders}")
else:
    k1.metric("💰 Ventas (sum)", "—")
    k2.metric("🧾 Órdenes (sum)", "—")

if df_cart is not None and {"event_type","count"}.issubset(df_cart.columns):
    cart_total = int(df_cart["count"].fillna(0).sum())
    adds = int(df_cart[df_cart["event_type"]=="add_to_cart"]["count"].fillna(0).sum()) if "add_to_cart" in set(df_cart["event_type"]) else 0
    rems = int(df_cart[df_cart["event_type"]=="remove_from_cart"]["count"].fillna(0).sum()) if "remove_from_cart" in set(df_cart["event_type"]) else 0
    k3.metric("🛒 Carrito (sum)", f"{cart_total}")
    k4.metric("➕ Add", f"{adds}")
    k5.metric("➖ Remove", f"{rems}")
else:
    k3.metric("🛒 Carrito (sum)", "—")
    k4.metric("➕ Add", "—")
    k5.metric("➖ Remove", "—")

st.divider()

# ===== SECTION 1: SALES =====
st.header("1) Ventas por minuto")
if df_sales is None:
    st.warning("No hay datos de ventas todavía. Asegúrate de tener corriendo el job streaming_sales_per_minute.")
else:
    # normaliza
    if "minute_start" in df_sales.columns:
        df_sales["minute_start"] = pd.to_datetime(df_sales["minute_start"])
        df_sales = df_sales.sort_values("minute_start")

    c1, c2 = st.columns(2)
    with c1:
        if {"minute_start","total_sales"}.issubset(df_sales.columns):
            st.line_chart(df_sales.set_index("minute_start")[["total_sales"]], height=320)
    with c2:
        if {"minute_start","orders_count"}.issubset(df_sales.columns):
            st.bar_chart(df_sales.set_index("minute_start")[["orders_count"]], height=320)

    st.dataframe(df_sales.tail(30), use_container_width=True)

st.divider()

# ===== SECTION 2: CART =====
st.header("2) Eventos de carrito por minuto")
if df_cart is None:
    st.info("Métrica no disponible aún (cart_per_minute). Si quieres, levanta el job de carrito y aparecerá aquí.")
else:
    if "minute_start" in df_cart.columns:
        df_cart["minute_start"] = pd.to_datetime(df_cart["minute_start"])
        df_cart = df_cart.sort_values("minute_start")

    # Pivot para add/remove por minuto
    if {"minute_start","event_type","count"}.issubset(df_cart.columns):
        piv = df_cart.pivot_table(index="minute_start", columns="event_type", values="count", aggfunc="sum").fillna(0)
        st.area_chart(piv, height=320)
        st.dataframe(df_cart.tail(30), use_container_width=True)
    else:
        st.dataframe(df_cart.tail(30), use_container_width=True)

st.divider()

# ===== SECTION 3: TOP PRODUCTS =====
st.header("3) Top productos más vistos (por minuto)")
if df_top is None:
    st.info("Métrica no disponible aún (top_products_per_minute). Si levantas el job de top productos, aparece aquí.")
else:
    # Esperado: product_id, views, minute_start (o window.start)
    # Si no hay minute_start, igual mostramos tabla
    if "minute_start" in df_top.columns:
        df_top["minute_start"] = pd.to_datetime(df_top["minute_start"])
        last_min = df_top["minute_start"].max()
        current = df_top[df_top["minute_start"] == last_min].copy()
    else:
        current = df_top.copy()

    # Intentar top 5 por views
    if {"product_id","views"}.issubset(current.columns):
        top5 = (current.groupby("product_id", as_index=False)["views"].sum()
                .sort_values("views", ascending=False)
                .head(5))
        st.subheader("Top 5 (último minuto disponible)")
        st.bar_chart(top5.set_index("product_id")[["views"]], height=280)
        st.dataframe(top5, use_container_width=True)
    st.subheader("Raw (últimos registros)")
    st.dataframe(df_top.tail(30), use_container_width=True)

st.divider()

# ===== SECTION 4: CONVERSION =====
st.header("4) Conversión (vista → compra) por minuto")
if df_conv is None:
    st.info("Métrica no disponible aún (conversion). Si levantas el job de conversion, aparece aquí.")
else:
    if "minute_start" in df_conv.columns:
        df_conv["minute_start"] = pd.to_datetime(df_conv["minute_start"])
        df_conv = df_conv.sort_values("minute_start")

    if {"minute_start","event_type","count"}.issubset(df_conv.columns):
        piv = df_conv.pivot_table(index="minute_start", columns="event_type", values="count", aggfunc="sum").fillna(0)

        # conversion_rate = purchases / page_views
        pv = piv["page_view"] if "page_view" in piv.columns else 0
        pu = piv["purchase"] if "purchase" in piv.columns else 0
        conv_rate = (pu / (pv.replace(0, pd.NA))).fillna(0)

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Eventos por minuto")
            st.line_chart(piv, height=320)
        with c2:
            st.subheader("Tasa de conversión (purchase / page_view)")
            st.line_chart(conv_rate, height=320)

        st.dataframe(df_conv.tail(30), use_container_width=True)
    else:
        st.dataframe(df_conv.tail(30), use_container_width=True)

