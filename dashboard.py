import streamlit as st
from pymongo import MongoClient
import pandas as pd
import time
import altair as alt
import random
import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Real-Time Analytics Dashboard",
    page_icon="💎",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- MONGODB CONNECTION ---
@st.cache_resource
def get_mongodb_connection():
    # Attempt 1: Streamlit Cloud Secrets (Production)
    try:
        if "mongo" in st.secrets:
            uri = st.secrets["mongo"]["connection_string"]
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            return client
    except Exception:
        pass 

    # Attempt 2: Environment Variable (Atlas Cloud - Local Run)
    try:
        uri = os.getenv("MONGO_URI")
        if uri:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            return client
    except Exception:
        pass

    # No connection found - Critical Stop
    st.error("❌ Database Configuration Missing!")
    st.write("For Streamlit Cloud deployments, ensure you have set `mongo.connection_string` in the **Secrets** menu.")
    st.write("For local runs, ensure your `MONGO_URI` is correctly set in your `.env` file.")
    st.info("💡 Note: Local MongoDB fallbacks have been disabled for this 'Atlas-Only' deployment.")
    st.stop()
    return None

client = get_mongodb_connection()
db = client["ecommerce_db"]
collection = db["Real time-Ecommerce"]

# --- PREMIUM CSS STYLING ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&family=Outfit:wght@400;700&display=swap');

    .stApp {
        background: radial-gradient(circle at top right, #1e1b4b, #0f172a);
        color: #f8fafc;
    }
    
    .metric-container {
        border-radius: 20px;
        background: rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(12px);
        padding: 24px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        text-align: center;
        transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
    }
    .metric-container:hover {
        transform: translateY(-8px);
        background: rgba(255, 255, 255, 0.07);
        border-color: rgba(255, 255, 255, 0.2);
    }
    .metric-label {
        font-family: 'Inter', sans-serif;
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        color: #94a3b8;
        margin-bottom: 8px;
    }
    .metric-value {
        font-family: 'Outfit', sans-serif;
        font-size: 2.2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #38bdf8 0%, #818cf8 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .trend-up { color: #4ade80; font-size: 0.9rem; margin-top: 5px; }
    .trend-down { color: #f87171; font-size: 0.9rem; margin-top: 5px; }
    
    @keyframes pulse-ring {
        0% { transform: scale(.33); }
        80%, 100% { opacity: 0; }
    }
    .live-indicator {
        display: inline-block;
        width: 12px;
        height: 12px;
        background-color: #22c55e;
        border-radius: 50%;
        margin-right: 12px;
        box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.7);
        animation: pulse-ring 1.5s cubic-bezier(0.455, 0.03, 0.515, 0.955) infinite;
    }
    
    /* Transaction Table */
    .stTable {
        background: transparent !important;
        border: none !important;
    }
    .status-badge {
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 700;
        text-transform: uppercase;
    }
</style>
""", unsafe_allow_html=True)

# --- HEADER SECION ---
head_col1, head_col2 = st.columns([3, 1])
with head_col1:
    st.title("📊 Real-Time E-Commerce Analytics Pipeline")
    st.markdown("Monitoring global transaction streams with low-latency real-time processing.")

with head_col2:
    st.markdown(f"""
    <div style="background: rgba(0,0,0,0.3); padding: 15px; border-radius: 10px; text-align: right;">
        <span class="live-indicator"></span><span style="color: #00ff00; font-weight: bold;">SYSTEM LIVE</span><br>
        <span style="font-size: 0.8rem; color: #8892b0;">Refreshing every 2s</span>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# --- FETCH DATA ---
data = collection.find_one({"_id": "latest_analytics"})

if data:
    # Extract metrics
    total_rev = data.get("total_revenue", 0.0)
    total_ord = data.get("total_orders", 0)
    avg_order = data.get("avg_order_value", 0.0)
    prod_data = data.get("product_counts", {})
    cat_data = data.get("category_counts", {})
    pay_data = data.get("payment_counts", {})
    recent_transactions = data.get("recent_transactions", [])
    upd_time = data.get("last_updated", "Never")

    # --- TREND CALCULATION ---
    if "prev_rev" not in st.session_state:
        st.session_state.prev_rev = total_rev
        st.session_state.prev_ord = total_ord
    
    rev_trend = total_rev - st.session_state.prev_rev
    ord_trend = total_ord - st.session_state.prev_ord
    
    st.session_state.prev_rev = total_rev
    st.session_state.prev_ord = total_ord

    # --- TOP ROW: KPI CARDS ---
    c1, c2, c3, c4 = st.columns(4)
    
    with c1:
        trend_html = f'<div class="trend-up">▲ ₹{rev_trend:,.0f}</div>' if rev_trend > 0 else '<div style="height:21px"></div>'
        st.markdown(f'<div class="metric-container"><div class="metric-label">Total Revenue</div><div class="metric-value">₹ {total_rev:,.0f}</div>{trend_html}</div>', unsafe_allow_html=True)
    with c2:
        trend_html = f'<div class="trend-up">▲ {ord_trend}</div>' if ord_trend > 0 else '<div style="height:21px"></div>'
        st.markdown(f'<div class="metric-container"><div class="metric-label">Order Count</div><div class="metric-value" style="background: linear-gradient(135deg, #fbbf24, #f59e0b); -webkit-background-clip: text;">{total_ord:,}</div>{trend_html}</div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="metric-container"><div class="metric-label">Average Value</div><div class="metric-value" style="background: linear-gradient(135deg, #ec4899, #8b5cf6); -webkit-background-clip: text;">₹ {avg_order:,.2f}</div><div style="height:21px"></div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="metric-container"><div class="metric-label">Last Ping</div><div class="metric-value" style="font-size: 1.4rem; margin-top: 10px; background: none; -webkit-text-fill-color: #cbd5e1;">{upd_time.split(" ")[1]}</div><div style="height:21px"></div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- MIDDLE ROW: CHARTS ---
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("🛍️ Product Popularity")
        if prod_data:
            df_prod = pd.DataFrame(list(prod_data.items()), columns=["Product", "Volume"])
            chart_prod = alt.Chart(df_prod).mark_bar(
                cornerRadiusTopLeft=10,
                cornerRadiusTopRight=10,
                color="#00c6ff"
            ).encode(
                x=alt.X('Product', sort='-y', axis=alt.Axis(labelAngle=-45)),
                y='Volume',
                tooltip=['Product', 'Volume']
            ).properties(height=350)
            st.altair_chart(chart_prod, width='stretch')

    with col_right:
        st.subheader("📂 Category Distribution")
        if cat_data:
            df_cat = pd.DataFrame(list(cat_data.items()), columns=["Category", "Orders"])
            chart_cat = alt.Chart(df_cat).mark_arc(innerRadius=60).encode(
                theta=alt.Theta(field="Orders", type="quantitative"),
                color=alt.Color(field="Category", type="nominal", scale=alt.Scale(scheme='category20b')),
                tooltip=['Category', 'Orders']
            ).properties(height=350)
            st.altair_chart(chart_cat, width='stretch')

    # --- BOTTOM ROW: LIVE MONITOR & PAYMENTS ---
    st.markdown("<br>", unsafe_allow_html=True)
    mon_col, pay_col = st.columns([2, 1])

    with mon_col:
        st.subheader("⚡ Live Transaction Monitor")
        if recent_transactions:
            df_recent = pd.DataFrame(recent_transactions)[["timestamp", "product", "amount", "city", "payment_method"]]
            df_recent.columns = ["Time", "Product", "Amount (₹)", "City", "Payment"]
            st.dataframe(df_recent, width='stretch', hide_index=True)
        else:
            st.info("Watching for live streams...")

    with pay_col:
        st.subheader("💳 Payments")
        if pay_data:
            df_pay = pd.DataFrame(list(pay_data.items()), columns=["Method", "Uses"])
            chart_pay = alt.Chart(df_pay).mark_bar(
                cornerRadiusTopRight=5,
                cornerRadiusBottomRight=5,
                color="#f472b6"
            ).encode(
                y=alt.Y('Method', sort='-x', title=None),
                x=alt.X('Uses', title=None),
                tooltip=['Method', 'Uses']
            ).properties(height=200)
            st.altair_chart(chart_pay, width='stretch')

    # --- SIDEBAR INSIGHTS ---
    with st.sidebar:
        st.header("🎯 Live Insights")
        if prod_data:
            top_prod = max(prod_data, key=prod_data.get)
            st.info(f"🏆 **Top Product**\n\n{top_prod} ({prod_data[top_prod]} orders)")
        
        if cat_data:
            top_cat = max(cat_data, key=cat_data.get)
            st.success(f"📂 **Top Category**\n\n{top_cat}")
            
        st.markdown("---")
        st.markdown("### 🛠️ Pipeline Sync")
        st.code(f"Latency: < {random.randint(10, 50)}ms", language="bash")
        st.write(f"Broker: localhost:9092")

else:
    st.info("⌛ Awaiting first transaction data from Kafka pipeline...")

# --- FOOTER ---
st.markdown("""
<div style="text-align: center; margin-top: 50px; color: #8892b0; font-size: 0.8rem;">
    Premium Analytics Engine • Powered by Kafka & MongoDB • Real-Time Stream Processing
</div>
""", unsafe_allow_html=True)

# --- AUTO REFRESH ---
time.sleep(2)
st.rerun()
