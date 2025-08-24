import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go
import plotly.express as px
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar: Bridging Blockchain Ecosystems",
    page_icon="https://img.cryptorank.io/coins/axelar1663924228506.png",
    layout="wide"
)

# --- Title  -----------------------------------------------------------------------------------------------------
st.title("🚀Cross-chain Transfers Analysis")

# --- attention ---------------------------------------------------------------------------------------------------------
st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Sidebar Footer Slightly Left-Aligned ---
st.sidebar.markdown(
    """
    <style>
    .sidebar-footer {
        position: fixed;
        bottom: 20px;
        width: 250px;
        font-size: 13px;
        color: gray;
        margin-left: 5px; # -- MOVE LEFT
        text-align: left;  
    }
    .sidebar-footer img {
        width: 16px;
        height: 16px;
        vertical-align: middle;
        border-radius: 50%;
        margin-right: 5px;
    }
    .sidebar-footer a {
        color: gray;
        text-decoration: none;
    }
    </style>

    <div class="sidebar-footer">
        <div>
            <a href="https://x.com/axelar" target="_blank">
                <img src="https://img.cryptorank.io/coins/axelar1663924228506.png" alt="Axelar Logo">
                Powered by Axelar
            </a>
        </div>
        <div style="margin-top: 5px;">
            <a href="https://x.com/0xeman_raz" target="_blank">
                <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz">
                Built by Eman Raz
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Snowflake Connection ----------------------------------------------------------------------------------------
snowflake_secrets = st.secrets["snowflake"]
user = snowflake_secrets["user"]
account = snowflake_secrets["account"]
private_key_str = snowflake_secrets["private_key"]
warehouse = snowflake_secrets.get("warehouse", "")
database = snowflake_secrets.get("database", "")
schema = snowflake_secrets.get("schema", "")

private_key_pem = f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----".encode("utf-8")
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
    backend=default_backend()
)
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# --- Date Inputs ---------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])

with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2025-01-01"))

with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-08-31"))

import streamlit as st
import pandas as pd

# --- Cached Query Execution ---------------------------------------------------------------------------------
@st.cache_data
def get_source_chain_data(conn, start_date, end_date):
    query = f"""
    WITH overview AS (
        -- همان کوئری طولانی شما در اینجا
    )
    SELECT source_chain as "📤Source Chain",
           COUNT(DISTINCT id) as "🚀Transfers Count",
           COUNT(DISTINCT user) as "👥Users Count",
           ROUND(SUM(amount_usd),1) as "💸Transfers Volume (USD)",
           ROUND(AVG(amount_usd),1) as "📊Avg Volume per Txn (USD)",
           ROUND(SUM(fee),1) as "⛽Transfer Fees (USD)",
           ROUND(AVG(fee),1) as "💨Avg Transfer Fee (USD)",
           COUNT(DISTINCT destination_chain) as "📥Number of Destination Chains",
           COUNT(DISTINCT raw_asset) as "💎Number of Tokens Transferred"
    FROM overview
    WHERE created_at::date >= '{start_date}' AND created_at::date <= '{end_date}'
    GROUP BY 1
    ORDER BY 2 DESC
    """
    df = pd.read_sql(query, conn)
    return df

# --- Load Data from Snowflake ---------------------------------------------------------------------------------
df_source_chains = get_source_chain_data(conn, start_date, end_date)

# --- Format Numbers and Reset Index Starting from 1 ------------------------------------------------------------
df_display = df_source_chains.copy()
for col in df_display.columns[1:]:
    df_display[col] = df_display[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "-")
df_display.index = range(1, len(df_display)+1)

# --- Display Table ------------------------------------------------------------------------------------------------
st.subheader("Monitoring Source Chains")
st.table(df_display)

# --- KPIs --------------------------------------------------------------------------------------------------------
# برترین Source Chainها
top_transfers = df_source_chains.loc[df_source_chains["🚀Transfers Count"].idxmax()]
top_users = df_source_chains.loc[df_source_chains["👥Users Count"].idxmax()]
top_volume = df_source_chains.loc[df_source_chains["💸Transfers Volume (USD)"].idxmax()]

top_fees = df_source_chains.loc[df_source_chains["⛽Transfer Fees (USD)"].idxmax()]
top_dest_chains = df_source_chains.loc[df_source_chains["📥Number of Destination Chains"].idxmax()]
top_tokens = df_source_chains.loc[df_source_chains["💎Number of Tokens Transferred"].idxmax()]

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Top Source Chain by Transfers Count", f"{top_transfers['📤Source Chain']} ({top_transfers['🚀Transfers Count']:,})")
with col2:
    st.metric("Top Source Chain by Users Count", f"{top_users['📤Source Chain']} ({top_users['👥Users Count']:,})")
with col3:
    st.metric("Top Source Chain by Transfers Volume (USD)", f"{top_volume['📤Source Chain']} (${top_volume['💸Transfers Volume (USD)']:,})")

col4, col5, col6 = st.columns(3)
with col4:
    st.metric("Top Source Chain by Transfer Fees (USD)", f"{top_fees['📤Source Chain']} (${top_fees['⛽Transfer Fees (USD)']:,})")
with col5:
    st.metric("Top Source Chain by Number of Destination Chains", f"{top_dest_chains['📤Source Chain']} ({top_dest_chains['📥Number of Destination Chains']:,})")
with col6:
    st.metric("Top Source Chain by Number of Tokens Transferred", f"{top_tokens['📤Source Chain']} ({top_tokens['💎Number of Tokens Transferred']:,})")

