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
def get_source_chain_data(_conn, start_date, end_date):
    query = f"""
    with overview as (
WITH axelar_service AS (
  
  SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, 

    CASE 
      WHEN IS_ARRAY(data:send:amount) THEN NULL
      WHEN IS_OBJECT(data:send:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
      WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
      ELSE NULL
    END AS amount_usd,

    CASE 
      WHEN IS_ARRAY(data:send:fee_value) THEN NULL
      WHEN IS_OBJECT(data:send:fee_value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
      ELSE NULL
    END AS fee,

    id, 
    'Token Transfers' AS "Service", 
    data:link:asset::STRING AS raw_asset

  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed'
    AND simplified_status = 'received'
    

  UNION ALL

  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id, 
    'GMP' AS "Service", 
    data:symbol::STRING AS raw_asset

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    )

SELECT created_at, id, user, source_chain, destination_chain,
     "Service", amount, amount_usd, fee, raw_asset

FROM axelar_service)

select source_chain as "📤Source Chain", count(distinct id) as "🚀Transfers",
count(distinct user) as "👥Users", round(sum(amount_usd),1) as "💸Volume($)",
round(avg(amount_usd),1) as "📊Avg Volume($)", round(sum(fee),1) as "⛽Fees($)",
round(avg(fee),5) as "💨Avg Fee($)", count(distinct destination_chain) as "📥#Dest Chains",
count(distinct raw_asset) as "💎#Tokens"
from overview
    WHERE created_at::date >= '{start_date}' AND created_at::date <= '{end_date}'
    GROUP BY 1
    ORDER BY 2 DESC
    """
    df = pd.read_sql(query, _conn)
    return df

# --- Load Data from Snowflake ---------------------------------------------------------------------------------
df_source_chains = get_source_chain_data(conn, start_date, end_date)

# --- Format Numbers and Reset Index Starting from 1 ------------------------------------------------------------
df_display = df_source_chains.copy()
for col in df_display.columns[1:]:
    if col == "💨Avg Fee($)":
        df_display[col] = df_display[col].apply(lambda x: f"{x:,.3f}" if pd.notnull(x) else "-")
    else:
        df_display[col] = df_display[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "-")
df_display.index = range(1, len(df_display)+1)

# --- Display Table ------------------------------------------------------------------------------------------------
st.subheader("Monitoring Source Chains")
st.dataframe(df_display.head(10)) 

# --- KPIs --------------------------------------------------------------------------------------------------------

top_transfers = df_source_chains.loc[df_source_chains["🚀Transfers"].idxmax()]
top_users = df_source_chains.loc[df_source_chains["👥Users"].idxmax()]
top_volume = df_source_chains.loc[df_source_chains["💸Volume($)"].idxmax()]

top_fees = df_source_chains.loc[df_source_chains["⛽Fees($)"].idxmax()]
top_dest_chains = df_source_chains.loc[df_source_chains["📥#Dest Chains"].idxmax()]
top_tokens = df_source_chains.loc[df_source_chains["💎#Tokens"].idxmax()]

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Top Source Chain by Transfers Count", f"{top_transfers['📤Source Chain']} ({top_transfers['🚀Transfers']:,})")
with col2:
    st.metric("Top Source Chain by Users Count", f"{top_users['📤Source Chain']} ({top_users['👥Users']:,})")
with col3:
    st.metric("Top Source Chain by Transfers Volume (USD)", f"{top_volume['📤Source Chain']} (${top_volume['💸Volume($)']:,})")

col4, col5, col6 = st.columns(3)
with col4:
    st.metric("Top Source Chain by Transfer Fees (USD)", f"{top_fees['📤Source Chain']} (${top_fees['⛽Fees($)']:,})")
with col5:
    st.metric("Top Source Chain by Number of Destination Chains", f"{top_dest_chains['📤Source Chain']} ({top_dest_chains['📥#Dest Chains']:,})")
with col6:
    st.metric("Top Source Chain by Number of Tokens Transferred", f"{top_tokens['📤Source Chain']} ({top_tokens['💎#Tokens']:,})")

