import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from llama_index.core import Settings, VectorStoreIndex, StorageContext
from llama_index.llms.mistralai import MistralAI
from llama_index.embeddings.mistralai import MistralAIEmbedding
from llama_index.vector_stores.postgres import PGVectorStore

from config import MISTRAL_API_KEY, DATABASE_URL, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

engine = create_engine(DATABASE_URL)

# Models
llm = MistralAI(model="open-mistral-nemo", api_key=MISTRAL_API_KEY)
embed_model = MistralAIEmbedding(
    model_name="mistral-embed",
    api_key=MISTRAL_API_KEY,
    embed_batch_size=50
)

Settings.llm = llm
Settings.embed_model = embed_model
Settings.chunk_size = 512
Settings.chunk_overlap = 50

# Preset Charts
CHARTS = {
    "Goals For by Team (Bar)": {
        "sql": "SELECT team_name, SUM(goals_for) as goals_for FROM ucl_standings GROUP BY team_name ORDER BY goals_for DESC LIMIT 20",
        "type": "bar",
        "x": "team_name",
        "y": "goals_for",
        "title": "Goals For by Team"
    },
    "Points by Team (Bar)": {
        "sql": "SELECT team_name, SUM(points) as points FROM ucl_standings GROUP BY team_name ORDER BY points DESC LIMIT 20",
        "type": "bar",
        "x": "team_name",
        "y": "points",
        "title": "Points by Team"
    },
    "Wins by Team (Bar)": {
        "sql": "SELECT team_name, SUM(wins) as wins FROM ucl_standings GROUP BY team_name ORDER BY wins DESC LIMIT 20",
        "type": "bar",
        "x": "team_name",
        "y": "wins",
        "title": "Wins by Team"
    },
    "Team Points Across Seasons (Line)": {
        "sql": "SELECT season, SUM(points) as total_points FROM ucl_standings GROUP BY season ORDER BY season ASC",
        "type": "line",
        "x": "season",
        "y": "total_points",
        "title": "Total Points Across Seasons"
    },
    "Goals For Across Seasons (Line)": {
        "sql": "SELECT season, SUM(goals_for) as total_goals FROM ucl_standings GROUP BY season ORDER BY season ASC",
        "type": "line",
        "x": "season",
        "y": "total_goals",
        "title": "Total Goals For Across Seasons"
    }
}

# Load Vector Index
@st.cache_resource
def load_vector_index():
    vector_store = PGVectorStore.from_params(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        table_name="embeddings",
        embed_dim=1024
    )
    storage_context = StorageContext.from_defaults(vector_store=vector_store)
    index = VectorStoreIndex.from_vector_store(
        vector_store,
        storage_context=storage_context
    )
    return index

# Render Chart
def render_chart(chart_config: dict):
    with engine.connect() as conn:
        df = pd.read_sql(text(chart_config["sql"]), conn)

    if df.empty:
        st.warning("No data found.")
        return

    if chart_config["type"] == "bar":
        fig = px.bar(
            df,
            x=chart_config["x"],
            y=chart_config["y"],
            title=chart_config["title"],
            color=chart_config["y"],
            color_continuous_scale="blues"
        )
    else:
        fig = px.line(
            df,
            x=chart_config["x"],
            y=chart_config["y"],
            title=chart_config["title"],
            markers=True
        )

    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

# App
index = load_vector_index()
vector_engine = index.as_query_engine(similarity_top_k=5)

st.set_page_config(page_title="UCL Assistant", page_icon="⚽")
st.title("UCL Assistant ⚽")
st.caption("Ask me anything about UCL players and standings.")

# Charts Section
st.subheader("📊 Charts")
selected_chart = st.selectbox("Select a chart", list(CHARTS.keys()))
if st.button("Generate Chart"):
    render_chart(CHARTS[selected_chart])

st.divider()

# Chat Section
st.subheader("💬 Ask a Question")

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])

if prompt := st.chat_input("Ask a question about UCL..."):
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.write(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            response = vector_engine.query(prompt)
            answer = str(response)
        st.write(answer)
        st.session_state.messages.append({
            "role": "assistant",
            "content": answer
        })
