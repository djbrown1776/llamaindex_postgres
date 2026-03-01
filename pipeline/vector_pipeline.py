import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine, text
from llama_index.core import Settings, Document, VectorStoreIndex, StorageContext
from llama_index.core.embeddings import BaseEmbedding
from llama_index.llms.mistralai import MistralAI
from llama_index.vector_stores.postgres import PGVectorStore
from typing import List

from config import MISTRAL_API_KEY, DATABASE_URL, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

engine = create_engine(DATABASE_URL)

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {MISTRAL_API_KEY}",
    "Content-Type": "application/json"
})
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5,
    backoff_factor=2,       # waits 2s, 4s, 8s, 16s, 32s between retries
    status_forcelist=[429, 500, 502, 503]
)))

class MistralSessionEmbedding(BaseEmbedding):

    def _embed(self, texts: List[str]) -> List[List[float]]:
        response = session.post(
            "https://api.mistral.ai/v1/embeddings",
            json={
                "model": "mistral-embed",
                "input": texts
            }
        )
        response.raise_for_status()
        data = response.json()
        # Sort by index to ensure order matches input
        embeddings = sorted(data["data"], key=lambda x: x["index"])
        return [e["embedding"] for e in embeddings]

    def _get_text_embedding(self, text: str) -> List[float]:
        return self._embed([text])[0]

    def _get_query_embedding(self, query: str) -> List[float]:
        return self._embed([query])[0]

    def _get_text_embeddings(self, texts: List[str]) -> List[List[float]]:
        # Process in small batches to stay under rate limits
        all_embeddings = []
        batch_size = 10
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            all_embeddings.extend(self._embed(batch))
            time.sleep(0.5)  # small pause between batches
        return all_embeddings

    async def _aget_query_embedding(self, query: str) -> List[float]:
        return self._get_query_embedding(query)

    async def _aget_text_embedding(self, text: str) -> List[float]:
        return self._get_text_embedding(text)


# Models
llm = MistralAI(model="open-mistral-nemo", api_key=MISTRAL_API_KEY)
embed_model = MistralSessionEmbedding()

Settings.llm = llm
Settings.embed_model = embed_model
Settings.chunk_size = 512
Settings.chunk_overlap = 50

# Load Documents from dbt Mart Views
def load_documents_from_postgres():
    with engine.connect() as conn:
        players = conn.execute(text("SELECT * FROM mart_players_for_embedding")).mappings().all()
        standings = conn.execute(text("SELECT * FROM mart_standings_for_embedding")).mappings().all()

    documents = []

    for row in players:
        doc = Document(
            text=row["embedding_text"],
            metadata={
                "source": "players",
                "id": str(row["id"]),
                "display_name": row["display_name"]
            }
        )
        documents.append(doc)

    for row in standings:
        doc = Document(
            text=row["embedding_text"],
            metadata={
                "source": "standings",
                "team_id": str(row["team_id"]),
                "team_name": row["team_name"],
                "season": str(row["season"])
            }
        )
        documents.append(doc)

    print(f"Loaded {len(documents)} documents from Postgres")
    return documents

# Build Index in Batches
def build_index(documents, batch_size=100):
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

    total = len(documents)
    print(f"Embedding {total} documents in batches of {batch_size}...")

    for i in range(0, total, batch_size):
        batch = documents[i:i + batch_size]
        VectorStoreIndex.from_documents(
            batch,
            storage_context=storage_context,
            show_progress=False
        )
        print(f"✅ Batch {i // batch_size + 1} done ({i + len(batch)}/{total})")
        time.sleep(1)

    print("✅ Done — all embeddings stored in Postgres.")

if __name__ == "__main__":
    docs = load_documents_from_postgres()
    build_index(docs)
