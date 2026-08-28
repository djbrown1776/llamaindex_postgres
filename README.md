# UCL Analytics RAG ⚽

A hybrid analytics platform and Retrieval-Augmented Generation (RAG) assistant for UEFA Champions League data.

## What It Does
* **Data Pipeline:** Ingests player and standings data from the BallDontLie API into PostgreSQL.
* **SQL Data Shaping:** Uses dbt to build analytical views and format structured text for embeddings.
* **Vector Search:** Embeds data using Mistral AI and stores vectors directly in PostgreSQL using `pgvector`.
* **User Interface:** A Streamlit web app providing Plotly analytics charts alongside an LLM chat assistant powered by LlamaIndex.

## Tech Stack
Python, PostgreSQL (`pgvector`), dbt, LlamaIndex, Mistral AI, Streamlit, Plotly.
