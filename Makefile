.PHONY: up down ingest dbt embed app

up:
	docker compose up -d

down:
	docker compose down

ingest:
	python pipeline/ingestion_pipeline.py

dbt:
	cd ucl_dbt && dbt run

embed:
	python pipeline/vector_pipeline.py

app:
	streamlit run main.py
