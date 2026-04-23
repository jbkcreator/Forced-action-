"""
Postgres checkpoint store for LangGraph.

Thin wrapper over langgraph.checkpoint.postgres.PostgresSaver. Uses the same
DATABASE_URL as the rest of the platform and writes its tables into a
dedicated schema (default: "langgraph") so checkpoint rows never mix with
application tables.

Usage:
	# One-time setup (idempotent) — run once before graphs execute
	from src.agents.checkpoint import run_checkpoint_migration
	run_checkpoint_migration()

	# Obtain a saver for graph compilation
	from src.agents.checkpoint import checkpoint_saver
	with checkpoint_saver() as saver:
		graph = builder.compile(checkpointer=saver)
		graph.invoke(initial_state, config={"configurable": {"thread_id": "..."}})
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

from langgraph.checkpoint.postgres import PostgresSaver
from psycopg import Connection, errors

from config.agents import get_agents_settings

logger = logging.getLogger(__name__)


def _agents_conn_string() -> str:
	"""
	Return the Postgres connection string the checkpoint store should use.

	Converts SQLAlchemy's `postgresql://...` or `postgresql+psycopg2://...`
	into a plain libpq URI that psycopg3 accepts.
	"""
	settings = get_agents_settings()
	url = settings.database_url
	if url.startswith("postgresql+psycopg2://"):
		url = "postgresql://" + url[len("postgresql+psycopg2://"):]
	elif url.startswith("postgresql+psycopg://"):
		url = "postgresql://" + url[len("postgresql+psycopg://"):]
	return url


def _ensure_schema(conn: Connection, schema: str) -> str:
	"""
	Ensure the checkpoint schema exists and is the default for this connection.

	Returns the schema name actually used. Falls back to "public" if the
	database user lacks CREATE privilege on the target schema.
	"""
	if schema == "public":
		with conn.cursor() as cur:
			cur.execute("SET search_path TO public")
		conn.commit()
		return "public"

	try:
		with conn.cursor() as cur:
			cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
			cur.execute(f'SET search_path TO "{schema}", public')
		conn.commit()
		return schema
	except errors.InsufficientPrivilege:
		conn.rollback()
		logger.warning(
			"Database user cannot create schema %r — falling back to 'public'. "
			"Grant CREATE ON DATABASE to use a dedicated checkpoint schema.",
			schema,
		)
		with conn.cursor() as cur:
			cur.execute("SET search_path TO public")
		conn.commit()
		return "public"


def run_checkpoint_migration() -> None:
	"""
	Idempotently create checkpoint tables in the agents schema.

	Call this once at agents-process startup (or via scripts/run_agents.py
	--migrate). Safe to run repeatedly.
	"""
	settings = get_agents_settings()
	requested = settings.agents_checkpoint_schema

	with Connection.connect(_agents_conn_string(), autocommit=True) as conn:
		effective = _ensure_schema(conn, requested)
		with conn.cursor() as cur:
			cur.execute(f'SET search_path TO "{effective}", public')

		saver = PostgresSaver(conn)
		saver.setup()
		logger.info("Checkpoint migration complete (schema=%s)", effective)


@contextmanager
def checkpoint_saver() -> Generator[PostgresSaver, None, None]:
	"""
	Context-managed PostgresSaver scoped to the agents checkpoint schema.

	Use this when compiling a graph:

		with checkpoint_saver() as saver:
			graph = builder.compile(checkpointer=saver)
			graph.invoke(...)
	"""
	settings = get_agents_settings()
	schema = settings.agents_checkpoint_schema

	with Connection.connect(_agents_conn_string()) as conn:
		effective = _ensure_schema(conn, schema)
		with conn.cursor() as cur:
			cur.execute(f'SET search_path TO "{effective}", public')
		yield PostgresSaver(conn)
