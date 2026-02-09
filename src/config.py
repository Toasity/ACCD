"""Configuration: read from environment and provide DB DSN.

This module exposes simple helpers used by the minimal ETL.
"""
import os

from typing import Dict


COINMETRICS_API_KEY = os.getenv("COINMETRICS_API_KEY", "")


def get_db_params() -> Dict[str, str]:
	"""Return DB connection params from environment with sensible defaults."""
	return {
		# In docker-compose the DB service is reachable as `db`
		"host": os.getenv("POSTGRES_HOST", "db"),
		"port": os.getenv("POSTGRES_PORT", "5432"),
		"dbname": os.getenv("POSTGRES_DB", "coinmetrics"),
		"user": os.getenv("POSTGRES_USER", "coinmetrics"),
		"password": os.getenv("POSTGRES_PASSWORD", "coinmetrics"),
	}


def get_db_dsn() -> str:
	"""Return a libpq-style DSN string for psycopg2."""
	p = get_db_params()
	return f"host={p['host']} port={p['port']} dbname={p['dbname']} user={p['user']} password={p['password']}"


def get_cm_config() -> Dict[str, str]:
	"""Return simple CoinMetrics-related defaults from env (used for params)."""
	return {
		"assets": os.getenv("CM_ASSETS", "btc,eth"),
		"metrics": os.getenv("CM_METRICS", "PriceUSD,TxCnt"),
		"start_date": os.getenv("CM_START_DATE", "2013-01-01"),
		"end_date": os.getenv("CM_END_DATE", "2015-12-31"),
		"frequency": os.getenv("CM_FREQUENCY", "1d"),
	}
