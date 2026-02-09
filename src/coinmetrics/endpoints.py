"""Thin endpoint wrappers around `CoinMetricsClient` for convenience."""

from typing import List, Any, Dict

from src.coinmetrics.client import CoinMetricsClient


def fetch_assets(client: CoinMetricsClient) -> Dict[str, Any]:
    return client.get_catalog_assets()


def fetch_asset_metrics(client: CoinMetricsClient, asset: str, metrics: List[str], start: str, end: str, frequency: str = "1d") -> Dict[str, Any]:
    return client.get_asset_metrics(asset=asset, metrics=metrics, start=start, end=end, frequency=frequency)


__all__ = ["fetch_assets", "fetch_asset_metrics"]
