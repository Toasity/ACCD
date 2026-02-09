"""Minimal CoinMetrics API client for Sprint1 / Task1 evidence.

This client is intentionally lightweight: it wraps `requests.Session` and
provides helper methods to call a couple of endpoints used in the course.
"""
from __future__ import annotations

from typing import Dict, Any, List, Optional
import json
import requests

from src.utils.logging import logger


def normalize_time(s: str) -> str:
    """Normalize a time string to strict ISO8601 UTC format.

    - If s is like 'YYYY-MM-DD', return 'YYYY-MM-DDT00:00:00Z'
    - If s contains 'T', return as-is (but normalize trailing '+00:00' to 'Z')
    - Raise ValueError on empty/None
    """
    if not s:
        raise ValueError("empty time string")
    s = str(s).strip()
    if not s:
        raise ValueError("empty time string")
    if len(s) == 10 and s.count("-") == 2 and "T" not in s:
        return f"{s}T00:00:00Z"
    if s.endswith("+00:00"):
        return s[:-6] + "Z"
    return s


class CoinMetricsError(RuntimeError):
    def __init__(self, status_code: int, path: str, error_payload: Any):
        super().__init__(f"CoinMetrics API error {status_code} for {path}")
        self.status_code = status_code
        self.path = path
        self.error_payload = error_payload


class CoinMetricsClient:
    def __init__(self, api_key: Optional[str] = None, base_url: str = "https://community-api.coinmetrics.io/v4", timeout: int = 30):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def _build_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def request_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Perform GET request to the given path and return parsed JSON.

        Raises RuntimeError on non-200 responses. Logs method/path/status.
        """
        if not path.startswith("/"):
            path = "/" + path
        url = self.base_url + path
        headers = self._build_headers()

        resp = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
        status = resp.status_code
        logger.info("CoinMetrics request: GET %s -> %s", path, status)

        if status != 200:
            # Try to parse JSON error payload, otherwise use text. Truncate to 2000 chars if needed.
            err_payload: Any
            try:
                err_payload = resp.json()
                # If JSON serializes to a very long string, truncate its string form
                s = json.dumps(err_payload, ensure_ascii=False)
                if len(s) > 2000:
                    err_payload = s[:2000]
            except Exception:
                text = resp.text or ""
                err_payload = text[:2000]

            raise CoinMetricsError(status, path, err_payload)

        return resp.json()

    def get_catalog_assets(self) -> Dict[str, Any]:
        # Do not send 'limit' by default; pagination can be added later.
        return self.request_json("/catalog/assets", params=None)

    def get_asset_metrics(self, asset: str, metrics: List[str], start: str, end: str, frequency: str = "1d") -> Dict[str, Any]:
        # Normalize time parameters to ISO8601 UTC so API honors the window
        try:
            start_t = normalize_time(start)
        except Exception:
            start_t = start
        try:
            end_t = normalize_time(end)
        except Exception:
            end_t = end

        params = {
            "assets": asset,
            "metrics": ",".join(metrics) if isinstance(metrics, (list, tuple)) else metrics,
            "frequency": frequency,
            "start_time": start_t,
            "end_time": end_t,
        }
        return self.request_json("/timeseries/asset-metrics", params=params)


__all__ = ["CoinMetricsClient"]
