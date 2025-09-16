from __future__ import annotations

import concurrent.futures
import time
from typing import List, Dict, Any

from utils.logging_setup import get_logger
from utils.static_data import FALLBACK_COUNTRIES

logger = get_logger(__name__)

BASE_URL = "https://date.nager.at/api/v3"

try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore


class NagerHolidayClient:
    def __init__(self, retries: int = 3, timeout: float = 15.0, concurrency: int = 6, offline: bool = False):
        self.retries = retries
        self.timeout = timeout
        self.concurrency = concurrency
        self.offline = offline

    def _request_json(self, url: str) -> Any:
        if self.offline:
            raise RuntimeError("_request_json called in offline mode")
        if requests is None:
            raise RuntimeError("'requests' package not installed. Run: pip install -r requirements.txt")
        delay = 1.0
        for attempt in range(1, self.retries + 1):
            try:
                resp = requests.get(url, timeout=self.timeout)
                if resp.status_code >= 500:
                    raise RuntimeError(f"Server error {resp.status_code}")
                resp.raise_for_status()
                return resp.json()
            except Exception as e:  # noqa: BLE001
                if attempt == self.retries:
                    raise
                logger.debug("Retry %d for %s after error: %s", attempt, url, e)
                time.sleep(delay)
                delay *= 2
        raise RuntimeError("Unreachable retry logic")

    def get_public_holidays(self, year: int, country_code: str) -> List[Dict[str, Any]]:
        if self.offline:
            # Simulated minimal structure for offline mode.
            return [
                {
                    "date": f"{year}-01-01",
                    "localName": "New Year's Day",
                    "name": "New Year's Day",
                    "countryCode": country_code,
                    "fixed": True,
                    "global": True,
                    "counties": None,
                    "types": ["Public"],
                }
            ]
        url = f"{BASE_URL}/PublicHolidays/{year}/{country_code}"  # docs: https://date.nager.at/swagger/index.html
        data = self._request_json(url)
        # Ensure list
        if not isinstance(data, list):
            logger.warning("Unexpected response shape for %s: %s", country_code, type(data))
            return []
        return data

    def bulk_get_public_holidays(self, year: int, country_codes: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        results: Dict[str, List[Dict[str, Any]]] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            future_map = {executor.submit(self.get_public_holidays, year, code): code for code in country_codes}
            for future in concurrent.futures.as_completed(future_map):
                code = future_map[future]
                try:
                    results[code] = future.result()
                except Exception as e:  # noqa: BLE001
                    logger.warning("Holiday fetch failed for %s: %s", code, e)
        return results

    def available_country_codes(self) -> List[str]:
        if self.offline:
            return [c["countryCode"] for c in FALLBACK_COUNTRIES]
        url = f"{BASE_URL}/AvailableCountries"
        data = self._request_json(url)
        return [d.get("countryCode") for d in data if isinstance(d, dict) and d.get("countryCode")]
