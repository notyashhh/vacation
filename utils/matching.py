from __future__ import annotations

import difflib
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from utils.logging_setup import get_logger
from utils.static_data import FALLBACK_COUNTRIES

logger = get_logger(__name__)


@dataclass
class MatchResult:
    source_name: str
    code: Optional[str]
    matched: bool
    score: float
    method: str
    target_name: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_name": self.source_name,
            "code": self.code,
            "matched": self.matched,
            "score": round(self.score, 4),
            "method": self.method,
            "target_name": self.target_name,
        }


class CountryMatcher:
    def __init__(self, offline: bool = False):
        self.offline = offline
        self._countries_cache: Optional[List[Dict[str, str]]] = None

    def get_available_countries(self) -> List[Dict[str, str]]:
        if self._countries_cache is not None:
            return self._countries_cache
        if self.offline:
            logger.info("Offline mode: using fallback country codes list (%d entries)", len(FALLBACK_COUNTRIES))
            self._countries_cache = FALLBACK_COUNTRIES
            return self._countries_cache
        # Online mode: fetch from Nager.Date API (countries endpoint)
        import requests
        try:
            resp = requests.get("https://date.nager.at/api/v3/AvailableCountries", timeout=15)
            resp.raise_for_status()
            data = resp.json()
            # Normalize field names to name/code
            for d in data:
                if "name" not in d and "country" in d:
                    d["name"] = d.get("country")
            self._countries_cache = data
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed fetching countries online, fallback used: %s", e)
            self._countries_cache = FALLBACK_COUNTRIES
        return self._countries_cache

    def match_one(self, source_name: str) -> MatchResult:
        countries = self.get_available_countries()
        # Exact (case-insensitive) match
        for c in countries:
            if c.get("name", "").lower() == source_name.lower():
                return MatchResult(source_name, c.get("countryCode") or c.get("code"), True, 1.0, "exact", c.get("name"))
        # Fuzzy
        target_names = [c.get("name", "") for c in countries]
        matches = difflib.get_close_matches(source_name, target_names, n=1, cutoff=0.75)
        if matches:
            best = matches[0]
            country = next(c for c in countries if c.get("name") == best)
            # Similarity ratio
            score = difflib.SequenceMatcher(a=source_name.lower(), b=best.lower()).ratio()
            return MatchResult(source_name, country.get("countryCode") or country.get("code"), True, score, "fuzzy", best)
        return MatchResult(source_name, None, False, 0.0, "none", None)

    def match_many(self, names: List[str]) -> List[MatchResult]:
        return [self.match_one(n) for n in names]
