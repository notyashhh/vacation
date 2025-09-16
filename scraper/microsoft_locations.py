from __future__ import annotations

import os
import re
from typing import List, Dict

import json

from utils.logging_setup import get_logger
from utils.static_data import FALLBACK_MICROSOFT_COUNTRIES

logger = get_logger(__name__)

MS_LOCATIONS_URL = "https://www.microsoft.com/en-us/worldwide.aspx"

# Simple regex fallback parsing patterns; real HTML parsing would use BeautifulSoup if installed.
COUNTRY_BLOCK_PATTERN = re.compile(r'<li class="directory-item".*?</li>', re.DOTALL)
COUNTRY_NAME_PATTERN = re.compile(r'data-countryname="([^"]+)"')


class MicrosoftLocationScraper:
    def __init__(self, offline: bool = False):
        self.offline = offline

    def _fetch_html(self) -> str:
        import requests  # local import to avoid dependency if unused
        logger.debug("Fetching Microsoft worldwide page %s", MS_LOCATIONS_URL)
        resp = requests.get(MS_LOCATIONS_URL, timeout=20)
        resp.raise_for_status()
        return resp.text

    def get_countries(self) -> List[Dict[str, str]]:
        if self.offline or os.environ.get("OFFLINE") == "1":
            logger.info("Offline mode: using fallback Microsoft countries list (%d entries)", len(FALLBACK_MICROSOFT_COUNTRIES))
            return FALLBACK_MICROSOFT_COUNTRIES
        try:
            html = self._fetch_html()
            blocks = COUNTRY_BLOCK_PATTERN.findall(html)
            countries = []
            for block in blocks:
                m = COUNTRY_NAME_PATTERN.search(block)
                if m:
                    country = m.group(1).strip()
                    countries.append({"country": country})
            # Deduplicate preserving order
            seen = set()
            unique = []
            for c in countries:
                if c["country"].lower() not in seen:
                    seen.add(c["country"].lower())
                    unique.append(c)
            logger.info("Parsed %d unique countries from Microsoft page", len(unique))
            return unique
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to scrape online page, using fallback: %s", e)
            return FALLBACK_MICROSOFT_COUNTRIES
