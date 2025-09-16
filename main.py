from __future__ import annotations

"""Main orchestrator for collecting Microsoft operating countries and their public holidays.

Usage (basic):
	python main.py --year 2025

Key steps:
 1. Scrape (or fallback) list of countries where Microsoft operates.
 2. Match country names to ISO country codes using Nager.Date API (or cached list).
 3. Fetch public holidays for the target year for each matched country.
 4. Store raw and aggregated data under data/<year>/.

Set OFFLINE=1 to skip network calls and use embedded fallback samples.

Scheduling suggestion (macOS launchd or cron after Jan 1):
  CRON: 0 6 2 1 * /usr/bin/env OFFLINE=0 /usr/local/bin/python /path/to/main.py --year $(date +"%Y") >> /path/to/log 2>&1

Disclaimer: Microsoft may operate in territories not publicly listed or updated; manual review recommended.
"""

 

import argparse
import json
import os
from pathlib import Path
from typing import List, Dict, Any

from scraper.microsoft_locations import MicrosoftLocationScraper
from holidays_api.nager import NagerHolidayClient
from utils.logging_setup import get_logger
from utils.matching import CountryMatcher, MatchResult

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Collect Microsoft operating countries and public holidays")
	parser.add_argument("--year", type=int, default=None, help="Target year (default: current year)")
	parser.add_argument("--limit", type=int, default=None, help="Limit number of countries (for testing)")
	parser.add_argument("--output", type=str, default="data", help="Output root directory")
	parser.add_argument("--concurrency", type=int, default=8, help="Concurrent holiday fetch workers")
	parser.add_argument("--retry", type=int, default=3, help="Retry attempts for API calls")
	parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout seconds")
	parser.add_argument("--offline", action="store_true", help="Force offline mode (no network)")
	parser.add_argument("--countries-file", type=str, default=None, help="Optional path to JSON or CSV file listing countries (overrides scraping)")
	# Azure Table export options (optional)
	parser.add_argument("--export-azure-table", action="store_true", help="After collecting, export aggregated CSV to Azure Table")
	parser.add_argument("--azure-table-name", type=str, default="PublicHolidays", help="Azure Table name")
	parser.add_argument("--azure-upsert", action="store_true", help="Use upsert (merge) instead of create for entities")
	return parser.parse_args()


def ensure_dir(path: Path) -> None:
	path.mkdir(parents=True, exist_ok=True)


def save_json(path: Path, data: Any) -> None:
	with path.open("w", encoding="utf-8") as f:
		json.dump(data, f, indent=2, ensure_ascii=False)


def aggregate_to_csv(holidays_by_country: Dict[str, List[Dict[str, Any]]], csv_path: Path) -> None:
	import csv
	headers = [
		"country_code",
		"country_name",
		"date",
		"local_name",
		"name",
		"fixed",
		"global",
		"counties",
		"types",
	]
	with csv_path.open("w", newline="", encoding="utf-8") as f:
		writer = csv.writer(f)
		writer.writerow(headers)
		for code, holidays in sorted(holidays_by_country.items()):
			for h in holidays:
				writer.writerow([
					code,
					h.get("countryName") or h.get("country_name") or "",
					h.get("date"),
					h.get("localName"),
					h.get("name"),
					h.get("fixed"),
					h.get("global"),
					";".join(h.get("counties") or []) if h.get("counties") else "",
					";".join(h.get("types") or []) if h.get("types") else "",
				])


def main() -> None:
	import datetime as _dt
	args = parse_args()
	year = args.year or _dt.date.today().year
	offline = args.offline or os.environ.get("OFFLINE") == "1"

	output_root = Path(args.output) / str(year)
	ensure_dir(output_root)

	logger.info("Starting collection for year=%s offline=%s", year, offline)

	# 1. Acquire Microsoft operating countries
	if args.countries_file:
		path = Path(args.countries_file)
		if not path.exists():
			raise SystemExit(f"--countries-file not found: {path}")
		if path.suffix.lower() == ".json":
			with path.open() as f:
				loaded = json.load(f)
			if isinstance(loaded, list):
				if loaded and isinstance(loaded[0], str):
					countries_raw = [{"country": c} for c in loaded]
				else:
					countries_raw = loaded
			else:
				raise SystemExit("JSON countries file must contain a list")
		elif path.suffix.lower() in {".csv", ".txt"}:
			import csv
			countries_raw = []
			with path.open(newline="", encoding="utf-8") as f:
				reader = csv.reader(f)
				for row in reader:
					if not row:
						continue
					countries_raw.append({"country": row[0].strip()})
		else:
			raise SystemExit("Unsupported countries file extension (use .json or .csv/.txt)")
		logger.info("Loaded %d countries from user file %s", len(countries_raw), path)
	else:
		scraper = MicrosoftLocationScraper(offline=offline)
		countries_raw = scraper.get_countries()
		if not countries_raw:  # final guard
			logger.warning("No countries obtained; using minimal fallback sample")
			countries_raw = [{"country": c} for c in ["United States", "Canada", "United Kingdom"]]
	if args.limit:
		countries_raw = countries_raw[: args.limit]
	save_json(output_root / "microsoft_countries_raw.json", countries_raw)
	logger.info("Collected %d raw countries", len(countries_raw))

	# Normalize to names list
	ms_country_names = sorted({c["country"] for c in countries_raw})
	save_json(output_root / "microsoft_country_names.json", ms_country_names)

	# 2. Fetch available country codes and match
	matcher = CountryMatcher(offline=offline)
	available = matcher.get_available_countries()
	save_json(output_root / "available_countries_source.json", available)

	match_results: List[MatchResult] = matcher.match_many(ms_country_names)
	match_serializable = [mr.to_dict() for mr in match_results]
	save_json(output_root / "country_match_results.json", match_serializable)
	logger.info("Matched %d/%d countries (exact or fuzzy)", sum(1 for m in match_results if m.matched), len(match_results))

	# 3. Fetch public holidays
	holiday_client = NagerHolidayClient(retries=args.retry, timeout=args.timeout, offline=offline, concurrency=args.concurrency)
	holidays_by_country: Dict[str, List[Dict[str, Any]]] = {}
	for mr in match_results:
		if not mr.matched or not mr.code:
			continue
		code = mr.code
		try:
			holidays = holiday_client.get_public_holidays(year, code)
			for h in holidays:
				# enrich with matched country name for downstream analysis
				h.setdefault("countryName", mr.source_name)
			holidays_by_country[code] = holidays
			save_json(output_root / f"holidays_{code}.json", holidays)
			logger.debug("Saved holidays for %s (%d items)", code, len(holidays))
		except Exception as e:  # noqa: BLE001
			logger.warning("Failed holiday fetch for %s: %s", code, e)

	save_json(output_root / "holidays_all.json", holidays_by_country)
	# 4. Aggregate CSV
	aggregate_to_csv(holidays_by_country, output_root / "holidays_all.csv")

	logger.info("Done. Countries with holidays: %d", len(holidays_by_country))

	# Optional Azure Table export
	if args.export_azure_table:
		try:
			from azure.export_table import export_csv_to_table  # local import to avoid dependency if unused
			csv_file = output_root / "holidays_all.csv"
			if not csv_file.exists():
				logger.error("CSV file for Azure export not found: %s", csv_file)
			else:
				logger.info("Exporting to Azure Table '%s' (upsert=%s)", args.azure_table_name, args.azure_upsert)
				count = export_csv_to_table(
					str(csv_file),
					table_name=args.azure_table_name,
					upsert=args.azure_upsert,
				)
				logger.info("Azure export complete: %d entities written", count)
		except Exception as e:  # noqa: BLE001
			logger.error("Azure Table export failed: %s", e)


if __name__ == "__main__":
	main()

