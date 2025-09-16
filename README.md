# Microsoft Locations & Public Holidays Collector

Automates collection of countries where Microsoft operates and retrieves public holiday data for a given year using the public Nager.Date API.

## Features
- Scrapes (or falls back to static list) of Microsoft operating countries.
- Maps country names to ISO country codes with fuzzy matching.
- Fetches public holidays for each matched country.
- Saves per-country JSON plus aggregated JSON and CSV.
- Offline mode with fallback sample data for testing.

## Quick Start
```bash
python main.py --year 2025
```
Add `--offline` to avoid network activity (uses small static samples).

## Arguments
- `--year` Target year (defaults to current year)
- `--limit` Limit number of countries (debug)
- `--output` Output directory root (default `data`)
- `--concurrency` Threads for holiday requests
- `--retry` Retry attempts for API calls
- `--timeout` HTTP timeout seconds
- `--offline` Force offline mode

## Data Outputs (example for year 2025)
```
data/2025/
  microsoft_countries_raw.json
  microsoft_country_names.json
  available_countries_source.json
  country_match_results.json
  holidays_US.json
  holidays_GB.json
  ...
  holidays_all.json
  holidays_all.csv
```

## Scheduling Example (cron)
Run on January 2nd at 06:00 local time:
```
0 6 2 1 * /usr/bin/env OFFLINE=0 /usr/local/bin/python /path/to/main.py --year $(date +"%Y") >> /path/to/collector.log 2>&1
```

## Notes
- Public holiday data accuracy depends on upstream API and announcement timing.
- Manual validation recommended for critical compliance uses.

## License
MIT (adjust as needed).
