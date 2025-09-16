from __future__ import annotations

import os
import csv
from typing import Iterable, Dict, Any, List

from utils.logging_setup import get_logger

logger = get_logger(__name__)

try:
    from azure.data.tables import TableServiceClient
except Exception:  # pragma: no cover - optional dependency not installed
    TableServiceClient = None  # type: ignore


def _slug(val: str) -> str:
    s = ''.join(c.lower() for c in val if c.isalnum())[:40]
    return s or 'x'


def _batch_iter(items: List[Dict[str, Any]], size: int = 100) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def export_csv_to_table(
    csv_path: str,
    table_name: str = "PublicHolidays",
    connection_string: str | None = None,
    upsert: bool = False,
) -> int:
    """Export a holidays CSV (holidays_all.csv) to Azure Table Storage.

    Returns number of entities written.
    """
    if TableServiceClient is None:
        raise RuntimeError("azure-data-tables not installed. Run: pip install azure-data-tables")

    if not connection_string:
        connection_string = os.environ.get("AZURE_TABLE_CONNECTION_STRING")
    if not connection_string:
        raise RuntimeError("Missing Azure connection string (set AZURE_TABLE_CONNECTION_STRING)")

    service = TableServiceClient.from_connection_string(connection_string)
    table_client = service.create_table_if_not_exists(table_name=table_name)

    entities: List[Dict[str, Any]] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        seen_keys = set()
        for row in reader:
            country = row["country_code"].strip()
            date = row["date"].strip()
            name = row["name"].strip()
            rk_base = f"{date}_{_slug(name)}"
            rk = rk_base
            idx = 1
            while (country, rk) in seen_keys:
                idx += 1
                rk = f"{rk_base}_{idx}"
            seen_keys.add((country, rk))
            entity = {
                "PartitionKey": country,
                "RowKey": rk,
                "CountryName": row.get("country_name") or "",
                "Date": date,
                "LocalName": row.get("local_name") or "",
                "Name": name,
                "Fixed": row.get("fixed", "").lower() == "true",
                "Global": row.get("global", "").lower() == "true",
                "Counties": row.get("counties") or "",
                "Types": row.get("types") or "",
                "Year": int(date.split("-")[0]),
            }
            entities.append(entity)

    total = 0
    # Group by partition for separate batches
    from collections import defaultdict

    partitions = defaultdict(list)
    for e in entities:
        partitions[e["PartitionKey"]].append(e)

    for part, part_entities in partitions.items():
        for batch in _batch_iter(part_entities, 100):
            operations = []
            for ent in batch:
                mode = 'upsert' if upsert else 'create'
                if upsert:
                    operations.append(('upsert', ent, {"mode": "merge"}))
                else:
                    operations.append(('create', ent))
            # submit_transaction expects list of tuples
            table_client.submit_transaction(operations)
            total += len(batch)
            logger.debug("Pushed %d entities for partition %s", len(batch), part)
    logger.info("Azure Table export complete: %d entities", total)
    return total
