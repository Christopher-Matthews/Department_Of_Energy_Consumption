"""Small EIA client for monthly retail sales ingestion."""

from __future__ import annotations

from datetime import date
import json
from typing import Any, Dict, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class EIAClient:
    def __init__(self, api_key: str, base_url: str = "https://api.eia.gov/v2") -> None:
        if not api_key:
            raise ValueError("EIA API key is required")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def _get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        params_with_key = dict(params)
        params_with_key["api_key"] = self.api_key
        url = f"{self.base_url}/{endpoint.lstrip('/')}?{urlencode(params_with_key, doseq=True)}"

        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))

    def _last_n_complete_months(self, n: int) -> tuple[str, str]:
        today = date.today()
        year = today.year
        month = today.month - 1  # previous month
        if month == 0:
            month = 12
            year -= 1

        end = f"{year:04d}-{month:02d}"

        start_year = year
        start_month = month - (n - 1)
        while start_month <= 0:
            start_month += 12
            start_year -= 1
        start = f"{start_year:04d}-{start_month:02d}"
        return start, end

    def get_monthly_retail_sales(self, sector_id: str = "ALL", months: int = 5) -> List[Dict[str, Any]]:
        start, end = self._last_n_complete_months(months)
        payload = self._get(
            endpoint="electricity/retail-sales/data/",
            params={
                "data[]": ["sales"],
                "frequency": "monthly",
                "facets[sectorid][]": [sector_id],
                "start": start,
                "end": end,
            },
        )

        rows = payload.get("response", {}).get("data", [])
        return [
            {
                "stateId": row.get("stateid"),
                "stateDescription": row.get("stateDescription"),
                "period": row.get("period"),
                "sales": row.get("sales"),
                "salesUnits": row.get("sales-units"),
            }
            for row in rows
        ]
