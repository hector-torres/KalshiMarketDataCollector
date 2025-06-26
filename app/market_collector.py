# market_collector.py

import os
from dotenv import load_dotenv
import logging
import requests
import time
import pandas as pd
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from database_client import DatabaseClient, db_file

# Load environment variables
load_dotenv()

class MarketCollector:
    """
    Encapsulates API calls and data persistence for Kalshi market events.
    Debug toggled via DEBUG env var ("1" for on).
    """
    BASE_URL = "https://api.elections.kalshi.com/trade-api/v2/events"

    CLOSED_STATUSES = {'finalized', 'inactive', 'closed', 'settled'}

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        debug_env = os.getenv("DEBUG", "0") == "1"
        self.logger = logging.getLogger(self.__class__.__name__)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG if debug_env else logging.INFO)
        self.logger.debug(f"Debug mode is {'ON' if debug_env else 'OFF'}.")

        self.session = session or requests.Session()
        self.headers = {"accept": "application/json"}

    def get_open_events(
        self,
        limit: int = 200,
        status: str = "open",
        cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "status": status, "with_nested_markets": 'true'}
        if cursor:
            params["cursor"] = cursor
        resp = self.session.get(self.BASE_URL, headers=self.headers, params=params)
        resp.raise_for_status()
        return resp.json()

    def get_events_dataframe(
        self,
        limit: int = 200,
        status: str = "open"
    ) -> pd.DataFrame:
        # Fetch initial page
        response = self.get_open_events(limit, status)
        events = response.get('events', [])[:]
        cursor = response.get('cursor')

        # Continue pagination
        while cursor:
            self.logger.debug(f"Fetching page with cursor {cursor}")
            response = self.get_open_events(limit, status, cursor)
            events.extend(response.get('events', []))
            cursor = response.get('cursor')
            time.sleep(0.1)

        # Normalize data
        events_df = pd.DataFrame(events)
        markets_df = pd.json_normalize(
            events,
            record_path=['markets'],
            meta=['event_ticker', 'series_ticker', 'title'],
            record_prefix='market_', meta_prefix='event_'
        )
        df = markets_df.merge(
            events_df,
            left_on='event_event_ticker', right_on='event_ticker',
            how='left', suffixes=('_market', '_event')
        )

        # Clean up and reorder columns
        cols = ['title', 'sub_title', 'market_rules_primary', 'market_rules_secondary'] + [c for c in df.columns if c not in ['title', 'sub_title', 'market_rules_primary', 'market_rules_secondary']]
        df = df[cols]
        df = df.loc[:, ~df.columns.str.startswith('market_custom_strike')]
        df = df.drop(columns=['markets'], errors='ignore')

        # Add placeholder for change time
        df['market_status_change_time'] = None

        # Initialize DB client
        schema = {col: 'TEXT' for col in df.columns}
        db = DatabaseClient(schema, self.logger)

        # Existing tickers and statuses
        existing = set()
        if os.path.exists(db_file):
            existing = db.get_existing_market_tickers()
        status_map = db.get_market_status_map()

        # Insert new active records
        new_active = df[~df['market_ticker'].isin(existing) & df['market_status'].eq('active')]
        opened_count = len(new_active)
        if opened_count:
            db.append_dataframe(new_active)
            self.logger.info(f"Inserted {opened_count} new active records.")

        # Handle closed statuses and delete
        closed_count = 0
        for _, row in df.iterrows():
            ticker = row['market_ticker']
            new_status = row['market_status']
            if ticker in existing and status_map.get(ticker) == 'active' and new_status in self.CLOSED_STATUSES:
                change_time = datetime.now(timezone.utc).isoformat()
                df.loc[df['market_ticker'] == ticker, 'market_status_change_time'] = change_time
                db.delete_market(ticker)
                closed_count += 1

        # Summary report
        total_active = len(db.get_existing_market_tickers())
        summary_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        self.logger.info(f"Summary for {summary_date}:")
        self.logger.info(f"   {total_active}: number of active markets")
        self.logger.info(f"   {closed_count}: number of markets closed since last run")
        self.logger.info(f"   {opened_count}: number of markets opened since last run")

        return df