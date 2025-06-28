# database_client.py

import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, inspect, exc, text
from typing import Dict, Any

# Load environment variables
load_dotenv()

# Determine database file path from environment
DATABASE_URL = os.getenv(
    "DATABASE_URL"
)
# Resolve to absolute path and ensure directory exists
db_file = os.path.abspath(DATABASE_URL)
data_dir = os.path.dirname(db_file)
os.makedirs(data_dir, exist_ok=True)

class DatabaseClient:
    """
    Encapsulates all database operations. Swap out this module to support
    another DB system without touching ingest logic.
    """
    def __init__(self, schema: Dict[str, str], logger: Any) -> None:
        self.logger = logger
        self.db_path = db_file
        self.table_name = "markets"
        self.schema = schema

        self.logger.info(f"Connected to database at {self.db_path}")
        self.engine = self._create_engine()
        self._ensure_table()
        self._ensure_status_change_column()

    def _create_engine(self):
        return create_engine(f"sqlite:///{self.db_path}", future=True)

    def _ensure_table(self) -> None:
        insp = inspect(self.engine)
        if not insp.has_table(self.table_name):
            df = pd.DataFrame(columns=list(self.schema.keys()))
            df.to_sql(self.table_name, self.engine, index=False, if_exists="fail")
            self.logger.info(
                f"Created table '{self.table_name}' with columns: {list(self.schema.keys())}"
            )

    def _ensure_status_change_column(self) -> None:
        insp = inspect(self.engine)
        cols = [col['name'] for col in insp.get_columns(self.table_name)]
        if 'market_status_change_time' not in cols:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(
                        f"ALTER TABLE {self.table_name} ADD COLUMN market_status_change_time TEXT"
                    ))
                    conn.commit()
                self.logger.info("Added column 'market_status_change_time'.")
            except exc.SQLAlchemyError:
                self.logger.exception("Failed to add status change column.")
                raise

    def get_existing_market_tickers(self) -> set[str]:
        insp = inspect(self.engine)
        if not insp.has_table(self.table_name):
            return set()
        df = pd.read_sql_table(self.table_name, self.engine, columns=["market_ticker"])
        return set(df["market_ticker"].dropna().unique())

    def get_market_status_map(self) -> Dict[str, str]:
        """
        Returns a mapping of market_ticker to its last stored market_status.
        """
        insp = inspect(self.engine)
        if not insp.has_table(self.table_name):
            return {}
        df = pd.read_sql_table(self.table_name, self.engine, columns=["market_ticker", "market_status"])
        return dict(zip(df["market_ticker"], df["market_status"]))

    def append_dataframe(self, df: pd.DataFrame) -> None:
        try:
            df.to_sql(self.table_name, self.engine, if_exists="append", index=False)
            self.logger.info(f"Inserted {len(df)} new rows into '{self.table_name}'.")
        except exc.SQLAlchemyError:
            self.logger.exception("DB insert failed.")
            raise

    def delete_market(self, ticker: str) -> None:
        """
        Deletes a market row by ticker from the database.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text(f"DELETE FROM {self.table_name} WHERE market_ticker = :ticker"),
                    {"ticker": ticker}
                )
                conn.commit()
            self.logger.info(f"Deleted market '{ticker}' from database.")
        except exc.SQLAlchemyError:
            self.logger.exception(f"Failed to delete market '{ticker}'.")
            raise