import sqlite3
from pathlib import Path
from typing import Iterable, Any
import time
import subprocess
import json


class WarehouseManager:
    def __init__(self, db_path: str, *, timeout: float = 5.0):
        """
        Connect to a SQLite database at db_path

        - Sets row_factory to sqlite3.Row for dict-like access
        - Keeps a global cursor for convenience operations
        """

        self.db_path = db_path

        # Create parent directory so SQLite can create the DB file if missing
        if db_path and not db_path.startswith(":memory:") and not db_path.startswith("file:"):
            self.created = Path(db_path).exists()
            Path(db_path).parent.mkdir(parents=True, exist_ok=True) # exist_ok only makes if it doesnt exist

            
        # Initialize important global stuff
        self.conn = sqlite3.connect(db_path, timeout=timeout)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    #--------------------------------
    # DDL Methods
    #--------------------------------
    
    def execute(self, sql: str, params: Iterable[Any] | None = None) -> int:
        """Execute a write statement and commit. Returns lastrowid."""
        if params is None:
            params = ()
        self.cursor.execute(sql, params)
        self.conn.commit()
        return self.cursor.lastrowid or 0

    def execute_many(self, sql: str, seq_of_params: Iterable[Iterable[Any]]) -> None:
        """Execute many with commit for batch inserts/updates."""
        self.cursor.executemany(sql, seq_of_params)
        self.conn.commit()

    def query(self, sql: str, params: Iterable[Any] | None = None) -> list[sqlite3.Row]:
        """Run a read-only query and return all rows."""
        if params is None:
            params = ()
        cur = self.cursor.execute(sql, params)
        return cur.fetchall()

    def query_dicts(self, sql: str, params: Iterable[Any] | None = None) -> list[dict[str, Any]]:
        """Run a SELECT and return rows as a list of plain dicts.

        Useful for readable printing or JSON serialization.
        """
        rows = self.query(sql, params)
        return [dict(r) for r in rows]

    #--------------------------------
    # Schema Methods
    #--------------------------------

    def table_columns(self, table: str) -> list[str]:
        """Return column names for a table (empty if table missing)."""
        try:
            cur = self.cursor.execute(f'PRAGMA table_info("{table}")')
            info = cur.fetchall()
        except sqlite3.OperationalError:
            return []
        return [row[1] for row in info]

    def get_schemas(self) -> dict[str, Any]:
        """
        Return a JSON-serializable dict describing the schema of all tables in the database.
        Structure:
        """

        tables: dict[str, list[dict[str, Any]]] = {}
        # Get all table names
        table_rows = self.query('SELECT name FROM sqlite_master WHERE type="table"')
        table_names = [row["name"] for row in table_rows]
        for table in table_names:
            columns_info = []
            col_rows = self.query(f'PRAGMA table_info("{table}")')
            for col in col_rows:
                columns_info.append({
                    "name": col["name"],
                    "type": col["type"],
                    "notnull": col["notnull"],
                    "dflt_value": col["dflt_value"],
                    "pk": col["pk"]
                })
            tables[table] = columns_info
        return {"tables": tables}
    

    #--------------------------------
    # CSV Methods
    #--------------------------------

    def create_csv(self, table: str, csv_path: str, include_header: bool = True) -> None:
        """Export an entire table to a CSV file.

        Args:
            table: Table name to export (e.g., 'items').
            csv_path: Destination CSV path (e.g., 'items.csv').
            include_header: Whether to write a header row of column names.
        """
        import csv

        # Query all rows and get column names
        cur = self.cursor.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        col_names = [d[0] for d in cur.description]

        # Write CSV
        with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if include_header:
                writer.writerow(col_names)
            for row in rows:
                # sqlite3.Row is indexable; convert to a plain list
                writer.writerow([row[col] for col in col_names])

    def import_csv(self, csv_path: str, table: str, create_if_missing: bool, replace: bool, check_type: bool) -> int:
        """Import a CSV into a table using the first row as headers.

        Args:
            csv_path: Path to the CSV file.
            table: Destination table name.
            create_if_missing: Create the table if it doesn't exist.
            replace: Drop and recreate the table before inserting.
            check_type: If True, call external C helper to infer SQLite column types.
        Returns:
            Number of data rows inserted.
        """
        import csv
        from pathlib import Path

        if not Path(csv_path).exists():
            raise FileNotFoundError(csv_path)

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            try:
                raw_headers = next(reader)
            except StopIteration:
                # Empty file
                return 0

            # Normalize headers to safe identifiers
            normalized_headers = [self._normalize_identifier(h) for h in raw_headers]

            # Optionally drop existing table
            if replace:
                self.cursor.execute(f'DROP TABLE IF EXISTS "{table}"')
                self.conn.commit()

            existing_cols = self.table_columns(table)

            # Decide column definitions for CREATE TABLE
            create_sql = None
            if not existing_cols:
                if not create_if_missing:
                    raise sqlite3.OperationalError(f"Table '{table}' does not exist and create_if_missing=False")
                
                if check_type:
                    # Ask external C helper for types; requires executable `./csv_type_infer`
                    inferred_headers, inferred_types = self.infer_types_via_c(csv_path)
                    # Ensure inferred headers align (case-insensitive, same order)
                    print(f"{inferred_headers} types: {inferred_types}")
                    if [h.lower() for h in inferred_headers] != [h.lower() for h in normalized_headers]:
                        # Favor the CSV's original header order/labels but keep inferred types length check
                        if len(inferred_types) != len(normalized_headers):
                            raise sqlite3.OperationalError("Inferred types length does not match CSV header count.")
                        headers_for_create = normalized_headers
                        types_for_create = inferred_types
                    else:
                        headers_for_create = inferred_headers
                        types_for_create = inferred_types
                    
                    cols_sql_parts = [f'"{h}" {t}' for h, t in zip(headers_for_create, types_for_create)]
                    create_sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(cols_sql_parts)})'
                else:
                    # Default all TEXT
                    cols_sql = ", ".join([f'"{c}" TEXT' for c in normalized_headers])
                    create_sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_sql})'

                self.cursor.execute(create_sql)
                self.conn.commit()
            else:
                # Validate column compatibility (case-insensitive match by order)
                if [c.lower() for c in existing_cols] != [c.lower() for c in normalized_headers]:
                    raise sqlite3.OperationalError(
                        "CSV headers do not match existing table columns: "
                        f"{existing_cols} vs {normalized_headers}"
                    )

            # Prepare insert statement
            placeholders = ", ".join(["?"] * len(normalized_headers))
            cols_part = ", ".join([f'"{c}"' for c in normalized_headers])
            insert_sql = f'INSERT INTO "{table}" ({cols_part}) VALUES ({placeholders})'

            # Stream insert rows
            rows_inserted = 0
            batch: list[list[Any]] = []
            for row in reader:
                # If the row is shorter, pad with None; if longer, trim
                if len(row) < len(normalized_headers):
                    row = row + [None] * (len(normalized_headers) - len(row))
                elif len(row) > len(normalized_headers):
                    row = row[: len(normalized_headers)]
                batch.append(row)
                # Commit in batches for performance
                if len(batch) >= 500:
                    self.cursor.executemany(insert_sql, batch)
                    self.conn.commit()
                    rows_inserted += len(batch)
                    batch.clear()

            if batch:
                self.cursor.executemany(insert_sql, batch)
                self.conn.commit()
                rows_inserted += len(batch)

            return rows_inserted
    
    def infer_types_via_c(self, csv_path: str, c_executable: str = "bin/csv_type_infer") -> tuple[list[str], list[str]]:
        """
        Call an external C program to infer SQLite column types from a CSV.
        The C helper must print a JSON object to stdout like:
          {"columns": ["colA","colB"], "types": ["INTEGER","TEXT"]}
        Returns (normalized_headers, types).
        """
        try:
            result = subprocess.run(
                [c_executable, csv_path],
                check=True,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as e:
            raise FileNotFoundError(f"C type-inference helper not found at '{c_executable}'. Compile it and place it alongside your app.") from e
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"C helper exited with status {e.returncode}: {e.stderr.strip()}") from e
        
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            raise ValueError("C helper did not return valid JSON on stdout.") from e
        
        if not isinstance(payload, dict) or "columns" not in payload or "types" not in payload:
            raise ValueError("C helper JSON missing required 'columns' and 'types' keys.")
        
        raw_headers = payload["columns"]
        types = payload["types"]
        if not isinstance(raw_headers, list) or not isinstance(types, list):
            raise ValueError("'columns' and 'types' must be lists.")
        if len(raw_headers) != len(types):
            raise ValueError(f"Mismatched columns/types length: {len(raw_headers)} vs {len(types)}.")
        
        # Normalize headers to safe SQLite identifiers (keep order)
        headers = [self._normalize_identifier(h) for h in raw_headers]
        return headers, types

    @staticmethod
    def _normalize_identifier(name: str) -> str:
        """Normalize a header to a safe SQLite identifier.

        - Strips whitespace
        - Replaces spaces with underscores
        - Removes non-alphanumeric/underscore characters
        - Prefixes with '_' if starting with a digit or empty
        """
        import re

        s = name.strip().replace(" ", "_")
        s = re.sub(r"[^0-9A-Za-z_]", "", s)
        if not s or s[0].isdigit():
            s = f"_{s}"
        return s
    
    #--------------------------------
    # Database File Ops
    #--------------------------------

    @staticmethod
    def delete_database_at_path(db_path: str) -> bool:
        """Delete the SQLite database file at `db_path`.

        Returns True if a file was deleted, False if there was nothing to delete.
        Skips deletion for in-memory or URI-style paths.
        """
        # Ignore in-memory DBs and URI mode (e.g., "file:..."), which are not plain files
        if not db_path or db_path.startswith(":memory:") or db_path.startswith("file:"):
            return False

        p = Path(db_path)
        try:
            if p.exists() and p.is_file():
                p.unlink()
                return True
            return False
        except Exception:
            # Let callers decide how to handle errors if needed; keep API simple
            raise

    #--------------------------------    
    # Connection Closing
    #--------------------------------

    def close(self) -> None:
        """Close the connection."""
        try:
            self.cursor.close()
        finally:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # If an exception occurred, let it propagate after closing.
        self.close()


if __name__ == "__main__":
    db_path = "sample_warehouse.db"
    csv_path = "datasets/test_types.csv"
    table_name = "items"

    start = time.perf_counter()
    try:
        with WarehouseManager(db_path) as wm:
            inserted = wm.import_csv(
                csv_path=csv_path,
                table=table_name,
                create_if_missing=True,
                replace=True,
                check_type=True  # uses external C helper ./csv_type_infer
            )
            print(f"Imported {inserted} rows into '{table_name}' from {csv_path}.")
    except Exception as e:
        print(f"Error during demo: {e}")
    end = time.perf_counter()
    print(f"{(end-start):.2f}")
