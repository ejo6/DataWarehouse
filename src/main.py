# main.py
from fastapi import FastAPI, HTTPException
from fastapi import Query as Q

# When running as a package, e.g. `uvicorn src.main:app`
from .WarehouseManager import WarehouseManager  # type: ignore



app = FastAPI(
    title="Data Warehouse API",
    description="Endpoints for connecting to a SQLite database, executing SQL, querying data, inspecting schemas, and importing/exporting CSVs.",
    docs_url="/",
    redoc_url=None,
    openapi_tags=[
        {"name": "Database", "description": "Connect to the DB, execute SQL, run queries, inspect schemas, and delete the database file."},
        {"name": "CSV", "description": "Import CSVs into tables and export tables to CSV."}
    ],
)


# --- Global Variable to Store Active WarehouseManager Instance ---
wm: WarehouseManager | None = None

#--------------------------------
# API Functions
#--------------------------------

# Init db connection
@app.post("/database", tags=["Database"])
def init_db(db_path = Q(..., description="Path to database")):
    """
    Initialize or connect to the SQLite database.
    - Creates the file if it doesn't exist.
    - Sets up a reusable connection for later queries.
    """

    global wm
    try:
        wm = WarehouseManager(db_path)
        return {"status": "success", "message": f"Connected to {db_path}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Execute SQL command
@app.post("/database/execute", tags=["Database"])
def execute(sql : str = Q(..., description="Command to execute"), 
            params: list[str] | None = Q(None, description="Optional parameters")):
    """Execute a write statement and commit. Returns lastrowid."""

    global wm
    if wm is None:
        raise HTTPException(status_code=400, detail="Database connection not initialized. Use /ConnectDB first.")

    try:
        rowid = wm.execute(sql, params)
        return {"status": "success", "last_row_id": rowid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Execute String of SQL
@app.post("/database/execute/many", tags=["Database"])
def execute_query_many(sql : str = Q(..., description="Command to execute (executemany)"), 
            params: list[str] | None = Q(None, description="Optional parameters")):
    """Execute many with commit for batch inserts/updates."""

    global wm
    if wm is None:
        raise HTTPException(status_code=400, detail="Database connection not initialized. Use /ConnectDB first.")

    try:
        rowid = wm.execute_many(sql, params)
        return {"status": "success", "last_row_id": rowid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Query db
@app.get("/database/query", tags=["Database"])
def query(sql: str = Q(..., description="SQL SELECT statement"), params: list[str] | None = Q(None, description="Optional parameters")):
    """Run a read-only query and return all rows."""
    global wm
    if wm is None:
        raise HTTPException(status_code=400, detail="Database not initialized. Use /ConnectDB first.")
    try:
        response = wm.query(sql, params)
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Get Schemas
@app.get("/database/getschemas", tags=["Database"])
def get_schemas():
    """
    Return a JSON-serializable dict describing the schema of all tables in the database.
    """
    global wm
    if wm is None:
        raise HTTPException(status_code=400, detail="Database not initialized. Use /ConnectDB first.")

    try:
        response = wm.get_schemas()
        return {"status": "success", "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.put("/database/toCSV", tags=["CSV"])
def to_csv(table_name : str = Q(..., description="Name of table to be converted"), 
           csv_path : str = Q(..., description="Path of output CSV"),
           use_headers : bool = Q(..., description="Whether to write a header row of column names")):
    
    global wm
    if wm is None:
        raise HTTPException(status_code=400, detail="Database not initialized. Use /ConnectDB first.")

    try:
        response = wm.create_csv(table_name, csv_path, use_headers)
        return {"status": "success", "created_at": {csv_path}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.put("/database/importCSV", tags=["CSV"])
def import_csv(csv_path : str = Q(..., description="Path to CSV to be converted"),
               table_name : str = Q(..., description="Name of table for entry"),
               create_if_missing : bool = Q(..., description="Create table if non-existent"),
               replace : bool = Q(..., description="Replace table content if true"),
               check_types : bool = Q(..., description="Infer/validate column types before import")):
    global wm
    if wm is None:
        raise HTTPException(status_code=400, detail="Database not initialized. Use /ConnectDB first.")

    try:
        wm.import_csv(csv_path, table_name, create_if_missing, replace, check_types)
        return {"status": "success", "created_at_table": {table_name}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.delete("/dropdatabase", tags=["Database"])
def delete_database(db_path : str = Q(..., description="Path to the database file to delete")):
    global wm
    if wm is None:
        raise HTTPException(status_code=400, detail="Database not initialized. Use /ConnectDB first.")

    try:
        wm.delete_database_at_path(db_path)
        return {"status": "success", "deleted_at_path": {db_path}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))