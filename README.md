# Data Warehouse 

A small and lightweight FastAPI service to manage SQLite databases through swagger UI. Allows users to import csv files in relational databases, using a small C helper to read through and infer the type affinity for each column. 

### Coming soon:
1. Support for pointing to Excel files


### Quick Start
- Python 3.10+
- GCC/Clang for building the C helper

1) Create a virtual env and install deps: 

   Mac/Unix: python -m venv .venv
   source .venv/bin/activate   
   Windows: .venv\Scripts\activate 
   
   pip install -r requirements.txt

2) Build the C helper (outputs bin/csv_type_infer)
   make


### Example Usage
1) Initialize/connect to a database file
   curl -X POST "http://127.0.0.1:8000/database?db_path=sample.db"

2) Import a CSV into a table (infers types using the C helper)
   curl -X PUT "http://127.0.0.1:8000/database/importCSV?csv_path=datasets/test_types.csv&table_name=items&create_if_missing=true&replace=true&check_types=true"

3) Query data
   curl "http://127.0.0.1:8000/database/query?sql=SELECT%20*%20FROM%20items%20LIMIT%2010"

4) Check CSV type affinity worked correctly: "http://127.0.0.1:8000/database/getschemas"




