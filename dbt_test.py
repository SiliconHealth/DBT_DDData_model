from dbt.cli.main import dbtRunner
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import Optional
import pandas as pd
import json
from sqlalchemy.exc import ProgrammingError
from os import getenv
from dotenv import load_dotenv


SERVER = 'localhost:1433'
DATABASE = 'DD_CU'
USERNAME = 'sa'
PASSWORD = 'PassWord000'
SCHEMA = 'ddc_internal'



class CredentialKeys(BaseModel):
    server: Optional[str] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


def get_connection_string(credential: CredentialKeys = CredentialKeys()):
    server = credential.server or SERVER
    database = credential.database or DATABASE
    username = credential.username or USERNAME
    password = credential.password or PASSWORD
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    return connection_string

def run_dbt_with_commands(connection_string):

    # Initialize dbt runner
    dbt = dbtRunner()

    result = dbt.invoke(["test"])  # Replace with other commands like "test" or "build"

    engine = create_engine(connection_string, echo=False)

    rows_dict = {}
    table_dict = {"test_unique_id": [],
                  "test_namespace": [],
                  "test_name": [],
                  "test_table": [],
                  "test_column": [],
                  "test_metadata": [],
                  "conformance": [],
                  "completeness": [],
                  "temporal_plausibility": [],
                  "atemporal_plausibility": [],
                  "active": []}
    result_dict = {
        "test_timestamp": [],
        "test_id": [],
        "failures": [],
        "total_rows": [],
    }
    for metadata in result.result.results:
        table_name = metadata.node.depends_on.nodes[0].split(".")[-1]

        table_dict["test_unique_id"].append(metadata.node.name)
        table_dict["test_name"].append(metadata.node.test_metadata.name)
        table_dict["test_namespace"].append(metadata.node.test_metadata.namespace)
        table_dict["test_metadata"].append(json.dumps(metadata.node.test_metadata.kwargs))
        table_dict["test_table"].append(table_name)
        table_dict["test_column"].append(metadata.node.column_name)
        table_dict["conformance"].append(True) if "conformance" in metadata.node.tags else table_dict["conformance"].append(False)
        table_dict["completeness"].append(True) if "completeness" in metadata.node.tags else table_dict["completeness"].append(False)
        table_dict["temporal_plausibility"].append(True) if "temporal_plausibility" in metadata.node.tags else table_dict["temporal_plausibility"].append(False)
        table_dict["atemporal_plausibility"].append(True) if "atemporal_plausibility" in metadata.node.tags else table_dict["atemporal_plausibility"].append(False)
        table_dict["active"].append(True)

        result_dict["test_timestamp"].append(metadata.timing[1].completed_at)
        result_dict["test_id"].append(metadata.node.name)
        result_dict["failures"].append(metadata.failures or 0)

        try:
            result_dict["total_rows"].append(rows_dict[table_name] or 0)
        except KeyError:
            query = f"SELECT count(*) FROM {SCHEMA}.{table_name}"
            with engine.connect() as connection:
                cursor = connection.execute(text(query))
                query_result = cursor.fetchall()
                rows_dict[table_name] = query_result[0][0]
                result_dict["total_rows"].append(rows_dict[table_name])
        except Exception as e:
            print(e)

    test_data = pd.DataFrame(table_dict)
    try:
        existing_ids = pd.read_sql("SELECT test_unique_id FROM test_result.test_metadata", con=engine)
    except ProgrammingError:
        existing_ids = pd.DataFrame({"test_unique_id":[]})
    test_data = test_data[~test_data["test_unique_id"].isin(existing_ids["test_unique_id"])]
    test_data.to_sql("test_metadata", engine, "test_result", "append", False)
    result_df = pd.DataFrame(result_dict)
    result_df.to_sql("test_runs", engine, "test_result", "append", False)


if __name__ == "__main__":
    load_dotenv()
    print(getenv("USER"))
    print(getenv("PASSWORD"))
    cred = CredentialKeys(
        server=getenv("SERVER"),
        database=getenv("DATABASE"),
        username=getenv("USER"),
        password=getenv("PASSWORD")
    )
    run_dbt_with_commands(connection_string=get_connection_string(cred))
