from google.cloud import bigquery
import pandas as pd

def check_pk_uniqueness(client, project_id, dataset, table, primary_key):
    # Construct the query dynamically
    query = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT {primary_key}) AS unique_rows
    FROM `{project_id}.{dataset}.{table}`;
    """
    # Run the query
    query_job = client.query(query)
    result = query_job.result().to_dataframe()
    total_rows = result.loc[0, "total_rows"]
    unique_rows = result.loc[0, "unique_rows"]
    return total_rows, unique_rows

def key_existence_check(client, project_id, dataset, table, key):
    query = f"""
    SELECT COUNT(*) as records
    FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table}' AND column_name = '{key}';
    """

    query_job = client.query(query)
    result = query_job.result().to_dataframe()
    records = result.loc[0, "records"]
    return records

def verify_foreign_key(client, project_id, dataset, child_table, foreign_key, parent_table, primary_key):
    query = f"""
    SELECT
        SUM(CASE WHEN parent.{primary_key} IS NOT NULL THEN 1 ELSE 0 END) AS valid_references,
        SUM(CASE WHEN parent.{primary_key} IS NULL THEN 1 ELSE 0 END) AS invalid_references
    FROM `{project_id}.{dataset}.{child_table}` AS child
    LEFT JOIN `{project_id}.{dataset}.{parent_table}` AS parent
    ON child.{foreign_key} = parent.{primary_key};
    """
    query_job = client.query(query)
    result = query_job.result().to_dataframe()
    valid_references = result.loc[0, "valid_references"]
    invalid_references = result.loc[0, "invalid_references"]

    return valid_references, invalid_references

def valid_and_invalid_matches():
    return true