import subprocess
import logging
from google.cloud import bigquery
import pandas as pd
from symbolic_analysis import find_pk, find_fk
from validate_keys import check_pk_uniqueness, key_existence_check, verify_foreign_key
import json
from domain_model_diagram import generate_mermaid_programmatically, print_mermaid
import sys
from pathlib import Path
import argparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def authenticate_with_gcloud():
    """
    Ensure the user is authenticated via gcloud. Automatically open the browser if needed.
    """
    credentials_path = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
    if credentials_path.exists():
        print("Cached credentials found. Skipping authentication.")
    else:
        try:
            print("Authenticating with Google Cloud...")
            result = subprocess.run(
                ["gcloud", "auth", "application-default", "login"],
                check=True
            )
            if result.returncode == 0:
                print("Authentication successful.")
            else:
                print("Authentication failed. Please try manually.")
        except subprocess.CalledProcessError as e:
            print(f"Error during authentication: {e}")
            print("Please run `gcloud auth application-default login` manually.")
            raise  # Re-raise the error to indicate failure.

def list_datasets(client):
    # list all datasets
    datasets = client.list_datasets()

    if not datasets:
        print(f"No datasets found in project.")
        return []

    dataset_names = [dataset.dataset_id for dataset in datasets]
    return dataset_names

def extract_schema(client, project_id, dataset_id):
    # query to fetch table schema
    query = f"""
    SELECT
        table_name, 
        column_name
    FROM 
        `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    ORDER BY 
        table_name, ordinal_position
    """

    # execute the query
    query_job = client.query(query)
    results = query_job.to_dataframe()

    return results

def validate_keys(client, project_id, dataset, candidates):
    candidates['records'] = None
    candidates['unique_records'] = None
    candidates['exists'] = None
    candidates['valid_references'] = None
    candidates['invalid_references'] = None

    for index, row in candidates.iterrows():
        try:
            res = check_pk_uniqueness(client, project_id, dataset, row['table_name'], row['column_name'])
            if res:
                candidates.loc[index, 'records'] = res[0]
                candidates.loc[index, 'unique_records'] = res[1]
                print(res)
            else:
                print("Error")
        except Exception as e:
            print(f"Error: {e}")

        try:
            table_name = row['table_name'] if row['key_type'] == 'primary' else row['referenced_table']
            column_name = row['column_name'] if row['key_type'] == 'primary' else row['referenced_column']
            res = key_existence_check(client, project_id, dataset, table_name, column_name)
            if res:
                candidates.at[index, 'exists'] = res
                print(res)
            else:
                print("Error")
        except Exception as e:
            print(f"Error: {e}")

        try:
            if row['key_type'] == 'foreign':
                res = verify_foreign_key(client, project_id, dataset, row['table_name'], row['column_name'], row['referenced_table'], row['referenced_column'])
                if res:
                    candidates.at[index, 'valid_references'] = res[0]
                    candidates.at[index, 'invalid_references'] = res[1]
                    print(res)
                else:
                    print("Error")
            else:
                print('Skip')
        except Exception as e:
            print(f"Error: {e}")
        if index in candidates.index:
            print(candidates.loc[index])
        else:
            print(f"Index {index} no longer exists in the DataFrame.")
    return candidates

def main():
    parser = argparse.ArgumentParser(description="Process some variables.")
    parser.add_argument("--project_id", required=True, help="Google Cloud Project ID")
    parser.add_argument("--dataset", required=True, help="Dataset Name")
    parser.add_argument("--billing_project_id", required=False, help="Billing project ID (optional)")
    args = parser.parse_args()
    
    print(f"Project ID: {args.project_id}")
    print(f"Dataset Name: {args.dataset}")
    project_id = args.project_id
    dataset = args.dataset

    if args.billing_project_id:
        print(f"Billing project id: {args.billing_project_id}")
        billing_project_id = args.billing_project_id
    else:
        print(f"Billing project id: {args.billing_project_id}")
        billing_project_id = project_id
    

    try:
        authenticate_with_gcloud()
    except RuntimeError as e:
        print(e)

    client = bigquery.Client(project=billing_project_id)
    datasets = [dataset] if dataset else list_datasets(client)

    print(f"- {dataset}")
    user_input = input("Do you want to fetch a fresh database schema from BigQuery (yes/no)?")
    if user_input == 'yes':
        print("Fetching dataset schemas...")
        schema_df = pd.DataFrame()
        for dataset in datasets:
            schema_df = pd.concat([schema_df, extract_schema(client, project_id, dataset)], ignore_index=True)

        schema_df.to_csv("files/schema.csv", index=False)
        print("Schema saved to files/schema.csv")
    else:
        schema_df = pd.read_csv("files/schema.csv")
        print("Schema loaded from files/schema.csv")

    user_input = input("Do you want to search for primary keys with GPT-4 (yes/no)?")
    if user_input == 'yes':
        print('Searching for primary keys')
        pk_analysis_output = find_pk(schema_df)
        primary_keys = pd.DataFrame(pk_analysis_output["arguments"]["keys"])
        primary_keys.to_csv("files/pk_analysis.csv", index=False)
        print('Primary key analysis saved to files/pk_analysis.txt')
    else:
        primary_keys = pd.read_csv("files/pk_analysis.csv")

    user_input = input("Do you want to search for foreign keys with GPT-4 (yes/no)?")
    if user_input == 'yes':
        print('Searching for foreign keys')
        candidates = pd.DataFrame()
        for table in list(set(schema_df['table_name'])):
            print(table)
            table_data = schema_df[schema_df['table_name'] == table][['table_name', 'column_name']]
            fk_analysis_output = find_fk(table_data, primary_keys)
            print(fk_analysis_output)
            if fk_analysis_output is None:
                continue
            elif "arguments" in fk_analysis_output and "keys" in fk_analysis_output["arguments"]:
                foreign_keys = pd.DataFrame(fk_analysis_output["arguments"]["keys"])
                foreign_keys['table_name'] = table
            else:
                continue
            candidates = pd.concat([candidates, foreign_keys], ignore_index=True)
        candidates = candidates[candidates['column_name']!="id"]
        candidates.to_csv("files/fk_analysis.csv", index=False)
    else:
        candidates = pd.read_csv("files/fk_analysis.csv")


    user_input = input("Do you want to run foreign and primary key validation (yes/no)?")
    if user_input == 'yes':
        print("Validating the keys")
        filtered_candidates = candidates[
            candidates['referenced_table'].notnull() & 
            candidates['referenced_column'].notnull()
        ].copy()
        schema_validation = validate_keys(client, project_id, dataset, filtered_candidates)
        print("Saving results as schema_validation.csv")
        schema_validation.to_csv("files/schema_validation.csv", index=False)
    else:
        schema_validation = pd.read_csv("files/schema_validation.csv")


    filtered_schema = schema_validation[(
        (schema_validation['exists']==1) & 
        (schema_validation['valid_references']/(schema_validation['invalid_references']+schema_validation['valid_references'])>0.1)
    )]
    mermaid = generate_mermaid_programmatically(filtered_schema)
    mermaid_html = print_mermaid(mermaid)
    if mermaid_html:
        # Save to a file
        file = f'files/{dataset}_mermaid_chart.html'
        with open(f"files/{dataset}_mermaid_chart.html", "w") as file:
            file.write(mermaid_html)
        print(f"Mermaid chart saved to 'files/{dataset}_chart.html'.")
    else:
        print("Failed to generate Mermaid chart.")


if __name__ == "__main__":
    main()
    