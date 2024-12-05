import subprocess
from google.cloud import bigquery
import pandas as pd
from symbolic_analysis import symbolic_analysis
from validate_keys import check_pk_uniqueness, key_existence_check, verify_foreign_key
import json
from domain_model_diagram import generate_mermaid_programmatically, print_mermaid
import sys
from pathlib import Path
import argparse



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

def list_datasets(project_id):
    # initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # list all datasets
    datasets = client.list_datasets()

    if not datasets:
        print(f"No datasets found in project {project_id}.")
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

def validate_keys(client, candidates):
    candidates['records'] = None
    candidates['unique_records'] = None
    candidates['exists'] = None
    candidates['valid_references'] = None
    candidates['invalid_references'] = None

    for index, row in candidates.iterrows():
        try:
            res = check_pk_uniqueness(client, datasets[0], row['table_name'], row['column_name'])
            if res:
                candidates.at[index, 'records'] = res[0]
                candidates.at[index, 'unique_records'] = res[1]
                print(res)
            else:
                print("Error")
        except Exception as e:
            print(f"Error: {e}")

        try:
            table_name = row['table_name'] if row['key_type'] == 'primary' else row['referenced_table']
            column_name = row['column_name'] if row['key_type'] == 'primary' else row['referenced_column']
            res = key_existence_check(client, project_id, datasets[0], table_name, column_name)
            if res:
                candidates.at[index, 'exists'] = res
                print(res)
            else:
                print("Error")
        except Exception as e:
            print(f"Error: {e}")

        try:
            if row['key_type'] == 'foreign':
                res = verify_foreign_key(client, datasets[0], row['table_name'], row['column_name'], row['referenced_table'], row['referenced_column'])
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
        print(candidates.iloc[index])
    return candidates

def main():
    parser = argparse.ArgumentParser(description="Process some variables.")
    parser.add_argument("--project_id", required=True, help="Google Cloud Project ID")
    parser.add_argument("--dataset", required=True, help="Dataset Name")
    args = parser.parse_args()
    
    print(f"Project ID: {args.project_id}")
    print(f"Dataset Name: {args.dataset}")
    project_id = args.project_id
    dataset = args.dataset

    try:
        authenticate_with_gcloud()
    except RuntimeError as e:
        print(e)

    client = bigquery.Client(project=project_id)
    datasets = [dataset] if dataset else list_datasets(project_id)

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

    user_input = input("Do you want to run symbolic analysis with GPT-4 (yes/no)?")
    if user_input == 'yes':
        print('Searching for primary and foreign keys')
        analysis_output = symbolic_analysis(schema_df)
        with open("files/symbolic_analysis.txt", "w") as file:
            file.write(json.dumps(analysis_output, indent=4))
        print('Analysis saved to files/symbolic_analysis.txt')
    else:
        with open('files/symbolic_analysis.txt', "r") as file:
            analysis_output = json.load(file)

    user_input = input("Do you want to run foreign and primary key validation (yes/no)?")
    if user_input == 'yes':
        print("Validating the keys")
        candidates = pd.DataFrame(analysis_output["arguments"]["keys"])
        candidates = validate_keys(client, candidates)
        print("Saving results as candidates.csv")
        candidates.to_csv("files/candidates.csv", index=False)
    else:
        candidates = pd.read_csv("files/candidates.csv")

    mermaid = generate_mermaid_programmatically(candidates)
    mermaid_html = print_mermaid(mermaid)
    if mermaid_html:
        # Save to a file
        with open("files/mermaid_chart.html", "w") as file:
            file.write(mermaid_html)
        print("Mermaid chart saved to 'files/mermaid_chart.html'.")
    else:
        print("Failed to generate Mermaid chart.")


if __name__ == "__main__":
    main()
    