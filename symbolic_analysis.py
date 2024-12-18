import os
from openai import OpenAI
import pandas as pd
import json



# Initialize the OpenAI client
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def find_pk(schema_df):
    schema_string = schema_df.to_csv(index=False)
    messages = [
        {
            "role": "system",
            "content": "You are an AI assistant specializing in database schema analysis."
        },
        {
            "role": "user",
            "content": (
                "Identify all potential primary keys in the following database schema. "
                "Provide the results in JSON format.\n\n"
                f"{schema_string}\n\n"
            ),
        },
    ]

    # Define the function schema
    functions = [
        {
            "name": "validate_keys",
            "description": "Validate if any of the given keys exist in the database schema.",
            "parameters": {
                "type": "object",
                "properties": {
                    "keys": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "table_name": {"type": "string", "description": "Name of the table."},
                                "column_name": {"type": "string", "description": "Name of the column to validate."},
                                "key_type": {
                                    "type": "string",
                                    "enum": ["primary", "foreign"],
                                    "description": "Type of key."
                                },
                            },
                            "required": ["table_name", "column_name", "key_type"],
                        },
                    },
                },
                "required": ["keys"],  # Ensure the "keys" array is always provided
            },
        }
    ]

    # Make the API call
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=messages,
            functions=functions,  # Use the functions parameter
            function_call="auto",  # Automatically call the function if applicable
            max_tokens=1000,
        )
        
        # Extract function call details
        if response.choices[0].message.function_call:
            function_call = response.choices[0].message.function_call
            function_name = function_call.name
            function_arguments = json.loads(function_call.arguments)  # Parse the arguments JSON

            print(f"Function Name: {function_name}")
            print(f"Function Arguments: {function_arguments}")

            # Process arguments (example logic)
            return {
                "name": function_name,
                "arguments": function_arguments,
            }
        else:
            print("No function call detected.")
            return None

    except Exception as e:
        print(f"Error: {e}")
        return None


def find_fk(table_data, primary_keys):
    """
    Use OpenAI's API to analyze column names for patterns.
    """
    # Convert the DataFrame to a CSV string
    table_data_csv = table_data.to_csv(index=False)
    primary_keys_csv = primary_keys.to_csv(index=False)

    # Define the messages
    messages = [
        {
            "role": "system",
            "content": "You are an AI assistant specializing in database schema analysis."
        },
        {
            "role": "user",
            "content": (
                "Identify all potential foreign keys in the following database table:"
                f"{table_data_csv}\n\n"
                "Please ise list of tables with primary keys provided below as a reference:"
                f"{primary_keys_csv}\n\n"
                "Provide the results in JSON format.\n\n"
            ),
        },
    ]

    # Define the function schema
    functions = [
        {
            "name": "foreign_keys",
            "description": "Validate if the given keys exist in the database schema.",
            "parameters": {
                "type": "object",
                "properties": {
                    "keys": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "table_name": {"type": "string", "description": "Name of the table."},
                                "column_name": {"type": "string", "description": "Name of the column to validate."},
                                "key_type": {
                                    "type": "string",
                                    "enum": ["primary", "foreign"],
                                    "description": "Type of key."
                                },
                                "referenced_table": {
                                    "type": "string",
                                    "description": "Name of the table containing the referenced primary key (for foreign keys).",
                                    "nullable": True
                                },
                                "referenced_column": {
                                    "type": "string",
                                    "description": "Name of the column containing the referenced primary key (for foreign keys).",
                                    "nullable": True
                                }
                            },
                            "required": ["column_name", "key_type"],
                        },
                    },
                },
                "required": ["keys"],  # Ensure the "keys" array is always provided
            },
        },
    ]

    # Make the API call
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=messages,
            functions=functions,  # Use the functions parameter
            function_call="auto",  # Automatically call the function if applicable
            max_tokens=1000,
        )
        
        # Extract function call details
        if response.choices[0].message.function_call:
            function_call = response.choices[0].message.function_call
            function_name = function_call.name
            function_arguments = json.loads(function_call.arguments)  # Parse the arguments JSON

            print(f"Function Name: {function_name}")
            print(f"Function Arguments: {function_arguments}")

            # Process arguments (example logic)
            return {
                "name": function_name,
                "arguments": function_arguments,
            }
        else:
            print("No function call detected.")
            return None

    except Exception as e:
        print(f"Error: {e}")
        return None

# Example usage
if __name__ == "__main__":
    schema_df = pd.read_csv("schema.csv")
#    candidates = []
#    for index, row in schema_df.iterrows():
#        candidate = symbolic_analysis(row)
#        print(candidate)
#        if candidate:
#            candidate = pd.DataFrame(json.loads(candidate)["arguments"]["keys"])
#            candidates.append(candidate)
#    results = pd.concat(candidates, ignore_index=True)
    results = find_fk(schema_df)
    
    with open("files/symbolic_analysis.txt", "w") as file:
        file.write(json.dumps(results, indent=4))

