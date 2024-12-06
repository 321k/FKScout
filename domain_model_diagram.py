import pandas as pd
from openai import OpenAI
import os

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def create_domain_model_diagram_with_openai(schema):

    schema_string = schema.to_csv(index=False)

    # Define the messages
    messages = [
        {
            "role": "system",
            "content": "You are an AI assistant specializing in database schema analysis."
        },
        {
            "role": "user",
            "content": (
                "Create a Mermaid ER diagram of the domain model for the following database schema:"
                f"{schema_string}\n\n"
            ),
        },
    ]

    try:
        # Send request to OpenAI API
        response = client.chat.completions.create(
            model="gpt-4",
            messages=messages,
            max_tokens=1000
        )

        # Extract the content of the assistant's reply
        diagram_text = response.choices[0].message.content
        return diagram_text

    except Exception as e:
        print(f"Error: {e}")
        return None

def generate_mermaid_programmatically(df):
    diagram = "erDiagram\n"

    for _, row in df.iterrows():
        if pd.notna(row['referenced_table']) and row['referenced_table'] != "":
            diagram += row['table_name']
            diagram += " }o--||"
            diagram += row['referenced_table']
            diagram += ' : "" '
            diagram += "\n"
    return diagram

def print_mermaid(diagram):
    diagram = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    </head>
    <body>
        <div class="mermaid">
            { diagram }
        </div>
        <script>
            mermaid.initialize({{ startOnLoad: true }});
        </script>
    </body>
    </html>
    """
    return diagram

if __name__ == "__main__":
    schema_df = pd.read_csv("schema.csv")
    candidates = pd.read_csv("candidates.csv")
    schema = pd.merge(schema_df, candidates[['table_name', 'column_name', 'key_type', 'referenced_table'\
, 'referenced_column']], on=['table_name', 'column_name'], how='left')
    res = create_domain_model_diagram(schema)

    if res:
        # Save to a file
        with open("mermaid_chart.txt", "w") as file:
            file.write(res)
        print("Mermaid chart saved to 'mermaid_chart.txt'.")
    else:
        print("Failed to generate Mermaid chart.")

    # Print the schema for verification
    print(schema)