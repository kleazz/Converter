import pandas as pd
import yaml
from typing import Dict, List

def read_csv(file_path: str) -> pd.DataFrame:
    """
    Reads a CSV file into a pandas DataFrame.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the CSV file.
    """
    return pd.read_csv(file_path, sep=';')

def map_data_type(data_type: str) -> str:
    """
    Maps the data type from the CSV to a standard format used in the YAML metadata.

    Args:
        data_type (str): The data type string from the CSV.

    Returns:
        str: The standardized data type string.
    """
    type_mapping = {
        'VARCHAR2': 'string',
        'NUMBER': 'decimal(38,0)',
        'DATE': 'date',
    }
    return next((standard for key, standard in type_mapping.items() if key in data_type), 'string')

def process_table(df: pd.DataFrame, source: Dict) -> Dict:
    """
    Processes a DataFrame containing table metadata and converts it into a structured dictionary format suitable for YAML output.

    Args:
        df (pd.DataFrame): A DataFrame containing metadata for a single table.
        source (Dict): A dictionary containing source details.

    Returns:
        Dict: A dictionary, representing metadata for a single table.
    """
    table_name = df.iloc[0]['table_name']
    # Process columns using apply
    columns = df.apply(lambda row: {
        "name": row['COLUMN_NAME'].lower(),
        "data_type": map_data_type(row['DATA_TYPE']),
        **({"date_format": row['DATA_FORMAT']} if pd.notna(row['DATA_FORMAT']) else {}),
        **({"is_nullable": True} if row['NULLABLE'] == 'Y' else {})
    }, axis=1).tolist()
    # Create a dictionary with table metadata
    table = {
        "name": table_name,
        "source": source,
        "columns": columns,
        "primary_key": ["unid"],
        "cdc_column": "dml_flag",
        "cdc_type": "soft"
    }
    # Return the processed table information
    return table

def process_source(df: pd.DataFrame) -> List[Dict]:
    """
    Creates sources and anchors for the metadata based on the DataFrame.

    Args:
        df (pd.DataFrame): A DataFrame containing metadata information.

    Returns:
        List[Dict]: A list of dictionaries where each dictionary contains the columns of one table.
    """
    tables = []
    sources = {}
    
    # Group the DataFrame by file name, decimal separator, file type, and table name
    grouped = df.groupby(['FILE_NAME', 'DECIMAL_SEPARATOR', 'FILE_TYPE', 'table_name'])

    # Process each group
    for (file_name, decimal_separator, file_type, table_name), group in grouped:
        # Generate a unique source key for each group
        source_key = f"source_{file_name}_{decimal_separator}_{file_type}"

        if source_key not in sources:
            # Create a dictionary with source metadata
            source_data = {
                "load_type": "delta",
                "file_pattern": file_name,
                "partition_pattern": r'(\d+(?:\.\d+)?)_(?:\d+\D+)$',
                "params": {
                    "decimal_separator": decimal_separator,
                    "format": file_type.lower()
                }
            }
            sources[source_key] = source_data

        # Process each table's metadata and append it to the tables list
        table_info = process_table(group, source=sources[source_key])
        tables.append(table_info)

    return tables

def generate_metadata_yaml(df: pd.DataFrame) -> str:
    """
    Generates a YAML string containing the metadata for all tables.

    Args:
        df (pd.DataFrame): A DataFrame containing metadata information.

    Returns:
        str: A YAML-formatted string representing the metadata.
    """
    tables = process_source(df)

    # Create the metadata structure
    metadata = {
        "version": 2,
        "name": "dwh",
        "tables": tables
    }

    yaml_content = yaml.dump(metadata, sort_keys=False, default_flow_style=False)
    return yaml_content

def generate_gdpr_yaml(df: pd.DataFrame) -> str:
    """
    Generates a YAML string containing GDPR compliance information.

    Args:
        df (pd.DataFrame): A DataFrame containing GDPR flags and column information.

    Returns:
        str: A YAML-formatted string representing GDPR compliance information.
    """
    gdpr = [
        {
            "name": table_name,
            "personal_data": {
                "hash": df[df['GDPR_FLAG']]['COLUMN_NAME'].str.lower().tolist()
            }
        }
        for table_name, df in df.groupby('table_name')
        if not df[df['GDPR_FLAG']].empty
    ]
    
    gdpr_content = yaml.dump(gdpr, sort_keys=False)
    return gdpr_content

def save_yaml(file_path: str, content: str) -> None:
    """
    Saves a string to a file in YAML format.

    Args:
        file_path (str): The path where the YAML file will be saved.
        content (str): The YAML content to be written to the file.

    Returns:
        None
    """
    with open(file_path, 'w') as file:
        file.write(content)

def main(csv_file_path: str, metadata_yaml_path: str, gdpr_yaml_path: str) -> None:
    """
    The main function where we read the CSV file, generate the metadata and GDPR YAML files,
    and save them to the specified paths.

    Args:
        csv_file_path (str): The path to the input CSV file.
        metadata_yaml_path (str): The path where the metadata YAML file will be saved.
        gdpr_yaml_path (str): The path where the GDPR YAML file will be saved.

    Returns:
        None
    """
    df = read_csv(csv_file_path)
    
    metadata_yaml = generate_metadata_yaml(df)
    gdpr_yaml = generate_gdpr_yaml(df)
    
    save_yaml(metadata_yaml_path, metadata_yaml)
    save_yaml(gdpr_yaml_path, gdpr_yaml)

# Example usage
if __name__ == "__main__":
    main('metadata.csv', 'metadata.yaml', 'gdpr.yaml')
