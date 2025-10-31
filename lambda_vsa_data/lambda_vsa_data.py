import pygsheets
import psycopg2
from psycopg2 import extras
from pprint import pprint
import boto3
import json
from botocore.exceptions import ClientError
import tempfile
import os

def get_secrets(secret_name, region_name):
    """
    Retrieve secrets from AWS Secrets Manager.
    The credentials for Google Sheets and Databnase are stored here.

    :param secret_name: Name of the secret in AWS Secrets Manager.
    :param region_name: AWS region where the secret is stored.
    :return: Dictionary containing the secret key-value
    """
    boto3_session = boto3.session.Session()
    client = boto3_session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    response = client.get_secret_value(SecretId=secret_name)
    secret = response['SecretString']
    return json.loads(secret) if secret.startswith('{') else secret

def read_google_sheet_data(sheet_url, worksheet_name, credentials):
    """
    Reads data from a Google Sheet.

    :param sheet_url: The URL of the Google Sheet.
    :param worksheet_name: The name of the worksheet to read from.
    :param credentials: Google service account credentials dictionary.
    :return: List of lists containing the rows of the worksheet.
    """

    print(type(credentials))
    
    try:
        # Write credentials to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(credentials, temp_file)
            temp_file_path = temp_file.name
        
        try:
            # Use service_file with the temporary file path
            gc = pygsheets.authorize(service_file=temp_file_path)
            
            sh = gc.open_by_url(sheet_url)
            wks = sh.worksheet_by_title(worksheet_name)
            data = wks.get_all_values(include_tailing_empty=False)
            
            return data
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            
    except Exception as e:
        print(f"Error reading Google Sheet: {e}")
        raise

def import_emp_data_to_postgres(data, db_config, table_name, unique_columns):
    """
    Imports data into a PostgreSQL database, performing an upsert (insert or update).

    :param data: List of lists containing the rows of the worksheet.
    :param db_config: Dictionary containing database connection parameters.
    :param table_name: The name of the table to import data into.
    :param unique_columns: List of columns that uniquely identify a row for upsert.
    """
    
    # Establish connection to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    columns = ['name', 'email', 'vsa_uspr_access', 'vsa_pe_access', 'vsa_noc_access', 'vsa_dci_access', 'vsa_sc_access']
    
    # Prepare the SQL query to insert data
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

    # Prepare the data for insertion, excluding the header row
    values = [tuple(row) for row in data[1:]] #skip the first row

    # Prepare the ON CONFLICT clause for upsert
    conflict_clause = f"ON CONFLICT ({', '.join(unique_columns)}) DO UPDATE SET "
    update_clause = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col not in unique_columns])

    # Complete the upsert query
    upsert_query = insert_query + f" {conflict_clause} {update_clause}"

    # Execute the upsert query
    extras.execute_values(cursor, upsert_query, values)

    # Commit the transaction
    conn.commit()
    # Close connection
    cursor.close()
    conn.close()

def import_app_data_to_postgres(data, db_config, table_name, unique_columns):
    """
    Imports data into a PostgreSQL database, performing an upsert (insert or update).

    :param data: List of lists containing the rows of the worksheet.
    :param db_config: Dictionary containing database connection parameters.
    :param table_name: The name of the table to import data into.
    :param unique_columns: List of columns that uniquely identify a row for upsert.
    """

    # Establish connection to the PostgreSQL database
    conn = psycopg2.connect(**db_config, connect_timeout=10, options='-c statement_timeout=30000')
    cursor = conn.cursor()

    columns = ['name', 'owner', 'vsa_type', 'vsa_uspr', 'vsa_pe', 'vsa_noc', 'vsa_dci', 'vsa_sc']

    # Prepare the SQL query to insert data
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

    # Prepare the data for insertion, excluding the header row
    values = [tuple(row) for row in data[1:]]  # Skip the first row

     # Remove duplicates based on unique columns
    seen = set()
    unique_values = []
    for row in values:
        unique_key = tuple(row[columns.index(col)] for col in unique_columns)
        if unique_key not in seen:
            seen.add(unique_key)
            unique_values.append(row)

    # Prepare the ON CONFLICT clause for upsert
    conflict_clause = f"ON CONFLICT ({', '.join(unique_columns)}) DO UPDATE SET "
    update_clause = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col not in unique_columns])

    # Complete the upsert query
    upsert_query = insert_query + f" {conflict_clause} {update_clause}"

    # Execute the upsert query
    extras.execute_values(cursor, upsert_query, unique_values)

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()


def lambda_handler(event, context):
    try:
        # List of Secrets to retrieve
        secret_name_gapi = 'vonage/googleapi/sheets'
        secret_name_db = 'Vonage/cloudquery/cloudquery'
        region_name = 'us-east-1'  # Update if needed
        sheet_url_emp = 'https://docs.google.com/spreadsheets/d/19vvQgQkJg0y7g_P6L4yENgOnO-vAHDsg7dOX556ZXJM/edit?usp=sharing'
        sheet_url_apps = 'https://docs.google.com/spreadsheets/d/1lAWbVaBkee1ruKvIdIly33hRLlVv4b_HlexzXu1l2kI/edit?usp=sharing'
        worksheet_name_apps = 'Current VSA Master List'
        worksheet_name_emp = 'VonagePersonVSAAttributes'
        table_name_apps = 'vsa_app_classifications'
        table_name_emp = 'employee_vsa_attributes'

        db_secrets = get_secrets(secret_name_db, region_name)
        google_secrets = get_secrets(secret_name_gapi, region_name)

        # Ensure google_secrets is a dictionary
        if isinstance(google_secrets, str):
            google_secrets = json.loads(google_secrets)

        db_config = {
            'host': db_secrets['host'],
            'port': db_secrets['port'],
            'user': db_secrets['rw-user'],
            'password': db_secrets['password']
        }
        
        # Read data from Google Sheets
        data_emp = read_google_sheet_data(sheet_url_emp, worksheet_name_emp, google_secrets)
        data_apps = read_google_sheet_data(sheet_url_apps, worksheet_name_apps, google_secrets)

        # Print data to verify
        for row in data_emp:
            print(row)

        for row in data_apps:
            print(row)

        # Import data into PostgreSQL - employee VSA attributes
        unique_columns_emp = ['email']
        import_emp_data_to_postgres(data_emp, db_config, table_name_emp, unique_columns_emp)

        # Import data into PostgreSQL - VSA app classifications
        unique_columns_apps = ['name']
        import_app_data_to_postgres(data_apps, db_config, table_name_apps, unique_columns_apps)
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Success'})
        }
        
    except Exception as e:
        print(f"Lambda execution failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }