# Import statements
import os
import sys
import pytz
import json
import shutil
import findspark
import pandas as pd
from io import BytesIO
import configparser
from dateutil.parser import parse
from datetime import datetime, timedelta
from pyspark.sql.functions import *
import boto3
from botocore.exceptions import NoCredentialsError
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pymongo import MongoClient
from DLA_TEST.src.conn import query_execution, snow_conn, s3_client
from DLA_TEST.constant import *


def parse_config(config_path):
    # Initialize configparser
    config = configparser.ConfigParser()

    # Load configuration file
    config.read(config_path)

    # Initialize dictionary to store parsed values
    parsed_config = {}

    # Access sections and their keys
    parsed_config['s3'] = {
        'aws_access_key_id': config.get('s3', 'aws_access_key_id'),
        'aws_secret_access_key': config.get('s3', 'aws_secret_access_key'),
        'bucket_name': config.get('s3', 'bucket_name'),
        'bucket_path': config.get('s3', 'bucket_path'),
        'source_folder': config.get('s3','source_folder'),
        'destination_folder_base': config.get('s3','destination_folder_base'),
        'archive_folder': config.get('s3','archive_folder'),
        'region_name': config.get('s3','region_name')
    }

    parsed_config['staging_folder'] = {
        'staging_folder_path': config.get('staging_folder', 'staging_folder_path'),
        'doc_per_file': config.get('staging_folder', 'doc_per_file')
    }

    parsed_config['spark'] = {
        'mysql_jar_file_path': config.get('spark', 'mysql_jar_file_path'),
        'postgresql_jar_file_path': config.get('spark', 'postgresql_jar_file_path')
    }

    parsed_config['metadata'] = {
        'metadata_file_path': config.get('metadata', 'metadata_file_path'),
        'scripts_folder_path': config.get('metadata', 'scripts_folder_path'),
        'dtype_comp_file_path': config.get('metadata', 'dtype_comp_file_path')
    }

    parsed_config['postgresql'] = {
        'ip_address': config.get('postgresql', 'ip_address')
    }
    
    parsed_config['sql_server'] = {
        'ip_address': config.get('sql_server', 'server_name')
    }

    parsed_config['mongodb'] = {
        'uri': config.get('mongodb', 'uri'),
        'ip_address': config.get('mongodb', 'ip_address')
    }

    parsed_config['SNOWFLAKE'] = {
        'database': config.get('SNOWFLAKE', 'database'),
        'schema': config.get('SNOWFLAKE', 'schema'),
        'stg_database' : config.get('SNOWFLAKE','stg_database'),
        'stg_schema': config.get('SNOWFLAKE', 'stg_schema'),
        'warehouse': config.get('SNOWFLAKE', 'warehouse'),
        'ext_stg_name': config.get('SNOWFLAKE', 'ext_stg_name'),
        'account': config.get('SNOWFLAKE', 'account'),
        'user': config.get('SNOWFLAKE', 'user'),
        'password': config.get('SNOWFLAKE', 'password'),
        'cntrl_tbl': config.get('SNOWFLAKE', 'cntrl_tbl'),
        'json_file_format': config.get('SNOWFLAKE', 'json_file_format')
    }

    parsed_config['EMAIL'] = {
        'sender': config.get('EMAIL', 'sender'),
        'password': config.get('EMAIL', 'password'),
        'receiver': config.get('EMAIL', 'receiver'),
        'smtp': config.get('EMAIL', 'smtp'),
        'port': config.get('EMAIL', 'port')
    }

    return parsed_config


def list_all_files(s3_client, bucket_name, prefix):
    """List all files in an S3 bucket under a specified prefix, handling pagination."""
    files = []
    kwargs = {'Bucket': bucket_name, 'Prefix': prefix}

    while True:
        response = s3_client.list_objects_v2(**kwargs)
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.xlsx'):  # Only include files that end with .xlsx
                    files.append(obj)

        if response.get('IsTruncated'):  # Check if there are more pages
            kwargs['ContinuationToken'] = response['NextContinuationToken']
        else:
            break

    return files


def get_latest_modified(files, config_path):
    parsed_config = parse_config(config_path)
    source_folder = parsed_config['s3']['source_folder']
    archive_folder = parsed_config['s3']['archive_folder']
    latest_modified = {}
    for obj in files:
        key = obj['Key'].replace(source_folder, '').replace(archive_folder, '')
        latest_modified[key] = obj['LastModified']
    return latest_modified


def column_nms(spark, src_db_name, table_name, schema, column_names, config_path, obj_type):
    if obj_type.lower() == 'file':
        # Parse configuration parameters
        parsed_config = parse_config(config_path)
        source_folder = parsed_config['s3']['source_folder']
        bucket_name = parsed_config['s3']['bucket_name']
        region_name = parsed_config['s3']['region_name']
        aws_access_key_id = parsed_config['s3']['aws_access_key_id']
        aws_secret_access_key = parsed_config['s3']['aws_secret_access_key']
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

        # List all files in the source folder
        source_files = list_all_files(s3, bucket_name, source_folder)
        print(source_files)
        if not source_files:
            raise ValueError("No Excel files found in the source folder.")
        
        table_name_lower = table_name.lower()

        print(f"Found {len(source_files)} files in the source folder: {source_folder}")

        # Find the specific file that matches the table name
        source_key = None
        for file in source_files:
            file_key = file['Key']
            if table_name_lower == file_key.lower() or f'/{table_name_lower}' in file_key.lower() or f'_{table_name_lower}' in file_key.lower():
               source_key = file_key
               break
        if not source_key:
            raise ValueError(f"No file found for table '{table_name}' in the source folder.")
            # Get the object from S3
        obj = s3.get_object(Bucket=bucket_name, Key=source_key)
        data = obj['Body'].read()

        # Read Excel file into pandas DataFrame
        df_pandas = pd.read_excel(BytesIO(data), header=0)

        # Retrieve column names and data types from pandas DataFrame
        col_names = df_pandas.columns.tolist()
        data_types = df_pandas.dtypes.apply(lambda dt: dt.name).tolist()

        # Create a DataFrame with column names, data types, and ordinal positions
        columns_data = list(zip(col_names, data_types, range(1, len(col_names) + 1)))
        col_nms = spark.createDataFrame(columns_data, ["COLUMN_NAME", "DATA_TYPE", "ORDINAL_POSITION"])

        # Display the retrieved column names, data types, and ordinal positions
        col_nms.show()
        print(f"Retrieved column names from '{source_key}': {col_names}")

        return col_nms
    else:
        if column_names == '*':
            # Query to retrieve all column names, data types, and ordinal positions
            query = f"""select column_name, data_type, ordinal_position 
                        from information_schema.columns 
                        where table_name = '{table_name}' and TABLE_SCHEMA = '{schema}'"""
            # print(query) 
        else:
            # Split the input string into a list of columns
            columns = column_names.split(',')

            # Use list comprehension to add single quotes to each column
            formatted_columns = ["'{}'".format(col.strip()) for col in columns]

            # Join the formatted columns into a single string
            col_string = ', '.join(formatted_columns)

            # Query to retrieve specific column names, data types, and ordinal positions
            query = f"""select column_name, data_type, ordinal_position 
                        from information_schema.columns 
                        where table_name = '{table_name}' and TABLE_SCHEMA = '{schema}' and column_name in ({col_string})"""

        print(f"Querying SQL Server for column information for table '{table_name}' in schema '{schema}'.")

        # Read the source table from SQL Server
        col_nms = query_execution(spark, src_db_name, query, config_path)

        # Display the retrieved column names
        print("Retrieved column names, data types, and ordinal positions:")
        col_nms.show()

        return col_nms
    

def cols_in_snowflake(snowflake_conn, sf_tbl, config_path):
    parsed_config = parse_config(config_path)
    database = parsed_config['SNOWFLAKE']['database']
    sf_schema = parsed_config['SNOWFLAKE']['schema']
    sf_cntrl_tbl = parsed_config['SNOWFLAKE']['cntrl_tbl']

    cursor = snowflake_conn.cursor()
    
    sf_tbl = sf_tbl.upper()
    sf_schema = sf_schema.upper()
    cols_query = f"""SELECT COLUMN_NAME FROM {database}.INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{sf_tbl}' AND TABLE_SCHEMA = '{sf_schema}'
                    AND COLUMN_NAME NOT IN ('EDW_INSERTED_TIMESTAMP', 'EDW_UPDATED_TIMESTAMP')
                    ORDER BY ORDINAL_POSITION"""
    # print(cols_query)
    data = cursor.execute(cols_query)
    # Fetch all rows from the cursor
    data = cursor.fetchall()

    # Extracting column names from the fetched data
    column_names = [row[0] for row in data]

    return column_names


def mongo_ext_full(src_db_name, database_name, schema_name, obj_type, object_name, 
                    sf_tbl_name, insrt_time_col, updt_time_col, primary_key_col, config_path):
    """
    Extract data from a source MongoDB and save it to a staging file.

    Args:
        src_db_name (str): Source name (e.g., SQL, PostgreSQL & MongoDB).
        database_name (str): Source database name.
        schema_name (str): Source schema name.
        obj_type (str): Object type (e.g., table, collection).
        object_name (str): Name of the source table.
        sf_tbl_name (str): Target table name which has to be created at Snowflake.
        insrt_time_col (str): Column name for insertion timestamp.
        updt_time_col (str): Column name for update timestamp.
        primary_key_col (str): Column name(s) for primary key.
        config_path (str): Path to the configuration file.

    Returns:
        tuple: A tuple containing the staging file path, maximum timestamp.
        dataframe: Source IDs DataFrame.
    """


    def parse_datetime(date_str):
        """Parse a datetime string into a datetime object, handling multiple formats."""
        if date_str == 'NaT' or pd.isnull(date_str):
            return None
        if isinstance(date_str, datetime):
            return date_str
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        raise ValueError(f"Date string '{date_str}' does not match any known format.")
    
    # Retrieve configuration parameters by parsing the configuration file
    parsed_config = parse_config(config_path)
    staging_folder_path = parsed_config['staging_folder']['staging_folder_path']
    doc_per_file = parsed_config['staging_folder']['doc_per_file']

    if src_db_name.lower() == 'mongodb':
        # Extract MongoDB connection parameters
        url = parsed_config['mongodb']['uri']
        database = database_name
        collection = object_name

        # Connect to MongoDB
        client = MongoClient(url)
        db = client[database]
        collection = db[collection]

        # Retrieve data from MongoDB collection
        data = list(collection.find())

        # Convert to DataFrame
        df = pd.DataFrame(data)
        print(df.head())

        # Split the primary key columns into a list and stripping any whitespace
        primary_key_columns = [col.strip() for col in primary_key_col.split(",")]

        # Select only the primary key columns from the DataFrame
        source_df_ids = df[primary_key_columns]

        # Ensure the columns are in string format before applying parse_datetime
        df[insrt_time_col] = df[insrt_time_col].astype(str)
        df[updt_time_col] = df[updt_time_col].astype(str)

        # Convert insertion and update timestamp columns to datetime
        df[insrt_time_col] = df[insrt_time_col].apply(parse_datetime)
        df[updt_time_col] = df[updt_time_col].apply(parse_datetime)

        # Calculate the maximum values for the insertion and update timestamp columns
        max_insrt_time = df[insrt_time_col].max()
        print(f"\nmaximum insertion timestamp: {max_insrt_time}")

        max_updt_time = df[updt_time_col].max()
        print(f"maximum updation timestamp: {max_updt_time}\n")


        def custom_max(val1, val2):
            """Returns the maximum of two values."""
            return val1 if val1 > val2 else val2
        
        # Determine the maximum timestamp between insertion and update timestamps
        max_timestamp = custom_max(max_insrt_time, max_updt_time)
        print(f"maximum timestamp: {max_timestamp}\n")

        # Construct the staging folder path
        staging_folder_path = f'{project_path}/{staging_folder_path}'
        print(f"staging folder path: {staging_folder_path}")

        # Construct the staging file path
        staging_file_path = f"{staging_folder_path}/{src_db_name}/{database_name}/{schema_name}/{obj_type}/{sf_tbl_name}"

        # Ensure the directory exists before saving the file
        if not os.path.exists(staging_file_path):
            os.makedirs(staging_file_path)

        # Determine the chunk size for saving data to files
        chunk_size = int(doc_per_file)  # Number of documents per chunk

        # Split the DataFrame into chunks and save each chunk to a JSON file
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            chunk_file_name = f"{staging_file_path}/{sf_tbl_name}_chunk_{i // chunk_size + 1}.json"

            # Write the chunk to a JSON file
            with open(chunk_file_name, 'w', encoding='utf-8') as file:
                json.dump(chunk.to_dict(orient='records'), file, ensure_ascii=False, indent=4, default=str) 
            print(f"Chunk {i // chunk_size + 1} written to {chunk_file_name}")

        print(f"data extracted to json file\n")

        return staging_file_path, max_timestamp, source_df_ids


def extraction_full(spark, src_db_name, database_name, schema_name, obj_type, object_name, column_names, insrt_time_col,
                    updt_time_col, primary_key_col, config_path, sf_tbl):
    """
    Extract data from a source SQL Server and save it to a staging file.
    """
    parsed_config = parse_config(config_path)
    source_folder = parsed_config['s3']['source_folder']
    staging_folder_path = parsed_config['staging_folder']['staging_folder_path'] 
    archive_folder = parsed_config['s3']['archive_folder']
    destination_folder_base = parsed_config['s3']['destination_folder_base']
    aws_access_key_id = parsed_config['s3']['aws_access_key_id']
    aws_secret_access_key = parsed_config['s3']['aws_secret_access_key']
    bucket_name = parsed_config['s3']['bucket_name']
    region_name = parsed_config['s3']['region_name']

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

    if obj_type.lower() == 'file':
        # List all Excel files in the source and archive folders
        source_files = list_all_files(s3, bucket_name, source_folder)
        archive_files = list_all_files(s3, bucket_name, archive_folder)

        if not source_files:
            raise ValueError("No Excel files found in the source folder.")
        
        print("Files listed successfully.") 

        table_name_lower = object_name.lower()       

        # Get the latest modified times for source and archive files
#        source_latest = get_latest_modified(source_files,config_path)
#        archive_latest = get_latest_modified(archive_files,config_path)

        # Find the specific file that matches the table name
        source_key = None
        for file in source_files:
            file_key = file['Key']
            if table_name_lower == file_key.lower() or f'/{table_name_lower}' in file_key.lower() or f'_{table_name_lower}' in file_key.lower():
               source_key = file_key
               print(source_key)
               break
 
        if not source_key:
            raise ValueError(f"No file found for table '{table_name}' in the source folder.")
#            # Check if the file is more recent in the source folder
#            if file_name in archive_latest and source_latest[file_name] <= archive_latest[file_name]:
#                print(f"Skipping {source_key} as it is not more recent than the archive version.")
#                continue
            
        print(f"Reading Excel file {source_key}...")

        # Read Excel file into Pandas DataFrame
        obj = s3.get_object(Bucket=bucket_name, Key=source_key)
        data = obj['Body'].read()
        print(f"Data type: {type(data)}, Data length: {len(data)}")

        # Check if data is empty
        if not data:
           raise ValueError(f"No data found in the object {source_key}.")

        # Attempt to read the data into a DataFrame
        try:
            df = pd.read_excel(BytesIO(data))
        except Exception as e:
            raise ValueError(f"Error reading Excel file {source_key}: {e}")

        print(f"File {source_key} read successfully.")

        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S.%f')

        print("Datetime columns formatted.")

        # Split the primary_key_col string and strip whitespace
        primary_key_columns = [col.strip() for col in primary_key_col.split(",")]

        # Select the columns from the DataFrame
        source_df_ids = df[primary_key_columns]

#            print(source_df_ids)

        max_insrt_time = pd.to_datetime(df[insrt_time_col]).max()
        max_updt_time = pd.to_datetime(df[updt_time_col]).max()
        print(max_insrt_time)
        print(max_updt_time)
        if max_insrt_time >= max_updt_time:
            max_timestamp = max_insrt_time
        else:
            max_timestamp = max_updt_time
              
        print(max_timestamp)            
        # Define dynamic Parquet file path in S3
        destination_folder = f'{destination_folder_base}{sf_tbl}/'
        parquet_file = f"s3://{bucket_name}/{destination_folder}{sf_tbl}.parquet"
        print(destination_folder)
        print(parquet_file)

        # Save DataFrame to Parquet (emulating Spark DataFrame to Parquet)
        df.to_parquet(parquet_file, engine='pyarrow', compression='snappy')

        archive_key = f"{archive_folder}{source_key.replace(source_folder, '')}"
        s3.copy_object(Bucket=bucket_name, Key=archive_key, CopySource=f"{bucket_name}/{source_key}")
        print(f"Moved {source_key} to {archive_key}")

        return max_timestamp, source_df_ids

    else:
        # Read the source table
        query = f"SELECT {column_names} FROM {schema_name}.{object_name}"

        source_df = query_execution(spark, src_db_name, query, config_path)
        source_df.show()

        primary_key_columns = [col.strip() for col in primary_key_col.split(",")]
        source_df_ids = source_df.select(*primary_key_columns)
    
        # Assuming 'insrt_time' and 'updt_date' are the column names in your DataFrame
        max_values = source_df.agg(max(insrt_time_col).alias("max_insrt_time"), max(updt_time_col).alias("max_updt_date"))

        # Calculate the maximum of max_insrt_time and max_updt_date and assign it to a variable
        max_timestamp = max_values.select(greatest("max_insrt_time", "max_updt_date").alias("max_result")).collect()[0][
            "max_result"]

        # get the path for extract to save the parquet
        staging_folder_path = f'{project_path}/{staging_folder_path}'

        # Generate a timestamp for unique file names
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"{sf_tbl}_{timestamp}.parquet"
        staging_file_path = f"{staging_folder_path}/{src_db_name}/{database_name}/{schema_name}/{obj_type}/{sf_tbl}/{file_name}"

        # Write the source DataFrame to a Parquet file in the staging folder
        source_df.write.option('header', 'true').parquet(staging_file_path)
        print(f"data extracted to parquet file")
        return staging_file_path, file_name, max_timestamp, source_df_ids

def last_run_time_def(snowflake_conn, table_name, sf_database, sf_schema, sf_cntrl_tbl, obj_type, sf_tbl):
    cursor = snowflake_conn.cursor()
    if obj_type.lower() == 'file':
        query = f"""SELECT MAX_TIMESTAMP FROM {sf_database}.{sf_schema}.{sf_cntrl_tbl} 
                    WHERE TARGET_TABLE_NAME = '{sf_tbl}' AND STATUS = 'Success' 
                    AND MAX_TIMESTAMP <> '9999-12-31 23:59:59.999' and ROWS_INSERTED <> 0  
                    ORDER BY RUN_ID DESC LIMIT 1
                """
        cursor.execute(query)
        result = cursor.fetchone()
        if result and result[0] is not None:
            last_run_time = result[0]
            # Subtract 10 minutes from last_run_time
            new_last_run_time = last_run_time - timedelta(minutes=10)
            # Format the new last run time
            formatted_last_run_time = new_last_run_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            print("Last Run Time:", formatted_last_run_time)
            return formatted_last_run_time
        else:
            print("No successful runs found for the specified conditions.")
     
    else:
        query = f"""select MAX_TIMESTAMP from {sf_database}.{sf_schema}.{sf_cntrl_tbl} 
                    where (SOURCE_OBJECT_NAME) = '{table_name}'
                    AND STATUS = 'SUCCESS' AND MAX_TIMESTAMP <> '9999-12-31 23:59:59.999' and ROWS_INSERTED <> 0 ORDER BY RUN_ID DESC LIMIT 1
                """
        cursor.execute(query)
        # print(query)
        result = cursor.fetchone()
        if result and result[0] is not None:
            last_run_time = result[0]
            # Subtract 10 minutes from last_run_time
            new_last_run_time = last_run_time - timedelta(minutes=10)
            # Format the new last run time
            formatted_last_run_time = new_last_run_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            print("Last Run Time:", formatted_last_run_time)
            return formatted_last_run_time
        else:
            print("No successful runs found for the specified conditions.")


def mongo_incr_load(snowflake_conn, src_db_name, database_name, schema_name, obj_type, object_name,
                    sf_tbl, table_name_in_ctrl_tbl, insrt_time_col, updt_time_col, config_path):
    """
    Extract incremental data from a SQL Server table and save it to a staging file.

    Args:
        snowflake_conn (object): The Snowflake connection object.
        src_db_name (str): Source name (e.g., SQL, PostgreSQL & MongoDB).
        database_name (str): Source database name.
        schema_name (str): Source schema name.
        obj_type (str): Object type (e.g., table, collection).
        object_name (str): Name of the source table.
        sf_tbl_name (str): Target table name which has to be created at Snowflake.
        table_name_in_ctrl_tbl (str): Source table name in the control table.
        insrt_time_col (str): Column name for insertion timestamp.
        updt_time_col (str): Column name for update timestamp.
        config_path (str): Path to the configuration file.

    Returns:
        Tuple: Tuple containing staging file path and file name.
    """

    def parse_datetime(date_str):
        """Parse a datetime string into a datetime object, handling multiple formats."""
        if date_str == 'NaT' or pd.isnull(date_str):
            return None
        if isinstance(date_str, datetime):
            return date_str
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        raise ValueError(f"Date string '{date_str}' does not match any known format.")
    
    # Retrieve configuration parameters by parsing the configuration file
    parsed_config = parse_config(config_path)
    sf_database = parsed_config['SNOWFLAKE']['database']
    sf_schema = parsed_config['SNOWFLAKE']['schema']
    sf_cntrl_tbl = parsed_config['SNOWFLAKE']['cntrl_tbl']
    staging_folder_path = parsed_config['staging_folder']['staging_folder_path']

    # MongoDB connection parameters
    url = parsed_config['mongodb']['uri']
    database = database_name
    collection = object_name

    # Retrieve the last run time from the control table
    last_run_time = last_run_time_def(snowflake_conn, table_name_in_ctrl_tbl, sf_database, sf_schema,
                                      sf_cntrl_tbl, obj_type, sf_tbl)

    if src_db_name.lower() == 'mongodb':
        # Connect to MongoDB
        client = MongoClient(url)
        db = client[database]
        collection = db[collection]

        # Convert last_run_time to datetime
        last_run_time = str(last_run_time)
        last_run_timestamp = datetime.strptime(last_run_time, "%Y-%m-%d %H:%M:%S.%f")

        # Define the query to fetch records updated or inserted after the last run time
        query = {
            "$or": [
                {"createdAt": {"$gt": last_run_timestamp}},
                {"updatedAt": {"$gt": last_run_timestamp}}
            ]
        }

        # Perform the query
        documents = collection.find(query)

        # Convert the result to a Pandas DataFrame
        df = pd.DataFrame(list(documents))
        print(df.head())

        # Ensure the columns are in string format before applying parse_datetime
        df[insrt_time_col] = df[insrt_time_col].astype(str)
        df[updt_time_col] = df[updt_time_col].astype(str)

        # Convert insertion and update timestamp columns to datetime
        df[insrt_time_col] = df[insrt_time_col].apply(parse_datetime)
        df[updt_time_col] = df[updt_time_col].apply(parse_datetime)

        # Calculate the maximum values for the insertion and update timestamp columns
        max_insrt_time = df[insrt_time_col].max()
        print(f"\nmaximum insertion timestamp: {max_insrt_time}")

        max_updt_time = df[updt_time_col].max()
        print(f"maximum updation timestamp: {max_updt_time}\n")

        # Determine the number of rows in the DataFrame
        num_rows = len(df)


        def custom_max(val1, val2):
            """Returns the maximum of two values."""
            return val1 if val1 > val2 else val2
        
        # Finding the maximum of the two maximum values
        max_timestamp = custom_max(max_insrt_time, max_updt_time)
        print(f"maximum timestamp: {max_timestamp}\n")

        if num_rows != 0:
            # Calculate the maximum insertion time
            max_insrt_time = df[insrt_time_col].max()
            max_timestamp = max_insrt_time
        else:
            max_timestamp = '9999-12-31 23:59:59.999'

        # Construct the path for the staging folder
        staging_folder_path = f'{project_path}/{staging_folder_path}'
        print(staging_folder_path)

        # Get the full path for the staging file
        staging_folder_path = os.path.join(os.getcwd(), staging_folder_path)
        staging_file_path = os.path.join(staging_folder_path, src_db_name, database_name, schema_name, obj_type, sf_tbl)
        sub_dir_path = staging_file_path

         # Remove the directory and its contents if it exists
        try:
            shutil.rmtree(staging_file_path)
        except FileNotFoundError:
            print(f"directory not found: {staging_file_path}")

        # Ensure the directory exists before saving the file
        os.makedirs(sub_dir_path, exist_ok=True)

        # Define the JSON file path
        json_file_name = os.path.join(sub_dir_path, f"{sf_tbl}_incr_load.json")

        # Write the source DataFrame to a Json file in the staging folder
        with open(json_file_name, 'w', encoding='utf-8') as file:
            json.dump(df.to_dict(orient='records'), file, ensure_ascii=False, indent=4, default=str)

        print(f"incremental extraction completed. Data saved to: {sub_dir_path}")

        return sub_dir_path, max_timestamp

    else:
       print(f"your source is not Mongodb check once, your source at config is \"{src_db_name}\"")
       return None, None


def extraction_incr(spark, snowflake_conn, src_db_name, database_name, schema_name, obj_type, object_name, is_deleted,
                    table_name_in_ctrl_tbl, column_names, insrt_time_col, updt_time_col, config_path, sf_tbl):
    """
    Extract incremental data from a SQL Server table and save it to a staging file.

    Args:
        spark (SparkSession): The Spark session.
        source_database (str): Name of the source database.
        database_name (str): Name of the target database.
        schema_name (str): Name of the schema.
        table_name (str): Name of the SQL Server table.
        config_file (str): Path to the configuration file.
        lmd_col (str): Column containing the last modified timestamp.
        last_run_time (str): Last run time for incremental extraction.

    Returns:
        Tuple: Tuple containing staging file path and file name.
    """
    print("Parsing configuration...")

    # Retrieve configuration parameters
    parsed_config = parse_config(config_path)
    sf_database = parsed_config['SNOWFLAKE']['database']
    sf_schema = parsed_config['SNOWFLAKE']['schema']
    sf_cntrl_tbl = parsed_config['SNOWFLAKE']['cntrl_tbl']
    staging_folder_path = parsed_config['staging_folder']['staging_folder_path']
    source_folder = parsed_config['s3']['source_folder']
    archive_folder = parsed_config['s3']['archive_folder']
    destination_folder_base = parsed_config['s3']['destination_folder_base']
    aws_access_key_id = parsed_config['s3']['aws_access_key_id']
    aws_secret_access_key = parsed_config['s3']['aws_secret_access_key']
    bucket_name = parsed_config['s3']['bucket_name']
    region_name = parsed_config['s3']['region_name']    
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    print("Configuration parsed successfully.")


    if obj_type.lower() == 'file':
        # List all Excel files in the source and archive folders
        source_files = list_all_files(s3, bucket_name, source_folder)
        archive_files = list_all_files(s3, bucket_name, archive_folder)

        if not source_files:
            raise ValueError("No Excel files found in the source folder.")
        
        print("Files listed successfully.")        

        # Get the latest modified times for source and archive files
 #       source_latest = get_latest_modified(source_files,config_path)
 #       archive_latest = get_latest_modified(archive_files,config_path)

        table_name_lower = object_name.lower()

        # Find the specific file that matches the table name
        source_key = None
        for file in source_files:
            file_key = file['Key']
            if table_name_lower == file_key.lower() or f'/{table_name_lower}' in file_key.lower() or f'_{table_name_lower}' in file_key.lower():
               source_key = file_key
               print(source_key)
               break

        if not source_key:
            raise ValueError(f"No file found for table '{object_name}' in the source folder.")

            
        print(f"Reading Excel file {source_key}...")

            # Read Excel file into Pandas DataFrame
        obj = s3.get_object(Bucket=bucket_name, Key=source_key)
        data = obj['Body'].read()
        print(f"Data type: {type(data)}, Data length: {len(data)}")

            # Check if data is empty
        if not data:
            raise ValueError(f"No data found in the object {source_key}.")

        # Attempt to read the data into a DataFrame
        try:
            df = pd.read_excel(BytesIO(data))
        except Exception as e:
            raise ValueError(f"Error reading Excel file {source_key}: {e}")

        print(f"File {source_key} read successfully.")

        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S.%f')

        print("Datetime columns formatted.")

        max_insrt_time = pd.to_datetime(df[insrt_time_col]).max()
        max_updt_time = pd.to_datetime(df[updt_time_col]).max()
        print(max_updt_time)
        if max_insrt_time >= max_updt_time:
            max_timestamp = max_insrt_time
        else:
            max_timestamp = max_updt_time
        
        last_run_time = last_run_time_def(snowflake_conn, table_name_in_ctrl_tbl, sf_database, sf_schema, sf_cntrl_tbl, obj_type, sf_tbl)
        print(last_run_time)
        incremental_df = df[(pd.to_datetime(df[insrt_time_col]) > pd.to_datetime(last_run_time)) |
                                (pd.to_datetime(df[updt_time_col]) > pd.to_datetime(last_run_time))]
        incremental_df.reset_index(drop=True, inplace=True)

        print(incremental_df)
        destination_folder = f'{destination_folder_base}{sf_tbl}/'
        parquet_file = f"s3://{bucket_name}/{destination_folder}{sf_tbl}.parquet"
        incremental_df.to_parquet(parquet_file, engine='pyarrow', compression='snappy')
            
        return "staging_file_path_placeholder", "file_name_placeholder", max_timestamp
    else:
        last_run_time = last_run_time_def(snowflake_conn, table_name_in_ctrl_tbl, sf_database, sf_schema, sf_cntrl_tbl, obj_type, sf_tbl)

        if is_deleted == 'yes':
            # Read the incremental data of source table
            query = f"""SELECT {column_names} FROM {schema_name}.{object_name}
                        WHERE {insrt_time_col} > '{last_run_time}'
                    """
            incr_data = query_execution(spark, src_db_name, query, config_path)
            incr_data.show()

            if incr_data.count() != 0:
                # Assuming 'insrt_time' and 'updt_date' are the column names in your DataFrame
                max_values = incr_data.agg(max(insrt_time_col).alias("max_insrt_time"))

                # Calculate the maximum of max_insrt_time and max_updt_date and assign it to a variable
                max_timestamp = max_values.select(max_values["max_insrt_time"].alias("max_result")).collect()[0][
                    "max_result"]

            else:
                max_timestamp = '9999-12-31 23:59:59.999'

        else:
            # Read the incremental data of source table from SQL Server
            query = f"""SELECT {column_names} FROM {schema_name}.{object_name}
                        WHERE {insrt_time_col}> '{last_run_time}' OR {updt_time_col} > '{last_run_time}'
                    """
            incr_data = query_execution(spark, src_db_name, query, config_path)
            incr_data.show()

            if incr_data.count() != 0:
                # Assuming 'insrt_time' are the column names in your DataFrame
                max_values = incr_data.agg(max(insrt_time_col).alias("max_insrt_time"))

                # Calculate the maximum of max_insrt_time and assign it to a variable
                max_timestamp = max_values.select("max_insrt_time").collect()[0]["max_insrt_time"]

            else:
                max_timestamp = '9999-12-31 23:59:59.999'

        # get the path for extract to save the parquet
        staging_folder_path = f'{project_path}/{staging_folder_path}'

        # Generate a timestamp for unique file names
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"{sf_tbl}_{timestamp}.parquet"
        staging_file_path = f"{staging_folder_path}/{src_db_name}/{database_name}/{schema_name}/{obj_type}/{sf_tbl}/{file_name}"

        # Write the incremental data to a Parquet file in the staging folder
        incr_data.write.option('header', 'true').parquet(staging_file_path)

        print(f"Incremental extraction completed. Data saved to: {staging_file_path}")

        return staging_file_path, file_name, max_timestamp


def extract_for_no_lmd(spark, load_type, src_db_name, database_name, column_names, schema_name, object_name, config_path):
    # Retrieve configuration parameters
    parsed_config = parse_config(config_path)
    staging_folder_path = parsed_config['staging_folder']['staging_folder_path']
    archive_folder_path = parsed_config['staging_folder']['archive_folder_path']

    query = f"SELECT {column_names} FROM {schema_name}.{object_name}"

    data = query_execution(spark, src_db_name, query, config_path)
    data.show()

    # get the path for extract to save the parquet
    staging_folder_path = f'{project_path}/{staging_folder_path}'
    file_name = f"{object_name}"
    staging_file_path = f"{staging_folder_path}/{src_db_name}/{database_name}/{schema_name}/{object_name}/{file_name}"

    # get the path for archive
    archive_folder_path = f'{project_path}/{archive_folder_path}'
    file_name = f"{object_name}"
    archive_file_path = f"{archive_folder_path}/{src_db_name}/{database_name}/{schema_name}/{object_name}/{file_name}"

    if load_type.lower() == 'full':
        print(f"Creating parquet file in archive as well as extract folder")
        data.write.option('header', 'true').parquet(staging_file_path, mode="overwrite")
        data.write.option('header', 'true').parquet(archive_file_path, mode="overwrite")
        print(f"extraction completed. Data saved to: {staging_file_path}")
    else:

        existing_df = spark.read.parquet(archive_file_path)

        hash_key_col = "hash_key"
        existing_df = existing_df.withColumn(hash_key_col, sha2(concat_ws("_", *existing_df.columns), 256))
        incremental_df = archive_file_path.withColumn(hash_key_col, sha2(concat_ws("_", *data.columns), 256))

        new_data = incremental_df.join(existing_df, hash_key_col, "left_anti")

        print(f"Creating parquet file for new data")
        new_data.write.option('header', 'true').parquet(staging_file_path, mode="overwrite")
        print(f"Appending new data to archive folder")
        incremental_df.write.option('header', 'true').parquet(archive_file_path, mode="overwrite")

        print(f"Incremental extraction completed. Data saved to: {staging_file_path}")


def move_fldr(folder_name, s3_folder_path, config_path):
    # Calling get_config function
    s3, bucket_name = s3_client(config_path)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    archive_path = f"{folder_name}/{s3_folder_path}/"

    # Check if the source folder exists
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder_path)
        if 'Contents' not in response:
            print(f"\nThe source folder '{s3_folder_path}' does not exist in S3 bucket {bucket_name}.")
            return
    except Exception as e:
        print(f"An error occurred: {e}")
        return

    # Create the archive folder if it does not exist
    try:
        s3.head_object(Bucket=bucket_name, Key=archive_path)
    except Exception as e:
        if e.response['Error']['Code'] == '404':
            s3.put_object(Bucket=bucket_name, Key=archive_path)
            print(f'\nThe folder "{archive_path}" has been created in the S3 bucket.')
        else:
            print(f'An error occurred: {e}')
            return
    else:
        print(f'\nThe folder "{archive_path}" already exists in the S3 bucket.')

    # Move parquet files from source to archive folder
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.parquet'):
            copy_source = {'Bucket': bucket_name, 'Key': key}
            destination_key = f"{archive_path}{key[len(s3_folder_path)+1:]}"
            try:
                s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)
                s3.delete_object(Bucket=bucket_name, Key=key)
                print(f'Moved {key} to {destination_key}')
            except Exception as e:
                print(f"An error occurred while moving {key}: {e}")

    print(f'\nAll parquet files from {s3_folder_path} have been moved to the archive folder {archive_path}.')


def upload_to_s3(local_folder, s3_folder_path, s3_file_name, config_path):
    # Retrieve configuration parameters
    parsed_config = parse_config(config_path)
    aws_access_key_id = parsed_config['s3']['aws_access_key_id']
    aws_secret_access_key = parsed_config['s3']['aws_secret_access_key']
    bucket_name = parsed_config['s3']['bucket_name']
    print(bucket_name)
    # Set AWS credentials as environment variables
    os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key

    s3 = boto3.client('s3')
    for file in os.listdir(local_folder):
        if file.endswith(".parquet"):
            local_file_path = os.path.join(local_folder, file)
            s3_file_path = s3_folder_path + '/' + s3_file_name
            try:
                s3.upload_file(local_file_path, bucket_name, s3_file_path)
                print(f"Upload Successful: {file}")
            except FileNotFoundError:
                print(f"The file {file} was not found")
            except NoCredentialsError:
                print("Credentials not available")
        elif file.endswith(".json"):
            local_file_path = os.path.join(local_folder, file)
            s3_file_path = os.path.join(s3_folder_path, file)
            try:
                s3.upload_file(local_file_path, bucket_name, s3_file_path)
                print(f"Upload Successful: {file}")
            except FileNotFoundError:
                print(f"The file {file} was not found")
            except NoCredentialsError:
                print("Credentials not available")


def convertdtypes(datatypes, source_database, config_path, obj_type):
    """
    Convert SQL Server datatypes or Excel datatypes to Snowflake datatypes based on a mapping file.
    Returns:
        list: List of corresponding Snowflake datatypes.
    """
    # Retrieve configuration parameters
    parsed_config = parse_config(config_path)
    dtype_comp_file_path = parsed_config['metadata']['dtype_comp_file_path']

    source_database = source_database.upper()

    # Construct input file path based on project path and configuration
    input_file = f'{project_path}/{dtype_comp_file_path}'
    # Read mapping from CSV file
    mapping = pd.read_csv(input_file)
    con_datatypes = []

    if obj_type.lower() == 'file':
        # Loop through the Excel datatypes list
        for f in datatypes:
            # Loop through the rows of the 'mapping' DataFrame
            for index, row in mapping.iterrows():
                # Check if the datatype without parentheses matches the 'EXCEL' column
                if row['EXCEL'] == f:
                    # Add the corresponding Snowflake datatype to the list
                    con_datatypes.append(row['SNOWFLAKE'])
                    break  # Move to the next input datatype
    else:
        # Loop through the input datatypes list
        for f in datatypes:
            # Loop through the rows of the 'mapping' DataFrame
            for index, row in mapping.iterrows():
                # Check if the datatype without parentheses matches the 'SQL_SERVER' column
                if row[source_database] == f:
                    # Add the corresponding Snowflake datatype to the list
                    con_datatypes.append(row['SNOWFLAKE'])
                    break  # Move to the next input datatype

    return con_datatypes


def generate_ext_column_definition(column_name, data_type, needs_quotes, obj_type):
    if needs_quotes and data_type == 'TIME':
        return f'(CAST(TO_TIME(TO_TIMESTAMP($1:"{column_name}")) AS TIME)) AS "{column_name}"'
    elif needs_quotes:
        return f'(CAST($1:"{column_name}" AS {data_type})) AS "{column_name}"'
    elif data_type == 'TIME':
        return f'(CAST(TO_TIME(TO_TIMESTAMP($1:"{column_name}")) AS TIME)) AS {column_name}'
    elif obj_type.lower == 'file':
        return f'(CAST($1:"{column_name}" AS {data_type})) AS "{column_name}"'
    else:
        return f'(CAST($1:"{column_name}" AS {data_type})) AS {column_name}'


def generate_column_definition(column_name, data_type, needs_quotes,obj_type):
    if needs_quotes:
        return f'"{column_name}" {data_type}'
    elif obj_type.lower() == 'file':
        return f'"{column_name}" {data_type}'
    else:
        return f"{column_name} {data_type}"


def create_scripts(snowflake_conn, col_nms, is_mapping_file, source_database, database_name,
                   schema_name, obj_type, table_name, is_deletion, deleted_table, config_path, sf_tbl):
    """
    Generate SQL scripts for Snowflake stage and target tables.

    Returns:
        None
    """
    # Retrieve configuration parameters
    parsed_config = parse_config(config_path)
    print(parsed_config)
    scripts_folder_path = parsed_config['metadata']['scripts_folder_path']
    database = parsed_config['SNOWFLAKE']['database']
    sf_stg_database = parsed_config['SNOWFLAKE']['stg_database']
    ext_stage = parsed_config['SNOWFLAKE']['ext_stg_name']
    aws_access_key_id = parsed_config['s3']['aws_access_key_id']
    aws_secret_access_key = parsed_config['s3']['aws_secret_access_key']
    s3_bckt_path = parsed_config['s3']['bucket_path']
    schema =  parsed_config['SNOWFLAKE']['schema']
    stg_schema = parsed_config['SNOWFLAKE']['stg_schema']
    file_format_name = parsed_config['SNOWFLAKE']['json_file_format']
 
    cursor = snowflake_conn.cursor()

    fscript = ""

    # Build script for the target schema
    ext_schema_script = f"CREATE DATABASE IF NOT EXISTS {database};\n"
    fscript += ext_schema_script + "\n"

    # Build script for the target schema
    ext_schema_script = f"CREATE SCHEMA IF NOT EXISTS {database}.{schema};\n"
    fscript += ext_schema_script + "\n"
    
    #Build script for the stage_database
    ext_schema_script = f"CREATE DATABASE IF NOT EXISTS {sf_stg_database};\n"
    fscript += ext_schema_script + "\n"
   
   # Build script for the stage schema   
    ext_schema_script = f"CREATE SCHEMA IF NOT EXISTS {sf_stg_database}.{stg_schema};\n"
    fscript += ext_schema_script + "\n"
    
    if source_database.lower() != 'mongodb':
        # Build script for the external stage
        ext_stg_script = f"""CREATE STAGE IF NOT EXISTS {sf_stg_database}.{stg_schema}.{ext_stage} URL = '{s3_bckt_path}'
            CREDENTIALS = (
                AWS_KEY_ID = '{aws_access_key_id}',
                AWS_SECRET_KEY = '{aws_secret_access_key}'
                )
                FILE_FORMAT = (TYPE = 'PARQUET')
            ;\n"""
        if obj_type.lower() == 'file':
            cursor.execute(ext_stg_script)
        else:
            fscript += ext_stg_script + "\n"
    
        data = col_nms.collect()
        column_names = col_nms.columns
    
        # Construct a list of rows for Pandas DataFrame
        rows = [row.asDict() for row in data]
        # Create a Pandas DataFrame
        pandas_df = pd.DataFrame(rows, columns=column_names)
        # Access columns as Python lists
        if obj_type.lower() == 'file':
            datatypes = pandas_df['DATA_TYPE'].tolist()
            colnames = pandas_df['COLUMN_NAME'].tolist()
        else:
            datatypes = pandas_df['data_type'].tolist()
            colnames = pandas_df['column_name'].tolist()
    
        if is_mapping_file == 'yes':
            # Now you can use datatypes and colnames as Python lists
            con_dtypes = datatypes
        else:
            con_dtypes = convertdtypes(datatypes, source_database, config_path, obj_type)

        print(colnames)
        print(datatypes)
        print(con_dtypes)
        # Build script for the staging table
        tbl_script = ""
        for i in range(len(colnames)):
            datatype = con_dtypes[i]

            colname = str(colnames[i])
            needs_quotes = any(c in colname for c in
                            [' ', '"', "'", "(", ")", ".", "#", "$", "%", "&", "*", "+", "-", "/", ":", ";", "<", "=",
                                ">", "?", "@", "[", "\\", "]", "^", "`", "{", "|", "}", "~"])
            tbl_script += generate_column_definition(colname, datatype, needs_quotes,obj_type) + ",\n"

        tbl_script = tbl_script.rstrip(",\n")

        # Construct the base script for the staging table
        base = (
                "CREATE OR REPLACE TABLE "
                + sf_stg_database
                + "."
                + stg_schema
                + "."
                + sf_tbl
                + "\n("
                + tbl_script
                + "); \n"
        )
 #       print(base)
        if obj_type=='FILE':
            cursor.execute(base)
        else:
            fscript += base + "\n"

        # Build script for the target table
        tbl_script = ""
        for i in range(len(colnames)):
            datatype = con_dtypes[i]

            colname = str(colnames[i])
            needs_quotes = any(c in colname for c in
                            [' ', '"', "'", "(", ")", ".", "#", "$", "%", "&", "*", "+", "-", "/", ":", ";", "<", "=",
                                ">", "?", "@", "[", "\\", "]", "^", "`", "{", "|", "}", "~"])
            tbl_script += generate_column_definition(colname, datatype, needs_quotes,obj_type) + ",\n"

        tbl_script = tbl_script.rstrip(",\n")

        # Construct the base script for the target table
        base = (
                "CREATE TABLE IF NOT EXISTS "
                + database
                + "."
                + schema
                + "."
                + f"{sf_tbl}"
                + "\n("
                + tbl_script
                + ", EDW_INSERTED_TIMESTAMP TIMESTAMP"
                + "\n"
                + ", EDW_UPDATED_TIMESTAMP TIMESTAMP);"
                + "\n"
        )
#        Print(base)
        if obj_type=='FILE':
            cursor.execute(base)
        else:
            fscript += base + "\n"
    else:
        # Build script for the external stage
        ext_stg_script = (
            f"""CREATE STAGE IF NOT EXISTS {sf_stg_database}.{stg_schema}.{ext_stage} 
                URL = '{s3_bckt_path}' 
                CREDENTIALS = (AWS_KEY_ID = '{aws_access_key_id}'  AWS_SECRET_KEY = '{aws_secret_access_key}');
            """
        )
        fscript = fscript + ext_stg_script + "\n"

        # SQL script for creating the staging table
        stg_tbl = (
            "CREATE TABLE IF NOT EXISTS "
            + sf_stg_database
            + "."
            + stg_schema
            + "."
            + sf_tbl
            + "\n("
            + "RAW_DATA VARIANT);"
            + "\n"
        )
        fscript = fscript + stg_tbl + "\n"

        # SQL script for creating the target table 
        trg_tbl = (
            "CREATE TABLE IF NOT EXISTS "
            + database
            + "."
            + schema
            + "."
            + sf_tbl
            + "\n("
            + "JSON_DATA VARIANT"
            + ", EDW_INSERTED_TIMESTAMP TIMESTAMP"
            + "\n"
            + ", EDW_UPDATED_TIMESTAMP TIMESTAMP);"
            + "\n"
        )
        fscript = fscript + trg_tbl + "\n"

        # SQL script for creating the file format
        file_format_script = (
            f"""CREATE FILE FORMAT IF NOT EXISTS {sf_stg_database}.{stg_schema}.{file_format_name}
                TYPE = 'JSON'
                STRIP_OUTER_ARRAY = TRUE;
            """
        )
        fscript = fscript + file_format_script + "\n"

    # get the path for sql scripts folder to write these generated scripts
    output_folder = f'{project_path}/{scripts_folder_path}/{database_name}/{schema_name}/{obj_type}'

    # create the folder if not exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # create a .sql file for these table scripts
    output_file = os.path.join(output_folder, sf_tbl)
    output_file = output_file + ".sql"

    # open the file and write these scripts into file
    file = open(output_file, "w")
    file.write(fscript)
    file.close()
    print(f"Scripts created for table {table_name}")


def runscripts(snowflake_conn, database_name, schema_name, obj_type, config_path, sf_tbl):
    """
    Executing the SQL scripts created by create_scripts module in Snowflake.

    Creates external and target tables in snowflake.
    """

    parsed_config = parse_config(config_path)
    scripts_folder_path = parsed_config['metadata']['scripts_folder_path']

    # get the path for sql scripts folder to write these generated scripts
    file_path = f'{project_path}/{scripts_folder_path}/{database_name}/{schema_name}/{obj_type}/{sf_tbl}.sql'
    print(file_path)

    # Read the contents of the SQL file
    with open(file_path, 'r') as file:
        sql_scripts = file.read()

    # Split the SQL scripts by the semicolon (;) delimiter
    scripts = sql_scripts.split(';')

    # Execute each SQL script
    for script in scripts:
        script = script.strip()  # Remove leading/trailing whitespaces
        if script:  # Skip empty scripts
            snowflake_conn.cursor().execute(script)

            print("Script executed successfully.")
            print("\n------------------")


# Function to determine if a column name needs double quotes
def needs_quotes(column_name, obj_type):
    if obj_type.lower() == 'file':
        return f'"{column_name}"'
    else:
        return " " in column_name or any(c in column_name for c in
                                     ['"', "'", ".", "(", ")", "#", "$", "%", "&", "*", "+", "-", "/", ":", ";", "<",
                                      "=", ">", "?", "@", "[", "\\", "]", "^", "`", "{", "|", "}", "~"])


def data_load(snowflake_conn, source_database, database_name, schema_name, obj_type, col_nms, table_name, is_deletion, deleted_table, primary_key_column, 
              insrt_time_col, updt_time_col, config_path, sf_tbl):

    # Retrieve configuration parameters
    parsed_config = parse_config(config_path)
    sf_database = parsed_config['SNOWFLAKE']['database']
    sf_schema = parsed_config['SNOWFLAKE']['schema']
    sf_stg_schema = parsed_config['SNOWFLAKE']['stg_schema']
    stg_database = parsed_config['SNOWFLAKE']['stg_database']
    ext_stage = parsed_config['SNOWFLAKE']['ext_stg_name']
    destination_folder_base = parsed_config['s3']['destination_folder_base']
    file_format_name = parsed_config['SNOWFLAKE']['json_file_format']

    current_timestamp_utc = datetime.now()
    # Convert UTC datetime to Asia/Kolkata timezone
    local_tz = pytz.timezone('Asia/Kolkata')
    current_timestamp = current_timestamp_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)

    if source_database.lower() != 'mongodb':
        # Collect data from Spark DataFrame
        data = col_nms.collect()
        column_names = col_nms.columns

        # Construct a list of rows for Pandas DataFrame
        rows = [row.asDict() for row in data]
        # Create a Pandas DataFrame
        pandas_df = pd.DataFrame(rows, columns=column_names)
        # Access columns as Python lists
        if obj_type.lower() == 'file':
            datatypes = pandas_df['DATA_TYPE'].tolist()
            colnames = pandas_df['COLUMN_NAME'].tolist()
        else:
            datatypes = pandas_df['data_type'].tolist()
            colnames = pandas_df['column_name'].tolist()
    
        con_dtypes = convertdtypes(datatypes, source_database, config_path,obj_type)
    
        print(colnames)
        print(datatypes)
        print(con_dtypes)

        # Build script for the columns with appropriate quoting
        columns = ",\n".join(['"' + col + '"' if needs_quotes(col,obj_type) else col for col in colnames])

        source_columns = ",\n".join(['source."' + col + '"' if needs_quotes(col,obj_type) else 'source.' + col for col in colnames])
        target_columns = ",\n".join(['target."' + col + '"' if needs_quotes(col,obj_type) else 'target.' + col for col in colnames])
        assignments = ",\n".join(
            ['target."' + col + '" = source."' + col + '"' if needs_quotes(col,obj_type) else 'target.' + col + ' = source.' + col
            for col in colnames])
        
        # Split the comma-separated primary key columns and wrap them in double quotes if needed
        primary_key_columns = [f'"{col.strip()}"' if needs_quotes(col.strip(),obj_type) else col.strip() for col in
                            primary_key_column.split(',')]

        # Modify the ON clause to include all primary key columns with appropriate quoting
        if obj_type != 'FILE':
            on_clause = " AND ".join(
            [f'target."{col}" = source."{col}"' if needs_quotes(col,obj_type) else f"target.{col} = source.{col}" for col in
            primary_key_columns])
        else:
            on_clause = " AND ".join(
            [f'target.{col} = source.{col}' if needs_quotes(col,obj_type) else f"target.{col} = source.{col}" for col in
            primary_key_columns])

        if obj_type.lower() == 'file':
            insrt_time_col = f'"{insrt_time_col}"'
            updt_time_col = f'"{updt_time_col}"'

        cursor = snowflake_conn.cursor()

        if is_deletion == 'yes':
            deletion_script = f"""DELETE FROM {sf_database}.{sf_schema}.{table_name}
                                  WHERE concat({primary_key_column}) IN (SELECT concat({primary_key_column})
                                  FROM {sf_database}.{sf_ext_schema}.{deleted_table});
                              """
            cursor.execute(deletion_script)
            rows_deleted = cursor.fetchall()
        else:
            rows_deleted = [(0,)]

            # Build script for the copy script
        copy_script = ""
        for i in range(len(colnames)):
            datatype = con_dtypes[i]

            colname = str(colnames[i])
            column_needs_quotes = needs_quotes(colname,obj_type)
            copy_script += generate_ext_column_definition(colname, datatype, column_needs_quotes, obj_type) + ",\n"

        copy_script = copy_script.rstrip(",\n")

        if obj_type.lower() == 'file':
            copy_into_script = (
            f"COPY INTO {stg_database}.{sf_stg_schema}.{sf_tbl} "
            +
            f"FROM (  "
            +
            "SELECT " 
            +
            f"{copy_script}"
            +
            f" FROM @{stg_database}.{sf_stg_schema}.{ext_stage}/{destination_folder_base}{sf_tbl}/{sf_tbl}.parquet "
            +
            f"(PATTERN => '.*{sf_tbl}.*')); "
            )
#            print(copy_into_script)
            cursor.execute(copy_into_script)
        else:
            # Add the COPY INTO command to load data from the ext_stage to the staging table
            copy_into_script = (
                f"COPY INTO {stg_database}.{sf_stg_schema}.{sf_tbl} "
                +
                f"FROM (  "
                +
                "SELECT " 
                +
                f"{copy_script}"
                +
                f" FROM @{stg_database}.{sf_stg_schema}.{ext_stage}/{source_database}/{database_name}/{schema_name}/{obj_type}/{sf_tbl} "
            +
            f"(PATTERN => '.*{sf_tbl}.*')); "
            )
            # print(copy_into_script)
            cursor.execute(copy_into_script)

        merge_script = (
                f"MERGE INTO {sf_database}.{sf_schema}.{sf_tbl} AS target \n"
                +
                "USING (WITH RankedData AS ("
                +
                "SELECT "
                +
                columns
                +
                ", ROW_NUMBER() OVER (PARTITION BY "
                +
                f"{', '.join(primary_key_columns)} ORDER BY "
                +
                f"""CASE WHEN {updt_time_col} IS NOT NULL THEN {updt_time_col} ELSE {insrt_time_col} END DESC"""
                +
                ") AS RowNum "
                +
                f" FROM {stg_database}.{sf_stg_schema}.{sf_tbl})"
                +
                " SELECT "
                +
                columns
                +
                f" FROM RankedData "
                +
                "WHERE RowNum = 1"
                +
                ") AS source "
                +
                f"ON {on_clause} "
                +
                f"WHEN MATCHED AND hash({source_columns}) != "
                +
                f"hash({target_columns})"
                +
                "THEN "
                +
                "UPDATE SET "
                +
                assignments
                +
                f", target.EDW_UPDATED_TIMESTAMP = '{current_timestamp}' "
                +
                " WHEN NOT MATCHED THEN "
                +
                f"INSERT ({columns}, EDW_INSERTED_TIMESTAMP) "
                +
                f"VALUES ({source_columns}, '{current_timestamp}')"
        )
#        print(merge_script)
        # Execute the merge script
        cursor.execute(merge_script)

        rows_upserted = cursor.fetchall()
    else:
        # Initialize cursor
        cursor = snowflake_conn.cursor()

        # Build COPY INTO script to load data from external stage to staging table
        copy_into_script = (
            f"COPY INTO {stg_database}.{sf_stg_schema}.{sf_tbl} \n"
            +
            f"FROM @{stg_database}.{sf_stg_schema}.{ext_stage}/{source_database}/{database_name}/{schema_name}/{obj_type}/{sf_tbl}/ \n"
            +
            f"FILE_FORMAT = {stg_database}.{sf_stg_schema}.{file_format_name}; \n"
        )
        # Print the query for debugging
        # print(copy_into_script)

        # Execute the copy into script
        cursor.execute(copy_into_script)

        # Build MERGE script to upsert data from staging table to target table
        merge_script = (
            f"MERGE INTO {sf_database}.{sf_schema}.{sf_tbl} AS target \n"
            +
            "USING (WITH RankedData AS ("
            +
            "SELECT "
            +
            f"RAW_DATA:{primary_key_column}::STRING AS {primary_key_column}"
            +
            ", RAW_DATA"
            +
            ", ROW_NUMBER() OVER (PARTITION BY "
            +
            f"RAW_DATA:{primary_key_column} ORDER BY "
            +
            f"CASE WHEN RAW_DATA:{updt_time_col} IS NOT NULL THEN RAW_DATA:{updt_time_col} ELSE RAW_DATA:{insrt_time_col} END DESC"
            +
            ") AS RowNum "
            +
            f" FROM {stg_database}.{sf_stg_schema}.{sf_tbl})"
            +
            " SELECT "
            +
            f"{primary_key_column}, RAW_DATA"
            +
            f" FROM RankedData "
            +
            "WHERE RowNum = 1"
            +
            ") AS source "
            +
            f"ON target.JSON_DATA:{primary_key_column}::STRING = source.{primary_key_column} "
            +
            "WHEN MATCHED AND HASH(target.JSON_DATA) != HASH(source.RAW_DATA)"
            +
            "THEN "
            +
            "UPDATE SET "
            +
            "target.JSON_DATA = source.RAW_DATA"
            +
            f", target.EDW_UPDATED_TIMESTAMP = '{current_timestamp}' "
            +
            " WHEN NOT MATCHED THEN "
            +
            "INSERT (JSON_DATA, EDW_INSERTED_TIMESTAMP) "
            +
            f"VALUES (source.RAW_DATA, '{current_timestamp}');"
        )
        # Print the query for debugging
        print(merge_script)

        # Execute the MERGE script
        cursor.execute(merge_script)

        # Fetch rows upserted
        rows_upserted = cursor.fetchall()
        rows_deleted = [(0,)]

    #trucate stage table
    truncate_stg_tbl_script = (
            f"TRUNCATE TABLE {stg_database}.{sf_stg_schema}.{sf_tbl};"
        )
 
    # print(truncate_stg_tbl_script)
    # Execute the truncate stage table script
#    cursor.execute(truncate_stg_tbl_script)

    cursor.close()

    # Print a success message
    print("Merge script executed successfully.")
    return rows_upserted, rows_deleted

    
def alter_tbl_add_col(snowflake_conn, additional_columns_data_types, config_path, sf_tbl):
    """
    Executes a query in snowflake to alter the table and add new columns if any

    """
    # Retrieve configuration parameters
    parsed_config = parse_config(config_path)
    sf_database = parsed_config['SNOWFLAKE']['database']
    sf_schema = parsed_config['SNOWFLAKE']['schema']
    sf_stg_schema = parsed_config['SNOWFLAKE']['stg_schema']
    stg_database = parsed_config['SNOWFLAKE']['stg_database']

    dtype_comp_file_path = parsed_config['metadata']['dtype_comp_file_path']

    dtype_comp_file_path = f'{project_path}/{dtype_comp_file_path}'

    cursor = snowflake_conn.cursor()

    # alter statement for each new column
    for col_name, data_type in additional_columns_data_types.items():
        # Read mapping from CSV file
        mapping = pd.read_csv(dtype_comp_file_path)
        print(data_type)
        # Loop through the rows of the 'mapping' DataFrame
        for index, row in mapping.iterrows():
            # Check if the datatype without parentheses matches the 'SQL_SERVER' column
            if row['SQLSERVER'].lower() == data_type.lower():
                # Return the corresponding Snowflake datatype
                sf_dtype = row['SNOWFLAKE']

                # generating alter statements
                altr_tbl = f"ALTER TABLE {sf_database}.{sf_schema}.{sf_tbl} add {col_name} {sf_dtype};"
                altr_tbl = f"ALTER TABLE {stg_database}.{sf_stg_schema}.{sf_tbl} add {col_name} {sf_dtype};"
                print(altr_tbl)
                # executing alter statements
                cursor.execute(altr_tbl)

    cursor.close()


def alter_tbl_del_col(snowflake_conn, object_name, col_name, config_path):
    # Retrieve configuration parameters
    parsed_config = parse_config(config_path)
    sf_database = parsed_config['SNOWFLAKE']['database']
    sf_schema = parsed_config['SNOWFLAKE']['schema']
    sf_stg_schema = parsed_config['SNOWFLAKE']['stg_schema']
    stg_database = parsed_config['SNOWFLAKE']['stg_database']

    cursor = snowflake_conn.cursor()

    tbl_cols_query = f"""ALTER TABLE {sf_database}.{sf_schema}.{sf_tbl} 
                        DROP COLUMN {col_name}"""

    cursor.execute(tbl_cols_query)

    cursor.close()


def insrt_into_ctrl_tbl(db_name, snowflake_conn, run_start, run_end, source_database_name, source_schema_name, source_object_name, obj_type, status,
                        sf_tbl, rows_inserted_ids, rows_upserted, rows_updated_ids, rows_deleted, error, max_timestamp, config_path):
    
    parsed_config = parse_config(config_path)
    database = parsed_config['SNOWFLAKE']['database']
    cntrl_tbl = parsed_config['SNOWFLAKE']['cntrl_tbl']
    schema =  parsed_config['SNOWFLAKE']['schema']
    stg_schema = parsed_config['SNOWFLAKE']['stg_schema']
    ip_address_postgres = parsed_config['postgresql']['ip_address']
    ip_address_sqlserver = parsed_config['sql_server']['ip_address']
    ip_address_mongodb = parsed_config['mongodb']['ip_address']

    current_timestamp_utc = datetime.now()
    # Convert UTC datetime to Asia/Kolkata timezone
    local_tz = pytz.timezone('Asia/Kolkata')
    current_timestamp = current_timestamp_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)

    cursor = snowflake_conn.cursor()

    duration = run_end - run_start
    duration_in_sec = duration.total_seconds()

    if obj_type.lower() == 'file':
        source_database_name = '-'
        source_schema_name = '-'
    else:
        print('not a file')
    
    # Initialize default values
    ROWS_INSERTED = 0
    ROWS_UPDATED = 0
 
    if rows_upserted and len(rows_upserted[0]) > 0:
       ROWS_INSERTED = rows_upserted[0][0]
       if len(rows_upserted[0]) > 1:
           ROWS_UPDATED = rows_upserted[0][1]

    if ROWS_INSERTED != 0:
       rows_inserted_ids = rows_inserted_ids
    else:
       rows_inserted_ids = [(0,)]

    if ROWS_UPDATED != 0:
       rows_updated_ids = rows_updated_ids
    else:
       rows_updated_ids = [(0,)]

    ROWS_DELETED = 0

    if rows_deleted and len(rows_deleted) > 0:
       ROWS_DELETED = rows_deleted[0][0]

    if db_name == 'gotxn-test':
        ip_address = ip_address_postgres
    elif db_name == 'gi_members':
        ip_address = ip_address_mongodb
    else:
        ip_address = ip_address_sqlserver

    # Convert the target table name to uppercase
    snowflake_table = sf_tbl.upper()
 
    insrt_qry = f"""insert into {database}.{schema}.{cntrl_tbl}
                        (RUN_START, RUN_END, DURATION_IN_SEC, SOURCE_DATABASE_NAME, SOURCE_SCHEMA_NAME,
                         SOURCE_OBJECT_NAME,TARGET_DATABASE_NAME, TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, TRIGGERED_BY,
                         STATUS, error_message, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, MAX_TIMESTAMP,CREATED_TIMESTAMP, CREATED_BY,
                         ROWS_INSERTED_IDS, ROWS_UPDATED_IDS, IP_ADDRESS)
                    VALUES ('{run_start}', '{run_end}', {duration_in_sec}, '{source_database_name}',
                    '{source_schema_name}', '{source_object_name}', '{database}', '{schema}',
                    '{snowflake_table}', CURRENT_ROLE(), '{status}', $${error}$$, {ROWS_INSERTED}, {ROWS_UPDATED}, {ROWS_DELETED},
                    '{max_timestamp}', '{current_timestamp}', CURRENT_USER(), $${rows_inserted_ids}$$ , $${rows_updated_ids}$$, '{ip_address}')"""   
    
    cursor.execute(insrt_qry)
    print(f"row inserted in control table")
    

def rows_ids(snowflake_conn, src_db_name, table_name, sf_tbl, primary_key_column, config_path, obj_type):
    parsed_config = parse_config(config_path)
    sf_database = parsed_config['SNOWFLAKE']['database']
    sf_schema = parsed_config['SNOWFLAKE']['schema']
    sf_cntrl_tbl = parsed_config['SNOWFLAKE']['cntrl_tbl']
 
    cursor = snowflake_conn.cursor()
   
    # Fetch the latest creation run time
    query = f"""select CREATED_TIMESTAMP from {sf_database}.{sf_schema}.{sf_cntrl_tbl}
                where SOURCE_OBJECT_NAME = '{table_name}'
                AND STATUS = 'SUCCESS' ORDER BY RUN_ID DESC LIMIT 1
            """
    cursor.execute(query)
    result = cursor.fetchone()
   
    if result and result[0] is not None:
        creation_run_time = result[0]
        print(f"creation_run_time: {creation_run_time}")
 
        # Convert primary_key_columns to list
        primary_key_columns_list = [col.strip() for col in primary_key_column.split(',')]
 
        # Construct SELECT queries dynamically
        columns_str = ', '.join(primary_key_columns_list)

        if obj_type.lower() == 'file':
            # Fetch rows based on inserted timestamp
            rows_inserted_query = f"""select "{columns_str}" from {sf_database}.{sf_schema}.{sf_tbl}
                                    where edw_inserted_timestamp >= '{creation_run_time}';
                                """
        
            # print(rows_inserted_query)
            cursor.execute(rows_inserted_query)
            rows_inserted = cursor.fetchall()
    
            # Fetch rows based on updated timestamp
            rows_updated_query = f"""select "{columns_str}" from {sf_database}.{sf_schema}.{sf_tbl}
                                    where edw_updated_timestamp >= '{creation_run_time}';
                                """
            # print(rows_updated_query)
            cursor.execute(rows_updated_query)
            rows_updated = cursor.fetchall()

        elif src_db_name.lower() != 'mongodb':
            # Fetch rows based on inserted timestamp
            rows_inserted_query = f"""select {columns_str} from {sf_database}.{sf_schema}.{sf_tbl}
                                    where edw_inserted_timestamp >= '{creation_run_time}';
                                """
        
            # print(rows_inserted_query)
            cursor.execute(rows_inserted_query)
            rows_inserted = cursor.fetchall()
    
            # Fetch rows based on updated timestamp
            rows_updated_query = f"""select {columns_str} from {sf_database}.{sf_schema}.{sf_tbl}
                                    where edw_updated_timestamp >= '{creation_run_time}';
                                """
            # print(rows_updated_query)
            cursor.execute(rows_updated_query)
            rows_updated = cursor.fetchall()
        else:
            # Fetch rows based on inserted timestamp
            rows_inserted_query = (
                f"""SELECT JSON_DATA:"{primary_key_column}"::STRING AS id
                    FROM {sf_database}.{sf_schema}.{sf_tbl}
                    WHERE EDW_INSERTED_TIMESTAMP >= '{creation_run_time}';
                """
            )
            # Print the query for debugging
            # print(rows_inserted_query)

            # Execute the rows inserted query
            cursor.execute(rows_inserted_query)
            rows_inserted = cursor.fetchall()

            # Fetch rows based on updated timestamp
            rows_updated_query = (
                f"""SELECT JSON_DATA:"{primary_key_column}"::STRING AS id
                    FROM {sf_database}.{sf_schema}.{sf_tbl}
                    WHERE EDW_UPDATED_TIMESTAMP >= '{creation_run_time}';
                """
            )
            # Print the query for debugging
            # print(rows_updated_query)

            # Execute the rows updated query
            cursor.execute(rows_updated_query)
            rows_updated = cursor.fetchall()
 
        # Convert rows to list of tuples based on primary_key_columns count
        def convert_to_tuples(rows, num_columns):
            return [tuple(row) if isinstance(row, tuple) and len(row) == num_columns else (row,) for row in rows]
 
        num_columns = len(primary_key_columns_list)
        inserted_tuples = convert_to_tuples(rows_inserted, num_columns)
        updated_tuples = convert_to_tuples(rows_updated, num_columns)
        
        # Print the fetched tuples for debugging
        print(f"Inserted tuples: {inserted_tuples}")
        print(f"Updated tuples: {updated_tuples}")
 
        return inserted_tuples, updated_tuples
 
    else:
        print("No valid creation run time found.")
        return [], []  # Return empty lists if no valid creation run time


def send_email(db_name, error_messages, tables_failed, config_path):
    # Retrieve configuration parameters
    parsed_config = parse_config(config_path)
    sender = parsed_config['EMAIL']['sender']
    password = parsed_config['EMAIL']['password']
    smtp = parsed_config['EMAIL']['smtp']
    port = int(parsed_config['EMAIL']['port'])
    receiver = [email.strip() for email in parsed_config['EMAIL']['receiver'].split(',')]

    # Get current time in UTC and convert to India time
    error_time_utc = datetime.utcnow()
    local_tz = pytz.timezone('Asia/Kolkata')
    error_time = error_time_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)

    email_subject = ''
    email_body = ''

    if db_name == 'gi_carbon_test':
        email_subject = 'Snowflake |Carbon gi_carbon_test job failed'
        email_body = f'''
Job Name :  gi_carbon_test_incr_load
Incidents : gi_carbon_test_incr_load is failed on {error_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")}
Impact : Data is not sync into snowflake on {error_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")}
        '''
    elif db_name == 'go-fc-test-new':
        email_subject = 'Snowflake |Bio go-fc-test-new job failed'
        email_body = f'''
Job Name :  go-fc-test-new
Incidents : go-fc-test-new is failed on {error_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")}
Impact : Data is not sync into snowflake on {error_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")}
        '''
    elif db_name == 'gotxn-test':
        email_subject = 'Snowflake |GrowOnline gotxn-test  job failed'
        email_body = f'''
Job Name :  gotxn-test_incr_load
Incidents : gotxn-test_incr_load is failed on {error_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")}
Impact : Data is not sync into snowflake on {error_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")}
        '''
    elif db_name == 'gi_members':
        email_subject = 'Snowflake | users gi_users_incr_load job failed'
        email_body = f'''
Job Name : gi_users_incr_load
Incidents : gi_users_incr_load failed on {error_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")}
Impact : Data not synced into Snowflake on {error_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")}
'''

    # Add tables failed under Details with serial numbering
    email_body += "Details:\n"
    if tables_failed:
        for i, table in enumerate(tables_failed, start=1):
            email_body += f"{i}. {table}\n"
    else:
        email_body += "1. None\n"
 
    # Add error messages under Reason with serial numbering
    email_body += "Reason:\n"
    for i, error in enumerate(error_messages, start=1):
        email_body += f"{i}. {error}\n"

    try:
        # Create the email
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ', '.join(receiver)
        msg['Subject'] = email_subject
        msg.attach(MIMEText(email_body, 'plain'))

        # Connect to the server and send the email
        with smtplib.SMTP(smtp, port) as server:
            server.starttls()  # Secure the connection
            server.login(sender, password)
            server.send_message(msg)

        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")


def contrl_tbl(snowflake_conn, config_path):
    # Retrieve configuration parameters
    parsed_config = parse_config(config_path)
    sf_database = parsed_config['SNOWFLAKE']['database']
    sf_schema =  parsed_config['SNOWFLAKE']['schema']
    cntrl_tbl_name = parsed_config['SNOWFLAKE']['cntrl_tbl']
   
    cursor = snowflake_conn.cursor()
 
    # Create target database if not exists
    db_create_script = f"CREATE DATABASE IF NOT EXISTS {sf_database};"
 
    print(db_create_script)
    # Execute the db_create_script
    cursor.execute(db_create_script)
 
    # Create target schema if not exists
    schema_create_script = f"CREATE SCHEMA IF NOT EXISTS {sf_database}.{sf_schema};"
 
    print(schema_create_script)
    # Execute the schema_create_script
    cursor.execute(schema_create_script)
 
    # Create control table if not exists
    cntrl_tbl_script = f"""
        CREATE TABLE IF NOT EXISTS {sf_database}.{sf_schema}.{cntrl_tbl_name} (
            RUN_ID NUMBER NOT NULL autoincrement start 1 increment 1 order,
            RUN_START TIMESTAMP_NTZ,
            RUN_END TIMESTAMP_NTZ,
            DURATION_IN_SEC FLOAT,
            IP_ADDRESS VARCHAR,
            SOURCE_DATABASE_NAME VARCHAR,
            SOURCE_SCHEMA_NAME VARCHAR,
            SOURCE_OBJECT_NAME VARCHAR,
            TARGET_DATABASE_NAME VARCHAR,
            TARGET_SCHEMA_NAME VARCHAR,
            TARGET_TABLE_NAME VARCHAR,
            TRIGGERED_BY VARCHAR,
            STATUS VARCHAR,
            ERROR_MESSAGE VARCHAR,
            ROWS_INSERTED_IDS VARCHAR,
            ROWS_INSERTED NUMBER,
            ROWS_UPDATED_IDS VARCHAR,
            ROWS_UPDATED NUMBER,
            ROWS_DELETED NUMBER,
            MAX_TIMESTAMP TIMESTAMP_NTZ,
            CREATED_TIMESTAMP TIMESTAMP_NTZ,
            CREATED_BY VARCHAR            
        );
        """
    #print(cntrl_tbl_script)
    # Execute the cntrl_tbl_script
    cursor.execute(cntrl_tbl_script)
 
    cursor.close()
 
    print("Pipeline control table is creation done!.")

