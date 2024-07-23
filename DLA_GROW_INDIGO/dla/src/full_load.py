# Import statements
import os
import sys
import pytz
from datetime import datetime
from pyspark.sql import SparkSession

#from airflow.exceptions import AirflowException

from DLA_TEST.src.libs import *
from DLA_TEST.src.conn import query_execution, snow_conn, s3_client
from DLA_TEST.constant import *


def spark_conn():
    try:
        # Initialize Spark session
        spark = SparkSession \
            .builder \
            .master("local") \
            .appName("growindigo") \
            .config("spark.driver.memory", "15g") \
            .config("spark.jars", "/home/growindigo/spark-3.4.1-bin-hadoop3/jars/postgresql-42.6.0.jar,/home/growindigo/spark-3.4.1-bin-hadoop3/jars/mssql-jdbc-12.4.1.jre11.jar") \
            .getOrCreate()

        sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        return spark
    except Exception as e:
        print(f"spark initialization failed : {e}")


def full_load(db_name):
    # Format the config file path based on the database name
    config_path = config_file_path.format(db_name)
    print(config_path)
    
    # Initialize empty lists to capture any errors and tables that failed to load
    error_messages = []
    tables_failed = []

    try:
        # Read the configuration file using a custom parser function
        parsed_config = parse_config(config_path)

        # Extract the metadata file path from the parsed configuration
        meta_file_path = parsed_config['metadata']['metadata_file_path']

        # Extract the  jar file paths from the parsed configuration
        mysql_jar_file_path = parsed_config['spark']['mysql_jar_file_path']
        postgresql_jar_file_path = parsed_config['spark']['postgresql_jar_file_path']

        # Calling the spark_conn() 
        spark = spark_conn()

        # Define a string to identify the Snowflake connection module
        module_string = 'snowflake connection'
        print(f"\n--------------------* {module_string} module is started *--------------------")
        # Get snowflake connection object
        snowflake_conn = snow_conn(config_path)
        print(f"{module_string} successfully established.\n")

        # Construct the full path to the metadata file using the project path
        meta_file_path = f'{project_path}/{meta_file_path}'

        # Read the metadata CSV file into a PySpark DataFrame
        meta_df = spark.read.csv(meta_file_path, header=True, inferSchema=True)

        # Display the contents of the metadata DataFrame
        print("displaying the contents of the metadata file:")
        meta_df.show()

        # Iterate through each row in the DataFrame
        for row in meta_df.collect():
            print(row)
            # Convert each row to a dictionary for easier access
            row_values = row.asDict()

            # Extract values from the row dictionary
            src_db_name = row_values["SRC_DB_NAME"]
            database_name = row_values["DATABASE_NAME"]
            obj_type = row_values["OBJ_TYPE"]
            schema_name = row_values["SCHEMA_NAME"]
            obj_name = row_values["OBJ_NAME"]
            insrt_time_col = row_values["INSRT_TIME_COL"]
            updt_time_col = row_values["UPDT_TIME_COL"]
            primary_key_col = row_values["PRIMARY_KEY_COL"]
            col_names = row_values["COL_NAMES"]
            is_deletion = row_values["IS_DELETION"]
            deleted_table = row_values["DELETED_TBL"]
            sf_tbl = row_values["SF_TBL"]
            deleted_tbl_cols = f"{primary_key_col}, {insrt_time_col}"

            # Get the current UTC datetime
            run_start_utc = datetime.now()
            # Convert UTC datetime to Asia/Kolkata timezone
            local_tz = pytz.timezone('Asia/Kolkata')
            run_start = run_start_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)

            try:
                print(f"load started for table {obj_name}.\n")

                # Define a string to identify the 'create control table' module
                module_string = 'create control table'
                print(f"--------------------* {module_string} module is started *--------------------")
                # Create control table at Snowflake
                contrl_tbl(snowflake_conn, config_path)
                print(f"{module_string} successfully completed.\n")

                # This if block is for files at s3 bucket
                if obj_type.lower() == 'file':
                    # Define a string to identify the 'extracting metadata from source' module
                    module_string = 'extracting metadata from source'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Process column names and get the details of columns
                    col_nms = column_nms(spark, src_db_name, obj_name, schema_name, col_names, config_path, obj_type)
                    print(f"{module_string} successfully completed.\n")
                    
                    # Define a string to identify the 'extraction from source' module
                    module_string = 'extraction from source'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Extract data and store into a parquet file
                    max_timestamp, source_df_ids = extraction_full(spark, src_db_name, database_name, schema_name, obj_type, obj_name, col_names, insrt_time_col,
                    updt_time_col, primary_key_col, config_path, sf_tbl)
                    
                    # Check if the extracted data (source_df_ids) is empty (no rows found)
                    if not source_df_ids.empty:
                        # Convert Pandas DataFrame to list of tuples
                        source_df_ids_list = [tuple(x) for x in source_df_ids.to_numpy()]
                        print("primary key id's extracted from source:")
                        print(source_df_ids_list)
                    else:
                        # Handle empty DataFrame case
                        print(f"No data found in collection {obj_name} during source extraction.")
                        source_df_ids_list = []

                        # Initialize an empty list to store IDs
                    rows_inserted_ids = []

                    # Iterate through each element in the list
                    for row_data in source_df_ids_list:
                         # Dynamically unpack the tuple and append it to rows_inserted_ids
                         rows_inserted_ids.append(row_data)

                    # Since we are performing a full load, there will be no updates
                    rows_updated_ids = [(0,)]

                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'creating scripts for snowflake' module
                    module_string = 'creating scripts for snowflake'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Create scripts for tables
                    create_scripts(snowflake_conn, col_nms, 'no', src_db_name, database_name, schema_name, obj_type, obj_name, is_deletion, 
                        deleted_table, config_path, sf_tbl)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'executing scripts' module
                    module_string = 'executing scripts'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Run scripts to create tables in snowflake
                    runscripts(snowflake_conn, database_name, schema_name, obj_type, config_path, sf_tbl)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'data load to snowflake' module
                    module_string = 'data load to snowflake'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    rows_upserted, rows_deleted = data_load(snowflake_conn, src_db_name, database_name, schema_name, obj_type, col_nms, 
                        obj_name, is_deletion, deleted_table, primary_key_col, insrt_time_col, updt_time_col, config_path, sf_tbl)
                    print(f"{module_string} successfully completed.\n")
                    
                    # Define a string to identify the 'inserting to cntrl tbl' module
                    module_string = 'inserting to cntrl tbl'
                    print(f"--------------------* {module_string} module is started *--------------------")

                    run_end_utc = datetime.now()
                    # Convert UTC datetime to Asia/Kolkata timezone
                    local_tz = pytz.timezone('Asia/Kolkata')
                    run_end = run_end_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)    

                    status = "SUCCESS"
                    error = "-"

                    insrt_into_ctrl_tbl(db_name, snowflake_conn, run_start, run_end, database_name, schema_name, obj_name, obj_type, status,
                        sf_tbl, rows_inserted_ids, rows_upserted, rows_updated_ids, rows_deleted, error, max_timestamp, config_path)
                    
                    print(f"{module_string} successfully completed.\n\n")
                    print(f"~~~~~~~~~~~~~~~~~~~~* load successfully completed for table {obj_name} *~~~~~~~~~~~~~~~~~~~~\n")
                   
                else:
                    # This if block is for mongodb extraction
                    if src_db_name.lower() == 'mongodb':
                        # Define a string to identify the 'extraction from source' module
                        module_string = 'extraction from source'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        # Extract data and store into a Json file
                        staging_file_path, max_timestamp, source_df_ids = mongo_ext_full(src_db_name, database_name, schema_name, obj_type, obj_name,
                            sf_tbl, insrt_time_col, updt_time_col, primary_key_col, config_path)

                        # Check if the extracted data (source_df_ids) is empty (no rows found)
                        if not source_df_ids.empty:
                            # Convert Pandas DataFrame to list of tuples
                            source_df_ids_list = [tuple(x) for x in source_df_ids.to_numpy()]
                            print("primary key id's extracted from source:")
                            print(source_df_ids_list)
                        else:
                            # Handle empty DataFrame case
                            print(f"No data found in collection {obj_name} during source extraction.")
                            source_df_ids_list = []

                        # Initialize an empty list to store IDs
                        rows_inserted_ids = []

                        # Iterate through each element in the list
                        for row_data in source_df_ids_list:
                            # Dynamically unpack the tuple and append it to rows_inserted_ids
                            rows_inserted_ids.append(row_data)

                        # Since we are performing a full load, there will be no updates
                        rows_updated_ids = [(0,)]
                    
                        print(f"{module_string} successfully completed.\n")

                    # This else block is for remaining 2 source's(SQLServer & PostgreSQL) extraction
                    else:
                        # Define a string to identify the 'extraction from source' module
                        module_string = 'extraction from source'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        # Extract data and store into a parquet file
                        staging_file_path, file_name, max_timestamp, source_df_ids = extraction_full(spark, src_db_name, database_name, schema_name,
                            obj_type, obj_name, col_names, insrt_time_col, updt_time_col, primary_key_col, config_path, sf_tbl)
                        
                        # Check if the Spark DataFrame is empty
                        if not source_df_ids.isEmpty():
                            # Convert Spark DataFrame to list of tuples
                            source_df_ids_list = source_df_ids.rdd.map(tuple).collect()
                            print("primary key id's extracted from source:")
                            print(source_df_ids_list)
                        else:
                            # Handle empty DataFrame case
                            print(f"No data found in collection {obj_name} during source extraction.")
                            source_df_ids_list = []

                        # Print the length of the extracted source IDs
                        print(f"length of the extracted source id's: {len(source_df_ids_list)}")

                        # Initialize rows_inserted_ids as an empty list
                        rows_inserted_ids = []
    
                        # Iterate through each element in the list
                        for row_data in source_df_ids_list:
                            # Dynamically append the tuple to rows_inserted_ids
                            rows_inserted_ids.append(row_data)
    
                        # Since we are performing a full load, there will be no updates
                        rows_updated_ids = [(0,)]
                                                                
                        print(f"{module_string} successfully completed.")   

                    # Define a string to identify the 'move files to archive folder (s3)' module
                    module_string = 'move files to archive folder (s3)'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Define s3 storage path for the file to be archived or uploaded
                    s3_file_path = f"{src_db_name}/{database_name}/{schema_name}/{obj_type}/{sf_tbl}"
                    # Clean up the source folder to retain only incremental load files
                    move_fldr('archive', s3_file_path, config_path)
                    print(f"{module_string} successfully completed.\n")
                    
                    # Define a string to identify the 'upload files to s3 bucket' module
                    module_string = 'upload files to s3 bucket'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Transfer the local file to s3 bucket
                    if src_db_name.lower() == 'mongodb':
                        upload_to_s3(staging_file_path, s3_file_path, None, config_path)
                    else:
                        upload_to_s3(staging_file_path, s3_file_path, file_name, config_path)

                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'extracting metadata from source' module
                    module_string = 'extracting metadata from source'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Process column names and get the details of columns
                    if src_db_name.lower() != 'mongodb':
                        col_nms = column_nms(spark, src_db_name, obj_name, schema_name, col_names, config_path, obj_type)
                        if is_deletion == 'yes':
                            del_col_nms = column_nms(spark, src_db_name, deleted_table, schema_name, deleted_tbl_cols, config_path)
                        else:
                            del_col_nms = None
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'creating scripts for snowflake' module
                    module_string = 'creating scripts for snowflake'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    if src_db_name.lower() == 'mongodb':
                        # Create scripts for tables
                        create_scripts(snowflake_conn, None, None, src_db_name, database_name, schema_name, obj_type, sf_tbl, is_deletion, deleted_table, config_path, sf_tbl)
                    else:
                        # Create scripts for tables
                        create_scripts(snowflake_conn, col_nms, 'no', src_db_name, database_name, schema_name, obj_type, sf_tbl, is_deletion, deleted_table, config_path, sf_tbl)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'executing scripts' module
                    module_string = 'executing scripts'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Run scripts to create tables in snowflake
                    runscripts(snowflake_conn, database_name, schema_name, obj_type, config_path, sf_tbl)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'data load to snowflake' module
                    module_string = 'data load to snowflake'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Load data to tgt table
                    if src_db_name.lower() == 'mongodb':
                        rows_upserted, rows_deleted = data_load(snowflake_conn, src_db_name, database_name, schema_name, obj_type, None, obj_name, is_deletion, deleted_table,
                                                        primary_key_col, insrt_time_col, updt_time_col, config_path, sf_tbl)
                    else:
                        rows_upserted,rows_deleted = data_load(snowflake_conn, src_db_name, database_name, schema_name, obj_type, col_nms, obj_name, is_deletion, deleted_table, primary_key_col,
                                insrt_time_col, updt_time_col, config_path, sf_tbl)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'inserting to cntrl tbl' module
                    module_string = 'inserting to cntrl tbl'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    
                    run_end_utc = datetime.now()
                    # Convert UTC datetime to Asia/Kolkata timezone
                    local_tz = pytz.timezone('Asia/Kolkata')
                    run_end = run_end_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)   

                    status = "SUCCESS"
                    error = "-"
                    
                    insrt_into_ctrl_tbl(db_name, snowflake_conn, run_start, run_end, database_name, schema_name,
                                        obj_name, obj_type, status, sf_tbl, rows_inserted_ids, rows_upserted, rows_updated_ids, rows_deleted, error, max_timestamp, config_path)
                    
                    print(f"{module_string} successfully completed.\n\n")
                    print(f"~~~~~~~~~~~~~~~~~~~~* load successfully completed for table {obj_name} *~~~~~~~~~~~~~~~~~~~~\n")

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

                print(f"error occured in {module_string} module")
                print(f"load failed for {obj_type} {obj_name}: {e}\n\n")

                run_end_utc = datetime.now()
                # Convert UTC datetime to Asia/Kolkata timezone
                local_tz = pytz.timezone('Asia/Kolkata')
                run_end = run_end_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)

                rows_inserted_ids = [(0,)]
                rows_updated_ids = [(0,)]
                status = "FAILED"
                error = f"eror occurred in module {module_string}, file {fname} in line {exc_tb.tb_lineno}, error_type : {exc_type}, error_stmt {e}"
                
                rows_upserted = [(0, 0)]
                rows_deleted = [(0,)]
                max_timestamp = '9999-12-31 23:59:59.999'

                insrt_into_ctrl_tbl(db_name, snowflake_conn, run_start, run_end, database_name, schema_name, obj_name, obj_type, status, sf_tbl,
                    rows_inserted_ids, rows_upserted, rows_updated_ids, rows_deleted, error, max_timestamp, config_path)
                    
                print(f"\n\n")

                # Collect error messages
                error_messages.append(error)
    
                # Collect table name
                tables_failed.append(obj_name)
    
        # Send email if there are any errors
        # if error_messages:
           # send_email(db_name, error_messages, tables_failed, config_path)

        # Stop Spark session
        spark.stop()
        
    except Exception as e:
        print(e)
        error_messages.append(e)
        # send_email(db_name, error_messages, None, config_path)
        # raise AirflowException(f"migration failed : {e}")

