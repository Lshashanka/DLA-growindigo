# Import statements
import os
import sys
import pytz
from datetime import datetime
from pyspark.sql import SparkSession

# from airflow.exceptions import AirflowException

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
            .config('spark.jars.packages','com.crealytics:spark-excel_2.12:3.4.1_0.20.4,org.apache.hadoop:hadoop-aws:3.3.4')\
            .getOrCreate()
        
        sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        return spark
    except Exception as e:
        print(f"spark initialization failed : {e}")


def incr_load(db_name):
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
            row_values = row.asDict()
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

            run_start_utc = datetime.now()
            # Convert UTC datetime to Asia/Kolkata timezone
            local_tz = pytz.timezone('Asia/Kolkata')
            run_start = run_start_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)
            
            try:
                print(f"load started for table {obj_name}\n")

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
                    # Extract incremental data and store into a parquet file
                    staging_file_path, file_name, max_timestamp = extraction_incr(spark, snowflake_conn, src_db_name, database_name, schema_name, obj_type, obj_name,
                        is_deletion, obj_name, col_names, insrt_time_col, updt_time_col, config_path, sf_tbl)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'creating scripts for snowflake' module
                    module_string = 'creating scripts for snowflake'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Create scripts for tables
                    create_scripts(snowflake_conn, col_nms, 'no', src_db_name, database_name, schema_name, obj_type, obj_name, is_deletion, deleted_table, config_path, sf_tbl)
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
                    rows_upserted, rows_deleted = data_load(snowflake_conn, src_db_name, database_name, schema_name, obj_type, col_nms, obj_name, is_deletion,
                        deleted_table, primary_key_col, insrt_time_col, updt_time_col, config_path, sf_tbl)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'fetch incremental rows ids' module
                    module_string = 'fetch incremental rows ids'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    rows_inserted_ids, rows_updated_ids = rows_ids(snowflake_conn, src_db_name, obj_name, sf_tbl, primary_key_col, config_path, obj_type)
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

                    insrt_into_ctrl_tbl(db_name, snowflake_conn, run_start, run_end, database_name, schema_name, obj_name,obj_type, status, sf_tbl, rows_inserted_ids,
                        rows_upserted, rows_updated_ids, rows_deleted, error, max_timestamp, config_path)
                    
                    print(f"{module_string} successfully completed.\n\n")
                    print(f"~~~~~~~~~~~~~~~~~~~~* load successfully completed for table {obj_name} *~~~~~~~~~~~~~~~~~~~~\n")

                elif src_db_name.lower() == 'mongodb':
                    # Define a string to identify the 'extraction from source' module
                    module_string = 'extraction from source'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Extract incremental data and store into a Json file
                    staging_file_path, max_timestamp = mongo_incr_load(snowflake_conn, src_db_name, database_name, schema_name, obj_type, obj_name, sf_tbl, obj_name,
                        insrt_time_col, updt_time_col, config_path)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'move files to archive folder (s3)' module
                    module_string = 'move files to archive folder (s3)'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Define s3 storage path for the folder to be archived
                    s3_file_path = f"{src_db_name}/{database_name}/{schema_name}/{obj_type}/{sf_tbl}"
                    # Clean up the source folder to retain only incremental load files
                    move_fldr('archive', s3_file_path, config_path)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'upload files to s3 bucket' module
                    module_string = 'upload files to s3 bucket'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Transfer the local file to s3 bucket
                    upload_to_s3(staging_file_path, s3_file_path, None, config_path)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'data load to snowflake' module
                    module_string = 'data load to snowflake'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Load data in snowflake
                    rows_upserted, rows_deleted = data_load(snowflake_conn, src_db_name, database_name, schema_name, obj_type, None, obj_name, is_deletion, deleted_table,
                        primary_key_col, insrt_time_col, updt_time_col, config_path, sf_tbl)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'fetch incremental rows ids' module
                    module_string = 'fetch incremental rows ids'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    rows_inserted_ids, rows_updated_ids = rows_ids(snowflake_conn, src_db_name, obj_name, sf_tbl, primary_key_col, config_path, obj_type)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'inserting to cntrl tbl' module
                    module_string = 'inserting to cntrl tbl'
                    print(f"--------------------* {module_string} module is started *--------------------")

                    # Get the current UTC datetime
                    run_end_utc = datetime.now()
                    # Convert UTC datetime to Asia/Kolkata timezone
                    local_tz = pytz.timezone('Asia/Kolkata')
                    run_end = run_end_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)

                    status = "SUCCESS"
                    error = "-"
                    
                    insrt_into_ctrl_tbl(db_name, snowflake_conn, run_start, run_end, database_name, schema_name, obj_name, obj_type, status, sf_tbl, rows_inserted_ids,
                        rows_upserted, rows_updated_ids, rows_deleted, error, max_timestamp, config_path)
                    
                    print(f"{module_string} successfully completed.\n\n")
                    print(f"~~~~~~~~~~~~~~~~~~~~* load successfully completed for table {obj_name} *~~~~~~~~~~~~~~~~~~~~\n")

                else:
                    # Define a string to identify the 'extracting metadata from source' module
                    module_string = 'extracting metadata from source'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    col_nms = column_nms(spark, src_db_name, obj_name, schema_name, col_names, config_path, obj_type)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'checking the columns in target' module
                    module_string = 'checking the columns in target'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    tgt_cols = cols_in_snowflake(snowflake_conn, sf_tbl, config_path)
                    print(f"{module_string} successfully completed.\n")

                    # Define a string to identify the 'validating columns' module
                    module_string = 'validating columns'
                    print(f"--------------------* {module_string} module is started *--------------------")
                    # Collect data from Spark DataFrame
                    data = col_nms.collect()

                    if obj_type.lower() == 'file':
                       data_type = 'DATA_TYPE'
                       column_name = 'COLUMN_NAME'
                    else:
                       data_type = 'data_type'
                       column_name = 'column_name'

                    # Create a Pandas DataFrame from the collected data
                    pandas_df = pd.DataFrame(data, columns=col_nms.columns)

                    # Add a new column to the DataFrame containing lowercase column names
                    pandas_df['lower_column_name'] = pandas_df[column_name].str.lower()

                    # Access columns as Python lists
                    src_cols = pandas_df['lower_column_name'].tolist()

                    src_cols = {col.lower() for col in src_cols}
                    tgt_cols = {col.lower() for col in tgt_cols}

                    print(src_cols)
                    print(tgt_cols)
                    # Get the column count from the source DataFrame
                    source_col_count = len(src_cols)

                    # Get the column count from the target list
                    target_col_count = len(tgt_cols)

                    print(f"Source Column Count: {source_col_count}, Target Column Count: {target_col_count}\n")
                    print(f"{module_string} successfully completed.\n")

                    additional_columns = set(src_cols) - set(tgt_cols)
                    missing_columns = set(tgt_cols) - set(src_cols)

                    if additional_columns:
                        print(f"Additional columns in source: {additional_columns}")

                        # Get the data types for additional columns
                        additional_columns_data_types = {}
                        for col_name in additional_columns:
                            # Find the row corresponding to the additional column
                            clean_col_name = col_name.strip().lower()
                            col_row = pandas_df[pandas_df[column_name].str.strip().str.lower() == clean_col_name]

                            # Extract the data type for the column
                            data_type = col_row[data_type].iloc[0]

                            # Store the data type in the dictionary
                            original_col_name = col_row[column_name].iloc[0]
                            additional_columns_data_types[original_col_name] = data_type

                        # Define a string to identify the 'extraction from source' module
                        module_string = 'extraction from source'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        # Extract complete data and store into a parquet file
                        staging_file_path, file_name, max_timestamp, source_df_ids = extraction_full(spark, src_db_name, database_name, schema_name, obj_type,
                            obj_name, col_names, insrt_time_col, updt_time_col, primary_key_col, config_path, sf_tbl)
                        print(f"{module_string} successfully completed.\n")

                        # Define s3 storage path
                        s3_file_path = f"{src_db_name}/{database_name}/{schema_name}/{obj_type}/{sf_tbl}"
                        s3_folder_path = f"{src_db_name}/{database_name}/{schema_name}/{obj_type}/{sf_tbl}"

                        # Define a string to identify the 'move files to archive folder (s3)' module
                        module_string = 'move files to archive folder (s3)'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        move_fldr('archive', s3_folder_path, config_path)
                        print(f"{module_string} successfully completed.\n")

                        # Define a string to identify the 'upload files to s3 bucket' module
                        module_string = 'upload files to s3 bucket'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        # Transfer the local file to s3 bucket
                        upload_to_s3(staging_file_path, s3_file_path, file_name, config_path)
                        print(f"{module_string} successfully completed.\n")

                        # Define a string to identify the 'adding new columns in snowflake table' module
                        module_string = 'adding new columns in snowflake table'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        # alter table to add new column if any
                        alter_tbl_add_col(snowflake_conn, additional_columns_data_types, config_path, sf_tbl)
                        print(f"{module_string} successfully completed.\n")

                        # Define a string to identify the 'data load to tgt table' module
                        module_string = 'data load to tgt table'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        rows_upserted, rows_deleted = data_load(snowflake_conn, src_db_name, database_name, schema_name,obj_type, col_nms, obj_name, is_deletion,
                            deleted_table, primary_key_col, insrt_time_col, updt_time_col, config_path)
                        print(f"{module_string} successfully completed.\n")

                        # Define a string to identify the 'fetch incremental rows ids' module
                        module_string = 'fetch incremental rows ids'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        rows_inserted_ids, rows_updated_ids = rows_ids(snowflake_conn, src_db_name, obj_name, sf_tbl, primary_key_col, config_path, obj_type)
                        print(f"{module_string} successfully completed.\n")

                        print("target tables are altered with new columns")

                    else:
                        if missing_columns:
                            # Define a string to identify the 'altering snowflake table to remove extra columns' module
                            module_string = 'altering snowflake table to remove extra columns'
                            print(f"--------------------* {module_string} module is started *--------------------")
                            print(f"Additional columns in source: {missing_columns}")
                            for col_name in missing_columns:
                                alter_tbl_del_col(snowflake_conn, sf_tbl, col_name, config_path)
                            print(f"{module_string} successfully completed.\n")

                        print("no new columns found")

                        # Define a string to identify the 'extraction from source' module
                        module_string = 'extraction from source'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        # Extract incremental data and store into a parquet file
                        staging_file_path, file_name, max_timestamp = extraction_incr(spark, snowflake_conn, src_db_name, database_name, schema_name, obj_type,
                            obj_name, 'no', obj_name, col_names, insrt_time_col, updt_time_col, config_path, sf_tbl)
                        print(f"{module_string} successfully completed.\n")
                        
                        # Define s3 storage path
                        s3_file_path = f"{src_db_name}/{database_name}/{schema_name}/{obj_type}/{sf_tbl}"
                        s3_folder_path = f"{src_db_name}/{database_name}/{schema_name}/{obj_type}/{sf_tbl}"

                        # Define a string to identify the 'move files to archive folder (s3)' module
                        module_string = 'move files to archive folder (s3)'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        move_fldr('archive', s3_folder_path, config_path)
                        print(f"{module_string} successfully completed.\n")
                        
                        # Define a string to identify the 'upload to s3 bucket' module
                        module_string = 'upload to s3 bucket'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        # Transfer the local file to s3 bucket
                        upload_to_s3(staging_file_path, s3_file_path, file_name, config_path)
                        print(f"{module_string} successfully completed.\n")

                        if is_deletion == 'yes':
                            # Define a string to identify the 'extraction from source of deleted table' module
                            module_string = 'extraction from source of deleted table'
                            print(f"--------------------* {module_string} module is started *--------------------")
                            # Extract complete data and store into a parquet file
                            staging_file_path, file_name, del_max_timestamp = extraction_incr(spark, snowflake_conn, src_db_name, database_name, schema_name,
                                deleted_table, is_deletion, obj_name, deleted_tbl_cols, insrt_time_col, updt_time_col, config_path, sf_tbl)
                            print(f"{module_string} successfully completed.\n")

                            # Define a string to identify the 'move files to archive folder (s3)' module
                            module_string = 'move files to archive folder (s3)'
                            print(f"--------------------* {module_string} module is started *--------------------")
                            # Define s3 storage path
                            s3_file_path = f"{src_db_name}/{database_name}/{schema_name}/{obj_type}/{sf_tbl}"
                            move_fldr('archive', s3_file_path, config_path)
                            print(f"{module_string} successfully completed.\n")

                            # Define a string to identify the 'upload to s3 bucket' module
                            module_string = 'upload to s3 bucket'
                            print(f"--------------------* {module_string} module is started *--------------------")
                            # Transfer the local file to s3 bucket
                            upload_to_s3(staging_file_path, s3_file_path, file_name, config_path)
                            print(f"{module_string} successfully completed.\n")

                        # Define a string to identify the 'creating scripts for snowflake' module
                        module_string = 'creating scripts for snowflake'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        # Create scripts for tables
                        create_scripts(snowflake_conn, col_nms, 'no', src_db_name, database_name, schema_name, obj_type, obj_name, is_deletion, deleted_table, config_path, sf_tbl)
                        print(f"{module_string} successfully completed.\n")

                        # Define a string to identify the 'executing scripts' module
                        module_string = 'executing scripts'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        # Run scripts to create tables in snowflake
                        runscripts(snowflake_conn, database_name, schema_name, obj_type, config_path, sf_tbl)
                        print(f"{module_string} successfully completed.\n")  

                        # Define a string to identify the 'load data to tgt table' module
                        module_string = 'load data to tgt table'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        # Load data to tgt table
                        rows_upserted,rows_deleted = data_load(snowflake_conn, src_db_name, database_name, schema_name, obj_type, col_nms, obj_name, is_deletion, deleted_table, primary_key_col,
                              insrt_time_col, updt_time_col, config_path, sf_tbl)
                        print(f"{module_string} successfully completed.\n")

                        # Define a string to identify the 'fetch incremental rows ids' module
                        module_string = 'fetch incremental rows ids'
                        print(f"--------------------* {module_string} module is started *--------------------")
                        rows_inserted_ids, rows_updated_ids = rows_ids(snowflake_conn, src_db_name, obj_name, sf_tbl, primary_key_col, config_path, obj_type)
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

                    insrt_into_ctrl_tbl(db_name, snowflake_conn, run_start, run_end, database_name, schema_name, obj_name, obj_type, status, sf_tbl, rows_inserted_ids,
                        rows_upserted, rows_updated_ids, rows_deleted, error, max_timestamp, config_path)
                    
                    print(f"{module_string} successfully completed.\n\n")
                    print(f"~~~~~~~~~~~~~~~~~~~~* load successfully completed for table {obj_name} *~~~~~~~~~~~~~~~~~~~~\n")

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(f"load failed for {obj_type} {obj_name} in module {module_string}: {e}\n\n")

                run_end_utc = datetime.now()
                # Convert UTC datetime to Asia/Kolkata timezone
                local_tz = pytz.timezone('Asia/Kolkata')
                run_end = run_end_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)

                rows_inserted_ids = [(0,)]
                rows_updated_ids = [(0,)]     

                status = "FAILED"
                error = f"eror occurred in file {fname} in line {exc_tb.tb_lineno} in module {module_string}, error_type : {exc_type}, error_stmt {e}"

                rows_upserted = [(0, 0)]
                rows_deleted = [(0,)]
                max_timestamp = '9999-12-31 23:59:59.999'

                insrt_into_ctrl_tbl(db_name, snowflake_conn, run_start, run_end, database_name, schema_name, obj_name, obj_type,  status, sf_tbl, rows_inserted_ids,
                    rows_upserted, rows_updated_ids, rows_deleted, error, max_timestamp, config_path)
                
                print(f"\n\n")

                # Collect error message
                error_messages.append(error)

                # Collect table name
                tables_failed.append(obj_name)
     
        # Send email if there are any errors
        # if error_messages:
          # send_email(db_name,error_messages, tables_failed, config_path)

        # Stop Spark session
        spark.stop()

    except Exception as e:
        print(e)
        error_messages.append(e)
        # send_email(db_name,e, None, config_path)
        # raise AirflowException(f"migration failed : {e}")

