# Import statements
import boto3
import configparser
import snowflake.connector
from pyspark.sql import *


def parse_config(config_path):
    # Initialize configparser
    config = configparser.ConfigParser()

    # Load configuration file
    config.read(config_path)

    # Initialize dictionary to store parsed values
    parsed_config = {}

    # Access sections and their keys

    parsed_config['sql_server'] = {
        'server_name': config.get('sql_server', 'server_name'),
        'user': config.get('sql_server', 'user'),
        'password': config.get('sql_server', 'password'),
        'database_name': config.get('sql_server', 'database_name')
    }

    parsed_config['postgresql'] = {
        'user': config.get('postgresql', 'user'),
        'password': config.get('postgresql', 'password'),
        'jdbcUrl': config.get('postgresql', 'jdbcUrl')
    }

    parsed_config['SNOWFLAKE'] = {
        'database': config.get('SNOWFLAKE', 'database'),
        'schema': config.get('SNOWFLAKE', 'schema'),
        'warehouse': config.get('SNOWFLAKE', 'warehouse'),
        'ext_stg_name': config.get('SNOWFLAKE', 'ext_stg_name'),
        'account': config.get('SNOWFLAKE', 'account'),
        'user': config.get('SNOWFLAKE', 'user'),
        'password': config.get('SNOWFLAKE', 'password'),
        'cntrl_tbl': config.get('SNOWFLAKE', 'cntrl_tbl')
    }
    parsed_config['s3'] = {
        'bucket_name': config.get('s3','bucket_name')
    }
    return parsed_config


def snow_conn(config_path):
    """
    creating snowflake connection
    """
    parsed_config = parse_config(config_path)

    # Extracting parameters from parsed_config dictionary
    snowflake_account = parsed_config['SNOWFLAKE']['account']
    snowflake_user = parsed_config['SNOWFLAKE']['user']
    snowflake_password = parsed_config['SNOWFLAKE']['password']
    snowflake_database = parsed_config['SNOWFLAKE']['database']
    snowflake_warehouse = parsed_config['SNOWFLAKE']['warehouse']

    # Snowflake connection
    snowflake_conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        database=snowflake_database,
        warehouse=snowflake_warehouse
    )

    print("snowflake connection established")
    return snowflake_conn


def query_execution(spark, src_db_name, query, config_path):
    """
    creating sql server connection

    """
    if src_db_name.lower() == "sqlserver":
        # read config file
        parsed_config = parse_config(config_path)  # Assuming get_parsed_config returns the parsed configuration

        # Extract parameters
        sql_server_name = parsed_config['sql_server']['server_name']
        sql_server_user = parsed_config['sql_server']['user']
        sql_server_password = parsed_config['sql_server']['password']
        sql_server_database = parsed_config['sql_server']['database_name']

        # Define options for connecting to the source SQL Server
        source_sql_server_options = {
            "url": f"jdbc:sqlserver://{sql_server_name}:1433;databaseName={sql_server_database};encrypt=true;trustServerCertificate=true",
            "user": sql_server_user,
            "password": sql_server_password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

        result = spark.read.format("jdbc").options(**source_sql_server_options) \
            .option("query", query) \
            .load()

        return result

    elif src_db_name.lower() == "postgresql":
        # read config file
        parsed_config = parse_config(config_path)  # Assuming get_parsed_config returns the parsed configuration

        # Extract parameters
        jdbcUrl = parsed_config['postgresql']['jdbcUrl']
        user = parsed_config['postgresql']['user']
        password = parsed_config['postgresql']['password']

        properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver"
        }

        # Read data using the SQL query into a DataFrame
        result = spark.read.jdbc(jdbcUrl, table=f"({query}) as subquery", properties=properties)
        return result


def s3_client(config_path):
    """
    create an S3 client
    """
    # Retrieve configuration parameters
    parsed_config = parse_config(config_path)
    bucket_name = parsed_config['s3']['bucket_name']
 
    client = boto3.client(
        service_name="s3"
    )

    return client, bucket_name
