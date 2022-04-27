# Task 1

# ***************** ASSUMPTIONS AND THOUGHT PROCESS ***********************
"""
Usually when OLTP databases are optimized for transaction processing. If there's a lot of data and used for analytics/reporting, it might be wise to look into OLAP system that use Columnar storage. For bulk operations in database imports, parquet files work efficiently especially for columnar storage and compression.

Preprocessing large files using PySpark is also ideal, especially in a complex data pipeline. Pandas can also do the work but it does not scale as well since it has limited parallel processing capability. However, PySpark is not as idea for files with small size.

In cases where we do not want to scale database and need to optimize the SQL ETL, we can do the following:

1) Use unlogged tables for staging tables. This will avoid overheads such as Write Ahead Logs that makes database more reliable in cases of failures. We do not require that for staging tables. The transformation will also be faster if UNLOGGED tables are used. The INSERT to main table from will still take time since it should be logged. However, the initial load and transformation will faster.
2) Dropping Indexes and recreating after import. When a new row is created in DB, index is generated after each INSERT. In cases where the tables are not already huge, it may be worth dropping the index, loading the data, and recreating index. 
3) Avoiding Trigger. Like indexes, trigger happen immediately before or after a CRUD operation. For bulk import, we should find strategies that may avoid triggers in DB.
4) Use Postgres COPY command. That is way more efficient than INSERT command.
5) Optimize the columns. In general, database stores in pages inside of memory blocks. So, databases work efficiently when fixed width datatypes such as integers, timestamps should be store in the front of the table.

For this ETL, we will are assuming that there are no triggers and indexes. Since the data size is less than 10GB, we will not use PySpark. We will be unlogging the staging tables. These changes should be the script more efficient. We are also not using TRANSACTION in this script because transformation is happening in staging tables.

"""


# ***************** COMMENTS ***********************
# The following target table needs to be in Postgres for the script to work

"""
CREATE TABLE test_od (
    VendorID int,
    tpep_pickup_datetime   timestamp,
    tpep_dropoff_datetime  timestamp,
    passenger_count        int,
    trip_distance          int,
    RatecodeID             int,
    store_and_fwd_flag     varchar(1),
    PULocationID           int,
    DOLocationID           int,
    payment_type           int,
    fare_amount            float,
    extra                  float,
    mta_tax                float,
    tip_amount             float,
    tolls_amount           float,
    improvement_surcharge  float,
    total_amount           float,
    congestion_surcharge   float
);
"""
# **************************************************

"""
Script to load CSV data to corresponding Postgres table.
"""

# ***************** IMPORTS ***********************
import time
import logging
import os
import psycopg2
# **************************************************

# *************** LOGGER SETUP *********************
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.DEBUG)

logging.basicConfig(level=logging.DEBUG)
# **************************************************

# *************** CONFIG SETTINGS ******************
""" It's better to retrieve and decrypt DB information and password from tools such as AWS password manager. 
    For scripts running on premise, password and DB information should be set in environment variables 
    of the user profile on the server.
    
    For this example, we will make this simple by directly inputing password.
"""

password_decrypted = 'root' # Do not actually paste password like this in production -> READ the above comment.
user_decrypted = 'root'
dbname="travel"
port="5432"
host_url = "localhost"

file_name = "/Users/shrestm1/tripdata.csv"

# **************************************************
# Database Connection
# **************************************************

def connect_to_db(dbname=dbname, port=port, user=user_decrypted, password=password_decrypted, hostname=host_url):
    """
    Used to connect to the database
    
    :inputs - DB information as optional parameters
    :return: DB connection handle
    """

    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
                    .format(dbname, port, user, password, hostname)
    
    LOGGER.info('Connecting to the Postgres')
    LOGGER.info(f'Hostname: {hostname}')
    LOGGER.info(f'Database: {dbname}')
    
    conn = psycopg2.connect(conn_string)
    LOGGER.info('Connection Successful')
    
    return conn

# **************************************************
# Date Loader
# **************************************************
def loader():
    """
    Method to load CSV to DB
    
    :input: None
    :return: None
    """
    
    # Connection Handler
    conn = connect_to_db()
    cur = conn.cursor()
    
    # Query used to clean DB before and after import
    cleanup_query = """ BEGIN;
                        DROP TABLE IF EXISTS tripdata_staging;
                        END;
                    """
    
    # ***** Query to create unlogged table for staging table *****
    """ Unlogged table is to avoid Write Ahead Log for staging table since it will be dropped after the load operation.
    In case of database crashes, the data in this statging table cannot be recovered but it should not be a problem since the processing is not complete.
    
    The intent of staging table it to transform the data before loading to main table.
    """
    
    staging_table_query = """CREATE UNLOGGED TABLE tripdata_staging (
                            VendorID int,
                            tpep_pickup_datetime   timestamp,
                            tpep_dropoff_datetime  timestamp,
                            passenger_count        int,
                            trip_distance          float,
                            RatecodeID             int,
                            store_and_fwd_flag     varchar(1),
                            PULocationID           int,
                            DOLocationID           int,
                            payment_type           int,
                            fare_amount            float,
                            extra                  float,
                            mta_tax                float,
                            tip_amount             float,
                            tolls_amount           float,
                            improvement_surcharge  float,
                            total_amount           float,
                            congestion_surcharge   float
                            );
                            """
    
    # ***** Query used to load data to staging table *****
    copy_to_staging_query = """ COPY tripdata_staging
                                FROM '{}'
                                DELIMITER ','
                                CSV HEADER;
                            """.format(file_name)
    
    # ***** Query used to copy only required data to main table. *****
    """Assuming that there are only insert and no updates.
    Note that this query will run slower than the insert in the unlogged table since this table is logged.
    """
    
    insert_to_staging_query = """INSERT INTO test_od 
    (tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, total_amount)
    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, total_amount 
    FROM tripdata_staging;
    """

    try:
        startTime = time.time()
        
        LOGGER.info("Running Cleanup Query...")
        cur.execute(cleanup_query)
        conn.commit()
        LOGGER.info("Running Cleanup Query Completed.")

        LOGGER.info("Creating staging table...")
        cur.execute(staging_table_query)
        conn.commit()
        LOGGER.info("Creating staging created.")
        
        LOGGER.info("Loading the CSV data to the staging table...")
        cur.execute(copy_to_staging_query)
        conn.commit()
        LOGGER.info("Data loaded to the staging table.")
        
        # ******** NOTE ******
        """ The Transformation queries should run over here. For this example, we are skipping this step.
        """
        # *******************
        
        LOGGER.info("Inserting data to the main table...")
        cur.execute(insert_to_staging_query)
        conn.commit()
        LOGGER.info("Data loaded to the main table.")
        
        
        # ******** NOTE ******
        """ It's better to run VACUUM ANALYZE command after this step because it will do garbage collections and 
        free up memory. Analyze updates statistics used by the planner to determine the most efficient way to execute 
        a query.
        For this example, we are skipping this step.
        """
        # *******************
        
        LOGGER.info("Running Cleanup Query...")
        cur.execute(cleanup_query)
        conn.commit()
        LOGGER.info("Running Cleanup Query Completed.")
        
        LOGGER.info("*** Command executed successfully ***")
        
        executionTime = (time.time() - startTime)
        LOGGER.info('Execution time in seconds: %s', str(executionTime))

    except Exception as ex:
        LOGGER.error("Main Exception: \n%s", str(ex))
        LOGGER.error("The Loader Failed!")
            
    cur.close()
    conn.close()
    
if __name__ == "__main__":  
    loader()
    
    
"""
SAMPLE RUN OUTPUT:

INFO:root:Connecting to the Postgres
INFO:root:Hostname: localhost
INFO:root:Database: travel
INFO:root:Connection Successful
INFO:root:Running Cleanup Query...
INFO:root:Running Cleanup Query Completed.
INFO:root:Creating staging table...
INFO:root:Creating staging created.
INFO:root:Loading the CSV data to the staging table...
INFO:root:Data loaded to the staging table.
INFO:root:Inserting data to the main table...
INFO:root:Data loaded to the main table.
INFO:root:Running Cleanup Query...
INFO:root:Running Cleanup Query Completed.
INFO:root:*** Command executed successfully ***
INFO:root:Execution time in seconds: 150.2567880153656
"""