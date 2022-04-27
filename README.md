
# Python Assessment 2022

## TASK 1

One of our products is in charge of downloading and ingesting millions of records from our clients.

Recently during ingesting a large dataset we had our entire DB(postgres) go down and the entire ingestion process from a pandas dataframe to sql took around 2-3 hours because of the RAM unavailability. Now this has two simple fixes

- Increase ram/ scale the db on demand
- change our code to accommodate these restrictions and make the entire ingestion process much faster on the way.

How would you approach this? We are not looking for a full blown ingestion logic. Just a small script to take a given csv file and upload it to DB in an efficient manner.

Write code to take a large csv file( > 1GB ) and ingest it to table - public.test_od

## Task 1 Solution
Usually when OLTP databases are optimized for transaction processing. If there's a lot of data and used for analytics/reporting, it might be wise to look into OLAP system that use Columnar storage. For bulk operations in database imports, parquet files work efficiently especially for columnar storage and compression.

Preprocessing large files using PySpark is also ideal, especially in a complex data pipeline. Pandas can also do the work but it does not scale as well since it has limited parallel processing capability. However, PySpark is not as idea for files with small size.

In cases where we do not want to scale database and need to optimize the SQL ETL, we can do the following:

1) Use unlogged tables for staging tables. This will avoid overheads such as Write Ahead Logs that makes database more reliable in cases of failures. We do not require that for staging tables. The transformation will also be faster if UNLOGGED tables are used. The INSERT to main table from will still take time since it should be logged. However, the initial load and transformation will faster.
2) Dropping Indexes and recreating after import. When a new row is created in DB, index is generated after each INSERT. In cases where the tables are not already huge, it may be worth dropping the index, loading the data, and recreating index. 
3) Avoiding Trigger. Like indexes, trigger happen immediately before or after a CRUD operation. For bulk import, we should find strategies that may avoid triggers in DB.
4) Use Postgres COPY command. That is way more efficient than INSERT command.
5) Optimize the columns. In general, database stores in pages inside of memory blocks. So, databases work efficiently when fixed width datatypes such as integers, timestamps should be store in the front of the table.

For this ETL, we will are assuming that there are no triggers and indexes. Since the data size is less than 10GB, we will not use PySpark. We will be unlogging the staging tables. These changes should be the script more efficient. We are also not using TRANSACTION in this script because transformation is happening in staging tables.

#### For this problem, I am using the NYC Yellow Taxi Trip Data from Kaggle at https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data
I have merged the data for Jan and Feb 2019 which is about 1.3 GB. If you need the copy of the csv file, please reach out to me.

***In order to run the code the following should be configured:***
1) Create a Postgres DB called 'travel'
2) Create user with superadmin privilege on the database 'travel'
3) Create a table called "test_od" using the following sql:
```
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
```
4) Update the DB config in the script

#### The code can be found in Task1.py. Run it using this following command:
```
python Task1.py
```

## Task 2
Our customer records are sometimes a little messy and contain duplicate entires. For instance, we might have two records like "Pfizer" and "Pfizer Incorporated (old DO NOT USE)". Please write a small basic function to merge these records together. We've provided a few example inputs below. We are aware this is a very difficult problem: we're looking for what you can do quickly as a basic case, not an ideal solution.

> Equipment ONLY - Saama Technologies
Saama Technologies
SaamaTech, Inc.
Takeda Pharmaceutical SA - Central Office
*** DO NOT USE *** Takeda Pharmaceutical
Takeda Pharmaceutical, SA
Ship to AstraZeneca
AstraZeneca, gmbh Munich
AstraZeneca (use AstraZeneca, gmbh Munich acct 84719482-A)

Use your own interpretation of the question and feel free to provide a written explanation for your choices as well.

## Task 2 Solution
The sample the line items all have Pharma company names. I am assuming that there is a predefined list of Pharma companies. In this case, the easiest way to do this would be to use have a quick scan over the list and create mapping to standard company titles. For example:
|Keyword|Standarded name  |
|--|--|
| Saama Technologies |Saama Technologies  |
| SaamaTech, Inc |Saama Technologies  |
| Takeda Pharmaceutical |Saama Technologies  |
| AstraZeneca |AstraZeneca |
| AstraZeneca plc |AstraZeneca |

We can use this mapping as a hashmap and to search for the keywords in the text and map them to the respective standardized company titles.

#### The code can be found in Task1.py. Run it using this following command:
```
python Task2.py
```

## Task 3 

**Visits**
| Customer\_id | City\_id\_visited | Date\_visited |
| ------------ | ----------------- | ------------- |
| 1001         | 2003              | 1-Jan-03      |
| 1001         | 2004              | 1-Jan-04      |
| 1002         | 2001              | 1-Jan-01      |
| 1004         | 2003              | 1-Jan-03      |

**Customer**
| Customer\_id | Customer\_name | Gender | Age |
| ------------ | -------------- | ------ | --- |
| 1001         | John           | M      | 25  |
| 1002         | Mark           | M      | 40  |
| 1003         | Martha         | F      | 55  |
| 1004         | Selena         | F      | 34  |

**City**
| City\_id | City\_name | Expense |
| -------- | ---------- | ------- |
| 2001     | Chicago    | 500     |
| 2002     | Newyork    | 1000    |
| 2003     | SFO        | 2000    |
| 2004     | Florida    | 800     |

 **SQL Questions**
1) Cities frequently visited?
2) Customers visited more than 1 city?
3) Cities visited breakdown by gender?
4) List the city names that are not visited by every customer and order them by the expense budget in ascending order?
5) Visit/travel Percentage for every customer?
6) Total expense incurred by customers on their visits?
7) list the Customer details along with the city they first visited and the date of visit?

## Task 3 Solutions

The solution for this problem can be found at Task3.sql. 

Before running the code, you will need to do the following:
1) Create a Postgres DB called 'travel'
2) Create user with superadmin privilege on the database 'travel'

To run this sql file, enter the following in the command line:
```
psql -U [USERNAME] -d travel -a -f Task3.sql
```
