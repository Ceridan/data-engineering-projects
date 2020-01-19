# Project: Data Warehouse

## Project Goals

`Sparkify` is a startup which focused on music streaming. They have a lot of archive data with information about songs and artists (metadata). Also Sparkify collects information which tracks users of their service listen to.
Sparkify wants to analyze collected data to better understand preferences of their users. 
Sparkify chooses [Amazon Web Services (AWS)](https://aws.amazon.com/) as cloud provider and store all collected raw data as JSON files on the Amazon S3 storage.

The main goal of this project is to build the Date Warehouse (DWH) to solve the analytical tasks of the company.
According to the fact that Sparkify already choose the AWS as cloud provider the Amazon Redshift is chosen as the DWH solution.  

Project goals summary:

- Create data model in Amazon Redshift includes staging area for raw date and star schema for analytic queries.
- Build ETL pipeline to load raw data from Amazon S3 storage to the staging area and then load data from staging area to the star schema tables.
- Prepare dashboard with examples of analytic queries against relational database.

## ETL pipeline clarification

The ETL pipeline consists of two stages:

1. Load raw data from the Amazon S3 storage to the staging area in the Amazon Redshift as is. This stage allows to use COPY operation for the batch load the raw data. The data is extracted from JSON and landed to the staging tables. On this stage all the data is saved as is without quality checking and any transformation or aggregation.
This stage is necessary because it will speed up data loading from blobs and also prepare data for the second ETL pipeline stage which includes JOINs to repartition the raw data and load it to star schema.

2. Load data from staging tables to the star schema tables. This stage includes repartition of data and data quality checks to ensure data integrity.

## Data model for the Sparkify database in Amazon Redshift

Sparkify database uses schemas mechanism to separate staging area from the star schema tables:
- `stg` (short for _staging_) schema is used for staging tables.
- `public` schema (default schema in Amazon Redshift) is used for data analysis team and contains star schema.

### Staging area data model

`stg` schema contains two tables:
- `events` table contains raw events from the Sparkify application/website with information about user activities.
- `songs` table contains meta information about songs and authors. 

Staging area tables is shown on the following picture:

![star_schema](images/staging_area.png)

### Data model for data analysis team

Data analysis team need to build various reports and ad-hoc queries. To meet these requirements star schema is used. This allows us to present data in a more convenient way for analysts and also to reduce the number of joins in queries.

Star schema contains 4 dimension tables (songs, artists, users and time) and 1 fact table (songplays):
- `songs` dimension table contains information about each song in the Sparkify collection.
- `artists` dimension table contains information about artists.
- `users` dimension table describes Sparkify service users.
- `time` dimension table serves for better describe time intervals.
- `songplays` fact table keeps information what (and when) users are listen to.

> Remark 1. `songplays` table contains auto-increment key to unique identify each row. 

> Remark 2. `songplays` contains some redundant data, for example field `level` is duplicated in the `users` table. This is done intentionally to avoid unnecessary table joins while querying the data which can significantly improve query performance.
   

Star schema is shown on the following image:

![star_schema](images/dwh_star_schema.png)

## Project files

This project is implemented using [Python](https://www.python.org/) programming language and [Jupyter notebooks](https://jupyter.org/) to test and represent results.

Here is the list of project files and their purpose:

- `sql_queries.py` contains all SQL queries for DROP and CREATE all tables in the Amazon Redshift and contains load data scripts for both steps of the ETL pipeline.
- `create_tables.py` is used to prepare a new database in the Amazon Redshift for future work. It uses `sql_queries.py` to run DROP and CREATE table statements to re-create all the tables.
- `etl.py` implements the ETL pipeline. This script load (and processes) JSON files from Amazon S3 storage to Amazon Redshift.
- `dashboard.ipynb` is a Jupyter notebook for the BI-team to run analytic queries against Amazon Redshift.
- `dwh.cfg` configuration file which contains settings to connect to the Amazon Redshift cluster.
- `README.md` – this README file. 

## Prerequisites: Setup Amazon Redshift cluster

It is required to setup Amazon Redshift cluster first. There are several approaches how to achieve this goal:

- Amazon Console using web browser and create roles, security groups and Amazon Redshift cluster in point-and-click manner.
- AWS CLI - command line utility which can be used by hand or in automated scripts.
- Infrastructure-as-Code - the modern approach using SDK libraries and keep all the infrastructure as code.

For this project we assume that the Amazon Redshift cluster already up and running because the main goal of the project is to create proper data model in the Sparkify Data Warehouse and implement ETL pipeline.

### The requirements to Amazon Redshift cluster are

The following table shows the cluster requirements:

| Parameter          | Value      |   
|--------------------|------------|
| Region (location)* | us-west-2  |
| Cluster type       | multi-node |
| Nodes count        | 4          |
| Node type          | dc2.large  |

_(*): Region is important because cross-region data ingestion to Redshift is not allowed. Sparkift raw data currently located in the `us-west-2` region._

## How to prepare tables and run ETL pipeline

1. Edit `dwh.cfg` file and fill all variables with values for your cluster setup.
    > CAUTION. This information can be stored in the repository because using this variables anyone can get access to the cluster!
    
    Configuration file contains three sections:
    - `CLUSTER` variables that allows to connect to the Amazon Redshift cluster.
    - `IAM_ROLE` - section for the IAM role with policy to access Amazon S3 storage to ingest data from S3 to Redshift.
    - `S3` - already filled section with the path to S3 buckets with Sparkify raw data.

2. Run `create_tables.py`. For example, you can do it from the Terminal:
    
    ```bash
    python create_tables.py
    ```
   
3. Run `etl.py` to execute the ETL pipeline:
   
   ```bash
   python etl.py 
   ```

If all steps are executed correctly without errors then the DWH is ready for analytic queries.

## Dashboard for analytic queries

`dashboard.ipynb` has examples of analytic queries against Sparkify Date Warehouse.

Currently, you can run following queries:
- Find top 10 most popular songs to build top charts.
- Report: Weekly statistics to understand how many songs users listen weekly and how many unique users use Sparkify.

and of course you can write your own queries!