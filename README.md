# dl_auto_relational_models
- active repo for relational db scraping related to Manning book **Deep Learning with Structured Data** https://www.manning.com/books/deep-learning-with-structured-data
- the code in this repo extracts table metadata from the catalog of a Postgres database and uses it to automatically generate simple DL models trained with the data in the database

## Directory structure
- **data** - processed datasets and pickle files for intermediate datasets
- **models** - saved trained models
- **notebooks** - code
- **pipelines** - pickled pipeline files
- **sql** - accompanying queries

## To exercise the code

1. Install Postgres https://www.postgresql.org/download/ including pgadmin
2. follow instructions to create sample db: https://www.postgresqltutorial.com/postgresql-sample-database/
3. once you have created the sample db, update the config file notebooks/scrape_db_catalog_config.yml to ensure that the **user, host, port** and **database** settings match the credentials for your database
4. run notebooks/scrape_db_catalog.py. This module will:
- prompt you to enter the Postgres password corresponding with the **user** you specified in the config file
- connect to the database using the credentials you specified in the config file and the password you supplied interactively
- run a query to get details about the columns of every table in the specified schema
- save the results of the query in a dataframe that gets persisted as a pickle file


## Background

- Python PostgreSQL Tutorial Using Psycopg2: https://pynative.com/python-postgresql-tutorial/
- main repo for **Deep Learning with Structured Data**: https://github.com/ryanmark1867/deep_learning_for_structured_data