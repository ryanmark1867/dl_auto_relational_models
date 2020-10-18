''' Module to:
 - create a connection to a Postgres db using credentials
 read from a config file (pw provided interactively)
 - load the contents of the table into a Pandas dataframe
 - persist the dataframe as a pickle file
 postgres db connection code - from
 https://pynative.com/python-postgresql-tutorial/ and
 https://pythontic.com/pandas/serialization/postgresql#:~:text=Data%20from%20a%20PostgreSQL%20table,SQLAlchemy%20Engine%20as%20a%20parameter.
'''

import getpass
# import pickle
import logging
import os
import yaml
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

logging.getLogger().setLevel(logging.WARNING)
logging.warning("logging check")


def get_config(config_file):
    ''' open config file with name config_file that contains parameters
    for this module and return Python object

    Args:
        config_file: filename containing config parameters

    Returns:
        config: Python dictionary with config parms from config file - dictionary


    '''
    current_path = os.getcwd()
    print("current directory is: " + current_path)

    path_to_yaml = os.path.join(current_path, config_file)
    print("path_to_yaml " + path_to_yaml)
    try:
        with open(path_to_yaml, 'r') as c_file:
            config = yaml.safe_load(c_file)
        return config
    except Exception as error:
        print('Error reading the config file '+error)


def get_pw():
    ''' prompt user for password - do this interactively to avoid saving
    the password in the config file

    Returns:
        pw: password string entered by user

    '''
    try:
        pw = getpass.getpass(prompt='Postgres Password: ')
    except Exception as error:
        print('ERROR', error)
    else:
        return pw


def load_df_from_table(user, pw, host, port, db, table):
    ''' get a dataframe with the contents of the table

        Args:
            user: Postgres user ID for connection
            pw: password of user
            host: host of database
            port: port of database
            db: database name
            table: table to dump into a dataframe

        Returns:
            df: dataframe containing the contents of the table

    '''
    try:
        # dialect+driver://username:password@host:port/database
        connection_url = "postgresql+psycopg2://" + user + \
            ":" + pw + "@" + host + ":" + port + "/" + db
        logging.debug("connection_url is: " + connection_url)
        # create connection, recycle after 1 hour = 3600 seconds
        alchemyEngine = create_engine(connection_url, pool_recycle=3600)
        # connect to the server
        # Connect to PostgreSQL server
        dbConnection = alchemyEngine.connect()
        # load dataframe
        query_string = "select * from " + table
        logging.debug("query_string is: " + query_string)
        df = pd.read_sql(query_string, dbConnection)
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        dbConnection.close()
        return df


def get_path():
    ''' get the path for data files

    Returns:
        path: path for data directory

    '''
    rawpath = os.getcwd()
    # data is in a directory called "data" that is a sibling to the directory
    # containing the notebook
    path = os.path.abspath(os.path.join(rawpath, '..', 'data'))
    return path


def save_catalog_df(df, pickle_name, modifier):
    ''' persist a dataframe as a pickle file with the specified filename and path

        Args:
            df: dataframe to persist
            pickle_name: name of pickle file in which to save dataframe
            modifier: qualifier to create distinct names for multiple runs


    '''
    file_name = pickle_name + '_' + modifier + '.pkl'
    pickle_path = os.path.join(get_path(), file_name)
    logging.debug("output file_name is " + str(pickle_path))
    df.to_pickle(pickle_path)


def main():
    ''' main function for module - ingest config file,
    get dataframe containing table contents, and save as a pickle file'''
    print("Hello World!")
    config = get_config('scrape_db_catalog_config.yml')
    pw = get_pw()
    print("Got pw")
    # load table (with table name and credentials coming from config file)
    # into df
    table_df = load_df_from_table(
        config['general']['user'],
        pw,
        config['general']['host'],
        config['general']['port'],
        config['general']['database'],
        config['query_scope']['to_df_table'])
    # save the df as a pickle file
    save_catalog_df(
        table_df,
        config['files']['output_table_df_pickle_name'],
        config['files']['modifier'])
    catalog_df = pd.read_pickle(
        os.path.join(
            get_path(),
            config['files']['input_pickle_name']))
    # get the columns for this table
    col_list = catalog_df[catalog_df['table_name'] ==
                          config['query_scope']['to_df_table']]['column_name']
    print(table_df.head(40))
    print("rows in table " + config['query_scope']
          ['to_df_table'] + " " + str(len(table_df)))
    print("columns in table " + str(col_list))
    unique_col_types = catalog_df['data_type'].unique()
    print("types of columns " + str(unique_col_types))


if __name__ == "__main__":
    main()
