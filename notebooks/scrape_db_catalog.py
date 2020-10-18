'''Module to:
 - create a connection to a Postgres db using credentials read from a config file
 (pw provided interactively)
 - build an SQL query to extract metadata from the Postgres catalog with col list,
 target table, schema and condition value read from a config file
 - save the result in a dataframe
 - persist the dataframe as a pickle file
 postgres db connection code - from https://pynative.com/python-postgresql-tutorial/

'''


import getpass
#import pickle
import logging
import os
import yaml
import pandas as pd
import psycopg2

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
        print('Error reading the config file ' + error)


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


def get_catalog_df(
        user,
        pw,
        host,
        port,
        db,
        col_list,
        from_table,
        schema,
        order_by_col):
    ''' get the catalog table describing columns for tables in the
    given schema and return as a dataframe

        Args:
            user: Postgres user ID for connection
            pw: password of user
            host: host of database
            port: port of database
            db: database name
            col_list: list of columns to extract in SQL query
            from_table: catalog table from which to extract column values
            schema: schema of table to get catalog info from
            order_by_col: column used to order results of SQL query

        Returns:
            df: dataframe containing the results of the query

    '''
    try:
        connection = psycopg2.connect(user=user,
                                      password=pw,
                                      host=host,
                                      port=port,
                                      database=db)
        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(connection.get_dsn_parameters(), "\n")
        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
        # create a dataframe with details about the columns
        i = 0
        col_string = ", ".join(col_list)
        query_string = "SELECT " + col_string + "  FROM " + from_table + \
            " where table_schema='" + schema + "' order by " + order_by_col
        logging.debug("query string is " + query_string)
        cursor.execute(query_string)
        record_col_details = cursor.fetchall()
        col_details_list = []
        for item_col in record_col_details:
            logging.debug(
                "record cols from tables table " +
                str(i) +
                " is:" +
                str(item_col) +
                "\n")
            # table_table_cols_list = table_table_cols_list + list(item_col)
            col_details_list.append(item_col)
            i = i + 1
        df = pd.DataFrame(
            col_details_list,
            columns=col_list)
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
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
    
def save_catalog_df_as_csv(df,csv_name, modifier):
    ''' persist a dataframe as a pickle file with the specified filename and path

        Args:
            df: dataframe to persist
            csv_name: name of csv file in which to save dataframe
            modifier: qualifier to create distinct names for multiple runs

    '''
    file_name = csv_name + '_' + modifier + '.csv'
    csv_path = os.path.join(get_path(), file_name)
    logging.debug("output file_name is " + str(csv_path))
    df.to_csv(csv_path)


def main():
    ''' main function for module - ingest config file,
    get dataframe containing catalog details, and save as a pickle file'''
    print("Hello World!")
    config = get_config('scrape_db_catalog_config.yml')
    pw = get_pw()
    print("Got pw")
    # get dataframe with db catalog details, using parameters from config file
    catalog_df = get_catalog_df(
        config['general']['user'],
        pw,
        config['general']['host'],
        config['general']['port'],
        config['general']['database'],
        config['query_scope']['cols'],
        config['query_scope']['from_table'],
        config['query_scope']['schema'],
        config['query_scope']['order_by_col'])
    # save the df as a pickle file
    save_catalog_df(
        catalog_df,
        config['files']['output_pickle_name'],
        config['files']['modifier'])
    save_catalog_df_as_csv(
        catalog_df,
        config['files']['output_catalog_csv'],
        config['files']['modifier'])
    print(catalog_df.head(40))


if __name__ == "__main__":
    main()
