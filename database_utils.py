from sqlalchemy import create_engine 
from sqlalchemy import inspect

import pandas as pd
import psycopg2
import yaml

class DatabaseConnector:
    '''
    This class will  connect with and upload data to the database.
    Database connection details are read from file db_creds.yaml.
    
    Parameters:
    ----------
    None
    
    Attributes:
    ----------
    db_creds_file: str
        The yaml file containing database connection credentials

    Methods:
    -------

    read_db_creds()
        Reads db credentials file and returns a dictionary of the credentials.
    init_db_engine()
        Reads the credentials from the return of read_db_creds and initialises and returns an sqlalchemy database engine.
     list_db_tables(self):
        Lists all the tables in the database so you know which tables you can extract data from

    '''
    def __init__(self):
        self.db_creds_file = 'db_creds.yaml'

    def read_db_creds(self):

        '''
        Reads db_creds_file and returns a dictionary of the credentials.
        '''

        # opening a file
        try:
            # Converts yaml document to python object
            with open(self.db_creds_file, 'r') as file:
                creds = yaml.safe_load(file)

        	# Printing dictionary
            return(creds)

        #catch errors
        except yaml.YAMLError as e:
            print(e)

    def init_db_engine(self, db_name):

        '''
        Reads the credentials from the return of read_db_creds 
        and initialises and returns a sqlalchemy database engine.
        '''

        get_creds_dict = self.read_db_creds()

        #return sqlalchemy database engine
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = get_creds_dict[db_name +'_HOST']
        USER = get_creds_dict[db_name +'_USER']
        PASSWORD = get_creds_dict[db_name +'_PASSWORD']
        DATABASE = get_creds_dict[db_name +'_DATABASE']
        PORT = get_creds_dict[db_name +'_PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return engine
    
    def list_db_tables(self):
        '''
        Lists all the tables in the database so you know which tables you can extract data from
        '''
        engine = self.init_db_engine('RDS')

        #get table names
        inspector = inspect(engine)
        table_names_list = inspector.get_table_names()

        print(table_names_list)
        return table_names_list
    
    def upload_to_db(self, data_df, table_name):

        engine = self.init_db_engine('SD')

        #create active connection with autoconnect option
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()

        # YOUR QUERIES HERE
        data_df.to_sql(table_name, engine, if_exists='replace')


# only run if called directly
if __name__ == '__main__':
    runme = DatabaseConnector()

    #testing -------------------------------------
    #runme.list_db_tables()
    #testing -------------------------------------
    #from sklearn.datasets import load_iris
    #data = load_iris()
    #iris = pd.DataFrame(data['data'], columns=data['feature_names'])
    #runme.upload_to_db(iris,'test')




