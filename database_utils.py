from sqlalchemy import create_engine, inspect

import pandas as pd
import yaml

class DatabaseConnector:
    '''
    Connect to a PostgreSQL database, read database credentials from a YAML file,
    initialize a database engine, list database tables, and upload a clean DataFrame to a specified table.

    Parameters:
    - file (str): Path to the YAML file containing database credentials.

    Attributes:
    - db_creds_file (str): Path to the YAML file containing database credentials.

    Methods:
    -------

    read_db_creds()
        Reads db credentials file and returns a dictionary of the credentials.
    init_db_engine(db_name)
        Initialises and returns an sqlalchemy database engine.  Uses (db_name) to identify which database to connect to.
     list_db_tables():
        Lists all the tables in the database.
    upload_to_db(data_df, table_name)
        Upload a DataFrame (data_df) to the specified table in the database (table_name).
    test_DatabaseConnector (switch='')
        Allows internal testing of this class.  
        The methods to run can be specified using (switch).
    '''

    def __init__(self, file=None):
        '''
        Initialize DatabaseConnector instance.

        Parameters:
        - file (str): Path to the YAML file containing database credentials.

        '''
        self.db_creds_file = file
        

    def read_db_creds(self):

        '''
        Reads db_creds_file and returns a dictionary of the credentials.
        '''

        # opening a file
        try:
            # Converts yaml document to python object
            with open(self.db_creds_file, 'r') as file:
                db_creds = yaml.safe_load(file)

        	# Printing dictionary
            return(db_creds)

        #catch errors
        except yaml.YAMLError as e:
            print(e)

    def init_db_engine(self, db_name):

        '''
        Initialize the SQLAlchemy database engine.

        Parameters:
        - db_name (str): Can specify which database to connect to.
                         Format within credentials file is : [db_name]_HOST, [db_name]_PASSWORD etc.

        Returns: 
        - SQLAlchemy engine

        '''
        #create dictionary of database credentials
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
        List table names in the connected database.

        Returns:
        - list: List of table names.
        '''
        #connect to 'RDS' database
        engine = self.init_db_engine('RDS')

        #get table names
        inspector = inspect(engine)
        table_names_list = inspector.get_table_names()

        return table_names_list
    
    def upload_to_db(self, engine, data_df, table_name):
        '''
        Upload a DataFrame to the specified table in the database.

        Parameters:
        - engine(sqlaclemy engine): database to connect to
        - data_df (pd.DataFrame): cleaned DataFrame to upload.
        - table_name (str): name of the table to upload to.

        Returns:
        - str: the numbers or rows uploaded to the database.
        '''
        #create active connection with autoconnect option
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()

        # Upload dataframe to database
        data_df.to_sql(table_name, engine, if_exists='replace')
        print(f'\nupload_to_db(): {len(data_df)} rows uploaded to table \'{table_name}\'')


    def test_DatabaseConnector (self, switch=''):
        '''
        Allows testing of class methods.

        Parameters:
        - switch (str): Identifies which methods to run:
            1. test list_db_tables()
            2. test upload process using iris dataframe
        '''        
        if '1' in switch:
            # testing list_db_tables()
            list = self.list_db_tables()
            print (list)

        if '2' in switch:
            # following lines are specifically for testing upload
            from sklearn.datasets import load_iris
            data = load_iris()
            iris = pd.DataFrame(data['data'], columns=data['feature_names'])
            print('test_DatabaseConnector(): rows in dataframe = ',len(iris))
            self.upload_to_db(upload_engine,iris,'test')

# only run if called directly
if __name__ == '__main__':

    #internal code to test class methods
    runme = DatabaseConnector('db_creds.yaml')
    upload_engine = runme.init_db_engine('SD')
    runme.test_DatabaseConnector('0')







