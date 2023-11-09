import yaml
import psycopg2
from sqlalchemy import create_engine 
from sqlalchemy import inspect

class DatabaseConnector:
    '''
    This class will  connect with and upload data to the database.
    Database connection details are read from file db_creds.yaml.
    
    Parameters:
    ----------
    db_creds_file: str
        The yaml file containing database connection credentials
    
    Attributes:
    ----------
    None

    Methods:
    -------

    read_db_creds()
        Reads db credentials file and returns a dictionary of the credentials.
    init_db_engine()
        Reads the credentials from the return of read_db_creds and initialises and returns an sqlalchemy database engine.
     list_db_tables(self):
        Lists all the tables in the database so you know which tables you can extract data from

    '''
    def __init__(self,db_creds_file):
        self.db_creds_file = db_creds_file

    def read_db_creds(self):

        '''
        Reads db_creds.yaml file and returns a dictionary of the credentials.
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

    def init_db_engine(self):

        '''
        Reads the credentials from the return of read_db_creds 
        and initialises and returns a sqlalchemy database engine.
        '''

        get_creds_dict = self.read_db_creds()

        #return sqlalchemy database engine
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = get_creds_dict['RDS_HOST']
        USER = get_creds_dict['RDS_USER']
        PASSWORD = get_creds_dict['RDS_PASSWORD']
        DATABASE = get_creds_dict['RDS_DATABASE']
        PORT = get_creds_dict['RDS_PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return engine
    
    def list_db_tables(self):
        '''
        Lists all the tables in the database so you know which tables you can extract data from
        '''
        engine = self.init_db_engine()

        #get table names
        inspector = inspect(engine)
        table_names_list = inspector.get_table_names()

        #print(table_names_list)
        return table_names_list

# only run if called directly
if __name__ == '__main__':
    runme = DatabaseConnector('db_creds.yaml')
    runme.list_db_tables()




