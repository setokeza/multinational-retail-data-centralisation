import pandas as pd
from tabula import read_pdf
from database_utils import DatabaseConnector 

class DataExtractor():
    def __init__(self):
        pass

    def read_rds_table(self, db_connector, table_to_read):

        '''
        Extract the database table to a pandas DataFrame.

        Takes in an instance of the DatabaseConnector class and the table name as an argument and returns a pandas DataFrame.
        Uses DatabaseConnector.list_db_tables() method to get the list of table names in the database. the name of the table containing user data.
        Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.
        '''

        #connection will auto close after queries have run
        engine = db_connector.init_db_engine('RDS')
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            # YOUR QUERIES HERE

            #read data using pandas
            pd.set_option('display.max_columns', None) # use this setting to display all the columns in your dataframe
            data = pd.read_sql_table(table_to_read, engine)
            return data

    def retrieve_pdf_data(self,pdf_link):

        '''
        Takes in a link as an argument.
        Extracts & concats all pages from the pdf document at pdf_link .
        Returns a DataFrame of the extracted data.
        '''
        dfs = read_pdf(pdf_link,pages='all')
        card_df = pd.concat(dfs)
        return card_df


# only run if called directly
if __name__ == '__main__':
    db_connector = DatabaseConnector()
    runme = DataExtractor()
    #runme.read_rds_table(db_connector,'legacy_users')
    pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    runme.retrieve_pdf_data(pdf_link).info()
    