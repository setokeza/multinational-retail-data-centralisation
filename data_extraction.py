import boto3
import pandas as pd
import numpy as np
import json
import requests
from tabula import read_pdf
import awswrangler as wr
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
            data_df = pd.read_sql_table(table_to_read, engine)
            return data_df

    def retrieve_pdf_data(self,pdf_link):

        '''
        Takes in a link as an argument.
        Extracts & concats all pages from the pdf document at pdf_link .
        Returns a DataFrame of the extracted data.
        '''
        dfs = read_pdf(pdf_link,pages='all')
        card_df = pd.concat(dfs)
        return card_df

    def list_number_of_stores(self, endpoint, header_dict):
        num_of_stores = requests.get(endpoint, headers=header_dict)
        content = json.loads(num_of_stores.text) #load response into a dictionary
        number_of_stores = content['number_stores']
        return number_of_stores
        
    def retrieve_stores_data(self, endpoint2, header_dict):
        endpoint1 = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        number_of_stores = self.list_number_of_stores(endpoint1, header_dict)
        print(number_of_stores)
        data = pd.DataFrame()
        for store_number in range(0, number_of_stores):
            endpoint2 = endpoint2 + str(f'{store_number}')
            api_response = requests.get(endpoint2, headers=header_dict).json()
            print(store_number)
            print(api_response)
            if 'Store not found' in api_response:
                break
            data =  pd.concat([data, pd.DataFrame(api_response, index=[np.NaN])], ignore_index=True)
        print(data)
        return data
    
    def extract_from_s3(self, file_path, bucket, object):
        s3 = boto3.client('s3')
        with open(file_path, 'wb') as f:
            s3.download_fileobj(bucket, object, f)
        data = pd.read_csv(file_path)
        return data
    
        

# only run if called directly
if __name__ == '__main__':
    db_connector = DatabaseConnector()
    runme = DataExtractor()

    
    