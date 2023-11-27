from database_utils import DatabaseConnector 
from tabula import read_pdf

import boto3
import json
import numpy as np
import pandas as pd
import requests

class DataExtractor():
    '''
    Retrieve data from various sources in various forms and return the data requested.

    Parameters:
    - None.

    Attributes:
    - None

    Methods:
    -------

    read_rds_table(db_connector, table_to_read)
        Connect to a database (db_connector) and retrieve data from a specific table (table_to_read).
        Return a dataframe.
    retrieve_pdf_data(pdf_link)
        Retrieve data from a PDF file (pdf_link), and return a dataframe.
    list_number_of_stores(endpoint,header)
        Return the number of stores using an API call (endpoint,header).  
    retrieve_stores_data(endpoint, number_of_stores, header)
        Retrieve data set for multiple stores using an API call (endpoint,header), 
        for a specific number of stores(number_of_stores).  Return a dataframe.
    extract_from_s3(file_path, bucket, object)
        Extract data from a file stored on Amazon S3 (bucket + object).
        Store the file in a local path (file_path), and return a dataframe.
    test_DataExtractor (switch='')
        Allows internal testing of this class.  
        The methods to run can be specified using (switch).

    '''
    def __init__(self):
        pass

    def read_rds_table(self, db_connector, table_to_read):

        '''
        Retrieve data from a database table and return a dataframe

        Parameters:
        - db_connector : instance of DatabaseConnector class.
        - table_to_ead : Name of the table from which we will extract data.

        Returns:
        - pd.DataFrame: Dataframe from the RDS table.
        '''

        #connection will auto close after queries have run
        engine = db_connector.init_db_engine('RDS')
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            #QUERIES HERE

            #read data using pandas
            pd.set_option('display.max_columns', None) # use this setting to display all the columns in the dataframe
            data_df = pd.read_sql_table(table_to_read, engine)

        return data_df

    def retrieve_pdf_data(self, pdf_link):
        '''
        Retrieve data from a PDF file, and return a dataframe.

        Parameters:
        - pdf_link : Link to the PDF file.

        Returns:
        - pd.DataFrame : Dataframe from the PDF file.
        '''
        pdf_page = read_pdf(pdf_link, pages='all')
        pdf_df = pd.concat(pdf_page)
        return pdf_df

    def list_number_of_stores(self, endpoint, header_dict):
        '''
        Retrieve the number of stores using an API call.

        Parameters:
        - endpoint: API endpoint for retrieving the number of stores.
        - header_dict: Request header.

        Returns:
        - int: Number of stores.
        '''
        response  = requests.get(endpoint, headers=header_dict)
        stores_dict = response.json()
        return stores_dict['number_stores']
        
    def retrieve_stores_data(self, endpoint, number_of_stores, header_dict):
        '''
        Retrieve data set for multiple stores and return a dataframe.

        Parameters:
        - endpoint: API endpoint for retrieving store data.
        - header_dict: Request header.
        - number_of_stores: Number of stores to retrieve.

        Returns:
        - pd.DataFrame: Data for multiple stores.
        '''

        store_df = pd.DataFrame()
        print('\nPlease wait while stores data is retrieved...\n')
        for store in range(number_of_stores):
            api_response = requests.get(f'{endpoint}{store}', headers=header_dict).json()
            if 'Store not found' in api_response:
                break
            store_df =  pd.concat([store_df, pd.DataFrame(api_response, index=[np.NaN])], ignore_index=True)
        return store_df
    
    def extract_from_s3(self, file_path, bucket, object):
        '''
        Extract data from a file stored on Amazon S3 and return a dataframe.

        Parameters:
        - file_path: Local path to save the downloaded file.
        - bucket: Path to bucket on Amazon S3.
        - object: Name of csv file to download.

        Returns:
        - pd.DataFrame: Data from the file.
        '''
        s3 = boto3.client('s3')
        with open(file_path, 'wb') as f:
            s3.download_fileobj(bucket, object, f)
        s3_df = pd.read_csv(file_path)
        return s3_df
    
    def test_DataExtractor(self, switch):
        '''
        Allows testing of class methods.

        Parameters:
        - switch (str): Identifies which methods to run:
            1. extract users data
            2. extract card data
            3. extract stores data
            4. extract products data
            5. extract orders data
            6. extract dates data
        ''' 
        if '1' in switch:
            # extract users data
            df = self.read_rds_table(connector,'legacy_users')
            print(len(df), 'rows extracted from legacy_users table')

        if '2' in switch:
            # extract card data
            df = self.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
            print(len(df), 'rows extracted from card_details.pdf')

        if '3' in switch:
            # extract number of stores
            endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
            header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
            num_stores = self.list_number_of_stores(endpoint,header)
            print('Number of stores retrieved from Amazon api call: ', num_stores)

            # extract stores data
            endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
            header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
            df = self.retrieve_stores_data(endpoint, num_stores, header)
            print(len(df), 'rows extracted from Amazon stores data api call')

        if '4' in switch:
            # extract products data
            file_path = '/Users/shaha/Documents/AiCore/Downloads/products.csv'
            bucket = 'data-handling-public'
            object = 'products.csv'
            df = self.extract_from_s3(file_path,bucket,object)
            print(len(df), 'rows extracted from Amazon S3 bucket, products.csv')

        if '5' in switch:
            # extract orders data
            df = self.read_rds_table(connector,'orders_table')
            print(len(df), 'rows extracted from orders_table')

        if '6' in switch:
            #  extract dates data
            api_fetch = requests.get('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
            df = pd.DataFrame(api_fetch.json())
            print(len(df), 'rows extracted from Amazon S3 bucket, date_details.json')

# only run if called directly
if __name__ == '__main__':
    
    # internal code to test class methods
    connector = DatabaseConnector('db_creds.yaml')
    runme = DataExtractor()
    runme.test_DataExtractor('0')

    
    