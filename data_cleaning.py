import pandas as pd
import numpy as np # We will need the `nan` constant from the numpy library to apply to missing values
import re
import requests

from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:
    def __init__(self):
        self.connector = DatabaseConnector()
        self.extractor = DataExtractor()
        self.data = pd.DataFrame()
        
    def clean_user_data(self):
        '''
        Performs the cleaning of the user data.

        You will need clean the user data look out for:
          NULL values, 
          errors with dates, 
          incorrectly typed values and 
          rows filled with the wrong information.

        '''
        #get data from the users table
        self.data = self.extractor.read_rds_table(self.connector,'legacy_users')

        #drop nulls & duplicates
        self.df_pre_processing()
        
        #convert dtypes
        self.convert_to_type('string',['first_name','last_name','company','email_address','address','phone_number','user_uuid'])
        self.convert_to_type('category',['country', 'country_code'])
        self.convert_to_type('date', ['date_of_birth','join_date'])

        #validate emails, replace errors with Nan
        self.validate_email('email_address')
        
        #validate phone numbers, replace errors with Nan
        self.validate_phone('phone_number')

        #data specific cleanups
        self.data['country_code'] = self.data['country_code'].str.replace('GGB', 'GB', regex=False)
        
        #df post_processing
        self.df_post_processing()
        self.df_upload('dim_users')

    def clean_card_data(self):
                        
        #get data 
        pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        self.data = self.extractor.retrieve_pdf_data(pdf_link)

        #data pre-processing
        self.df_pre_processing()
  
        #convert dtypes
        self.convert_to_type('string',['card_number','expiry_date'])
        self.convert_to_type('category',['card_provider'])
        self.convert_to_type('date', ['date_payment_confirmed'])

        self.data.info()
        print(self.data.head(20))
        # Checking all the unique category values
        print(self.data['card_provider'].unique())
        
        # Removing any unique category values that are erroneous
        valid_cards_list = ['Diners Club / Carte Blanche','American Express','JCB 16 digit','JCB 15 digit','Maestro', 'Mastercard','Discover','VISA 19 digit', 'VISA 16 digit','VISA 13 digit']
        self.validate_category('card_provider', valid_cards_list)
        
        # Removing symbols from card_number & cast as int
        self.data['card_number'] = self.data['card_number'].str.replace('[^a-zA-Z0-9\s]', '', regex=True)
        #self.data['card_number'] =  self.data['card_number'].astype(int)
        int_list = ['card_number']
        self.convert_to_type('int64', int_list )

        #remove new nulls & duplicates
        self.df_post_processing()
        self.df_upload('dim_card_details')

    def clean_store_data(self):

        #get_data
        endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
        header_dict = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        self.data = self.extractor.retrieve_stores_data(endpoint, header_dict)
        
        #print(data.head())
        #print(self.data.info())

        self.data = self.data.set_index(['index'])
        self.data['continent'] = self.data['continent'].str.replace('eeEurope', 'Europe', regex=False)
        self.data['continent'] = self.data['continent'].str.replace('eeAmerica', 'America', regex=False)
        self.data = self.data[self.data['continent'].isin(['Europe', 'America'])]
        self.data['address'] = self.data['address'].str.replace('\n', ',', regex=False)

        self.data['opening_date'] = self.data['opening_date'].apply(pd.to_datetime, errors='ignore')
        self.data['staff_numbers'] = self.data['staff_numbers'].replace({r'J': '', r'e': '', r'R': '', r'A': '', r'n': ''}, regex=True)
        self.data['staff_numbers'] = self.data['staff_numbers'].astype('int64')
        self.data = self.data.drop(columns=['lat'])
        
        #convert dtypes
        self.convert_to_type('string', ['address','locality','store_code'])
        self.convert_to_type('category', ['longitude', 'latitude'])
        self.convert_to_type('float', ['store_type','country_code','continent'])

        #remove rows with nulls/duplicates
        self.df_post_processing()

        #upload to database
        self.df_upload('dim_store_data')
    
    def convert_product_weights (self, data):

        data['in_kg'] = data['weight'].str.contains('kg')
        data['weight_units'] = np.where(data['weight'].str.contains('kg'), 'kg', 'not in kg')

        data['weight'] = data['weight'].replace({'16oz': '0.454', '77g .': '77g'})
        data['weight'] = data['weight'].replace({r'kg': '', r'g': '', r'ml': ''}, regex=True)
        data['weight'] = data['weight'].replace({r'12 x 100': '1200', r'8 x 150': '1200', r'6 x 412': '2472', r'6 x 400': '2400', r'8 x 85': '680', r'40 x 100': '4000', r'12 x 85': '1020', r'3 x 2': '6', r'3 x 90': '270', r'16 x 10': '160', r'3 x 132': '396', r'5 x 145': '725', r'4 x 400': '1600', r'2 x 200': '400'})
        data['weight'] = pd.to_numeric(data['weight'], errors='coerce')

        data['weight'] = np.where(data['weight_units'] == 'not in kg', data['weight'] / 1000, data['weight'])

        data = data.rename(columns={'weight': 'weight_kg'})
        data = data.drop(columns=['in_kg', 'weight_units'])

        return data

    
    def clean_products_data (self):

        #get data
        file_path = '/Users/shaha/Documents/AiCore/Downloads/products.csv'
        bucket = 'data-handling-public'
        object = 'products.csv'
        self.data = self.extractor.extract_from_s3(file_path, bucket, object)
        
        #convert_product_weights
        self.data = self.convert_product_weights(self.data)
        
        #remove nulls/dups
        self.df_pre_processing()

        #data specific cleaning
        self.data = self.data.rename(columns={'Unnamed: 0': 'index'})
        self.data = self.data.set_index(['index'])
        categories = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 'food-and-drink', 'diy']
        self.data = self.data[self.data['category'].isin(categories)]
        self.data['product_price'] = self.data['product_price'].str.replace('£', '', regex=False)
        self.data = self.data.rename(columns={'product_price': 'product_price_£'})

        #convert dtypes
        self.convert_to_type('date', ['date_added'])
        self.convert_to_type('int64', ['EAN'])
        self.convert_to_type('string', ['product_name','category','uuid','removed','product_code'])
        self.convert_to_type('category', ['category'])
        self.convert_to_type('float', ['product_price_£'])

        #remove rows with nulls/duplicates
        self.df_post_processing()

        #upload to database
        self.df_upload('dim_products')


    def clean_orders_data(self):

        #get data from the orders table
        self.data = self.extractor.read_rds_table(self.connector,'orders_table')
     
        print('\nBefore changes:')
        print(self.data.info())

        #drop unnecessary columns
        self.data = self.data.drop(columns=['level_0', 'first_name', 'last_name', '1'])

        #convert dtypes
        self.convert_to_type('int64',['card_number','product_quantity'])
        self.convert_to_type('string', ['store_code',['product_code','date_uuid','user_uuid']])

        #remove rows with nulls and duplicates
        self.df_post_processing()

        #upload to database
        self.df_upload('orders_table')

    def clean_dates_data(self):
        
        #get data
        api_fetch = requests.get('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
        self.data = pd.DataFrame(api_fetch.json())

        #remove rows with nulls and duplicates        
        self.df_pre_processing()
        print('\n',self.data.head(10))

        #data specific cleaning
        print('\nUnique values in time_period: ',self.data['time_period'].unique(),'\n')
        self.data = self.data[self.data['time_period'].isin(['Evening', 'Morning', 'Midday', 'Late_Hours'])]

        #combine day, month, year, timestamp to create date series
        self.data['date_time'] = pd.to_datetime(self.data[['year', 'month', 'day']])
        self.data['date_time'] = self.data['date_time'].astype(str)
        self.data['date_time'] = pd.to_datetime(self.data['date_time'] + ' ' + self.data['timestamp'])
        self.data = self.data.drop(columns=['month', 'year', 'day', 'timestamp'])

        #convert dtypes
        self.convert_to_type('category',['time_period'])
        self.convert_to_type('string', ['date_uuid'])
 
        #remove rows with nulls and duplicates
        self.df_post_processing()

        #upload to database
        self.df_upload('dim_date_times')


    def convert_to_type (self, to_type, column_list):

        simple_convert = ['string', 'category','int64']
        for column_name in column_list:
            if to_type in simple_convert:   
                self.data[column_name] = self.data[column_name].astype(to_type)
            elif to_type == 'date':
                # convert object to datetime, replace non-parseable values with NaN
                self.data[column_name] = self.data[column_name].apply(pd.to_datetime, errors='coerce')
            elif to_type == 'float':
                self.data[column_name] = pd.to_numeric(self.data[column_name], errors='coerce')


    def validate_email(self, email_col):
        email_regex = r"^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$" 
        # For every row  where the email column does not match our regular expression, replace the value with NaN
        self.data.loc[~self.data[email_col].str.match(email_regex), email_col] = np.nan 

    def validate_phone(self, phone_col):
        phone_regex = r"^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$"
        # For every row  where the phone_number column does not match our regular expression, replace the value with NaN
        self.data.loc[~self.data[phone_col].str.match(phone_regex), phone_col] = np.nan 

    def validate_category(self,col_name,col_values):
        self.data =self.data[self.data[col_name].isin(col_values)]

    def df_pre_processing (self):
        #check before stats
        print('\nBefore changes:')
        print(self.data.info())

        # Check for any overall null values 
        total_nulls = self.data.isna().sum().sum()
        print(f"\nTotal null values in the dataframe are : {total_nulls}")
        #remove nulls if present
        if total_nulls > 0:
            self.data.dropna(inplace=True)
            print(f"\n{total_nulls} nulls dropped.  Current nulls in dataframe: {self.data.isna().sum().sum()}")

        #remove duplicates if present
        self.data.drop_duplicates(inplace=True)
        print(f'\nDuplicates dropped.')


    def df_post_processing (self):

        #drop any new nulls or duplicates
        self.data.dropna(inplace=True)
        self.data.drop_duplicates(inplace=True)

        #check after stats
        print('\nAfter Changes:')
        print(self.data.info())    
        print(f"\nTotal null values in the dataframe are : {self.data.isna().sum().sum()}")

    def df_upload (self, table_name):
    
        #test sample
        #test_data = self.data.head()
        #self.connector.upload_to_db(test_data,table_name)

        #upload to database
        self.connector.upload_to_db(self.data, table_name)
        print(f'{self.data.shape[0]} rows uploaded to "{table_name}" table')

# only run if called directly
if __name__ == '__main__':
    runme = DataCleaning()

    #testing ----------------
    #runme.clean_user_data()
    #runme.clean_card_data()
    #runme.clean_store_data()
    #runme.clean_products_data()
    #runme.clean_orders_data()
    runme.clean_dates_data()