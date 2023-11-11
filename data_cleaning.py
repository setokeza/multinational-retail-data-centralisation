import pandas as pd
import numpy as np # We will need the `nan` constant from the numpy library to apply to missing values
import re
import pycountry

from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:
    def __init__(self):
        self.connector = DatabaseConnector()
        self.extractor = DataExtractor()
        self.data = {}
        
    
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

        #check before stats
        print('Before changes:')
        print(self.data.info())
        print('total nulls: ',self.data.isna().sum())


        #set dtypes for objects
        string_list = ['first_name','last_name','company','email_address','address','country','country_code','phone_number','user_uuid']
        for column_name in string_list:   
            self.data[column_name] = self.data[column_name].astype('string')
            
        # convert object to datetime, replace non-parseable values with NaN
        date_list = ['date_of_birth','join_date']
        for column_name in date_list:
            self.data[column_name] = self.data[column_name].apply(pd.to_datetime, errors='coerce')

        #validate emails, replace errors with Nan
        email_regex = r"^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$" 
        # For every row  where the email column does not match our regular expression, replace the value with NaN
        self.data.loc[~self.data['email_address'].str.match(email_regex), 'email_address'] = np.nan 
        
        #validate phone numbers, replace errors with Nan
        phone_regex = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' 
        # For every row  where the phone_number column does not match our regular expression, replace the value with NaN
        self.data.loc[~self.data['phone_number'].str.match(phone_regex), 'phone_number'] = np.nan 
        
        #country and country_code
        #TODO

        #remove nulls
        self.data.dropna(inplace=True)

        #remove duplicates
        self.data.drop_duplicates(inplace=True)

        #check after stats
        print('After Changes:')
        print(self.data.info())    
        print('total nulls: ',self.data.isna().sum())

        #test sample
        #test_data = self.data.head()
        #self.connector.upload_to_db(test_data,'dim_users')

        #upload to database
        self.connector.upload_to_db(self.data,'dim_users')

# only run if called directly
if __name__ == '__main__':
    runme = DataCleaning()

    #testing ----------------
    #runme.clean_user_data()