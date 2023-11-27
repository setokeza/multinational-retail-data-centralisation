from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from numpy import nan

import requests
import pandas as pd
import numpy as np 


class DataCleaning():
    '''
    Extract, clean and return dataframes.

    Parameters:
    - None.

    Attributes:
    - None

    Methods:
    -------

    clean_user_data(user_df):
        Clean user-related data (user_df).
    clean_card_data(self, card_df):
        Clean user-related data (card_df).
    clean_store_data(store_df):
        Clean store-related data (store_df).        
    convert_product_weights (self, products_df):
        Convert product weights to kilograms in the DataFrame (products_df)
    clean_products_data (products_df):
        Clean products-related data (products_df)
    clean_orders_data(orders_df):
        Clean orders-related data (orders_df)
    clean_dates_data(dates_df):
        Clean dates-related data (dates_df).
    convert_to_type (df, to_type, column_list):
        Converts dtypes for a dataframe (df), given the type to convert to (to_type)
        and the columns (column_list) to apply the dtype to.
    validate_email(email_col):
        For every row  where the (email_col) does not match our regular expression, replace the value with NaN
    validate_phone(phone_col):
        For every row  where the (phone_col) column does not match our regular expression, replace the value with NaN
    test_DataExtractor (switch=''):
        Allows internal testing of this class.  
        The methods to run can be specified using (switch).
    '''

    def __init__(self): 
        pass
        
    def clean_user_data(self, user_df):
        '''
        Clean user-related data.

        Parameters:
        - user_df (pd.DataFrame): specifically cleans the 'legacy_users' table dataframe.

        Returns:
        - pd.DataFrame: returns cleaned dataframe.

        '''   
        print(f'\nclean_user_data(): cleaning ', len(user_df), ' dataframe rows')

        #convert dtypes
        user_df = self.convert_to_type(user_df,'string',['first_name','last_name','company','email_address','address','phone_number','user_uuid'])
        user_df = self.convert_to_type(user_df, 'category',['country', 'country_code'])
        user_df = self.convert_to_type(user_df, 'date', ['date_of_birth','join_date'])
        
        #validate phone numbers
        user_df.loc[:,'phone_number'] = user_df['phone_number'].str.replace('(0)', '', regex=False)
        user_df.loc[:,'phone_number'] = user_df['phone_number'].replace(r'\D+', '', regex=True)

        #data specific cleanups
        user_df.loc[:,'country_code'] = user_df['country_code'].str.replace('GGB', 'GB', regex=False)
        
        #drop duplicates
        user_df = user_df.dropna().drop_duplicates()
        print(f'\nclean_user_data(): {len(user_df)} rows cleaned & returned')

        return user_df

    def clean_card_data(self, card_df):
        '''
        Clean card-related data.

        Parameters:
        - card_df (pd.DataFrame): specifically cleans the 'card_details.pdf' dataframe.

        Returns:
        - pd.DataFrame: returns cleaned dataframe.

        '''     
        print(f'\nclean_card_data(): cleaning ', len(card_df), ' dataframe rows') 

        #convert dtypes
        card_df = self.convert_to_type(card_df, 'string',['card_number','expiry_date'])
        card_df = self.convert_to_type(card_df, 'category',['card_provider'])
        card_df = self.convert_to_type(card_df, 'date', ['date_payment_confirmed'])

        # Checking all the unique category values
        #print(card_df['card_provider'].unique())
        
        # Removing any unique category values that are erroneous
        valid_cards_list = ['Diners Club / Carte Blanche','American Express','JCB 16 digit','JCB 15 digit','Maestro', 'Mastercard','Discover','VISA 19 digit', 'VISA 16 digit','VISA 13 digit']       
        card_df =card_df[card_df['card_provider'].isin(valid_cards_list)]

        # Removing symbols from card_number & cast as int
        card_df.loc[:,'card_number'] = card_df['card_number'].str.replace('[^a-zA-Z0-9\s]', '', regex=True) 
        card_df = self.convert_to_type(card_df,'int64', ['card_number'] )

        print(f'\nclean_card_data(): {len(card_df)} rows cleaned & returned')

        return card_df

    def clean_store_data(self,store_df):
        '''
        Clean store-related data.

        Parameters:
        - store_df (pd.DataFrame): specifically cleans the store details dataframe from an amazon api call.

        Returns:
        - pd.DataFrame: returns cleaned dataframe.

        '''          
        print(f'\nclean_store_data(): cleaning ', len(store_df), ' dataframe rows') 

        store_df = store_df.set_index(['index'])
        store_df.loc[:,'continent'] = store_df['continent'].str.replace('eeEurope', 'Europe', regex=False)
        store_df.loc[:,'continent'] = store_df['continent'].str.replace('eeAmerica', 'America', regex=False)
        store_df = store_df[store_df['continent'].isin(['Europe', 'America'])]

        store_df.loc[:,'opening_date'] = store_df['opening_date'].apply(pd.to_datetime, errors='ignore')
        store_df.loc[:,'staff_numbers'] = store_df['staff_numbers'].replace({r'J': '', r'e': '', r'R': '', r'A': '', r'n': ''}, regex=True)
        store_df.loc[:,'staff_numbers'] = store_df['staff_numbers'].astype('int64')
        store_df = store_df.drop(columns=['lat'])
        
        #remove strings from float columns
        store_df.loc[:,'longitude'] = store_df['longitude'].replace({r'N/A': np.NaN}, regex=True)
        store_df.loc[:,'latitude'] = store_df['latitude'].replace({r'N/A': np.NAN}, regex=True)
        #update country_code to empty string for web-portal
        store_df.loc[store_df['store_code'] == 'WEB-1388012W', 'country_code'] = np.NaN

        #convert dtypes'
        store_df = self.convert_to_type(store_df, 'string', ['address','locality','store_code','store_type','country_code','continent'])
        store_df = self.convert_to_type(store_df, 'category', ['longitude', 'latitude'])
        
        print(f'\nclean_store_data(): {len(store_df)} rows cleaned & returned')

        return store_df
    
    def convert_product_weights (self, products_df):
        '''
        Convert product weights to kilograms in the DataFrame.

        Parameters:
        - products_df (pd.DataFrame):  takes a products dataframe.

        Returns:
        - pd.DataFrame: returns converted to kg dataframe.

        '''     
        products_df.loc[:,'weight'] = products_df['weight'].replace({'16oz': '0.454', '77g .': '77g'})
        products_df.loc[:,'weight'] = products_df['weight'].replace({r'kg': '', r'g': '/1000', r'ml': '/1000', r'x': '*'}, regex=True)
        products_df.loc[:,'weight'] = pd.to_numeric(products_df['weight'], errors='coerce')
        products_df.loc[:,'weight'] = products_df['weight'].apply(lambda x: eval(str(x)))

        products_df = products_df.rename(columns={'weight': 'weight_kg'})

        return products_df

    
    def clean_products_data (self, products_df):
        '''
        Clean products-related data.

        Parameters:
        - products_df (pd.DataFrame): specifically cleans the products.csv dataframe from an amazon api.

        Returns:
        - pd.DataFrame: returns cleaned dataframe.

        '''           
        print(f'\nclean_products_data(): cleaning ', len(products_df), ' dataframe rows') 

        #convert_product_weights
        products_df = self.convert_product_weights(products_df)
        
        #data specific cleaning
        products_df = products_df.rename(columns={'Unnamed: 0': 'index'})
        products_df = products_df.set_index(['index'])
        categories = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 'food-and-drink', 'diy']
        products_df = products_df[products_df['category'].isin(categories)]
        products_df.loc[:,'product_price'] = products_df['product_price'].str.replace('£', '', regex=False)
        products_df = products_df.rename(columns={'product_price': 'price_£'})

        #convert dtypes
        products_df = self.convert_to_type(products_df, 'date', ['date_added'])
        products_df = self.convert_to_type(products_df, 'int64', ['EAN'])
        products_df = self.convert_to_type(products_df, 'string', ['product_name','category','uuid','removed','product_code'])
        products_df = self.convert_to_type(products_df, 'category', ['category'])
        products_df = self.convert_to_type(products_df, 'float', ['price_£'])

        print(f'\nclean_products_data(): {len(products_df)} rows cleaned & returned')

        return products_df

    def clean_orders_data(self, orders_df):
        '''
        Clean orders-related data.  

        Parameters:
        - orders_df (pd.DataFrame): specifically cleans the 'orders_table' dataframe.

        Returns:
        - pd.DataFrame: returns cleaned dataframe.
        '''
     
        print(f'\nclean_orders_data(): cleaning ', len(orders_df), ' dataframe rows') 

        #drop unnecessary columns
        orders_df = orders_df.drop(columns=['level_0', 'first_name', 'last_name', '1'])

        #convert dtypes
        orders_df = self.convert_to_type(orders_df, 'int64',['card_number','product_quantity'])
        orders_df = self.convert_to_type(orders_df, 'string', ['store_code',['product_code','date_uuid','user_uuid']])

        print(f'\nclean_orders_data(): {len(orders_df)} rows cleaned & returned')

        return orders_df

    def clean_dates_data(self, dates_df):
        '''
        Clean dates-related data.  

        Parameters:
        - dates_df (pd.DataFrame): specifically cleans the 'date_details' dataframe from amazon.

        Returns:
        - pd.DataFrame: returns cleaned dataframe.
        '''

        print(f'\nclean_dates_data(): cleaning ', len(dates_df), ' dataframe rows') 
        
        #data specific cleaning
        #print('\nUnique values in time_period: ',dates_df['time_period'].unique(),'\n')
        dates_df = dates_df[dates_df['time_period'].isin(['Evening', 'Morning', 'Midday', 'Late_Hours'])]

        #convert dtypes
        dates_df = self.convert_to_type(dates_df, 'category',['time_period'])
        dates_df = self.convert_to_type(dates_df, 'string', ['date_uuid'])
 
        print(f'\nclean_dates_data(): {len(dates_df)} rows cleaned & returned')

        return dates_df


    def convert_to_type (self, df, to_type, column_list):
        '''
        Converts dtypes for a dataframe (df), given the type to convert to (to_type)
        and the columns (column_list) to apply the dtype to.  

        Parameters:
        - df (pd.DataFrame): dataframe to convert
        - to_type (str): dtype to convert to
        - column_list (list): the columns to which to apply the dtype

        Returns:
        - pd.DataFrame: returns converted dataframe.
        '''
        simple_convert = ['string', 'category','int64']
        for column_name in column_list:
            if to_type in simple_convert:   
                df.loc[:,column_name] = df[column_name].astype(to_type)
            elif to_type == 'date':
                # convert object to datetime, replace non-parseable values with NaN
                df.loc[:,column_name] = df[column_name].apply(pd.to_datetime, errors='coerce')
            elif to_type == 'float':
                df.loc[:,column_name] = pd.to_numeric(df[column_name], errors='coerce')
            
        return df

    def validate_email(self, email_col):
        '''
        For every row  where the email column does not match our regular expression, replace the value with NaN
        '''
        email_regex = r"^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$" 
        self.data.loc[~self.data[email_col].str.match(email_regex), email_col] = np.nan 

    def validate_phone(self, phone_col):
        '''
        For every row  where the phone_number column does not match our regular expression, replace the value with NaN
        '''
        phone_regex = '^(\(?\+?[0-9]*\)?)?[0-9_\- \(\)]*$'
        self.data.loc[~self.data[phone_col].str.match(phone_regex), phone_col] = np.nan 

    def test_DataCleaning(self, switch=''):
        '''
        Allows testing of class methods.

        Parameters:
        - switch (str): Identifies the methods to run:
            1. extract, clean and upload user data
            2. extract, clean and upload card data
            3. extract, clean and upload stores data
            4. extract, clean and upload products data
            5. extract, clean and upload orders data
            6. extract, clean and upload dates data
        '''

        if '1' in switch:
            # extract, clean and upload user data
            user_df = extractor.read_rds_table(connector,'legacy_users')
            user_df = runme.clean_user_data(user_df)
            print('test_DataCleaning(): ',len(user_df))
            connector.upload_to_db(upload_engine, user_df, 'dim_users')

        if '2' in switch:
            # extract, clean and upload card data
            pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
            card_df = extractor.retrieve_pdf_data(pdf_link)
            card_df = runme.clean_card_data(card_df)
            print('test_DataCleaning(): ',len(card_df))
            connector.upload_to_db(upload_engine, card_df, 'dim_card_details')

        if '3' in switch:
            # extract number of stores
            endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
            header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
            num_stores = extractor.list_number_of_stores(endpoint,header)
            
            # extract, clean and upload stores data
            endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
            header_dict = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
            store_df = extractor.retrieve_stores_data(endpoint, num_stores, header_dict)
            store_df = runme.clean_store_data(store_df)
            print('test_DataCleaning(): ',len(store_df))
            connector.upload_to_db(upload_engine, store_df, 'dim_store_details')

        if '4' in switch:
            #  extract, clean and upload products data
            file_path = '/Users/shaha/Documents/AiCore/Downloads/products.csv'
            bucket = 'data-handling-public'
            object = 'products.csv'
            products_df = extractor.extract_from_s3(file_path, bucket, object)
            products_df = runme.clean_products_data(products_df)
            print('test_DataCleaning(): ',len(products_df))
            connector.upload_to_db(upload_engine, products_df, 'dim_products')

        if '5' in switch:
            #  extract, clean and upload orders data
            orders_df = extractor.read_rds_table(connector,'orders_table')
            orders_df = runme.clean_orders_data(orders_df)
            print('test_DataCleaning(): ',len(orders_df))
            connector.upload_to_db(upload_engine, orders_df, 'orders_table') 

        if '6' in switch:
            #  extract, clean and upload dates data
            api_fetch = requests.get('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
            dates_df = pd.DataFrame(api_fetch.json())
            dates_df = runme.clean_dates_data(dates_df)
            print('test_DataCleaning(): ',len(dates_df))
            connector.upload_to_db(upload_engine, dates_df, 'dim_date_times')          

# only run if called directly
if __name__ == '__main__':
    #internal code to test class methods
    connector = DatabaseConnector('db_creds.yaml')
    extractor = DataExtractor()
    runme = DataCleaning()
    upload_engine = connector.init_db_engine('SD')
    runme.test_DataCleaning('0')
