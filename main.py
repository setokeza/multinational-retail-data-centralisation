from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from sql_queries import SqlQueries
from bi_queries import BIQueries

import pandas as pd
import requests

if __name__ == "__main__":
    '''
    Extract, clean and upload data to sales_data tables.
    Create database schema.
    Show database schema.
    Run business intelligence reports.
    '''

    # Create classes for database connector, extractor, cleaner, sql engine and bi engine
    connector = DatabaseConnector('db_creds.yaml')
    extractor = DataExtractor()
    cleaner = DataCleaning()
    schema_engine = SqlQueries()
    bi_engine = BIQueries()

    #create database engines
    extractor_engine = connector.init_db_engine('RDS')
    upload_engine = connector.init_db_engine('SD')

    # Run SQL to drop all database constraints, so we can over-write tables in the database
    schema_engine.run_queries(upload_engine,'1')

    # Extract, clean and upload user data
    user_df = extractor.read_rds_table(connector,'legacy_users')
    user_df = cleaner.clean_user_data(user_df)
    connector.upload_to_db(upload_engine, user_df, 'dim_users')

    # Extract, clean and upload card data
    pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_df = extractor.retrieve_pdf_data(pdf_link)
    card_df = cleaner.clean_card_data(card_df)
    connector.upload_to_db(upload_engine, card_df, 'dim_card_details')

    # Extract, clean and upload stores data
    # extract number of stores
    endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    num_stores = extractor.list_number_of_stores(endpoint, header)
    
    # extract, clean and upload stores data
    endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    header_dict = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    store_df = extractor.retrieve_stores_data(endpoint, num_stores, header_dict)
    store_df = cleaner.clean_store_data(store_df)
    connector.upload_to_db(upload_engine, store_df, 'dim_store_details')

    # Extract, clean and upload products data
    file_path = '/Users/shaha/Documents/AiCore/Downloads/products.csv'
    bucket = 'data-handling-public'
    object = 'products.csv'

    products_df = extractor.extract_from_s3(file_path, bucket, object)
    products_df = cleaner.clean_products_data(products_df)
    connector.upload_to_db(upload_engine, products_df, 'dim_products')

    # Extract, clean and upload orders data
    orders_df = extractor.read_rds_table(connector,'orders_table')
    orders_df = cleaner.clean_orders_data(orders_df)
    connector.upload_to_db(upload_engine, orders_df, 'orders_table') 

    # Extract, clean and upload orders data
    api_fetch = requests.get('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    dates_df = pd.DataFrame(api_fetch.json())
    dates_df = cleaner.clean_dates_data(dates_df)
    connector.upload_to_db(upload_engine, dates_df, 'dim_date_times') 

    # Run SQL to create the database schema
    schema_engine.run_queries(upload_engine,'23')

    # Run SQL to show the database schema 
    schema_engine.run_queries(upload_engine,'4')

    # Run Business intelligence reports
    bi_engine.business_intelligence_reports(upload_engine,'123456789', show_queries=False)



