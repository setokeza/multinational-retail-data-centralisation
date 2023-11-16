# multinational-retail-data-centralisation

# Milestone 2 : Extract and clean the data from the data source
## Task 1
### Set up a new database to store the data
- Step 1. Initialise a new database locally to store the extracted data.
- Step 2. Set up a new database within pgadmin4 and name it sales_data.
- Step 3. This database will store all the company information once yout extract it for the various data sources.

## Task 2
### Initialise the three project Classes.
- Step 1. Creat a new Python script named data_extraction.py and within it, create a class named DataExtractor.
- Step 2. Create a class DatabaseConnector which you will use to connect with and upload data to the database.
- Step 3: Create a script contain a class DaraCleaning with methods to clean data from each of the data sources.

## Task 3
### Extract and clean the user data.
- Step 1. Create a .yaml file containing the database credentials.Develop methods in your DatabaseConnector class to extract the data from the database.
- Step 2. Create a method read_db_creds - this will read the credentials yaml file and return a dictionary of the credentials.
- Step 3. Create a method init_dv_engine which will read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine.
- Step 4. Using the engine from init_db_engine create a method list_db_tables to list all the tables in the database so you know which tables you can extract data from. Develop a method inside your DataExtractor class to read the data from the RDS database.
- Step 5. Develop a method called read_rds_table in your DataExtractor class which will extract the database table to a pandas DataFrame. It will take in an instance of your DatabaseConnector class and the table name as an argument and return a pandas DataFrame. Use your list_db_tables method to get the name of the table containing user data. Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.
- Step 6. Create a method called clean_user_data in the DataCleaning class which will perform the cleaning of the user data. You will need clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.
- Step 7. Now create a method in your DatabaseConnector class called upload_to_db. This method will take in a Pandas DataFrame and table name to upload to as an argument.
- Step 8. Once extracted and cleaned use the upload_to_db method to store the data in your sales_data database in a table named dim_users.

## Task 4
### Extracting users and cleaning card details.
- Step 1. Install the Python package tabula-py this will help you to extract data from a pdf document.
- Step 2. Create a method in your DataExtractor class called retrieve_pdf_data, which takes in a link as an argument and returns a pandas DataFrame.
- Step 3. Create a method called clean_card_data in your DataCleaning class to clean the data to remove any erroneous values, NULL values or errors with formatting.
- Step 4. Once cleaned, upload the table with your upload_to_db method to the database in a table called dim_card_details.

## Task 5
### Extract and clean the details of each store.
The store data can be retrieved through the use of an API.  The API has two GET methods. One will return the number of stores in the business and the other to retrieve a store given a store number.  To connect to the API you will need to include the API key to connect to the API in the method header.
- Step 1. Create a method in your DataExtractor class called list_number_of_stores which returns the number of stores to extract. It should take in the number of stores endpoint and header dictionary as an argument.
- Step 2. Now that you know how many stores need to be extracted from the API.
- Step 3. Create another method retrieve_stores_data which will take the retrieve a store endpoint as an argument and extracts all the stores from the API saving them in a pandas DataFrame.
- Step 4. Create a method in the DataCleaning class called_clean_store_data which cleans the data retrieve from the API and returns a pandas DataFrame.
- Step 5. Upload your DataFrame to the database using the upload_to_db method storing it in the table dim_store_details.

## Task 6
### The information for each product the company currently sells is stored in CSV format in an S3 bucket on AWS.

## Task 7
### Retrieve and clean the orders table.
This table which acts as the single source of truth for all orders the company has made in the past is stored in a database on AWS RDS.
- Step 1. Using the database table listing methods you created earlier list_db_tables, list all the tables in the database to get the name of the table containing all information about the product orders.
- Step 2. Extract the orders data using the read_rds_table method you create earlier returning a pandas DataFrame.
- Step 3. Create a method in DataCleaning called clean_orders_data which will clean the orders table data.

## Task 8
### Retrieve and clean the data events data.
The final source of data is a JSON file containing the details of when each sale happened, as well as related attributes.

## Task 9
Update the latest code changes to GitHub
