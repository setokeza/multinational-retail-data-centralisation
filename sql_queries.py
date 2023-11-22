#import pandas as pd
from sqlalchemy import create_engine #this Object Relational Mapper ORM will transform the python objects into SQL tables
#import psycopg2 #creates sql pipeline so we can query and load data into python in one step
from database_utils import DatabaseConnector
from sqlalchemy import text

class SqlQueries:
    def __init__(self):
        self.connector = DatabaseConnector() 
        self.engine = self.connector.init_db_engine('SD')

    def get_max_length(self,column_name,table_name):
        #create active connection with autoconnect option
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            query = text(f"SELECT MAX(CHAR_LENGTH({column_name})) FROM {table_name}")
            print(query)
            result = conn.execute(query)
            max_length = result.first()[0]
            print(max_length)
            return max_length

    def orders_table_queries (self):

        max_cn = self.get_max_length('card_number','orders_table')
        max_sc = self.get_max_length('store_code','orders_table')
        max_pc = self.get_max_length('product_code','orders_table')
        
        #create active connection with autoconnect option
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #Changing the columns datatype in orders_table
            query = text(f"ALTER TABLE orders_table\n"
                        f"ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,\n"
                        f"ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,\n"
                        f"ALTER COLUMN card_number TYPE VARCHAR({max_cn}),\n"
                        f"ALTER COLUMN store_code TYPE VARCHAR({max_sc}),\n"
                        f"ALTER COLUMN product_code TYPE VARCHAR({max_pc}),\n"
                        f"ALTER COLUMN product_quantity TYPE SMALLINT;\n"
                        )
            
            print('orders_table_alter_dtype: \n', query)
            conn.execute(query)


    def dim_users_queries (self):
        
        #create active connection with autoconnect option
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #Changing the columns datatype in dim_users_table
            query = text(f"ALTER TABLE dim_users\n"
                        f"ALTER COLUMN first_name TYPE VARCHAR(255),\n"
                        f"ALTER COLUMN last_name TYPE VARCHAR(255),\n"
                        f"ALTER COLUMN date_of_birth TYPE DATE,\n"
                        f"ALTER COLUMN country_code TYPE VARCHAR(2),\n"
                        f"ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,\n"
                        f"ALTER COLUMN join_date TYPE DATE;\n"
                        )
            
            print('dim_users_alter_dtype: \n', query)
            conn.execute(query)

    def dim_store_details_queries(self):
        
        #create active connection with autoconnect option
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #Changing the columns datatype in dim_users_table
            query = text(f"ALTER TABLE dim_store_details\n"
                        f"ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,\n"
                        f"ALTER COLUMN locality TYPE VARCHAR(255),\n"
                        f"ALTER COLUMN store_code TYPE VARCHAR(50),\n"
                        f"ALTER COLUMN staff_numbers TYPE SMALLINT,\n"
                        f"ALTER COLUMN opening_date TYPE DATE,\n"
                        f"ALTER COLUMN store_type TYPE VARCHAR(255),\n"
                        f"ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,\n"
                        f"ALTER COLUMN country_code TYPE VARCHAR(2),\n"
                        f"ALTER COLUMN continent TYPE VARCHAR(255);\n"
                        )
            
            print('dim_store_detail_queries: \n', query)
            conn.execute(query)

    def dim_products_queries(self):
    
        #create active connection with autoconnect option
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #Add weight_class column
            query = text(f"ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(14);\n"
                        f"UPDATE dim_products\n"
                        f"SET weight_class = CASE\n"
                            f"WHEN weight_kg < 2 THEN 'Light'\n"
                            f"WHEN weight_kg BETWEEN 3 AND 40 THEN 'Mid_Sized'\n"
                            f"WHEN weight_kg BETWEEN 41 AND 140 THEN 'Heavy'\n"
                            f"ELSE 'Truck_required'\n"
                        f"END;\n"
                        )
            
            print('dim_products weight class: \n', query)
            conn.execute(query)

            #Change Column name
            query = text(f"ALTER TABLE dim_products RENAME COLUMN removed TO still_available;")
            print('dim_products rename column: \n', query)
            conn.execute(query)

            #Changing columns datatypes
            query = text(f'ALTER TABLE dim_products\n'
                        f'ALTER COLUMN price_£ TYPE FLOAT,\n'
                        f'ALTER COLUMN weight_kg TYPE FLOAT,\n'
                        f'ALTER COLUMN product_code TYPE VARCHAR(50),\n'
                        f'ALTER COLUMN "EAN" TYPE VARCHAR(50),\n'
                        f'ALTER COLUMN date_added TYPE DATE,\n'
                        f'ALTER COLUMN uuid TYPE UUID USING uuid::uuid,\n'
                        f'ALTER COLUMN still_available TYPE BOOL USING\n'
                            f'CASE\n'
                                f'WHEN still_available LIKE \'Still_%\' THEN true\n'
                                f'ELSE false\n'
                            f'END;\n'
                        
                        )
            
            print('dim_products alter dtypes: \n', query)
            conn.execute(query)

    def dim_date_time_queries(self):
        
        #create active connection with autoconnect option
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #Changing the columns datatype
            query = text(f"ALTER TABLE dim_date_times\n"
                        f"ALTER COLUMN month TYPE CHAR(2),\n"
                        f"ALTER COLUMN year TYPE CHAR(4),\n"
                        f"ALTER COLUMN day TYPE CHAR(2),\n"
                        f"ALTER COLUMN time_period TYPE VARCHAR(11),\n"
                        f"ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;\n"
                        )
            
            print('dim_date_time_queries: \n', query)
            conn.execute(query)


    def dim_card_details_queries(self):
    
        #create active connection with autoconnect option
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #Changing the columns datatype
            query = text(f"ALTER TABLE dim_card_details\n"
                        f"ALTER COLUMN card_number TYPE VARCHAR(19),\n"
                        f"ALTER COLUMN expiry_date TYPE VARCHAR(5),\n"
                        f"ALTER COLUMN date_payment_confirmed TYPE DATE;\n"
                        )
            
            print('dim_card_details_queries: \n', query)
            conn.execute(query)

    def make_primary_keys(self):
    
        #create active connection with autoconnect option
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #Changing the columns datatype
            query = text(f"ALTER TABLE dim_card_details\n"
                        f"ADD CONSTRAINT PK_card_number PRIMARY KEY (card_number);\n"

                        f"ALTER TABLE dim_date_times\n"
                        f"ADD CONSTRAINT PK_date_uuid PRIMARY KEY (date_uuid);\n"

                        f"ALTER TABLE dim_store_details\n"
                        f"ADD CONSTRAINT PK_store_code PRIMARY KEY (store_code);\n"

                        f"ALTER TABLE dim_products\n"
                        f"ADD CONSTRAINT PK_product_code PRIMARY KEY (product_code);\n"

                        f"ALTER TABLE dim_users\n"
                        f"ADD CONSTRAINT PK_user_uuid PRIMARY KEY (user_uuid);\n"
                        )
            
            print('make_primary_keys_queries: \n', query)
            conn.execute(query)

    def make_foreign_keys_queries(self):
    
        #create active connection with autoconnect option
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #Changing the columns datatype
            query = text(f"ALTER TABLE orders_table\n"
                        f"ADD CONSTRAINT FK_orders_table_card_number\n"
                        f"FOREIGN KEY (card_number)\n"
                        f"REFERENCES dim_card_details(card_number);\n"

                        f"ALTER TABLE orders_table\n"
                        f"ADD CONSTRAINT FK_orders_table_date_uuid\n"
                        f"FOREIGN KEY (date_uuid)\n"
                        f"REFERENCES dim_date_times(date_uuid);\n"

                        f"DELETE FROM orders_table\n"
                        f"WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);\n"

                        f"ALTER TABLE orders_table\n"
                        f"ADD CONSTRAINT FK_orders_table_store_code\n"
                        f"FOREIGN KEY (store_code)\n"
                        f"REFERENCES dim_store_details(store_code);\n"

                        f"DELETE FROM orders_table\n"
                        f"WHERE product_code NOT IN (SELECT product_code FROM dim_products);\n"

                        f"ALTER TABLE orders_table\n"
                        f"ADD CONSTRAINT FK_orders_table_product_code\n"
                        f"FOREIGN KEY (product_code)\n"
                        f"REFERENCES dim_products(product_code);\n"

                        f"DELETE FROM orders_table\n"
                        f"WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);\n"

                        f"ALTER TABLE orders_table\n"
                        f"ADD CONSTRAINT FK_orders_table_user_uuid\n"
                        f"FOREIGN KEY (user_uuid)\n"
                        f"REFERENCES dim_users(user_uuid);\n"
                        )
            
            print('make_foreign_keys_queries: \n', query)
            conn.execute(query)

    def drop_allkeys_queries(self):
    
        #create active connection with autoconnect option
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            query = text(f"ALTER TABLE orders_table\n"
                    f"DROP CONSTRAINT FK_orders_table_card_number;\n"

                    f"ALTER TABLE orders_table\n"
                    f"DROP CONSTRAINT FK_orders_table_date_uuid;\n"

                    f"ALTER TABLE orders_table\n"
                    f"DROP CONSTRAINT FK_orders_table_store_code;\n"

                    f"ALTER TABLE orders_table\n"
                    f"DROP CONSTRAINT FK_orders_table_product_code;\n"

                    f"ALTER TABLE orders_table\n"
                    f"DROP CONSTRAINT FK_orders_table_user_uuid;\n"
                    )                        
            
            print('drop_foreign_keys_queries: \n', query)
            conn.execute(query)
        
            #Changing the columns datatype
            query = text(f"ALTER TABLE dim_card_details\n"
                        f"DROP CONSTRAINT PK_card_number;\n"

                        f"ALTER TABLE dim_date_times\n"
                        f"DROP CONSTRAINT PK_date_uuid;\n"

                        f"ALTER TABLE dim_store_details\n"
                        f"DROP CONSTRAINT PK_store_code;\n"

                        f"ALTER TABLE dim_products\n"
                        f"DROP CONSTRAINT PK_product_code;\n"

                        f"ALTER TABLE dim_users\n"
                        f"DROP CONSTRAINT PK_user_uuid;\n"
                        )

            print('drop_primary_keys_queries: \n', query)
            conn.execute(query)


# only run if called directly
if __name__ == '__main__':
    runme = SqlQueries()
    #runme.get_max_length('card_number','orders_table')
    #runme.orders_table_queries()
    #runme.dim_users_queries()
    #runme.dim_store_details_queries()
    #runme.dim_products_queries()
    #runme.dim_date_time_queries()
    #runme.dim_card_details_queries()
    #runme.make_primary_keys()
    #runme.make_foreign_keys_queries()
    #runme.drop_allkeys_queries()




