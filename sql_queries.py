from database_utils import DatabaseConnector
from sqlalchemy import create_engine 
from sqlalchemy import text

class SqlQueries:
    '''
    Create database schema for 'sales_data' database.

    Query groups:
        1. drop primary & foreign keys
        2. change datatypes on all tables
        3. add primary & foreign keys
        4. show specified database schema:
            1. show row count
            2. show column_name, data_type, character_maximum_length
            3. show table constraints

    Parameters:
    - None.

    Attributes:
    - None

    Methods:
    -------

    get_max_length(sql_engine, column_name, table_name):
        Returns max length of values in given table, column
    orders_table_queries (sql_engine):
        Changes columns datatype in orders_table
    dim_users_queries (sql_engine):
        Changes columns datatype in dim_users table
    dim_store_details_queries(sql_engine):
        Changes columns datatype in store_details table 
    dim_products_queries(self, sql_engine):
        Add weight_class column;
        Rename column 'removed' to 'still_available';
        Change columns datatype in dim_products table.
    dim_date_time_queries(sql_engine):
        Change columns datatype for dim_date_times table            
    dim_card_details_queries(self, sql_engine):
        Change columns datatype for dim_card_details table
    make_primary_keys(sql_engine):
        Set primary key constraints for all tables
    make_foreign_keys_queries(sql_engine):
        Set foreign key constraints for all tables.
    drop_foreign_keys_queries(sql_engine):
        Drop foreign key constraints for all tables.
    drop_primary_keys_queries(sql_engine):
        Drop primay key constraints for all tables.
    row_counts_query(table):
        Query to retrieve row count for a given table.
    table_info_query(table):
        Query to retrieve column_name, data_type, character_maximum_length for a given table.
    constraints_query(table):
        Query to retrieve constraints for a given table.
    table_schema_queries(sql_engine, switch):
        Use (switch) to show required schema info.
    run_queries (sql_engine, switch):
        Choose which query groups to run using (switch)

    '''
    def __init__(self):
        pass

    def get_max_length(self, sql_engine, column_name, table_name):
        '''
        Get max length of values in given column

        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.
        - column_name (str): column to check
        - table_name (str): table to check

        Returns:
        - max_length(str): max length of values in column
        '''
        #create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #get max length of values in given column
            query = text(f"SELECT MAX(CHAR_LENGTH({column_name})) FROM {table_name}")
            result = conn.execute(query)
            max_length = result.first()[0]

        return max_length

    def orders_table_queries (self, sql_engine):
        '''
        Change columns datatype in orders_table.

        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.
        '''
            
        #create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            # change columns datatype in orders_table
            query = text(f"ALTER TABLE orders_table\n"
                        f"ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,\n"
                        f"ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,\n"
                        f"ALTER COLUMN card_number TYPE VARCHAR(50),\n"
                        f"ALTER COLUMN store_code TYPE VARCHAR(50),\n"
                        f"ALTER COLUMN product_code TYPE VARCHAR(50),\n"
                        f"ALTER COLUMN product_quantity TYPE SMALLINT;\n"                        
                        )
            
            conn.execute(query)
            
            # query table    
            max_cn = self.get_max_length(sql_engine, 'card_number','orders_table')
            max_sc = self.get_max_length(sql_engine, 'store_code','orders_table')
            max_pc = self.get_max_length(sql_engine, 'product_code','orders_table')
            
            # reset column types
            query = text(f"ALTER TABLE orders_table\n"
                        f"ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,\n"
                        f"ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,\n"
                        f"ALTER COLUMN card_number TYPE VARCHAR({max_cn}),\n"
                        f"ALTER COLUMN store_code TYPE VARCHAR({max_sc}),\n"
                        f"ALTER COLUMN product_code TYPE VARCHAR({max_pc}),\n"
                        f"ALTER COLUMN product_quantity TYPE SMALLINT;\n"
                        )
            
            print('\nUpdated columns datatype on orders_table.')
            conn.execute(query)


    def dim_users_queries (self, sql_engine):
        '''
        Change columns datatype in dim_users table.

        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.
        '''
        
        #create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #Change the columns datatype in dim_users_table
            query = text(f"ALTER TABLE dim_users\n"
                        f"ALTER COLUMN first_name TYPE VARCHAR(255),\n"
                        f"ALTER COLUMN last_name TYPE VARCHAR(255),\n"
                        f"ALTER COLUMN date_of_birth TYPE DATE,\n"
                        f"ALTER COLUMN country_code TYPE VARCHAR(2),\n"
                        f"ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,\n"
                        f"ALTER COLUMN join_date TYPE DATE;\n"
                        )
            
            print('\nUpdated columns datatype on dim_users.')
            conn.execute(query)

    def dim_store_details_queries(self, sql_engine):
        '''
        Change columns datatype in store_details table 

        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.
        '''
        
        # create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            # change columns datatype in store_details table
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
            
            print('\nUpdated columns datatype on dim_store_details.')
            conn.execute(query)

    def dim_products_queries(self, sql_engine):
        '''
        Add weight_class column;
        Rename column 'removed' to 'still_available';
        Change columns datatype in dim_products table.

        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.        
        '''
    
        # create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            # add weight_class column
            query = text(f"ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(14);\n"
                        f"UPDATE dim_products\n"
                        f"SET weight_class = CASE\n"
                            f"WHEN weight_kg < 2 THEN 'Light'\n"
                            f"WHEN weight_kg BETWEEN 3 AND 40 THEN 'Mid_Sized'\n"
                            f"WHEN weight_kg BETWEEN 41 AND 140 THEN 'Heavy'\n"
                            f"ELSE 'Truck_required'\n"
                        f"END;\n"
                        )
            
            print('\nAdded weight_class column on dim_products.')
            conn.execute(query)

            # rename column 'removed' to 'still_available'
            query = text(f"ALTER TABLE dim_products RENAME COLUMN removed TO still_available;")
            print('\nRenamed column on dim_products.')
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
            
            print('\nUpdated column datatypes on dim_products.')
            conn.execute(query)

    def dim_date_time_queries(self, sql_engine):
        '''
        Change columns datatype for dim_date_times table

        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.        
        '''
        
        #create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            # change columns datatype for dim_date_times table
            query = text(f"ALTER TABLE dim_date_times\n"
                        f"ALTER COLUMN month TYPE CHAR(2),\n"
                        f"ALTER COLUMN year TYPE CHAR(4),\n"
                        f"ALTER COLUMN day TYPE CHAR(2),\n"
                        f"ALTER COLUMN time_period TYPE VARCHAR(11),\n"
                        f"ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;\n"
                        )
            
            print('\nUpdated columns datatype on dim_date_times.')
            conn.execute(query)


    def dim_card_details_queries(self, sql_engine):
        '''
        Change columns datatype for dim_card_details table
        
        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.        
        '''
        #create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #Changing the columns datatype
            query = text(f"ALTER TABLE dim_card_details\n"
                        f"ALTER COLUMN card_number TYPE VARCHAR(19),\n"
                        f"ALTER COLUMN expiry_date TYPE VARCHAR(5),\n"
                        f"ALTER COLUMN date_payment_confirmed TYPE DATE;\n"
                        )
            
            print('\nUpdated columns datatype on dim_card_details.')
            conn.execute(query)

    def make_primary_keys(self, sql_engine):
        '''
        Set primary key constraints for all tables.
        
        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.        
        '''
    
        #create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #Adding primary keys
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
            
            print('\nAdded primary keys to all tables.')
            conn.execute(query)

    def make_foreign_keys_queries(self, sql_engine):
        '''
        Set foreign key constraints for all tables.
        
        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.        
        '''
    
        #create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #Adding foreign keys
            query = text(f"ALTER TABLE orders_table\n"
                        f"ADD CONSTRAINT FK_orders_table_card_number\n"
                        f"FOREIGN KEY (card_number)\n"
                        f"REFERENCES dim_card_details(card_number);\n"

                        f"ALTER TABLE orders_table\n"
                        f"ADD CONSTRAINT FK_orders_table_date_uuid\n"
                        f"FOREIGN KEY (date_uuid)\n"
                        f"REFERENCES dim_date_times(date_uuid);\n"

                        f"ALTER TABLE orders_table\n"
                        f"ADD CONSTRAINT FK_orders_table_store_code\n"
                        f"FOREIGN KEY (store_code)\n"
                        f"REFERENCES dim_store_details(store_code);\n"

                        f"ALTER TABLE orders_table\n"
                        f"ADD CONSTRAINT FK_orders_table_product_code\n"
                        f"FOREIGN KEY (product_code)\n"
                        f"REFERENCES dim_products(product_code);\n"

                        f"ALTER TABLE orders_table\n"
                        f"ADD CONSTRAINT FK_orders_table_user_uuid\n"
                        f"FOREIGN KEY (user_uuid)\n"
                        f"REFERENCES dim_users(user_uuid);\n"
                        )
            
            print('\nAdded foreign keys from orders table to all tables.')
            conn.execute(query)


    def drop_foreign_keys_queries(self, sql_engine):
        '''
        Drop foreign key constraints for all tables.
        
        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.        
        '''
        #create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            #dropping foreign keys
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
            
            print('\nDropped foreign keys from all tables.')
            conn.execute(query)

    def drop_primary_keys_queries(self, sql_engine):
        '''
        Drop primay key constraints for all tables.
        
        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.        
        '''

        #create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        
            #Dropping primary keys
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

            print('\nDropped primary keys from all tables.')
            conn.execute(query)

    def row_count_query(self, table):
        '''
        Returns query that counts the number of rows in (table).

        Parameters:
        - table (str): table to insert in query      

        Returns:
        - query (str): query.
        '''
        query = text(f"SELECT COUNT(*)\n"
                    f"FROM {table};\n"
                    )
        
        return query

    def table_info_query (self, table):
        '''
        Returns query for table information to show:
            column_name, data_type, character_maximum_length

        Parameters:
        - table (str): table to insert in query      

        Returns:
        - query (str): query.
        '''
        query = text(f"SELECT column_name, data_type, character_maximum_length\n"
                f"FROM information_schema.columns\n"
                f"WHERE table_name = '{table}';\n"
                )
        
        return query

    def constraints_query(self, table):
        '''
        Returns query that lists constraints for given table.

        Parameters:
        - table (str): table to insert in query      

        Returns:
        - query (str): query.
        '''
        query = text(f"SELECT conname AS constraint_name,\n"
                    f"contype AS constraint_type\n"
                    f"FROM pg_catalog.pg_constraint cons\n"
                    f"JOIN pg_catalog.pg_class t ON t.oid = cons.conrelid\n"
                    f"WHERE t.relname = '{table}';\n"
                    )
        
        return query
        
    def show_table_schema (self, sql_engine, table_list, switch=''):
        '''
        Show table schema for given tables.

        Use (switch) to choose queries:
            1. show row count
            2. show column_name, data_type, character_maximum_length
            3. show table constraints

        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.
        - table_list (list): list of tables to query
        - switch (str): select which queries to run.  
        '''
        #create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        
            if '1' in switch:
                for table in table_list:
                    # show row counts
                    query = self.row_count_query(table)
                    results = conn.execute(query)
                    print('\nrow_count_query(): number of rows for', table,':')
                    for row in results:
                        print(row)

            if '2' in switch:
                for table in table_list:
                    # show column_name, data_type, character_maximum_length
                    query = self.table_info_query(table)
                    results = conn.execute(query)
                    print('\ntable_info_query(): column_name, data_type, character_maximum_length', table,':')
                    for row in results:
                        print(row)

            if '3' in switch:
                for table in table_list:
                    # show constraints
                    query = self.constraints_query(table)
                    results = conn.execute(query)
                    print('\nconstraints_query(): constraints for ', table,':')
                    for row in results:
                        print(row)

    def run_queries (self,sql_engine, switch):
        '''
        Choose which query groups to run using (switch):
            Query groups:
            1. drop primary & foreign keys
            2. change datatypes on all tables
            3. add primary & foreign keys to all tables
            4. show database schema 
        
        Parameters:
        - sql_engine (sqlalchemy engine): database to connect to.
        - switch (str): select which queries to run.         
        '''

        if '1' in switch:
            self.drop_foreign_keys_queries(sql_engine)
            self.drop_primary_keys_queries(sql_engine)

        if '2' in switch:
            self.dim_users_queries(sql_engine)
            self.dim_store_details_queries(sql_engine)
            self.dim_products_queries(sql_engine)
            self.dim_date_time_queries(sql_engine)
            self.dim_card_details_queries(sql_engine)
            self.orders_table_queries(sql_engine)

        if '3' in switch:
            self.make_primary_keys(sql_engine)
            self.make_foreign_keys_queries(sql_engine)

        if '4' in switch:
            table_list = ['orders_table', 'dim_users','dim_card_details','dim_products','dim_store_details','dim_date_times']
            self.show_table_schema(sql_engine, table_list,'123')


# only run if called directly
if __name__ == '__main__':
    connector = DatabaseConnector('db_creds.yaml') 
    sql_engine = connector.init_db_engine('SD')
    runme = SqlQueries()
    runme.run_queries(sql_engine, '4')





