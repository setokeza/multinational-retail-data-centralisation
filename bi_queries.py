from database_utils import DatabaseConnector
from sqlalchemy import create_engine 
from sqlalchemy import text

class BIQueries:
    '''
    Creates reports for the sales_data database.
        1. How many stores does the business have and in which countries?
        2. Which locations currently have the most stores?
        3. Which months produce the most sales?
        4. How many sales are coming from online?
        5. What percentage of sales comes through from each type of store?
        6. Which month in which year produced the most sales?
        7. What is our staff headcount?
        8. Which German Store Type is selling the most?
        9. How quickly is the company making sales?

    Parameters:
    - None.

    Attributes:
    - None

    Methods:
    -------

    business_intelligence_reports(sql_engine, switch, show_queries=False):
        - sql_engine(sqlalchemy engine): database to connect to
        - switch (str): choose which reports to run.
        - show_queries (bool): show the sql queries that are being run.
    '''

    def __init__(self):
        pass

    def business_intelligence_reports(self, sql_engine, switch, show_queries=False):
        '''
        Creates reports for the sales_data database:
        1. How many stores does the business have and in which countries?
        2. Which locations currently have the most stores?
        3. Which months produce the most sales?
        4. How many sales are coming from online?
        5. What percentage of sales comes through from each type of store?
        6. Which month in which year produced the most sales?
        7. What is our staff headcount?
        8. Which German Store Type is selling the most?
        9. How quickly is the company making sales?

        Parameters:
        ----------

        -switch(str):  Choose which reports to run by passing report number. To run all reports use '123456789'. 
        -show_queries(bool): set to True to show the sql queries that are being run.

        '''

        #create active connection with autoconnect option
        with sql_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:

            if '1' in switch:
                # How many stores does the business have and in which countries?
                query = text(f"SELECT country_code AS country, COUNT(*) AS total_no_stores\n"
                            f"FROM dim_store_details\n"
                            f"GROUP BY country_code\n"
                            f"ORDER BY total_no_stores DESC;\n"
                            )
                
                result = conn.execute(query)
                
                print('\nHow many stores does the business have and in which countries: \n')
                if show_queries==True:
                    print('\n', query)
                print('(country, total_no_stores)')
                for row in result:
                    print(row)
            
            if '2' in switch:
                # Which locations currently have the most stores?
                query = text(f"SELECT locality, COUNT(*) AS total_no_stores\n"
                            f"FROM dim_store_details\n"
                            f"GROUP BY locality\n"
                            f"ORDER BY total_no_stores DESC\n"
                            f"LIMIT 7;\n"
                            )
                
                result = conn.execute(query)   
                
                print('\nWhich locations currently have the most stores: \n')
                
                if show_queries==True:
                    print('\n', query)
                print('(locality, total_no_stores)')
                for row in result:
                    print(row)          

            if '3' in switch:
                # Which months produce the most sales?
                query = text(f"SELECT SUM(orders_table.product_quantity * dim_products.price_£::numeric) as total_sales, dim_date_times.month\n"
                            f"FROM dim_date_times\n"
                            f"JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid\n"
                            f"JOIN dim_products ON orders_table.product_code = dim_products.product_code\n"
                            f"GROUP BY dim_date_times.month\n"
                            f"ORDER BY total_sales DESC\n"
                            f"LIMIT 7;\n"
                            )
                
                result = conn.execute(query)   
                
                print('\nWhich months produce the most sales: \n')
                if show_queries==True:
                    print('\n', query)
                print('(total_sales, month)')
                for row in result:
                    print(row)

            if '4' in switch:
                # How many sales are coming from online?
                query = text(f"SELECT COUNT(*) as number_of_sales, SUM(product_quantity) as product_quantity_count,\n"
                            f"CASE\n"
                                f"WHEN store_code LIKE 'WEB%' THEN 'Web'\n"
                                f"ELSE 'Offline'\n"
                            f"END AS location\n" 
                            f"FROM orders_table\n"
                            f"GROUP BY location\n"
                            )
                
                result = conn.execute(query)   
                
                print('\nHow many sales are coming from online: \n')
                if show_queries==True:
                    print('\n', query)
                print('(number_of_sales, product_quantity_count, location)')
                for row in result:
                    print(row)                 

            if '5' in switch:
                # What percentage of sales comes through from each type of store?
                query = text(f"SELECT\n"
                                f"store_type,\n"
                                f"ROUND(SUM(product_quantity * price_£)::numeric,2) AS total_sales,\n"
                                f"ROUND((SUM(product_quantity * price_£)::numeric / SUM(SUM(product_quantity * price_£)::numeric) OVER ()) * 100.0, 2) AS percentage_total\n"
                            f"FROM\n"
                                f"orders_table\n"
                                f"JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code\n"
                                f"JOIN dim_products ON orders_table.product_code = dim_products.product_code\n"
                            f"GROUP BY store_type\n"
                            f"ORDER BY total_sales DESC\n"
                            f"LIMIT 5;\n"
                            )
                
                result = conn.execute(query)   
                
                print('\nWhat percentage of sales comes through from each type of store: \n')
                if show_queries==True:
                    print('\n', query)
                print('(store_type, total_sales, percentage_total(%))')
                for row in result:
                    print(row)

            if '6' in switch:
                # Which month in which year produced the most sales?
                query = text(f"SELECT\n"
                                f"ROUND(SUM(product_quantity * price_£)::numeric,2) AS total_sales,\n"
                                f"year, month\n"
                            f"FROM orders_table\n"
                                f"JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid\n"
                                f"JOIN dim_products ON orders_table.product_code = dim_products.product_code\n"
                            f"GROUP BY year, month\n"
                            f"ORDER BY total_sales DESC\n"
                            f"LIMIT 11;\n"
                            )
                
                result = conn.execute(query)   
                
                print('\nWhich month in which year produced the most sales: \n')
                if show_queries==True:
                    print('\n', query)
                print('(total_sales, year, month)')
                for row in result:
                    print(row)
                
            if '7' in switch:
                # What is our staff headcount?
                query = text(f"SELECT\n"
                                f"SUM(staff_numbers) as total_staff_numbers, country_code\n"
                            f"FROM\n"
                                f"dim_store_details\n"
                            f"GROUP BY\n"
                                f"country_code\n"
                            f"ORDER BY\n"
                                f"total_staff_numbers DESC;\n"
                            )
                
                result = conn.execute(query)   
                
                print('\nWhat is our staff headcount: \n')
                if show_queries==True:
                    print('\n', query)
                print('(total_staff_numbers, country_code)')
                for row in result:
                    print(row)

            if '8' in switch:
                #  Which German Store Type is selling the most?
                query = text(f"SELECT\n"
                                f"ROUND(SUM(product_quantity * price_£)::numeric,2) AS total_sales,\n"
                                f"store_type,\n"
                                f"country_code\n"
                            f"FROM orders_table\n"
                                f"JOIN dim_products on orders_table.product_code = dim_products.product_code\n"
                                f"JOIN dim_store_details on orders_table.store_code = dim_store_details.store_code AND dim_store_details.country_code = 'DE'\n"
                            f"GROUP BY store_type, country_code\n"
                            f"ORDER BY total_sales\n"
                            )
                
                result = conn.execute(query)   
                
                print('\nWhich German Store Type is selling the most: \n')
                if show_queries==True:
                    print('\n', query)
                print('(total_sales, store_type, country_code)')
                for row in result:
                    print(row)

            if '9' in switch:
                #  How quickly is the company making sales?
                query = text(f"WITH cte AS(\n"
                                    f"SELECT TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') as datetimes, year FROM dim_date_times\n"
                                    f"ORDER BY datetimes DESC\n"
                                f"), cte2 AS(\n"
                                    f"SELECT \n"
                                        f"year, \n"
                                        f"datetimes, \n"
                                        f"LEAD(datetimes, 1) OVER (ORDER BY datetimes DESC) as next_sale_datetime\n"
                                        f"FROM cte\n"
                                f"), cte3 AS (\n"
                                    f"SELECT year, AVG((datetimes - next_sale_datetime)) as time_taken\n"
                                    f"FROM cte2\n"
                                    f"GROUP BY year\n"
                                    f"ORDER BY time_taken DESC\n"
                                f") SELECT year, CONCAT(\n"
                                        f"'\"hours:\" ',date_part('hour',time_taken),\n"
                                        f"',	\"minutes:\" ',date_part('minute',time_taken),\n"
                                        f"',	\"seconds:\" ',floor(date_part('second',time_taken)),' ',\n"
                                        f"',	\"milliseconds:\" ',floor(date_part('millisecond',time_taken))\n"
                                        f")\n"
                                    f"FROM cte3\n"
                                    f"ORDER BY time_taken DESC\n"
                                    f"LIMIT 5;\n"
                            )
                result = conn.execute(query)   
                
                print('\nHow quickly is the company making sales: \n')
                if show_queries==True:
                    print('\n', query)
                print('(year, actual_time_taken)')
                for row in result:
                    print(row)

# only run if called directly
if __name__ == '__main__':
    #following code allows internal testing
    connector = DatabaseConnector('db_creds.yaml') 
    engine = connector.init_db_engine('SD')
    runme = BIQueries()
    runme.business_intelligence_reports('123456789', show_queries=False)