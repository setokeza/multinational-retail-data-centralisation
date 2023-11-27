# multinational-retail-data-centralisation

## Table of Contents
- [Description](#description)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [File Structure](#file-structure)
- [Usage](#usage)
- [License](#license)

## Description

The Multinational Retail Data Centralisation project is designed to centralise and organize sales data from a multinational company, creating a unified source of information for analysis by team members. 

1. The project involves the creation of the central database 'sales_data'.

2. Data is extracted from various sources, cleaned and uploaded to the database as distinct tables.

3. These tables are configured into a meaningful relational database.

4.  We are then able to run business intelligence queries against this database in order to better understand the sales data.

## Prerequisites

Before using this project, make sure you have the following prerequisites installed and configured:

1. **Python:**
    - This project is written in Python. Ensure you have Python installed on your machine. You can download Python from [here](https://www.python.org/).
    - Verify your Python installation by running `python --version` in a new terminal.

2. **AWS CLI:**
    - The AWS Command Line Interface (CLI) is used for interacting with Amazon S3. Install the AWS CLI by following the instructions [here](https://aws.amazon.com/cli/).
    -  Verify your AWS CLI installation by running `aws --version` in a new terminal.

3. **PGAdmin 4 and PostgreSQL:**
    - Download and install PG4Admin and PostgreSQL. You can download PGAdmin 4 from [here](https://www.pgadmin.org/download/) and PostgreSQL from [here](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads)
    - Create a sales_data database in PG4Admin for later use.

4. **VSCODE with Python and SQLTools:**
    - Ensure Visual Studio Code (VSCode) is installed on your machine. You can download VSCODE from [here](https://code.visualstudio.com/download)
    - Install the Python extension for VSCode for a smooth development experience.
    - Install SQLTools extension in VSCode to facilitate SQL query execution.

5. **Miniconda3 for Virtual Environments:**
    - Install Miniconda3 to manage virtual environments easily. You can download Miniconda3 from [here](https://docs.conda.io/projects/miniconda/en/latest/)
    -  Verify Miniconda3 installation by running `conda --version` in a new terminal.

    - The relevant Python libraries are pre-configured in mrdc_env.yaml. Complete the installation by executing:

    ```bash
    conda env create -f mrdc_env.yaml
    conda activate mrdc
    ```
## Installation

Clone this repository to your local machine:

```bash
git clone https://github.com/setokeza/AiCore-Multinational-Retail-Data-Centralisation.git
```
## File Structure

- **milestone_2_utils**
    - `db_creds.yaml`: Database credentials configuration file.  Create entries  for both the database you are extracting from (keep RDS prefix), and the database you are uploading to (keep SD prefix), as follows:
        - file format:
        -    RDS_HOST: [host]\
            RDS_PASSWORD: [password]\
            RDS_USER: [user]\
            RDS_DATABASE: postgres\
            RDS_PORT: 5432

        -    SD_HOST: localhost\
            SD_USER: postgres\
            SD_PASSWORD: [password]\
            SD_DATABASE: sales_data\
            SD_PORT: 5432

  - `data_cleaning.py`: Script for cleaning user, card, store, product, date and order data.
  - `data_extraction.py`: Script for extracting data from RDS tables, PDFs, APIs, and S3.
  - `database_connector.py`: Script for connecting to and uploading data to a PostgreSQL database.

- **milestone_3_star_schema_sales_database**
  - **modify database schema** : 
    -   `sql_queries.py` sql script to change/update database schema. `main.py` runs `sql_queries.py` to create the final database schema.  The following query groups are run as desired and can be altered in `main.py`:
    -   1. drop primary & foreign keys
        2. change datatypes on all tables
        3. add primary & foreign keys
        4. show specified database schema:
            1. show row count
            2. show column_name, data_type, character_maximum_length
            3. show table constraints

- **milestone_4_data_querying**
  - `bi_queries.sql`: SQL script for business-related queries. `main.py` runs `bi_queries.sql` to generate business intelligence reports. 
  -  The following reports are generated and can be altered in `main.py`:
        1. How many stores does the business have and in which countries?
        2. Which locations currently have the most stores?
        3. Which months produce the most sales?
        4. How many sales are coming from online?
        5. What percentage of sales comes through from each type of store?
        6. Which month in which year produced the most sales?
        7. What is our staff headcount?
        8. Which German Store Type is selling the most?
        9. How quickly is the company making sales?

- `.gitignore`: Gitignore file which include the database credentials file `db_creds.yaml`.
- `README.md`: Documentation file.
- `mrdc_env.yaml`: Conda environment configuration file.
- `main.py`: Main script for executing the centralization process.

## Usage

### Setting up 
1. Ensure you have activated the Conda environment:

    ```bash
    conda activate mrdc
    ```
2. Ensure you have the extract database, and upload database, creds ready as `db_creds.yaml` file.

### Uploading Data to PostgreSQL and Centralising Data
1. Execute the main script to centralise data to PostgreSQL. 

    ```bash
    python main.py
    ```
    This script will:
    1. drop the current database schema
    2. read and clean the sales data from the specified sources and upload it to PostgreSQL.
    3. run the queries to create the new database schema
    4. run the business intelligence reports

    The sql queries and business intelligence queries to run can be selected on an ad-hoc basis by modifying `main.py`.


## License

This project is open to the public. 