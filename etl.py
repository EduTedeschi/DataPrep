import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Configurações de conexão
transacional_conn_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

star_schema_conn_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5431
}

# Conectar ao banco transacional
def connect_to_db(config):
    return psycopg2.connect(
        dbname=config['dbname'],
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port']
    )

# Extrair dados do banco transacional
def extract_data(query, connection):
    return pd.read_sql_query(query, connection)

# Criar as dimensões
def create_dimensions(transacional_conn, star_schema_conn):
    # Query to extract data for the dimension table
    customers_query = """
    SELECT  
        product_id, 
        product_category_name, 
        product_name_lenght, 
        product_description_lenght, 
        product_photos_qty, 
        product_weight_g, 
        product_length_cm, 
        product_height_cm, 
        product_width_cm
    FROM olist_products_dataset
    """

    # Extract data
    customers_df = extract_data(customers_query, transacional_conn)

    # Drop existing table if dependencies allow (or handle them manually)
    try:
        with star_schema_conn.begin() as conn:
            conn.execute("DROP TABLE IF EXISTS dim_olist_product CASCADE;")
    except Exception as e:
        print(f"Warning: Could not drop table due to dependencies. Error: {e}")

    # Load the data into the star schema
    customers_df.to_sql('dim_olist_product', star_schema_conn, if_exists='replace', index=False)

    import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Configurações de conexão
transacional_conn_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

star_schema_conn_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5431
}

# Conectar ao banco transacional
def connect_to_db(config):
    return psycopg2.connect(
        dbname=config['dbname'],
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port']
    )

# Extrair dados do banco transacional
def extract_data(query, connection):
    return pd.read_sql_query(query, connection)

# Criar as dimensões
def create_dimensions(transacional_conn, star_schema_conn):
    # Query to extract data for the dimension table
    customers_query = """
    SELECT  
        product_id, 
        product_category_name, 
        product_name_lenght, 
        product_description_lenght, 
        product_photos_qty, 
        product_weight_g, 
        product_length_cm, 
        product_height_cm, 
        product_width_cm
    FROM olist_products_dataset
    """

    # Extract data
    customers_df = extract_data(customers_query, transacional_conn)

    # Drop existing table if dependencies allow (or handle them manually)
    try:
        with star_schema_conn.begin() as conn:
            conn.execute("DROP TABLE IF EXISTS dim_olist_product CASCADE;")
    except Exception as e:
        print(f"Warning: Could not drop table due to dependencies. Error: {e}")

    # Load the data into the star schema
    customers_df.to_sql('dim_olist_product', star_schema_conn, if_exists='replace', index=False)