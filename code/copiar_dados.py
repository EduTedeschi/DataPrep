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
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM olist_sellers_dataset
    """

    # Extract data
    customers_df = extract_data(customers_query, transacional_conn)

    # Drop existing table if dependencies allow (or handle them manually)
    try:
        with star_schema_conn.begin() as conn:
            conn.execute("DROP TABLE IF EXISTS dim_olist_sellers CASCADE;")
    except Exception as e:
        print(f"Warning: Could not drop table due to dependencies. Error: {e}")

    # Load the data into the star schema
    customers_df.to_sql('dim_olist_sellers', star_schema_conn, if_exists='replace', index=False)


    # Processo principal
def main():
    # Conectar aos bancos
    transacional_conn = connect_to_db(transacional_conn_config)
    star_schema_conn = create_engine(
        f"postgresql://{star_schema_conn_config['user']}:{star_schema_conn_config['password']}@{star_schema_conn_config['host']}:{star_schema_conn_config['port']}/{star_schema_conn_config['dbname']}"
    )

    # Criar dimensões
    create_dimensions(transacional_conn, star_schema_conn)


    print("Star Schema criado com sucesso!")

if __name__ == "__main__":
    main()