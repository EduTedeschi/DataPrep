import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Configurações de conexão
transacional_conn_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5434
}

star_schema_conn_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
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
    -- Dimensão de clientes
    cust.customer_id,
    cust.customer_unique_id,
    cust.customer_zip_code_prefix,
    cust.customer_city,
    cust.customer_state,
 
    -- Dimensão de produtos
    prod.product_id,
    prod.product_category_name,
    prod.product_name_lenght,
    prod.product_description_lenght,
    prod.product_photos_qty,
    prod.product_weight_g,
    prod.product_length_cm,
    prod.product_height_cm,
    prod.product_width_cm,

    -- Dimensão de vendedores
    seller.seller_id,
    seller.seller_zip_code_prefix,
    seller.seller_city,
    seller.seller_state,

    -- Dimensão de reviews
    review.review_id,
    review.review_score,
    review.review_comment_title,
    review.review_comment_message,
    review.review_creation_date,
    review.review_answer_timestamp,

    -- Dimensão de pagamentos
    payment.payment_id,
    payment.payment_sequential,
    payment.payment_type,
    payment.payment_installments,
    payment.payment_value,

    -- Dimensão de datas
    date_dim.date AS order_date,
    date_dim.day AS order_day,
    date_dim.month AS order_month,
    date_dim.year AS order_year,

    -- Fato - Pedidos
    order.fato_id,
    order.order_id,
    order.shipping_limit_date,
    order.price,
    order.freight_value,
    order.order_status,
    order.order_purchase_timestamp,
    order.order_approved_at,
    order.order_delivered_carrier_datetime,
    order.order_delivered_customer_datetime,
    order.order_estimated_delivery_datetime

FROM fato_olist_order AS order
-- Relacionamentos com tabelas dimensionais
LEFT JOIN dim_olist_customers AS cust
    ON order.customer_id = cust.customer_id
LEFT JOIN dim_olist_product AS prod
    ON order.product_id = prod.product_id
LEFT JOIN dim_olist_seller AS seller
    ON order.seller_id = seller.seller_id
LEFT JOIN dim_olist_order_reviews AS review
    ON order.review_id = review.review_id
LEFT JOIN dim_olist_order_payments AS payment
    ON order.payment_id = payment.payment_id
LEFT JOIN dim_date AS date_dim
    ON DATE(order.order_purchase_date) = date_dim.date;t
    """

    # Extract data
    customers_df = extract_data(customers_query, transacional_conn)

    # Drop existing table if dependencies allow (or handle them manually)
    try:
        with star_schema_conn.begin() as conn:
            conn.execute("DROP TABLE IF EXISTS bigtable CASCADE;")
    except Exception as e:
        print(f"Warning: Could not drop table due to dependencies. Error: {e}")

    # Load the data into the star schema
    customers_df.to_sql('bigtable', star_schema_conn, if_exists='replace', index=False)


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