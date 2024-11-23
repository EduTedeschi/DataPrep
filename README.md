# Preparação e Transformação de Dados

1. Introdução
    Este projeto tem como objetivo construir um fluxo completo para a preparação e transformação de dados brutos, tornando-os utilizáveis por especialistas. O pipeline abrangerá desde a ingestão inicial até a geração de um conjunto de dados limpo e estruturado, pronto para análise e utilização.

2. Estrutura do Pipeline

    -	Ingestão de Dados
        - Entrada dos dados brutos de arquivos CSV obtidos do Kaggle (https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data?select=olist_geolocation_dataset.csv)

    -	Transformação dos Dados
        - Aplicação de processos como normalização, padronização, criação de novas colunas e agregação de dados.

    -	Armazenamento e Exportação
        - Salvamento dos dados processados em formato star schema e wide table em um banco de dados PostgreSQL.

3. Ferramentas e Tecnologias

	- Linguagens de Programação: Python, SQL
	- Armazenamento de Dados: PostgreSQL, Docker

4. Fluxo Detalhado do Processo

    - Ingestão de Dados
    
      - Identificar as fontes de dados: arquivos locais em formato CSV.
      - Configurar conectores e autenticação para a criação do container Postgres dentro do docker.
      - Copiar os arquivos da maquina local para o container do docker.
      ```
          $ docker cp olist_sellers_dataset.csv sqlserver:/var/opt/mssql/data/
      ```
      - Popular as tabelas no PostgreSQL com os arquivos copiados:
      ``` sql
          COPY olist_sellers_dataset
          FROM '/tmp/olist_sellers_dataset.csv'
          DELIMITER ','
          CSV HEADER;
      ```

    - Validação dos Dados

      - Verificar tipos de dados e formatos esperados

    - Desenvolvimento do relacionamento star schema conforme imagem abaixo.

    ![alt text](https://github.com/EduTedeschi/DataPrep/blob/main/Images/StarSchema.png?raw=true)
    
    - Criação das tabelas do star schema

    ``` sql
        -- Criação da tabela dim_olist_customers
        CREATE TABLE dim_olist_customers (
            customer_id BIGINT PRIMARY KEY,
            customer_unique_id BIGINT,
            customer_city TEXT,
            customer_state TEXT
        );

        -- Criação da tabela dim_olist_order_reviews
        CREATE TABLE dim_olist_order_reviews (
            review_id BIGINT PRIMARY KEY,
            review_score FLOAT,
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP
        );

        -- Criação da tabela dim_olist_order_payments
        CREATE TABLE dim_olist_order_payments (
            payment_id BIGINT PRIMARY KEY,
            payment_sequential TEXT,
            payment_type TEXT,
            payment_installments INT,
            payment_value FLOAT
        );

        -- Criação da tabela dim_olist_geo
        CREATE TABLE dim_olist_geo (
            geolocation_zip_code INT PRIMARY KEY,
            geolocation_lat FLOAT,
            geolocation_lng FLOAT,
            geolocation_city TEXT,
            geolocation_state TEXT
        );

        -- Criação da tabela dim_olist_product
        CREATE TABLE dim_olist_product (
            product_id BIGINT PRIMARY KEY,
            product_category_name TEXT,
            product_name_length TEXT,
            product_description_length TEXT,
            product_photos_qty BYTEA,
            product_weight_g FLOAT,
            product_length_cm FLOAT,
            product_height_cm FLOAT,
            product_width_cm FLOAT
        );

        -- Criação da tabela dim_olist_seller
        CREATE TABLE dim_olist_seller (
            seller_id BIGINT PRIMARY KEY,
            seller_city TEXT,
            seller_state TEXT
        );

        -- Criação da tabela dim_date
        CREATE TABLE dim_date (
            date DATE PRIMARY KEY,
            day INT,
            month INT,
            year INT
        );

        -- Criação da tabela fact_olist_order
        CREATE TABLE fact_olist_order (
            fato_id BIGINT PRIMARY KEY,
            order_id BIGINT,
            customer_id BIGINT,
            review_id BIGINT,
            payment_id BIGINT,
            product_id BIGINT,
            seller_id BIGINT,
            shipping_limit_date DATE,
            price FLOAT,
            freight_value FLOAT,
            order_status TEXT,
            order_purchase_timestamp TIMESTAMP,
            order_purchase_date DATE,
            order_purchase_time TIME,
            order_approved_at TIMESTAMP,
            order_approved_date DATE,
            order_approved_at_time TIME,
            order_delivered_carrier_date DATE,
            order_delivered_carrier_time TIME,
            order_delivered_customer_date TIMESTAMP,
            order_delivered_customer_time TIME,
            order_estimated_delivery_date DATE,
            order_estimated_delivery_datetime TIMESTAMP,
            order_estimated_delivery_time TIME,
            customer_zip_code_prefix TEXT,
            seller_zip_code_prefix TEXT,
            FOREIGN KEY (customer_id) REFERENCES dim_olist_customers(customer_id),
            FOREIGN KEY (review_id) REFERENCES dim_olist_order_reviews(review_id),
            FOREIGN KEY (payment_id) REFERENCES dim_olist_order_payments(payment_id),
            FOREIGN KEY (product_id) REFERENCES dim_olist_product(product_id),
            FOREIGN KEY (seller_id) REFERENCES dim_olist_seller(seller_id)
        );
    ```

    - Popular as tabelas do star schema utilizando script python para fazer a estração das tabelas raw e popular as tabelas specialistas

5. Requisitos do Projeto

    - Requisitos Técnicos
    
        - Dados armazenados em formato compatível com as ferramentas utilizadas.
	- Escalabilidade para grandes volumes de dados.
    
    - Requisitos de Negócio
    
        - Dados transformados precisam atender às necessidades dos especialistas.
        - Garantir a segurança e conformidade com regulamentos, como LGPD ou GDPR.

6. Conclusão

    - Este pipeline garante um fluxo robusto, escalável e confiável para transformar dados brutos em insights valiosos. Com sua implementação, espera-se aumentar a eficiência e a precisão dos processos analíticos, permitindo uma melhor tomada de decisão pelos especialistas.
