import psycopg2

# Задаём параметры подключения: хост, порт, базу, пользователя и пароль
conn_string = "host=localhost port=5432 dbname=postgres user=postgres password=7777"

# Подключаемся к базе
conn = psycopg2.connect(conn_string)

# Создаём курсор для работы с запросами
cur = conn.cursor()

# Создаем список для хранения схем
schema_list = [
    'vosxodyschaya_normalizaciya10',
    'vosxodyschaya_denormalizaciya10',
    'vosxodyschaya_normalizaciya100',
    'vosxodyschaya_denormalizaciya100',
    'vosxodyschaya_normalizaciya1000',
    'vosxodyschaya_denormalizaciya1000',
    'vosxodyschaya_normalizaciya10000',
    'vosxodyschaya_denormalizaciya10000',
    'vosxodyschaya_normalizaciya100000',
    'vosxodyschaya_denormalizaciya100000',
]

for schema in schema_list:
    if schema[14] != 'd':
        cur.execute(f"""
            DO $$
            DECLARE
                schema_record RECORD;
            BEGIN
                FOR schema_record IN
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name LIKE '%{schema}%'
                LOOP
                    EXECUTE format('DROP SCHEMA %I CASCADE', schema_record.schema_name);
                END LOOP;
            END $$;

            drop SCHEMA if exists {schema};
            create schema {schema};

            -- Заказы
            drop table if exists {schema}.orders CASCADE;
            CREATE TABLE {schema}.orders (
                order_id SERIAL PRIMARY KEY,
                created_dttm timestamp,
                completed_dttm timestamp,
                payment_type TEXT,
                item_amount INT
            );
            TRUNCATE TABLE {schema}.orders  RESTART identity CASCADE;
            
            -- Доставки
            drop table if exists {schema}.deliveries CASCADE;
            CREATE TABLE {schema}.deliveries (
                delivery_id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES {schema}.orders(order_id),
                delivery_type VARCHAR(255),
                delivery_cost NUMERIC(10, 2),
                created_dttm timestamp,
                delivery_status text
            );
            TRUNCATE TABLE {schema}.deliveries RESTART IDENTITY CASCADE;
            
            -- Доставки
            drop table if exists {schema}.order_products CASCADE;
            CREATE TABLE {schema}.order_products (
                product_id SERIAL,
                order_id INTEGER REFERENCES {schema}.orders(order_id),
                product_amount INTEGER,
                product_cost NUMERIC(10, 2),
                PRIMARY KEY (product_id, order_id)
            );
            TRUNCATE TABLE {schema}.order_products RESTART IDENTITY CASCADE;
        """)
    else:
        cur.execute(f"""
            DO $$
            DECLARE
                schema_record RECORD;
            BEGIN
                FOR schema_record IN
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name LIKE '%{schema}%'
                LOOP
                    EXECUTE format('DROP SCHEMA %I CASCADE', schema_record.schema_name);
                END LOOP;
            END $$;

            drop SCHEMA if exists {schema};
            create schema {schema};

            -- Заказы
            drop table if exists {schema}.orders CASCADE;
            CREATE TABLE {schema}.orders (
                order_id SERIAL PRIMARY KEY,
                created_dttm TIMESTAMP NOT NULL,
                completed_dttm timestamp,
                payment_type TEXT,
                item_amount INT,
                total_order_cost NUMERIC(10, 2)
            );
            TRUNCATE TABLE {schema}.orders  RESTART identity CASCADE;
            
            -- Доставки
            drop table if exists {schema}.deliveries CASCADE;
            CREATE TABLE {schema}.deliveries (
                delivery_id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES {schema}.orders(order_id),
                delivery_type VARCHAR(255),
                delivery_cost NUMERIC(10, 2),
                created_dttm timestamp,
                delivery_status text
            );
            TRUNCATE TABLE {schema}.deliveries RESTART IDENTITY CASCADE;
            
            -- Доставки
            drop table if exists {schema}.order_products CASCADE;
            CREATE TABLE {schema}.order_products (
                product_id SERIAL,
                order_id INTEGER REFERENCES {schema}.orders(order_id),
                product_amount INTEGER,
                product_cost NUMERIC(10, 2),
                PRIMARY KEY (product_id, order_id)
            );
            TRUNCATE TABLE {schema}.order_products RESTART IDENTITY CASCADE;
        """)
    conn.commit()
