import psycopg2


# Задаём параметры подключения: хост, порт, базу, пользователя и пароль
conn_string = "host=localhost port=5432 dbname=postgres user=postgres password=7777"

# Подключаемся к базе
conn = psycopg2.connect(conn_string)

# Создаём курсор для работы с запросами
cur = conn.cursor()

# Создаем список для хранения схем
schema_list = [
    'nisxodyaschaya_normalizaciya10',
    'nisxodyaschaya_denormalizaciya10',
    'nisxodyaschaya_normalizaciya100',
    'nisxodyaschaya_denormalizaciya100',
    'nisxodyaschaya_normalizaciya1000',
    'nisxodyaschaya_denormalizaciya1000',
    'nisxodyaschaya_normalizaciya10000',
    'nisxodyaschaya_denormalizaciya10000',
    'nisxodyaschaya_normalizaciya100000',
    'nisxodyaschaya_denormalizaciya100000',
]

for schema in schema_list:
    if schema[15] != 'd':
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
            
            -- Клиенты
            drop table if exists {schema}.clients CASCADE;
            CREATE TABLE {schema}.clients (
              client_id SERIAL PRIMARY KEY,
              name TEXT NOT NULL,
              surname TEXT NOT NULL,
              middle_name TEXT NOT NULL,
              email TEXT,
              phone VARCHAR(20)
            );
            TRUNCATE TABLE {schema}.clients RESTART IDENTITY CASCADE;
            
            -- Заказы
            drop table if exists {schema}.orders CASCADE;
            CREATE TABLE {schema}.orders (
              order_id SERIAL PRIMARY KEY,
              client_id INT NOT NULL REFERENCES {schema}.clients(client_id),
              created_dttm timestamp,
              completed_dttm timestamp,
              payment_type TEXT,
              item_amount INT
            );
            TRUNCATE TABLE {schema}.orders  RESTART identity CASCADE;
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

            -- Клиенты
            drop table if exists {schema}.clients CASCADE;
            CREATE TABLE {schema}.clients (
            client_id serial4 NOT NULL,
            "name" text NOT NULL,
            surname text NOT NULL,
            middle_name text NOT NULL,
            email text NULL,
            phone text NULL,
            CONSTRAINT clients_pkey PRIMARY KEY (client_id)
        );
            TRUNCATE TABLE {schema}.clients RESTART IDENTITY CASCADE;

            -- Заказы
            drop table if exists {schema}.orders CASCADE;
            CREATE TABLE {schema}.orders (
                order_id serial4 NOT NULL,
                client_id int4 NOT NULL,
                client_fullname text NULL,
                created_dttm timestamp,
                completed_dttm timestamp,
                payment_type TEXT,
                item_amount INT,
                CONSTRAINT orders_pkey PRIMARY KEY (order_id)
            );
            TRUNCATE TABLE {schema}.orders  RESTART identity CASCADE;
        """)
    conn.commit()
