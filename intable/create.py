import psycopg2

# Задаём параметры подключения: хост, порт, базу, пользователя и пароль
conn_string = "host=localhost port=5432 dbname=postgres user=postgres password=7777"

# Подключаемся к базе
conn = psycopg2.connect(conn_string)

# Создаём курсор для работы с запросами
cur = conn.cursor()

# Создаем список для хранения схем
schema_list = [
    'intable_normalizaciya10',
    'intable_denormalizaciya10',
    'intable_normalizaciya100',
    'intable_denormalizaciya100',
    'intable_normalizaciya1000',
    'intable_denormalizaciya1000',
    'intable_normalizaciya10000',
    'intable_denormalizaciya10000',
    'intable_normalizaciya100000',
    'intable_denormalizaciya100000',
]

for schema in schema_list:
    if schema[8] != 'd':
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

            -- Движение товаров
            drop table if exists {schema}.stock_movement CASCADE;
            CREATE TABLE {schema}.stock_movement (
                stock_movement_id SERIAL PRIMARY KEY,
                item_id INT NOT NULL,
                date DATE NOT NULL,
                beginning_amount NUMERIC NOT NULL,
                arrival_amount NUMERIC NOT NULL,
                disposal_amount NUMERIC NOT NULL
            );
            TRUNCATE TABLE {schema}.stock_movement  RESTART identity CASCADE;
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

            -- Движение товаров
            drop table if exists {schema}.stock_movement CASCADE;
            CREATE TABLE {schema}.stock_movement (
                stock_movement_id SERIAL PRIMARY KEY,
                item_id INT NOT NULL,
                date DATE NOT NULL,
                beginning_amount NUMERIC NOT NULL,
                arrival_amount NUMERIC NOT NULL,
                disposal_amount NUMERIC NOT NULL,
                final_amount NUMERIC NULL
            );
            TRUNCATE TABLE {schema}.stock_movement  RESTART identity CASCADE;
        """)
    conn.commit()
