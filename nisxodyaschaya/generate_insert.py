from datetime import datetime
import psycopg2


# Задаём параметры подключения: хост, порт, базу, пользователя и пароль
conn_string = "host=localhost port=5432 dbname=postgres user=postgres password=7777"

# Подключаемся к базе
conn = psycopg2.connect(conn_string)

# Создаём курсор для работы с запросами
cur = conn.cursor()

clients_data = [
    ("Ivan", "Ivanov", "Ivanovich", "john@example.com", "8800-555-35-35"),
    ("Peter", "Petrov", "Petrovich", "peter@example.ru", "7700-22-44"),
    ("Semyon", "Semyonov", "Semyonovich", "semyon@example.org", "910-87-65")
]

orders_data = [
    (datetime(2023,1,1), datetime(2023,1,1), 'card', 2),
    (datetime(2024,6,12), datetime(2023,1,1), 'qr-code', 3),
    (datetime(2023,3,1), datetime(2023,1,1), 'nalichka', 2)
]

decade_list = [
    10,
    100,
    1000,
    10000,
    100000,
] # порядок

for decade in decade_list:
    for i in range(decade):
        for client in clients_data:
            cur.execute(f"""
                INSERT INTO nisxodyaschaya_normalizaciya{decade}.clients (name, surname, middle_name, email, phone) 
                                                    VALUES('{client[0]}{i}', '{client[1]}{i}', '{client[2]}{i}', '{client[3]}{i}', '{client[4]}+{i}');\n""")
        for order in orders_data:
            cur.execute(f"""
                INSERT INTO nisxodyaschaya_normalizaciya{decade}.orders (client_id, created_dttm, completed_dttm, payment_type, item_amount) 
                                                            VALUES({i+1}, '{order[0]}', '{order[1]}', '{order[2]}', '{order[3]}');\n""")
        print(f"===================== {decade} success ===================== ")
    for i in range(decade):
        for client in clients_data:
            cur.execute(f"""
                INSERT INTO nisxodyaschaya_denormalizaciya{decade}.clients (name, surname, middle_name, email, phone) 
                                                    VALUES('{client[0]}{i}', '{client[1]}{i}', '{client[2]}{i}', '{client[3]}{i}', '{client[4]}+{i}');\n""")
        for order in orders_data:
            cur.execute(f"""
                INSERT INTO nisxodyaschaya_denormalizaciya{decade}.orders (client_id, client_fullname, created_dttm, completed_dttm, payment_type, item_amount) 
                                                    VALUES({i+1}, '{clients_data[0][0]+ ' ' + clients_data[0][1] + ' ' + clients_data[0][2]}{i}', '{order[0]}', '{order[1]}', '{order[2]}', '{order[3]}');\n""")
        print(f"===================== {decade} success ===================== ")
    conn.commit()