from datetime import datetime
import psycopg2


# Задаём параметры подключения: хост, порт, базу, пользователя и пароль
conn_string = "host=localhost port=5432 dbname=postgres user=postgres password=7777"

# Подключаемся к базе
conn = psycopg2.connect(conn_string)

# Создаём курсор для работы с запросами
cur = conn.cursor()

stock_data = [
    (1, datetime(2023,1,1), 100, 50, 10),
    (2, datetime(2023,1,2), 200, 30, 5),
    (3, datetime(2023,1,3), 150, 20, 15),
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
        for stock in stock_data:
            cur.execute(f"""
                INSERT INTO intable_normalizaciya{decade}.stock_movement (item_id, date, beginning_amount, arrival_amount, disposal_amount)
                                                    VALUES({stock[0]}{i}, '{stock[1]}', {stock[2]}+{i}, {stock[3]}+{i}, {stock[4]}+{i});\n""")
        print(f"===================== {decade} success ===================== ")
    for i in range(decade):
        for stock in stock_data:
            cur.execute(f"""
                INSERT INTO intable_denormalizaciya{decade}.stock_movement (item_id, date, beginning_amount, arrival_amount, disposal_amount, final_amount) 
                                                    VALUES({stock[0]}{i}, '{stock[1]}', {stock[2]}+{i}, {stock[3]}+{i}, {stock[4]}+{i}, {stock[2] + i + stock[3] + i - stock[4] - i});\n""")
        print(f"===================== {decade} success ===================== ")
    conn.commit()