from datetime import datetime
import psycopg2


# Задаём параметры подключения: хост, порт, базу, пользователя и пароль
conn_string = "host=localhost port=5432 dbname=postgres user=postgres password=7777"

# Подключаемся к базе
conn = psycopg2.connect(conn_string)

# Создаём курсор для работы с запросами
cur = conn.cursor()

orders_data = [
    (datetime(2023,1,1), datetime(2023,1,1), 'card', 2),
    (datetime(2024,6,12), datetime(2023,1,1), 'card', 2),
    (datetime(2023,3,1), datetime(2023,1,1), 'card', 2)
]

deliveries_data = [
    ('Courier', 5.00, datetime(2023,1,1), '5'),
    ('Postal', 2.50, datetime(2023,1,1), '4'),
    ('Courier', 4.75, datetime(2023,1,1), '6'),
]

order_products = [
    (1, 2, 15.00),
    (2, 1, 20.00),
    (3, 3, 5.00),
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
        for order in orders_data:
            cur.execute(f"""
                INSERT INTO vosxodyschaya_normalizaciya{decade}.orders (created_dttm, completed_dttm, payment_type, item_amount)
                                                    VALUES('{order[0]}', '{order[1]}', '{order[2]}', '{order[3]}');""")
        for delivery in deliveries_data:
            cur.execute(f"""
                INSERT INTO vosxodyschaya_normalizaciya{decade}.deliveries (order_id, delivery_type, delivery_cost, created_dttm, delivery_status)
                                                            VALUES({i+1}, '{delivery[0]}',{delivery[1]}, '{delivery[2]}', '{delivery[3]}');""")
        for order_product in order_products:
            cur.execute(f"""
                INSERT INTO vosxodyschaya_normalizaciya{decade}.order_products (product_id, order_id, product_amount, product_cost)
                                                            VALUES({order_product[0]}, {i+1}, {order_product[1]}, {order_product[2]});""")
        print(f"===================== {decade} success ===================== ")
    for i in range(decade):
        for order in orders_data:
            cur.execute(f"""
                INSERT INTO vosxodyschaya_denormalizaciya{decade}.orders (created_dttm, completed_dttm, payment_type, item_amount, total_order_cost)
                                                    VALUES('{order[0]}', '{order[1]}', '{order[2]}', '{order[3]}', {0});""")
        for delivery in deliveries_data:
            cur.execute(f"""
                INSERT INTO vosxodyschaya_denormalizaciya{decade}.deliveries (order_id, delivery_type, delivery_cost, created_dttm, delivery_status)
                                                            VALUES({i+1}, '{delivery[0]}',{delivery[1]}, '{delivery[2]}', '{delivery[3]}');""")
        for order_product in order_products:
            cur.execute(f"""
                INSERT INTO vosxodyschaya_denormalizaciya{decade}.order_products (product_id, order_id, product_amount, product_cost)
                                                            VALUES({order_product[0]}, {i+1}, {order_product[1]}, {order_product[2]});""")
        print(f"===================== {decade} success ===================== ")
    # Заполняем total_order_cost
    cur.execute(f""" 
        UPDATE vosxodyschaya_denormalizaciya{decade}.orders
            SET total_order_cost = subquery.delivery_cost + subquery.product_total_cost
            FROM (
                SELECT 
                    o.order_id, 
                    d.delivery_cost,
                    SUM(op.product_amount * op.product_cost) AS product_total_cost
                FROM vosxodyschaya_denormalizaciya{decade}.orders o
                JOIN vosxodyschaya_denormalizaciya{decade}.deliveries d ON o.order_id = d.order_id
                JOIN vosxodyschaya_denormalizaciya{decade}.order_products op ON o.order_id = op.order_id
                GROUP BY o.order_id, d.delivery_cost
            ) AS subquery
            WHERE vosxodyschaya_denormalizaciya{decade}.orders.order_id = subquery.order_id;
    """)
    print(f"===================== {decade} UPDATE success ===================== ")
    conn.commit()
