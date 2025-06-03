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

# Количество выполнений одного запроса
cnt = 100

# Создаем массив для хранения результатов анализа запроса
result_list = []

for schema in schema_list:
    if schema[14] != 'd':
        for i in range(cnt):
            # Выполняем запрос, заменяя SELECT * FROM table на нужный запрос
            cur.execute(f"""
                explain analyze 
                SELECT 
                    o.order_id, 
                    d.delivery_cost,
                    SUM(op.product_amount * op.product_cost) AS product_total_cost
                FROM {schema}.orders o
                INNER JOIN {schema}.deliveries d ON o.order_id = d.order_id
                INNER JOIN {schema}.order_products op ON o.order_id = op.order_id
                GROUP BY o.order_id, d.delivery_cost"""
            )

            if i != 0: # результат первого запроса не учитываем, тк он имеет отличное от других запросов поведение
                # Получаем время выполнения запроса
                result_list.append(float(str(cur.fetchall()[-1])[18:23]))
        print(f'{schema} MAX: {round(max(result_list),2)}')
        print('---------------------------')
        print(f'{schema} MIN: {round(min(result_list),2)}')
        print('---------------------------')
        print(f'{schema} AVG: {round(sum(result_list) / len(result_list),2)}')
        print('------------------------------------------------------')
        print('------------------------------------------------------')
        print('------------------------------------------------------')
        result_list = []
    else:
        for i in range(cnt):
            # Выполняем запрос, заменяя SELECT * FROM table на нужный запрос
            cur.execute(f"""
                explain analyze 
                select 
                    orders.order_id,
                    orders.total_order_cost,
                    orders.created_dttm
                from {schema}.orders as orders"""
            )

            if i != 0: # результат первого запроса не учитываем, тк он имеет отличное от других запросов поведение
                        # Получаем время выполнения запроса
                        result_list.append(float(str(cur.fetchall()[-1])[18:23]))
        print(f'{schema} MAX: {max(result_list)}')
        print('---------------------------')
        print(f'{schema} MIN: {min(result_list)}')
        print('---------------------------')
        print(f'{schema} AVG: {sum(result_list) / len(result_list)}')
        print('------------------------------------------------------')
        print('------------------------------------------------------')
        print('------------------------------------------------------')
        result_list = []
conn.commit()  # Подтверждение изменений, если они происходили во время сессии
conn.close()  # Закрытие курсора и соединения с базой