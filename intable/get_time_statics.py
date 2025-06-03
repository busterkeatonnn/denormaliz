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

# Количество выполнений одного запроса
cnt = 100

# Создаем массив для хранения результатов анализа запроса
result_list = []

for schema in schema_list:
    if schema[8] != 'd':
        for i in range(cnt):
            # Выполняем запрос, заменяя SELECT * FROM table на нужный запрос
            cur.execute(f"""
                explain analyze 
                select 
                    stock.stock_movement_id,
                    stock.item_id,
                    stock.date,
                    stock.beginning_amount,
                    stock.arrival_amount,
                    stock.disposal_amount,
                    stock.beginning_amount + stock.arrival_amount - stock.disposal_amount AS final_amount
                from {schema}.stock_movement as stock"""
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
                    stock.stock_movement_id,
                    stock.item_id,
                    stock.date,
                    stock.beginning_amount,
                    stock.arrival_amount,
                    stock.disposal_amount,
                    stock.final_amount
                from {schema}.stock_movement as stock"""
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

