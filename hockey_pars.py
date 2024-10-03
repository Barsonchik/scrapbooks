import requests
from lxml import html
import json
import csv
import mysql.connector
import configparser

# Загрузка конфигурации
config = configparser.ConfigParser()
config.read('B:/VSCode/hockey_pars/config.ini')

# Получение API ключей из конфигурации
db_host = config['database']['host']
db_port = config['database']['port']
db_database = config['database']['dbase']
db_user = config['database']['user']
db_password = config['database']['password']

# URL веб-сайта с табличными данными
url = 'https://livennov.ru/kxl/'

# Заголовок для имитации веб-браузера
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

cursor = None  # Инициализация курсора
connection = None  # Инициализация соединения

try:
    # Отправка GET-запроса
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Проверка на ошибки HTTP

    # Парсинг HTML-содержимого
    tree = html.fromstring(response.content)

    # XPath для выбора таблицы по классу
    standings_element = tree.xpath('/html/body')
    
    if not standings_element:
        raise SystemExit("Элемент с классом 'stat-table' не найден.")

    # Теперь выбираем строки таблицы внутри найденного элемента
    rows = standings_element[0].xpath('.//tbody/tr')

    # Сбор данных в список
    standings_data = []
    headers = [header.text_content().strip() for header in standings_element[0].xpath('.//thead/tr/th')]

    # Очистка заголовков
    cleaned_headers = []
    for header in headers:
        cleaned_header = header.split()[0]  # Берем только первую часть
        if cleaned_header not in cleaned_headers:
            cleaned_headers.append(cleaned_header)

    for row in rows:
        data = [cell.text_content().strip() for cell in row.xpath('.//td')]
        if data:  # Проверка, что строка не пустая
            standings_data.append(dict(zip(cleaned_headers, data)))

    # Сохранение данных в JSON-файл
    with open('khl_standings.json', mode='w', encoding='utf-8') as json_file:
        json.dump(standings_data, json_file, ensure_ascii=False, indent=4)
    print("Данные успешно сохранены в khl_standings.json")
    
     # Сохранение данных в CSV-файл
    with open('khl_standings.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
    print("Данные успешно сохранены в khl_standings.csv")

    # Подключение к базе данных MySQL
    db_config = {
        'host': db_host,
        'port': db_port,
        'database': db_database,
        'user': db_user,
        'password': db_password
    }

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # Создание таблицы с учетом обозначений
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS khl_standings (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Команда VARCHAR(100),  -- Название команды
        И INT,                   -- Количество сыгранных игр
        В INT,                   -- Количество побед
        ВО INT,                  -- Количество побед в овертайме
        ВБ INT,                  -- Количество побед по буллитам
        П INT,                   -- Количество поражений
        ПО INT,                  -- Количество поражений в овертайме
        ПБ INT,                  -- Количество поражений по буллитам
        ШРазница INT,             -- Разница забитых и пропущенных шайб
        О INT                    -- Количество набранных очков
    );
    '''
    cursor.execute(create_table_query)
    connection.commit()

    # Импорт данных из JSON в таблицу
    for entry in standings_data:
        # Извлечение и расчет разницы шайб
        goals = entry.get('Шайбы').split('-')  # Разделяем на забитые и пропущенные
        if len(goals) == 2:
            goals_scored = int(goals[0])
            goals_against = int(goals[1])
            goal_difference = goals_scored - goals_against
        else:
            goal_difference = 0  # Установите значение по умолчанию, если данные некорректны

        insert_query = '''
        INSERT INTO khl_standings (Команда, И, В, ВО, ВБ, П, ПО, ПБ, ШРазница, О)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        '''
        cursor.execute(insert_query, (
            entry.get('Команда'),
            entry.get('И'),
            entry.get('В'),
            entry.get('ВО'),
            entry.get('ВБ'),
            entry.get('П'),
            entry.get('ПО'),
            entry.get('ПБ'),
            goal_difference,  # Используем рассчитанную разницу шайб
            entry.get('О')    # Убедитесь, что ключ соответствует
        ))

    connection.commit()

except requests.exceptions.HTTPError as http_err:
    print(f"HTTP ошибка: {http_err}")
except mysql.connector.Error as db_err:
    print(f"Ошибка базы данных: {db_err}")
except Exception as err:
    print(f"Ошибка: {err}")
finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()