import requests
from bs4 import BeautifulSoup
import json
from pymongo import MongoClient
import configparser

# Загрузка конфигурации
config = configparser.ConfigParser()
config.read('B:/GB/MongoDB/config.ini')

# Получение данных из конфигурации

db_host = config['database']['host']
db_port = config['database']['port']
db_database = config['database']['dbase']
db_user = config['database']['user']
db_password = config['database']['password']
db_table = config['database']['table']

# Функция для извлечения информации о книгах
def get_books_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    books = []
    book_elements = soup.find_all('article', class_='product_pod')
    
    for element in book_elements:
        title = element.h3.a['title']
        price = float(element.find('p', class_='price_color').text[1:].replace('£', ''))
        
        # Получение ссылки на страницу с описанием продукта
        description_link = element.find('h3').a['href']
        description_url = url.replace('index.html', '') + description_link
        
        # Запрос страницы с описанием продукта
        description_response = requests.get(description_url)
        description_soup = BeautifulSoup(description_response.content, 'html.parser')
        
        # Получить информацию о наличии книг
        stock_text = description_soup.find('p', class_='instock availability').text.strip()
        in_stock = int(''.join(filter(str.isdigit, stock_text))) if any(char.isdigit() for char in stock_text) else 0

        # Ознакомление с описанием продукта
        description = description_soup.find('article', class_='product_page').find_all('p')[3].text

        book = {
            'title': title,
            'price': price,
            'in_stock': in_stock,
            'description': description
        }
        books.append(book)
        
    return books

def scrape_books():
    main_url = 'http://books.toscrape.com/'
    response = requests.get(main_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    categories = soup.find('ul', class_='nav').find_all('li')

    all_books = []
    for category in categories:
        category_link = category.a['href']
        category_url = main_url + category_link
        books_data = get_books_data(category_url)
        all_books.extend(books_data)

    return all_books

def save_to_mongodb(books):
    # Подключение к MongoDB с использованием предоставленных учетных данных
    client = MongoClient(f"mongodb://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}")
    db = client[db_database]  # Имя базы данных
    collection = db[db_table]  # Имя коллекции

    # Вставка данных о книгах в коллекцию
    collection.insert_many(books)

# Вызов функции для скрэйпинга книг
books = scrape_books()

# Сохранение информации в JSON файл
with open('books_data.json', 'w', encoding='utf-8') as file:
    json.dump(books, file, ensure_ascii=False, indent=4)
print("Данные успешно сохранены в books_data.json")

# Сохранение данных в MongoDB
save_to_mongodb(books)
print("Данные успешно перенесены в базу данных MongoDB.")