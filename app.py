import csv
import hashlib
import os
import psycopg2
import requests
import schedule
import threading
import time
from io import StringIO
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

disable_warnings(InsecureRequestWarning)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://user12345:password@localhost:5432/phone_numbers_db'

db = SQLAlchemy(app)


class PhoneNumbers(db.Model):
    __tablename__ = 'phone_numbers'
    __table_args__ = {'schema': 'phone_numbers_schema'}

    id = db.Column(db.Integer, primary_key=True)
    abc_def = db.Column(db.Integer)
    start_number = db.Column(db.Integer)
    end_number = db.Column(db.Integer)
    capacity = db.Column(db.Integer)
    operator = db.Column(db.String(500))
    region = db.Column(db.String(500))
    inn = db.Column(db.String(20))
    file = db.Column(db.String(20))

    def __init__(self, abc_def, start_number, end_number, capacity, operator, region, inn, file):
        self.abc_def = abc_def
        self.start_number = start_number
        self.end_number = end_number
        self.capacity = capacity
        self.operator = operator
        self.region = region
        self.inn = inn
        self.file = file


# Функция расписания для запуска функции update_data каждый день в 00:00
def run_scheduler():
    print('Start scheduler...')
    schedule.every().day.at("00:00").do(update_data)
    while True:
        schedule.run_pending()
        time.sleep(1)


# Функция обновления БД из данных csv файлов
def update_data():
    with app.app_context():
        print('Updating data...')

        base_url = 'https://opendata.digital.gov.ru/registry/numeric/downloads/'
        response = requests.get(base_url, verify=False)

        print('Downloading CSV files...')

        soup = BeautifulSoup(response.content, 'html.parser')
        urls = [urljoin(base_url, link.get('href')) for link in soup.find_all('a') if '.csv' in link.get('href')]
        urls = [url.split('.csv')[0] + '.csv' for url in urls]

        updated = False
        update_all = False

        if PhoneNumbers.query.count() == 0:
            update_all = True

        for url in urls:
            print(f'Reading {url}...')
            file_name = os.path.basename(url)
            print(f'Checking if {file_name} has changed...')
            response = requests.get(url, verify=False)
            data = response.content

            # Вычисляем хеш-сумму файла
            hash_md5 = hashlib.md5(data).hexdigest()
            file_path = f'./{file_name}.md5'

            if update_all:
                print('PhoneNumbers table is empty. Updating from all files.')
            else:
                # Если файл хеша уже существует, сравниваем хеш-суммы
                if os.path.exists(file_path):
                    with open(file_path, 'r') as f:
                        old_hash = f.read()
                        if old_hash == hash_md5:
                            print(f'{file_name} has not changed')
                            continue

            # Если файл хеша не существует или хеш-суммы не совпадают, обновляем базу данных
            with open(file_path, 'w') as f:
                f.write(hash_md5)

            print(f'{file_name} has changed. Updating database...')

            updated = True

            # Удаляем старые записи соответствующие файлу
            PhoneNumbers.query.filter(PhoneNumbers.file == file_name).delete()

            data = response.text
            reader = csv.reader(StringIO(data), delimiter=';')
            next(reader)  # Пропустить первую строку

            # Обновляем таблицу в базе данных
            for row in reader:
                abc_def = int(row[0])
                start_number = int(row[1])
                end_number = int(row[2])
                capacity = int(row[3])
                operator = row[4]
                region = row[5]
                inn = row[6]
                file = file_name

                range_row = PhoneNumbers(abc_def=abc_def, start_number=start_number, end_number=end_number,
                                         capacity=capacity, operator=operator, region=region, inn=inn, file=file)
                db.session.add(range_row)

            print(f'{file_name} has been updated')

        if updated:
            print('Committing changes to the database...')
            db.session.commit()
            print('Data updated')
        else:
            print('Data has not changed')


# Функция поиска телефона в БД
def get_operator_and_region(number):
    if number.startswith('+7'):
        number = '7' + number[2:]
    elif number.startswith('8'):
        number = '7' + number[1:]

    if len(number) != 11 or number[0] != '7':
        return None, None
    prefix = int(number[1:4])  # вычленяем код страны из номера телефона

    phone_number = PhoneNumbers.query.filter(
        PhoneNumbers.abc_def == prefix,
        PhoneNumbers.start_number <= int(number[4:]),
        PhoneNumbers.end_number >= int(number[4:])
    ).first()

    if phone_number:
        return phone_number.operator, phone_number.region
    else:
        return None, None


# Отрисовка главной страницы - форма для запроса и получания ответа
@app.route('/')
def index():
    return '''
        <form method="get" action="/api/lookup">
            <input type="text" name="number" placeholder="Введите номер телефона">
            <input type="submit" value="Искать">
        </form>
        <div id="result"></div>
        <script>
            const form = document.querySelector('form');
            const result = document.querySelector('#result');

            form.addEventListener('submit', (event) => {
                event.preventDefault();
                const formData = new FormData(form);
                const xhr = new XMLHttpRequest();
                xhr.open('GET', form.action + '?' + new URLSearchParams(formData));
                xhr.onload = () => {
                    if (xhr.status === 200) {
                        const response = JSON.parse(xhr.responseText);
                        if (response.error) {
                            result.innerHTML = 'Номер телефона не найден';
                        } else {
                            const { number, operator, region } = response;
                            result.innerHTML = `Номер: ${number}<br>Оператор: ${operator}<br>Регион: ${region}`;
                        }
                    } else {
                        result.innerHTML = 'Ошибка';
                    }
                };
                xhr.send();
            });
        </script>
    '''


# API эндпоинт для запроса данных по номеру телефона
@app.route('/api/lookup')
def api_lookup():
    number = request.args.get('number')
    operator, region = get_operator_and_region(number)
    if operator and region:
        return jsonify({
            'number': number,
            'operator': operator,
            'region': region
        })
    else:
        return jsonify({'error': 'Phone number not found'})


# Главная функция в которой: 1) Создаются необходимые БД, схема, пользователь, таблица, если они отсутствуют;
# 2) Запускается обновление при старте;
# 3) Запускается расписание по которому будет происходить регулярное обновление;
# 4) Запускается локальный Flask-сервер, доступный по адресу: http://127.0.0.1:5000/

if __name__ == '__main__':
    print('Connecting to PostgreSQL...')
    # Подключаемся к PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="1234"
    )
    print('Connected to PostgreSQL')

    conn.autocommit = True

    # Создаем курсор для выполнения SQL-запросов
    cur = conn.cursor()

    # Проверяем, существует ли база данных
    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'phone_numbers_db'")
    db_exists = cur.fetchone()

    # Если база данных не существует, то создаем ее
    if not db_exists:
        cur.execute("CREATE DATABASE phone_numbers_db")

    # Закрываем курсор и соединение
    cur.close()
    conn.close()

    # Подключаемся к базе данных phone_numbers_db
    print('Connecting to PostgreSQL...')
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="1234",
        dbname="phone_numbers_db"
    )
    print('Connected to PostgreSQL')

    conn.autocommit = True

    # Создаем курсор для выполнения SQL-запросов
    cur = conn.cursor()

    # Проверяем, существует ли схема
    cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = 'phone_numbers_schema'")
    schema_exists = cur.fetchone()

    # Если схема phone_numbers_schema не существует, то создаем ее внутри базы данных phone_numbers_db
    if not schema_exists:
        cur.execute("CREATE SCHEMA phone_numbers_schema")

    cur.execute("SELECT 1 FROM pg_roles WHERE rolname='user12345';")
    us_exists = cur.fetchone()

    if not us_exists:
        # создаем пользователя и назначаем ему права на базу данных
        cur.execute("CREATE USER user12345 WITH PASSWORD 'password'")

        cur.execute("GRANT ALL PRIVILEGES ON DATABASE phone_numbers_db TO user12345")

        # Назначаем права на схему для пользователя
        cur.execute("GRANT ALL ON SCHEMA phone_numbers_schema TO user12345")

    # Закрываем курсор и соединение
    cur.close()
    conn.close()

    with app.app_context():
        db.create_all()

        update_data()

        scheduler_thread = threading.Thread(target=run_scheduler)
        scheduler_thread.start()

        app.run()
