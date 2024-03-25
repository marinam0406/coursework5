import json
import logging
import psycopg2
import requests


def get_hh_data(employers):
    """
    Получает данные о работадателях и вакансиях в формате json.
    :return: list[dict[str, Any]]
    """
    data = []
    for employer in employers:
        url = f'https://api.hh.ru/employers/{employer}'
        company_data = requests.get(url).json()
        vacancy_data = requests.get(company_data['vacancies_url']).json()
        data.append({
            'employers': company_data,
            'vacancies': vacancy_data['items']
        })

    return data


def create_database(database_name, params):
    """
    Создает базу данных и таблицы для сохранения вакансий
    :param database_name: str
    :param params: dict
    :return: None
    """
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f'DROP DATABASE IF EXISTS {database_name}')
    cur.execute(f'CREATE DATABASE {database_name}')

    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE employers (
                    employer_id SERIAL PRIMARY KEY,
                    employer_name VARCHAR UNIQUE,
                    url TEXT
                )
            """)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE vacancies (
                    vacancy_id SERIAL PRIMARY KEY,
                    employer_name text REFERENCES employers(employer_name),
                    city VARCHAR(50),
                    title VARCHAR(200),
                    schedule TEXT,
                    requirement TEXT,
                    responsibility TEXT,
                    salary INT,
                    url VARCHAR(200),
                    foreign key(employer_name) references employers(employer_name)
                )
            """)

    conn.commit()
    conn.close()
    logging.info("Таблицы 'employers' и 'vacancies' успешно созданы.")


def save_data_to_database(data, database_name, params):
    """
    Сохраняет данные о вакансиях в базу данных
    :param database_name: list[dict[str, Any]]
    :param data: str
    :param params: dict
    :return: None
    """
    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for employer in data:
            employer_data = employer['employers']
            cur.execute(
                """
                INSERT INTO employers (employer_name, url)
                VALUES (%s, %s)
                RETURNING employer_name
                """,
                (employer_data['name'], employer_data['alternate_url'])
            )

            employer_name = cur.fetchone()[0]

            for vacancy in employer['vacancies']:
                salary = vacancy['salary']['from'] if vacancy['salary'] else None
                cur.execute(
                    """
                    INSERT INTO vacancies (employer_name, city, title, schedule, requirement, 
                    responsibility, salary, url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (employer_name, vacancy['area']['name'], vacancy['name'],
                     vacancy['schedule']['name'], vacancy['snippet']['requirement'],
                     vacancy['snippet']['responsibility'], salary,
                     vacancy['alternate_url'])
                )

    conn.commit()
    conn.close()
