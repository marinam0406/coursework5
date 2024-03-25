import psycopg2
from config import config


class DBManager:
    def __init__(self, database_name, params=config()):
        self.database_name = database_name
        self.params = params

    def get_companies_and_vacancies_count(self):
        """
        Получает список всех компаний и количество вакансий у каждой компании.
        """
        conn = psycopg2.connect(dbname=self.database_name, **self.params)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT employers.employer_name, COUNT(vacancies.vacancy_id) AS vacancy_count 
                FROM employers 
                JOIN vacancies USING (employer_name) 
                GROUP BY employers.employer_name;
                """)

            data = cur.fetchall()
        conn.close()
        return data

    def get_all_vacancies(self):
        """
        Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию.
        """
        conn = psycopg2.connect(dbname=self.database_name, **self.params)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT employers.employer_name, title, salary, vacancies.url 
                FROM vacancies
                JOIN employers USING (employer_name);
                """)

            data = cur.fetchall()
        conn.close()
        return data

    def get_avg_salary(self):
        """
        Получает среднюю зарплату по вакансиям.
        """
        conn = psycopg2.connect(dbname=self.database_name, **self.params)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT employers.employer_name, ROUND(AVG(salary)) 
                FROM vacancies
                JOIN employers USING (employer_name)
                GROUP BY employers.employer_name;
                """)

            data = cur.fetchall()
        conn.close()
        return data

    def get_vacancies_with_higher_salary(self):
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        """
        conn = psycopg2.connect(dbname=self.database_name, **self.params)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM vacancies
                WHERE salary > (SELECT AVG(salary) FROM vacancies);
                """)

            data = cur.fetchall()
        conn.close()
        return data

    def get_vacancies_with_keyword(self, keyword):
        """
        Получает список всех вакансий,
        в названии которых содержатся переданные в метод слова, например python.
        """
        conn = psycopg2.connect(dbname=self.database_name, **self.params)

        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM vacancies "
                        f"WHERE lower(title) LIKE '%{keyword}%' "
                        f"OR lower(title) LIKE '%{keyword}' "
                        f"OR lower(title) LIKE '{keyword}%';")

            data = cur.fetchall()
        conn.close()
        return data

    def get_avg_salary(self):
        """
        Получает среднюю зарплату по вакансиям.
        """
        conn = psycopg2.connect(dbname=self.database_name, **config())

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT employers.employer_name, ROUND(AVG(salary)) 
                FROM vacancies
                JOIN employers USING (employer_name)
                GROUP BY employers.employer_name;
                """)

            data = cur.fetchall()
        conn.close()
        return data

    def get_vacancies_with_higher_salary(self):
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        """
        conn = psycopg2.connect(dbname=self.database_name, **config())

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM vacancies
                WHERE salary > (SELECT AVG(salary) FROM vacancies);
                """)

            data = cur.fetchall()
        conn.close()
        return data

    def get_vacancies_with_keyword(self, keyword):
        """
        Получает список всех вакансий,
        в названии которых содержатся переданные в метод слова, например python.
        """
        conn = psycopg2.connect(dbname=self.database_name, **config())

        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM vacancies "
                        f"WHERE lower(title) LIKE '%{keyword}%' "
                        f"OR lower(title) LIKE '%{keyword}' "
                        f"OR lower(title) LIKE '{keyword}%';")

            data = cur.fetchall()
        conn.close()
        return data