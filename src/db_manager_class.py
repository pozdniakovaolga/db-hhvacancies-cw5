from abc import ABC, abstractmethod
import psycopg2
from config_db import db_config


class Manager(ABC):
    """Абстрактный класс для работы с вакансиями """

    @abstractmethod
    def get_companies_and_vacancies_count(self) -> int:
        """Получает список всех компаний и количество вакансий у каждой компании """
        pass

    @abstractmethod
    def get_all_vacancies(self) -> list:
        """Получает список всех вакансий с указанием названия компании, названия вакансии, зарплаты и ссылки на вак. """
        pass

    @abstractmethod
    def get_avg_salary(self) -> list:
        """Получает среднюю зарплату по вакансиям """
        pass

    @abstractmethod
    def get_vacancies_with_higher_salary(self) -> list:
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям """
        pass

    def get_vacancies_with_keyword(self, keyword) -> list:
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слово """
        pass


class DBManager(Manager):
    """Класс для работы с вакансиями из БД Postgres"""

    def __init__(self, db_name):
        """Создание экземпляра класса DBManager"""
        self.params = db_config()
        self.conn = psycopg2.connect(dbname=db_name, **self.params)

    def get_companies_and_vacancies_count(self) -> list:
        """Получает список всех компаний и количество вакансий у каждой компании """
        with self.conn.cursor() as cur:
            cur.execute("""SELECT employer_name, COUNT(*)
                        FROM vacancies
                        JOIN employers ON vacancies.vacancy_employer=employers.employer_id
                        GROUP BY employer_name""")

            return cur.fetchall()

    def get_all_vacancies(self) -> list:
        """Получает список всех вакансий с указанием названия компании, названия вакансии, зарплаты и ссылки на вак. """
        with self.conn.cursor() as cur:
            cur.execute("""SELECT vacancy_name, vacancy_url, salary_from, salary_to, employer_name
                        FROM vacancies
                        JOIN employers ON vacancies.vacancy_employer=employers.employer_id""")

            return cur.fetchall()

    def get_avg_salary(self) -> tuple:
        """Получает среднюю стартовую зарплату по вакансиям """
        with self.conn.cursor() as cur:
            cur.execute("""SELECT AVG(salary_from) as avg_salary
                        FROM vacancies""")

            return cur.fetchone()

    def get_vacancies_with_higher_salary(self) -> list:
        """Получает список всех вакансий, у которых стартовая зарплата выше средней стартовой по всем вакансиям """
        with self.conn.cursor() as cur:
            cur.execute("""SELECT vacancy_name
                        FROM vacancies
                        WHERE salary_from > (SELECT AVG(salary_from) FROM vacancies)""")

            return cur.fetchall()

    def get_vacancies_with_keyword(self, keyword) -> list:
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слово """
        keyword1 = keyword.title()
        keyword2 = keyword.lower()
        with self.conn.cursor() as cur:
            cur.execute(f"""SELECT vacancy_name
                        FROM vacancies
                        WHERE vacancy_name LIKE '%{keyword1}%' OR vacancy_name LIKE '%{keyword2}%'""")

            return cur.fetchall()

    def close_connection(self) -> None:
        """Закрывает connection """
        self.conn.close()
