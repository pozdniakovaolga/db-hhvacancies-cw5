import requests
from config_hh import hh_config
import psycopg2


def get_employers() -> list[dict]:
    """Получает с сайта hh.ru данные работодателей по их id """

    employers_list = []

    for employer_id in hh_config.get("employer_ids"):  # перебираем id работодателей из config_hh.py
        employer_hh_url = f"https://api.hh.ru/employers/{employer_id}"  # формируем ссылку на страницу работодателя
        response = requests.get(employer_hh_url)  # делаем request запрос
        if response.status_code == 200:
            employer = response.json()  # переводим полученные данные в json формат
            employers_list.append({"employer_id": employer_id,
                                   "employer_title": employer["name"],
                                   "employer_url": employer["site_url"]})  # собираем данные по работодателям в список
        else:
            print(f"Request for employer {employer_id} failed with status code: {response.status_code}")

    return employers_list


def get_vacancies(page=0) -> list[dict]:
    """Получает с сайта hh.ru данные о вакансиях конкретных работодателей """

    url = "https://api.hh.ru/vacancies"
    params = {
        "page": page,  # номер страницы
        "employer_id": hh_config.get("employer_ids"),  # работодатели
        "only_with_salary": hh_config.get("is_with_salary"),  # указана ли зарплата
        "per_page": hh_config.get("per_page"),  # количество вакансий на стр
    }

    response = requests.get(url, params=params)  # делаем request запрос

    if response.status_code == 200:
        vacancies = response.json()  # переводим полученные данные в json формат
        vacancies_list = []

        for vacancy in vacancies["items"]:
            vacancies_list.append({"vacancy_id": vacancy["id"],
                                   "title": vacancy["name"],
                                   "url": vacancy["alternate_url"],
                                   "employer": vacancy["employer"]["id"],
                                   "salary_from": vacancy["salary"]["from"],
                                   "salary_to": vacancy["salary"]["to"]})  # собираем данные по вакансиям в список

        return vacancies_list

    else:
        print(f"Request failed with status code: {response.status_code}")


def create_database(params, db_name) -> None:
    """Создает новую базу данных """
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"CREATE DATABASE {db_name}")

    cur.close()
    conn.close()


def create_employers_table(cur) -> None:
    """Создает таблицу с работодателями employers """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS employers (
            employer_id int PRIMARY KEY,
            employer_name varchar(100) NOT NULL,
            employer_url varchar(100)
        )
    """)


def create_vacancies_table(cur) -> None:
    """Создает таблицу с вакансиями vacancies """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vacancies (
            vacancy_id int PRIMARY KEY,
            vacancy_name varchar(100) NOT NULL,
            vacancy_url varchar(100),
            vacancy_employer int REFERENCES employers(employer_id),
            salary_from int,
            salary_to int
        )
    """)


def insert_employers_data(cur, employers: list[dict]) -> None:
    """Добавляет данные в таблицу employers """
    for emp in employers:
        cur.execute(
            """
            INSERT INTO employers (employer_id, employer_name, employer_url)
            VALUES (%s, %s, %s)
            """,
            (emp["employer_id"], emp["employer_title"], emp["employer_url"])
        )


def insert_vacancies_data(cur, vacancies: list[dict]) -> None:
    """Добавляет данные в таблицу vacancies """
    for vac in vacancies:
        cur.execute(
            """
            INSERT INTO vacancies (vacancy_id, vacancy_name, vacancy_url, vacancy_employer, salary_from, salary_to)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (vac["vacancy_id"], vac["title"], vac["url"], vac["employer"], vac["salary_from"], vac["salary_to"])
        )
