import psycopg2
from config_db import db_config
from utils import get_employers, get_vacancies, create_database
from utils import create_employers_table, create_vacancies_table, insert_employers_data, insert_vacancies_data
from db_manager_class import DBManager


def main():
    db_name = 'vacancies_db'  # название БД

    params = db_config()  # из database.ini получаем параметры для работы с БД
    conn = None

    create_database(params, db_name)  # создаем БД

    params.update({'dbname': db_name})  # обновляем параметры
    try:
        with psycopg2.connect(**params) as conn:  # подключаемся к БД
            with conn.cursor() as cur:

                create_employers_table(cur)  # создаем таблицу employers
                create_vacancies_table(cur)  # создаем таблицу vacancies

                employers = get_employers()  # по api получаем работодателей
                insert_employers_data(cur, employers)  # загружаем их таблицу employers

                vacancies = get_vacancies()  # по api получаем вакансии
                insert_vacancies_data(cur, vacancies)  # загружаем их таблицу vacancies

                conn.commit()

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    # запускаем цикл вопросов пользователю
    dbm = DBManager(db_name)  # создаем экземпляр класса DBManager
    user_answer = None
    while user_answer != 0:
        user_answer = input("""\nВыберите необходимый вам тип информации:
        1 - список всех компаний и количество вакансий у них
        2 - список всех вакансий с подробными данными по ним
        3 - среднюю стартовую зарплату
        4 - список всех вакансий, у которых стартовая зарплата выше средней
        5 - список всех вакансий, в названии которых содержатся ключевое слово
        0 - выйти из программы """)
        if user_answer == '1':
            print(dbm.get_companies_and_vacancies_count())
        elif user_answer == '2':
            print(dbm.get_all_vacancies())
        elif user_answer == '3':
            print(dbm.get_avg_salary())
        elif user_answer == '4':
            print(dbm.get_vacancies_with_higher_salary())
        elif user_answer == '5':
            kword = input("Введите ключевое слово: ")
            print(dbm.get_vacancies_with_keyword(kword))
        elif user_answer == '0':
            dbm.close_connection()
            break
        else:
            print("Вы ввели неверную команду, попробуйте еще раз")
            continue


if __name__ == '__main__':
    main()
