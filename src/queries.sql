-- Создает таблицу с работодателями employers
CREATE TABLE IF NOT EXISTS employers (
    employer_id int PRIMARY KEY,
    employer_name varchar(100) NOT NULL,
    employer_url varchar(100)
)

-- Создает таблицу с вакансиями vacancies
CREATE TABLE IF NOT EXISTS vacancies (
    vacancy_id int PRIMARY KEY,
    vacancy_name varchar(100) NOT NULL,
    vacancy_url varchar(100),
    vacancy_employer int REFERENCES employers(employer_id),
    salary_from int,
    salary_to int
)

-- Получает список всех компаний и количество вакансий у каждой компании
SELECT employer_name, COUNT(*)
FROM vacancies
JOIN employers ON vacancies.vacancy_employer=employers.employer_id
GROUP BY employer_name


-- Получает список всех вакансий с указанием названия компании, названия вакансии, зарплаты и ссылки на вак.
SELECT vacancy_name, vacancy_url, salary_from, salary_to, employer_name
FROM vacancies
JOIN employers ON vacancies.vacancy_employer=employers.employer_id


-- Получает среднюю стартовую зарплату по вакансиям
SELECT AVG(salary_from) as avg_salary
FROM vacancies

-- Получает список всех вакансий, у которых стартовая зарплата выше средней стартовой по всем вакансиям
SELECT vacancy_name
FROM vacancies
WHERE salary_from > (SELECT AVG(salary_from) FROM vacancies)

-- Получает список всех вакансий, в названии которых содержатся переданные в метод слово keyword
SELECT vacancy_name
FROM vacancies
WHERE vacancy_name LIKE '%Keyword%' OR vacancy_name LIKE '%keyword%'
