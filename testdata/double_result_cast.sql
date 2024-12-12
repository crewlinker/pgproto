SELECT
    id AS id_1,
    CAST(CAST(salary AS numeric(10, 2)) AS TEXT) AS salary_text_100
FROM
    employees;

