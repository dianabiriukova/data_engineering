/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT
    c.name,
    count(fc.film_id) AS cnt_films
FROM
    category AS c
JOIN
    film_category AS fc ON c.category_id = fc.category_id
GROUP BY
    c.name
ORDER BY
    cnt_films DESC
;
/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- Get the rental frequency for each movie
WITH rental_counts AS (
    SELECT
        fa.actor_id,
        COUNT(r.rental_id) AS total_rentals
    FROM
        film_actor AS fa
    JOIN
        film AS f ON fa.film_id = f.film_id
    JOIN
        inventory AS i ON f.film_id = i.film_id
    JOIN
        rental AS r ON i.inventory_id = r.inventory_id
    GROUP BY
        fa.actor_id
)
SELECT
    a.actor_id,
    CONCAT(a.first_name, ' ', a.last_name) AS actor_name,
    rc.total_rentals
FROM
    rental_counts AS rc
JOIN
    actor AS a ON rc.actor_id = a.actor_id
ORDER BY
    rc.total_rentals DESC
LIMIT 10
;
/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
SELECT
    c.name AS category_name,
    SUM(p.amount) AS total_revenue
FROM
    category AS c
JOIN
    film_category AS fc ON c.category_id = fc.category_id
JOIN
    film AS f ON fc.film_id = f.film_id
JOIN
    inventory AS i ON f.film_id = i.film_id
JOIN
    rental AS r ON i.inventory_id = r.inventory_id
JOIN
    payment AS p ON r.rental_id = p.rental_id
GROUP BY
    c.name
ORDER BY
    total_revenue DESC
LIMIT 1
;
/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
SELECT DISTINCT
    f.film_id,
    f.title
FROM
    film AS f
LEFT JOIN
    inventory AS i ON f.film_id = i.film_id
WHERE
    i.film_id IS NULL
;
/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
SELECT
    a.actor_id,
    first_name || ' ' || last_name AS actor_name,
    COUNT(*) AS cnt_films
FROM
    film_category AS fc
JOIN
    category AS c ON fc.category_id = c.category_id
                 AND c.name = 'Children'
JOIN
    film_actor AS fa ON fa.film_id = fc.film_id
JOIN
    actor AS a ON fa.actor_id = a.actor_id
GROUP BY
    a.actor_id,
    actor_name
ORDER BY
    cnt_films DESC
LIMIT 3
;