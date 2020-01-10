/* we are doing some ETL on sakila database*/

-- Lets see how much data we have

Select (Select Count(*) From store) as nStore,(Select Count(*) From film) as nFilms,
(Select Count(*) From customer) as nCustomers,(Select Count(*) From rental) as nRental,
(Select Count(*) From payment)as nPayment,(Select Count(*) From staff) as nStaff,
(Select Count(*) From city) as nCity,(Select Count(*) From country) as nCountry;


-- which time peroid are we talking about

Select min(payment_date) as start_,max(payment_date) as end_ From payment;

-- where are things happening ?

Select * From address LIMIT 10;

select district,Count(city_id) as n From address GROUP BY district ORDER BY Count(city_id) DESC LIMIT 20;

-- Let's do Some simple data-analysis.
-- # Top grossing movies : 
SELECT * FROM film;

SELECT * FROM payment;

-- as we can see payment and film are not directly linked 
-- payment -> rental -> inventory -> film
SELECT * FROM payment; -- rental_id
SELECT * From rental; -- rental_id
SELECT * FROM inventory;-- film_id

Select
    f.film_id,
    f.title,
    P.amount,
    P.payment_date,
    P.customer_id
From
    payment P
    Inner Join rental r on (P.rental_id = r.rental_id)
    INNER JOIN inventory I On(I.inventory_id = r.inventory_id)
    INNER JOIN film f ON(I.film_id = f.film_id)
LIMIT
    20;
-- some more indepth

SELECT
    title,
    sum(amount) as total_sale
From(
        Select
            f.film_id,
            f.title,
            P.amount,
            P.payment_date,
            P.customer_id
        From
            payment P
            Inner Join rental r on (P.rental_id = r.rental_id)
            INNER JOIN inventory I On(I.inventory_id = r.inventory_id)
            INNER JOIN film f ON(I.film_id = f.film_id)
    ) as L
GROUP BY
    title
Order BY
    sum(amount) desc
LIMIT
    25;


-- # Top grossing cities
-- Since payment amount and city details does not live in same table
-- Payment -> Customer -> address -> City

SELECT
    C.city,
    Sum(P.amount) as total_rev
From
    payment P
    Inner Join customer cus On(P.customer_id = cus.customer_id)
    Inner Join address A On(cus.address_id = A.address_id)
    Inner Join city C On (C.city_id = A.city_id) GROUP By C.city ORDER BY total_rev desc LIMIT 25
    ;

-- # Revenue of a movie by customer, city and month

-- By month

SELECT
    MONTH(payment_date) as Mnth,
    sum(amount) as total_rev
From
    payment
GROUP BY
    Mnth
Order by
    2 desc;
-- # each movie by customer,city and month(data cube)
-- we want title,amount,cust_id,city,payment_date,month

SELECT
    f.title,
    P.amount,
    P.customer_id,
    MONTH(P.payment_date) as 'Month',
    ci.city
From
    payment P
    inner join rental r on (P.rental_id = r.rental_id)
    INNER JOIN inventory i on (i.inventory_id = r.inventory_id)
    Inner Join film f on (i.film_id = f.film_id)
    INNER JOIN customer c On (P.customer_id = c.customer_id)
    Inner join address a on (c.address_id = a.address_id)
    Inner Join city ci On (ci.city_id = a.city_id)
Order by
    2 desc
LIMIT
    10;

-- Aggregation forming a data cube: 

SELECT
    f.title,
    MONTH(P.payment_date) as 'Month',
    ci.city,
    Sum(P.amount) as total_rev
From
    payment P
    inner join rental r on (P.rental_id = r.rental_id)
    INNER JOIN inventory i on (i.inventory_id = r.inventory_id)
    Inner Join film f on (i.film_id = f.film_id)
    INNER JOIN customer c On (P.customer_id = c.customer_id)
    Inner join address a on (c.address_id = a.address_id)
    Inner Join city ci On (ci.city_id = a.city_id)
GROUP BY
	f.title,ci.city,'Month'
ORDER BY
	total_rev DESC;


-- Now we will create facts and dimension table and populate them :

/*

fact_table : fact_sales {
sales_key,date_key,customer_key,movie_key,store_key,sales_amount
}

dimension tables :

1. dimDate{date_key,date,year,quarter,month,day,week,is_weekend}

2. dimCustomer{customer_key,customer_id,first_name,last_name,email,address,address2,district,
city,country,postal_code,phone,active,create_date,start_date,end_date}

3. dimMovie{movie_key,film_id,title,description,release_year,language,original_lang,
rental_duration,length,rating,special_features}

4. dimStore{store_key,store_id,address,address2,district,city,country,postal_code,
manager_first_name,manager_last_name,start_date,end_date}

*/


-- extra ## debug area : 

--set FOREIGN_KEY_CHECKS = 0;

--TRUNCATE TABLE dimdate;

-- drop table dimdate;

-- ## 

-- create dimension table dimDate :

CREATE TABLE dimDate(
date_key INT PRIMARY KEY,
`date` DATE NOT NULL,
`year` SMALLINT	NOT NULL,
quarter SMALLINT NOT NULL,
`month` SMALLINT NOT NULL,
`day` SMALLINT NOT NULL,
`week` SMALLINT NOT NULL,
is_weekend BOOLEAN NOT NULL
);

desc dimdate;

-- create dimension table dimCustomer:

CREATE TABLE dimCustomer(
customer_key Int PRIMARY KEY,
customer_id SMALLINT NOT NULL,
first_name VARCHAR(45) NOT NULL,
last_name VARCHAR(45) NOT NULL,
email VARCHAR(50),
address VARCHAR(50) NOT NULL,
address2 VARCHAR(50),
`district` VARCHAR(30) NOT NULL,
city VARCHAR(50) NOT NULL,
country VARCHAR(50) NOT NULL,
postal_code INT,
phone VARCHAR(20) NOT NULL,
active SMALLINT NOT NULL,
create_date TIMESTAMP NOT NULL,
start_date DATE NOT NULL,
end_date DATE NOT NULL
);

desc dimcustomer;
-- create dimension table dimMovie:

CREATE TABLE dimMovie(
movie_key INT PRIMARY KEY,
film_id INT NOT NULL,
title VARCHAR(255) NOT NULL,
description TEXT,
release_year YEAR,
`language` VARCHAR(20) NOT NULL,
original_language VARCHAR(20),
rental_duration SMALLINT NOT NULL,
`length` SMALLINT NOT NULL,
rating VARCHAR(5) NOT NULL,
special_features VARCHAR(60) NOT NULL
);

desc dimmovie;


-- create dimension table dimStore:

CREATE TABLE dimStore(
store_key INT PRIMARY KEY,
store_id SMALLINT NOT NULL,
address VARCHAR(50) NOT NULL,
address2 VARCHAR(50),
`district` VARCHAR(30) NOT NULL,
city VARCHAR(50) NOT NULL,
country VARCHAR(50) NOT NULL,
postal_code INT,
manager_first_name VARCHAR(45) NOT NULL,
manager_last_name VARCHAR(45) NOT NULL,
start_date DATE NOT NULL,
end_date DATE NOT NULL
);

desc dimstore;
-- creating fact table


CREATE TABLE fact_sales(
sales_key INT Auto_Increment PRIMARY KEY,
date_key INT NOT NULL,
customer_key INT NOT NULL,
movie_key INT NOT NULL,
store_key INT NOT NULL,
sales_amount FLOAT NOT NULL,
FOREIGN KEY(date_key) REFERENCES dimdate(date_key),
FOREIGN KEY(customer_key) REFERENCES dimcustomer(customer_key),
FOREIGN KEY(movie_key) REFERENCES dimmovie(movie_key),
FOREIGN KEY(store_key) REFERENCES dimstore(store_key)
);

desc fact_sales;


-- Now let's query data From our 3NF tables and fill our star schema database

-- dimDate

desc dimdate;

INSERT INTO
    dimdate(
    	`date_key`,
        `date`,
        `year`,
        `quarter`,
        `month`,
        `day`,
        `week`,
        `is_weekend`
    )
SELECT
	DISTINCT CAST(DATE_FORMAT(payment_date, "%y%m%d") AS UNSIGNED) as `date_key`,
    date(payment_date) as `date`,
    YEAR(payment_date) as `year`,
    QUARTER(payment_date) as `quarter`,
    MONTH(payment_date) as `month`,
    DAY(payment_date) as `day`,
    Week(payment_date) as `week`,
    IF(Weekday(payment_date) IN (6, 7), True, False) as `is_weekend`
FROM
    payment;

SELECT * FROM dimDate;

-- dimMovie

desc dimmovie;

INSERT INTO
    dimmovie(
        movie_key,
        film_id,
        title,
        description,
        release_year,
        `language`,
        original_language,
        rental_duration,
        `length`,
        rating,
        special_features
    )
SELECT
    film_id as `movie_key`,
    film_id,
    title,
    description,
    release_year,(
        SELECT
            name
        From
            language
        Where
            `language_id` = f.language_id
    ) as `language`,(
        SELECT
            name
        From
            language
        Where
            `language_id` = f.original_language_id
    ) as original_language,
    rental_duration,
    `length`,
    rating,
    special_features
From
    film f;


SELECT * From dimmovie;


-- dimStore : 
Alter table dimstore modify column postal_code VARCHAR(50);

DESC dimstore;
INSERT INTO
    dimstore(store_key,store_id,address,address2,district,city,country,postal_code,manager_first_name,
    manager_last_name,start_date,end_date)
SELECT
	St.store_id as `store_key`,
    St.store_id,
    ad.address,
    ad.address2,
    ad.district,
    c.city,
    con.country,
    ad.postal_code,
    Sta.first_name,
    Sta.last_name,
    now(),
    now()
From
    store St
    Inner Join address ad On (St.address_id = ad.address_id)
    Inner Join city c On (c.city_id = ad.city_id)
    Inner Join country con On (con.country_id = c.country_id)
    Inner Join staff Sta On (Sta.staff_id = St.manager_staff_id);
 
SELECT * From dimstore;



-- dimCustomer;
Alter table dimcustomer modify column postal_code VARCHAR(50);
desc dimcustomer;


INSERT into
    dimcustomer(customer_key,customer_id,first_name,last_name,email,address,address2,district,city,
    country,postal_code,phone,active,create_date,start_date,end_date)
SELECT
	cus.customer_id as `customer_key`,
    cus.customer_id,
    cus.first_name,
    cus.last_name,
    cus.email,
    ad.address,
    ad.address2,
    ad.district,
    ci.city,
    con.country,
    ad.postal_code,
    ad.phone,
    cus.active,
    cus.create_date,
    Now(),
    Now()
From
    customer cus
    Inner JOIN address ad On (ad.address_id = cus.address_id)
    Inner Join city ci On(ci.city_id = ad.city_id)
    INNER Join country con On(con.country_id = ci.country_id);

SELECT * From dimcustomer;



-- fact table;
desc fact_sales;

INSERT INTO
    fact_sales(
        date_key,
        customer_key,
        movie_key,
        store_key,
        sales_amount
    )
SELECT
    CAST(DATE_FORMAT(P.payment_date, "%y%m%d") AS UNSIGNED) as `date_key`,
    P.customer_id as `customer_key`,
    f.film_id as `movie_key`,
    i.store_id as `store_key`,
    P.amount as `sales_amount`
From
    payment P
    Inner JOIN rental r on (r.rental_id = P.rental_id)
    inner JOIN inventory i On(i.inventory_id = r.inventory_id)
    Inner Join film f On(f.film_id = i.film_id);


SELECT * From fact_sales;




/* Now let's do the same anlysis with our star schema in place that we did  from 3NF*/

-- Sales amount by movie,month and city

desc fact_sales;
desc dimmovie;
desc dimcustomer;

SELECT
    dm.title,
    dd.`month`,
    dc.city,
    f.sales_amount
From
    fact_sales f
    Inner Join dimmovie dm on(f.movie_key = dm.movie_key)
    Inner Join dimcustomer dc on(f.customer_key = dc.customer_key)
    Inner JOIN dimdate dd on(f.date_key = dd.date_key) LIMIT 25;


-- Now let's form that data cube from our start schema.

SELECT
    dm.title,
    dd.`month`,
    dc.city,
    sum(f.sales_amount) as total_rev
From
    fact_sales f
    Inner Join dimmovie dm on(f.movie_key = dm.movie_key)
    Inner Join dimcustomer dc on(f.customer_key = dc.customer_key)
    Inner JOIN dimdate dd on(f.date_key = dd.date_key)
    
GROUP BY dm.title,dd.`month`,dc.city
ORDER BY 4 desc;

-- as we can see now the joins are more clear and it takes less time to fetch the data.

