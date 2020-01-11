
-- OLAP Cubes 

-- Using sakila database to understand OLAP cube query optimization

-- OLAP (Online Analytical processing)


SELECT
    dd.day,
    dm.rating,
    ds.city,
    sum(f.sales_amount) as revenue
From
    fact_sales f
    INNER JOIN dimdate dd on(dd.date_key = f.date_key)
    INNER JOIN dimmovie dm on(dm.movie_key = f.movie_key)
    INNER JOIN dimstore ds on(ds.store_key = f.store_key)
GROUP By
    dd.`day`,
    dm.rating,
    ds.city
Order BY
    revenue desc;


-- let's do OLAP slicing on rating

-- fixing a particular attribute

SELECT
    dd.day,
    dm.rating,
    ds.city,
    sum(f.sales_amount) as revenue
From
    fact_sales f
    INNER JOIN dimdate dd on(dd.date_key = f.date_key)
    INNER JOIN dimmovie dm on(dm.movie_key = f.movie_key)
    INNER JOIN dimstore ds on(ds.store_key = f.store_key)
Where 
	dm.rating = "PG-13"
GROUP By
    dd.`day`,
    dm.rating,
    ds.city
Order BY
    revenue desc;



-- let's do OLAP dicing :

-- Extracting a small data cube by giving values in set for a column

SELECT
    dd.day,
    dm.rating,
    ds.city,
    sum(f.sales_amount) as revenue
From
    fact_sales f
    INNER JOIN dimdate dd on(dd.date_key = f.date_key)
    INNER JOIN dimmovie dm on(dm.movie_key = f.movie_key)
    INNER JOIN dimstore ds on(ds.store_key = f.store_key)
Where
	dm.rating IN ('PG-13','PG')
	AND
	dd.day IN ('1','15','30')
	AND
	ds.city IN ('Lethbridge')
GROUP By
    dd.`day`,
    dm.rating,
    ds.city
Order BY
    revenue desc;

-- Let's do OLAP roll-up

-- from city to Country level
-- Moving from low level overview to a high level overview

SELECT
    dd.day,
    dm.rating,
    ds.country,
    sum(f.sales_amount) as revenue
From
    fact_sales f
    INNER JOIN dimdate dd on(dd.date_key = f.date_key)
    INNER JOIN dimmovie dm on(dm.movie_key = f.movie_key)
    INNER JOIN dimstore ds on(ds.store_key = f.store_key)
GROUP By
    dd.`day`,
    dm.rating,
    ds.country
Order BY
    revenue desc;


-- Now Some OLAP drill down
-- city -> district
-- going into the details for a given attribute for more insights.
SELECT
    dd.day,
    dm.rating,
    ds.district,
    sum(f.sales_amount) as revenue
From
    fact_sales f
    INNER JOIN dimdate dd on(dd.date_key = f.date_key)
    INNER JOIN dimmovie dm on(dm.movie_key = f.movie_key)
    INNER JOIN dimstore ds on(ds.store_key = f.store_key)
GROUP By
    dd.`day`,
    dm.rating,
    ds.district
Order BY
    revenue desc;



-- Grouping sets

-- As we can see we can group our data by many different combinations like nothing,country,month etc

-- also we can take n no of different comibations.

--  we can do all of that using what's called a grouping set
-- we are adding different levels of analysis in a single query
-- Whenever there is a change in value of grouping column, mysql will add a super-aggregator with name
-- null that will sum up all the values.

-- Grouping set in implemented using RollUP keyword and Grouping function in MySql.. 
SELECT
    dd.`month`,
    ds.country,
    sum(f.sales_amount) as revenue
From
    fact_sales f
    INNER JOIN dimdate dd on(dd.date_key = f.date_key)
    INNER JOIN dimstore ds on(ds.store_key = f.store_key)
GROUP By dd.`month`,ds.country WITH ROLLUP;


SELECT
    dd.`month`,
    dm.rating,
    ds.country,
    sum(f.sales_amount) as revenue
From
    fact_sales f
    INNER JOIN dimdate dd on(dd.date_key = f.date_key)
    INNER JOIN dimstore ds on(ds.store_key = f.store_key)
    INNER JOIN dimmovie dm on(dm.movie_key = f.movie_key)
GROUP By dd.`month`,ds.country,dm.rating WITH ROLLUP;


-- read more about how mysql handle OLAP : https://dev.mysql.com/doc/refman/8.0/en/group-by-modifiers.html

-- doing grouping sets the right way
SELECT
    IF(GROUPING(dd.`month`),"Sub-total",dd.`month`) as `Month`,
    IF(GROUPING(ds.country),"total_month_wise",ds.country) as `Country`,
    sum(f.sales_amount) as revenue
From
    fact_sales f
    INNER JOIN dimdate dd on(dd.date_key = f.date_key)
    INNER JOIN dimstore ds on(ds.store_key = f.store_key)
GROUP By dd.`month`,ds.country WITH ROLLUP;

-- remember if you don't format your data like mentioned above make sure to remove null from any col
-- as null in Roll up have a special meaning.


-- we can do this with cubes also but mysql dosen't have that support yet. PostgresSQL has i think.