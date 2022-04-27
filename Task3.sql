# Task 3

-- *************************************************************************
-- DATABASE SETUP
-- *************************************************************************

-- ******* Table structure for table City *******

CREATE TABLE City (
  City_id int NOT NULL PRIMARY KEY,
  City_name varchar(200) NOT NULL,
  Expense bigint NOT NULL
);

-- ******* Dumping data for table City *******

INSERT INTO City (City_id, City_name, Expense) VALUES
(2001, 'Chicago', 500),
(2002, 'Newyork', 1000),
(2003, 'SFO', 2000),
(2004, 'Florida', 800);


-- ******* Table structure for table Customer *******

create type gender_t as enum('M', 'F');

CREATE TABLE Customer (
  Customer_id int NOT NULL PRIMARY KEY,
  Customer_name varchar(200) NOT NULL,
  Gender gender_t NOT NULL,
  Age int NOT NULL
);

-- ******* Dumping data for table Customer *******

INSERT INTO Customer (Customer_id, Customer_name, Gender, Age) VALUES
(1001, 'John', 'M', 25),
(1002, 'Mark', 'M', 40),
(1003, 'Martha', 'F', 55),
(1004, 'Selena', 'F', 34);

-- ******* Table structure for table Visits *******

CREATE TABLE Visits (
  Customer_id int NOT NULL,
  City_id_visited int NOT NULL,
  Date_visited date NOT NULL
); 

-- ******* Dumping data for table Visits *******

INSERT INTO Visits (Customer_id, City_id_visited, Date_visited) VALUES
(1001, 2003, '2003-01-01'),
(1001, 2004, '2004-01-01'),
(1002, 2001, '2001-01-01'),
(1004, 2003, '2003-01-01');


-- *************************************************************************
-- SQL QUESTIONS
-- *************************************************************************



-- ######################
-- 1) Cities frequently visited?
-- Number of times each city was visited.
-- ######################

SELECT city_id_visited, City_name, Frequency
        FROM (SELECT city_id_visited, COUNT(*) AS Frequency
                FROM Visits v
                GROUP BY city_id_visited
              ) f
INNER JOIN City c 
ON f.city_id_visited = c.City_id
ORDER BY Frequency DESC;


-- ######################
-- 2) Customers visited more than 1 city?
-- List of Customers who visited more than 1 city and the number of cities visited by each customer
-- ######################

SELECT c.Customer_id, Customer_name, cities_explored_count
        FROM (SELECT Customer_id, Count(city_id_visited) AS cities_explored_count
                FROM Visits
                GROUP BY Customer_id
                HAVING Count(city_id_visited) > 1
        ) f
INNER JOIN Customer c
ON f.Customer_id = c.Customer_id
ORDER BY cities_explored_count DESC;



-- ######################
-- 3) Cities visited breakdown by gender?
-- Cities visit count by gender
-- ######################

SELECT City_name, gender, COUNT(*)
FROM (
      SELECT gender, city_name 
                FROM Customer cs
                INNER JOIN Visits v
                        USING (Customer_id)
                INNER JOIN City c
                        ON v.city_id_visited = c.City_id
        ) g_table
GROUP BY (city_name, gender)
ORDER BY (city_name, gender);



-- ######################
-- 4) List the city names that are not visited by every customer and order them by the expense budget in ascending order?
-- Cities that are not visited by everyone sorted by expense ASC
-- ######################


SELECT DISTINCT cc.City_name, cc.Expense
FROM (
        SELECT DISTINCT City_id, City_name, Customer_id, Expense 
        FROM City, Customer
     ) cc
WHERE (cc.City_id, cc.Customer_id) 
        NOT IN (
                SELECT DISTINCT v.City_id_visited, v.Customer_id FROM Visits v
                )
ORDER BY Expense;


-- ######################
-- 5) Visit/travel Percentage for every customer?
-- Customer Visit as a Percentage for all the visits
-- ######################

SELECT Customer_name, visit_percent 
FROM (
        SELECT customer_id, COUNT(*) as customer_count,
               COUNT(*) * 100.0/ SUM(COUNT(*)) OVER () AS visit_percent
        FROM visits
        GROUP BY customer_id
     ) AS visit_percentage
INNER JOIN Customer 
USING (customer_id);


-- ######################
-- 6) Total expense incurred by customers on their visits?
-- Sum of all the expense per customer for all of time.
-- ######################

SELECT Customer_name, Total_Expenditure FROM 
        (SELECT Customer_id, SUM(Expense)AS Total_Expenditure FROM
                (SELECT Customer_id, City_id_visited, Expense
                FROM Visits v
                INNER JOIN City c
                ON v.City_id_visited = c.City_id) m
        GROUP BY Customer_id
        ) expenses
INNER JOIN
Customer USING (Customer_id)
ORDER BY Total_Expenditure;


-- ######################
-- 7) list the Customer details along with the city they first visited and the date of visit?
-- The first city each customer visited
-- ######################

SELECT c.*, ct.City_name, fv.First_Visited 
FROM Visits v, 
     (  SELECT Customer_id, MIN(Date_visited) AS First_Visited 
         FROM Visits vs
        GROUP BY Customer_id
     ) fv,
     Customer c,
     City ct 
WHERE v.Customer_id = fv.Customer_id
        AND v.Date_visited = fv.First_Visited
        AND fv.Customer_id = c.Customer_id
        AND ct.City_id = v.City_id_visited;


