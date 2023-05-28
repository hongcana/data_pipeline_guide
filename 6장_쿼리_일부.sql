DROP TABLE IF EXISTS Orders;
DROP TABLE IF EXISTS Customers;
DROP TABLE IF EXISTS order_summary_daily;

CREATE Table Orders (
	OrderId int,
    OrderStatus varchar(30),
    OrderDate timestamp,
    CustomerId int,
    OrderTotal numeric
);

INSERT INTO Orders
VALUES(1, 'Shipped', '2020-06-09', 100, 50.05);
INSERT INTO Orders
VALUES(2, 'Shipped', '2020-07-11', 101, 57.45);
INSERT INTO Orders
VALUES(3, 'Shipped', '2020-07-12', 102, 135.99);
INSERT INTO Orders
VALUES(4, 'Shipped', '2020-07-12', 100, 43.00);

CREATE TABLE Customers
(
	CustomerID int,
    CustomerName varchar(20),
    CustomerCountry varchar(16)
);

INSERT INTO Customers VALUES(100, 'John', 'USA');
INSERT INTO Customers VALUES(101, 'Kim', 'South Korea');
INSERT INTO Customers VALUES(102, 'Lee', 'South Korea');

CREATE TABLE IF NOT EXISTS order_summary_daily (
	order_date date,
    order_country varchar(16),
    total_revenue numeric,
    order_count int
);

INSERT INTO order_summary_daily
		(order_date, order_country, total_revenue, order_count)
SELECT
	o.OrderDate AS order_date,
    c.CustomerCountry AS order_country,
    SUM(o.OrderTotal) AS total_revenue,
    COUNT(o.OrderId) AS order_count
FROM Orders o
INNER JOIN Customers c on o.CustomerId = c.CustomerId
GROUP BY o.OrderDate, c.CustomerCountry;

SELECT * FROM order_summary_daily;

-- 특정 월에 특정 국가에서 발생한 주문으로 발생한 수익은 얼마인가?

SELECT
	date_format(order_date, '%m') as order_month,
	-- MID(order_date, 6, 2) as order_month,
    order_country,
    SUM(total_revenue) as order_revenue
FROM order_summary_daily
GROUP BY
	order_month,
    order_country
ORDER BY order_month, order_country;

-- 특정 날짜에 주문이 얼마나 들어왔는가?

SELECT order_date, SUM(order_count)
FROM order_summary_daily
GROUP BY order_date
ORDER BY order_date;
