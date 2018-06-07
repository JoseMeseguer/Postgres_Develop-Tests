set search_path to tests;

-- EJEMPLO DE UNA WINDOW FUNCTION SOBRE LA TABLA SALES DONDE SE MUESTRA EL PROMEDIO MENSUAL
-- DE VENTAS PARA PODER COMPARARLO CON LAS VENTAS INDIVIDUALMENTE
SELECT id,  begindate, extract (month from begindate) as month, price, client, avg(price) 
OVER (PARTITION BY extract (month from begindate) ) AS monthavg 
FROM sales order by price desc;

--Se puede reescribir la consulta anterior utilizando la clausula WINDOW:
SELECT id, begindate, extract (month from begindate) as month, price, client, avg(price) OVER window_month as monthavg 
FROM sales WINDOW window_month AS (PARTITION BY extract (month from begindate))
order by price desc;

--Añadiremos el valor maximo de venta del mes. Como las dos funciones ventana utilizan la misma ventana, 
--lo lógico es utilizar una sola cláusula WINDOW, si no la utilizáramos tendriamos que definir la ventana
--dos veces con OVER().
SELECT id, begindate, extract (month from begindate) as month, price, client, 
  avg(price) OVER window_month as monthavg, 
  max(price) OVER window_month as maxsale FROM sales 
WINDOW window_month AS (PARTITION BY extract (month from begindate))
order by id desc;

--A partit de este formato de presentacion podemos mostrar la diferencia entre la media de ventas y las ventas 
SELECT id, begindate, extract (month from begindate) as month, price, client, 
	price - avg(price) OVER window_month as diffpriceavg 
FROM sales WINDOW window_month AS (PARTITION BY extract (month from begindate))
order by id desc;


-- si en algun momento nos interesa podemos calcular la media con toda la informacion contenida en la tabla
-- sin aplicar filtros o particiones por mes, definiendo la cláusula WINDOW vacia para que la ventana definida 
-- abarque todos los datos
SELECT id, begindate, extract (month from begindate) as month, price, client, 
	price - avg(price) OVER window_month as diffpriceavg 
FROM sales WINDOW window_month AS ()
order by id desc;   


--Ejemplo con dos ventanas: Una servirá para hallar en que posición (ranking) se encuentra cada venta por mes
--y la otra para hallar en que posición (ranking) se encuentra cada venta por mes en relación a la fecha. 
--Utilizaremos la función dense_rank().
SELECT id, begindate, extract (month from begindate) as month, price, client, 
dense_rank() OVER window_price as SalePosByPrice,
dense_rank() OVER window_date as SalePosByDate
FROM sales 
WINDOW window_price AS (PARTITION BY extract (month from begindate) order by price desc ),
	   window_date  AS (PARTITION BY extract (month from begindate) order by begindate desc )
order by SalePosByPrice, SalePosByDate;


--Podemos utilizar una cláusula WHERE para acotar el resultado a los datos de un trimestre.
SELECT id, begindate, extract (month from begindate) as month, price, client, 
dense_rank() OVER window_price as SalePosByPrice,
dense_rank() OVER window_date as SalePosByDate
FROM sales
WHERE begindate between '01-01-2015' AND '31-03-2015'
WINDOW window_price AS (PARTITION BY extract (month from begindate) order by price desc ),
	   window_date  AS (PARTITION BY extract (month from begindate) order by begindate desc )
order by SalePosByPrice, SalePosByDate;

-- si los restringimos a un solo mes podremos conseguir el mismo resultado reescribiendo la consulta anterior
-- sin usar la clausula PARTITION BY, ya que el resultado solo tiene datos de un departamento.
SELECT id, begindate, extract (month from begindate) as month, price, client, 
dense_rank() OVER window_price as SalePosByPrice,
dense_rank() OVER window_date as SalePosByDate
FROM sales
WHERE begindate between '01-01-2015' AND '31-01-2015'
WINDOW window_price AS (PARTITION BY extract (month from begindate) order by price desc ),
	   window_date  AS (PARTITION BY extract (month from begindate) order by begindate desc )
order by SalePosByPrice, SalePosByDate;
 
-- seria equivalente a:
SELECT id, begindate, extract (month from begindate) as month, price, client, 
dense_rank() OVER window_price as SalePosByPrice,
dense_rank() OVER window_date as SalePosByDate
FROM sales
WHERE begindate between '01-01-2015' AND '31-01-2015'
WINDOW window_price AS (order by price desc),
	   window_date  AS (order by begindate desc)
order by SalePosByPrice, SalePosByDate;


-- y en este caso particular podriamos utilizar una subconsulta pero la eficiencia se resentiria bastante. 
SELECT * FROM (SELECT id, begindate, extract (month from begindate) as month, price, client, 
dense_rank() OVER window_price as SalePosByPrice,
dense_rank() OVER window_date as SalePosByDate
FROM sales
WINDOW window_price AS (order by price desc),
	   window_date  AS (order by begindate desc) ) AS subquery
WHERE begindate between '01-01-2015' AND '31-01-2015'
order by SalePosByPrice, SalePosByDate;

--La consulta equivalente para toda la tabla seria:
SELECT id, begindate, extract (month from begindate) as month, price, client, 
dense_rank() OVER window_price as SalePosByPrice,
dense_rank() OVER window_date as SalePosByDate
FROM sales
WINDOW window_price AS (order by price desc),
	   window_date  AS (order by begindate desc)
order by SalePosByPrice, SalePosByDate;
