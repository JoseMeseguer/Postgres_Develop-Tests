-- cambio del esquema por defecto para la simplificacion de nombres
-- como administradores debemos controlar en todo momento nuestro esquema de trabajo
-- por lo que he insistido en que lo escribamos para que nos quede grabado
set search_path to schemaTesteo;

-- OBJETIVO: obtener clientes con mayor volumen de compras del mes de marzo por cada pais, 
--ordenado por volumen de compra

-- UNA POSIBILIDAD A TRAVES DE CASE-WHEN-THEN CON LA COMPROBACION DEL MES CORRECTO
-- LA GENERACION DE CADENAS QUE PERSONALIZARAN LA EJECUCION DE LA QUERY SE PUEDE SIMPLIFICAR
-- PERO EL CONCEPTO ES QUE PUEDO GENERAR EL CODIGO SQL ESPECIFICO A DEMANDA SOBRE UNA 
-- QUERY GENERICA QUE TRAS INVERTIR UN TIEMPO HA RESULTADO LA MAS OPTIMIZADA 
CREATE OR REPLACE FUNCTION sales_bestcases(codemonth int, codeyear int) 
RETURNS TABLE (clientname varchar, bestcase float, countryname varchar) AS 
$$
declare myquery varchar;
declare dateini varchar;
declare datefin varchar;
declare error integer;
BEGIN 
	error := 0;
	CASE codemonth
		WHEN 1 THEN dateini:= '>=''01-01-' || codeyear::varchar || '''::timestamptz';
					datefin:= '<=''31-01-' || codeyear::varchar || '''::timestamptz';
		WHEN 2 THEN dateini:= '>=''01-02-' || codeyear::varchar || '''::timestamptz';
					datefin:= '<''01-03-' || codeyear::varchar || '''::timestamptz';
		WHEN 3 THEN dateini:= '>=''01-03-' || codeyear::varchar || '''::timestamptz';
					datefin:= '<=''31-03-' || codeyear::varchar || '''::timestamptz';
		WHEN 4 THEN dateini:= '>=''01-04-' || codeyear::varchar || '''::timestamptz';
					datefin:= '<=''30-04-' || codeyear::varchar || '''::timestamptz';
		WHEN 5 THEN dateini:= '>=''01-05-' || codeyear::varchar || '''::timestamptz';
					datefin:= '<=''31-05-' || codeyear::varchar || '''::timestamptz';
		WHEN 6 THEN dateini:= '>=''01-06-' || codeyear::varchar || '''::timestamptz';
					datefin:= '<=''30-06-' || codeyear::varchar || '''::timestamptz';
		WHEN 7 THEN dateini:= '>=''01-07-' || codeyear::varchar || '''::timestamptz';
					datefin:= '<=''31-07-' || codeyear::varchar || '''::timestamptz';
		WHEN 8 THEN dateini:= '>=''01-08-' || codeyear::varchar || '''::timestamptz';
					datefin:= '<=''31-08-' || codeyear::varchar || '''::timestamptz';
		WHEN 9 THEN dateini:= '>=''01-09-' || codeyear::varchar || '''::timestamptz';
					datefin:= '<=''30-09-' || codeyear::varchar || '''::timestamptz';
		WHEN 10 THEN dateini:= '>=''01-10-' || codeyear::varchar || '''::timestamptz';
					 datefin:= '<=''31-10-' || codeyear::varchar || '''::timestamptz';
		WHEN 11 THEN dateini:= '>=''01-11-' || codeyear::varchar || '''::timestamptz';
					 datefin:= '<=''30-11-' || codeyear::varchar || '''::timestamptz';
		WHEN 12 THEN dateini:= '>=''01-12-' || codeyear::varchar || '''::timestamptz';
					 datefin:= '<=''31-12-' || codeyear::varchar || '''::timestamptz';			
		ELSE error := 1;
	END CASE;
	
	if (error = 0)
		then myquery := 'select subc2.name, bestcase, cy.name as country 
				from (select  distinct on (c.country) c.country , c.name, max(v.sum) as bestcase
						from (	select client, (sum(price)) 
								from sales
								where begindate '  || 	dateini	||  ' and begindate ' || datefin ||
								' group by client) as v 
						inner join clients as c on client=c.id	
						group by c.country, c.name
						order by c.country, bestcase desc) as subc2
				inner join countrys as cy on country=cy.id
				order by bestcase desc';
		else myquery = '';
	end if; 

    RETURN QUERY execute myquery;
END;
$$ LANGUAGE plpgsql;
--LLamadas a la función
select sales_bestcases (3, 2017);
select clientname, bestcase, countryname from sales_bestcases (3, 2017);


-- SOLUCION,JAUME
CREATE OR REPLACE FUNCTION getBestCustomerByMonthAndYear(month int, year int) RETURNS TABLE(country varchar(50), client varchar(50), expense float) AS $$
    declare begin_date timestamptz;
    declare end_date timestamptz;
BEGIN
    if (month > 0 and month < 13 and year > 2015 ) 
    then
        begin_date := (year || '-' || month || '-01')::timestamptz;
        end_date := begin_date + interval '1 month';
        
        return query select c.name, q.client, q.expense
        from countrys as c inner join (
            select distinct on (c.country) c.country, c.name as client, q.expense as expense
            from (
                select sum(s.price) as expense, s.client as client 
                from sales as s 
                where beginDate >= begin_date and beginDate < end_date group by s.client
            ) as q
                inner join clients as c 
                on c.id = q.client
            order by c.country, q.expense desc
            ) as q on c.id = q.country
        order by q.expense desc;
    end if;
END;
$$ LANGUAGE plpgsql;

-- PRUEBAS DE LLAMADA A ESTA VERSION DE LA FUNCION
SELECT getBestCustomerByMonthYear(2, 2004)
SELECT getBestCustomerByMonthAndYear(23, 2017)
SELECT getBestCustomerByMonthAndYear(3, 2017)


-- VERSION DE JESUS, NOS APORTA OTRA POSIBILIDAD DE PARAMETRIZACION DE CONSULTAS
CREATE OR REPLACE FUNCTION sales_bestcases_jesus(month int, year int)
 RETURNS TABLE (clientname varchar, bestcase float, countryname varchar) as
 $$
 declare myquery varchar = 'select subc2.name, bestcase, cy.name as country
   from
    (select  distinct on (c.country) c.country , c.name, max(v.sum) as bestcase
    from ( select client, (sum(price))
      from sales
      where begindate >= make_timestamptz(%s, %s, 1, 0, 0, 0) and begindate <= make_timestamptz(%s, %s, 1, 0, 0, 0)
              + interval ''1 month'' - interval ''1 second''
      group by client) as v
    inner join clients as c on client=c.id
    group by c.country, c.name
    order by c.country, bestcase desc) as subc2
   inner join countrys as cy on country=cy.id
   order by bestcase desc;';
 begin
      if not month between 1 and 12 
      then 
      	RETURN QUERY execute '';
      ELSE RETURN QUERY execute format(myquery, year::text, month::text, year::text, month::text);   	
      end if;
 end;
$$ language plpgsql;
-- PRUEBA DE LLAMADAS A LA FUNCION
select sales_bestcases_jesus(3, 2017);
select sales_bestcases_jesus(23, 2017);


-- OBJETIVO: Mejor cliente (mayor volumen de compra) en cada mes de año recibido por cada pais, 
--ordenado el resultado por mes y luego por volumen de compra
--el mes se presentara como "enero", "febrero", "marzo" ...
-- RESULTADO: HEMOS DE PRESENTAR 60 FILAS DE 4 COLUMNAS (MES, VOLUMEN_COMPRA, CLIENTE, PAIS)
CREATE OR REPLACE FUNCTION sales_bestcases_by_year(codeyear int) 
RETURNS TABLE (month varchar, clientname varchar, bestcase float8, countryname varchar) 
PARALLEL SAFE ROWS 60   AS 
$$
declare year varchar;
BEGIN 
	year := (codeyear)::varchar;

	RETURN QUERY select case mmonth 
	            when 1 then 'gener'::varchar
	            when 2 then 'febrer'::varchar
	            when 3 then 'març'::varchar
	            when 4 then 'abril'::varchar
	            when 5 then 'maig'::varchar
	            when 6 then 'juny'::varchar
	            when 7 then 'juliol'::varchar
	            when 8 then 'agost'::varchar
	            when 9 then 'setembre'::varchar
	            when 10 then 'octubre'::varchar
	            when 11 then 'novembre'::varchar
	            when 12 then 'desembre'::varchar
				end as month,
				subc2.name, subc2.bestcase, cy.name as country
	from (
		select  distinct on (c.country, mmonth) c.country, date_part('month', v.vdata) as mmonth, c.name, 
			 		max(v.sum) as bestcase 
		from 
			(select client, (sum(price)), max(begindate) as vdata from sales
						where begindate >=('01-01-'||year)::timestamptz and begindate <('01-02-'||year)::timestamptz
						group by client
			union all	select client, (sum(price)), max(begindate) from sales
						where begindate >=('01-02-'||year)::timestamptz and begindate <('01-03-'||year)::timestamptz
						group by client
			union all	select client, (sum(price)), max(begindate) from sales
						where begindate >=('01-03-'||year)::timestamptz and begindate <('01-04-'||year)::timestamptz
						group by client
			union all	select client, (sum(price)), max(begindate) from sales
						where begindate >=('01-04-'||year)::timestamptz and begindate <('01-05-'||year)::timestamptz
						group by client
			union all	select client, (sum(price)), max(begindate) from sales
						where begindate >=('01-05-'||year)::timestamptz and begindate <('01-06-'||year)::timestamptz
						group by client
			union all	select client, (sum(price)), max(begindate) from sales
						where begindate >=('01-06-'||year)::timestamptz and begindate <('01-07-'||year)::timestamptz
						group by client
			union all	select client, (sum(price)), max(begindate) from sales
						where begindate >=('01-07-'||year)::timestamptz and begindate <('01-08-'||year)::timestamptz
						group by client	
			union all	select client, (sum(price)), max(begindate) from sales
						where begindate >=('01-08-'||year)::timestamptz and begindate <('01-09-'||year)::timestamptz
						group by client
			union all	select client, (sum(price)), max(begindate) from sales
						where begindate >=('01-09-'||year)::timestamptz and begindate <('01-10-'||year)::timestamptz
						group by client 
			union all	select client, (sum(price)), max(begindate) from sales
						where begindate >=('01-10-'||year)::timestamptz and begindate <('01-11-'||year)::timestamptz
						group by client	
			union all	select client, (sum(price)), max(begindate) from sales
						where begindate >=('01-11-'||year)::timestamptz and begindate <('01-12-'||year)::timestamptz
						group by client	
			union all	select client, (sum(price)), max(begindate) from sales
						where begindate >=('01-12-'||year)::timestamptz and begindate <=('31-12-'||year)::timestamptz
						group by client	) as v
		inner join clients as c on v.client=c.id
		group by c.country, mmonth, c.name
		order by c.country, mmonth, bestcase desc) as subc2
	inner join countrys as cy on country=cy.id
	order by mmonth, bestcase desc;
END;
$$ LANGUAGE plpgsql;
--LLamadas a la función
select * from sales_bestcases_by_year (2017);	


-- USANDO LA DE JESUS QUE PARECE IDONEA PARA EL PROCESAMIENTO PARALELO
CREATE OR REPLACE FUNCTION sales_bestcases_by_yearj(codeyear int) 
RETURNS TABLE (month varchar, clientname varchar, bestcase float8, countryname varchar) 
PARALLEL SAFE ROWS 60   AS 
$$
declare year varchar;
BEGIN 
	return query SELECT 'Enero'::varchar as month, * FROM sales_bestcases(1, codeyear)
			union all SELECT 'Febrero'::varchar  as month, * FROM sales_bestcases(2, codeyear)
			union all SELECT 'Marzo'::varchar  as month, * FROM sales_bestcases(3, codeyear)
			union all SELECT 'Abril'::varchar  as month, * FROM sales_bestcases(4, codeyear)
			union all SELECT 'Mayo'::varchar  as month, * FROM sales_bestcases(5, codeyear)
			union all SELECT 'Junio'::varchar  as month, * FROM sales_bestcases(6, codeyear)
			union all SELECT 'Julio'::varchar  as month, * FROM sales_bestcases(7, codeyear)
			union all SELECT 'Agosto'::varchar  as month, * FROM sales_bestcases(8, codeyear)
			union all SELECT 'Septiembre'::varchar  as month, * FROM sales_bestcases(9, codeyear)
			union all SELECT 'Octubre'::varchar  as month, * FROM sales_bestcases(10, codeyear)
			union all SELECT 'Noviembre'::varchar  as month, * FROM sales_bestcases(11, codeyear)
			union all SELECT 'Diciembre'::varchar  as month, * FROM sales_bestcases(12, codeyear);
 END;
$$ LANGUAGE plpgsql;

select * from sales_bestcases_by_yearJ (2017);	

