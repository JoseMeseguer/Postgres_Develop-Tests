-- EJEMPLO DE TABLAS PARTICIONADAS

-- PARTICIONAMIENTO DECLARATIVO (A partir de la version 10)
CREATE SCHEMA partitions;

set search_path to partitions;

CREATE TABLE logs_sales (
 	id SERIAL,
	client integer,
	created timestamp NOT NULL DEFAULT current_timestamp,
	products integer[],
	country integer 
)  PARTITION BY RANGE (created);

CREATE TABLE logs_sales2015 PARTITION OF logs_sales
    FOR VALUES FROM  ( '2015-01-01' )  TO  ('2015-12-31' );

CREATE TABLESPACE TesteoFast  OWNER postgres  LOCATION 'C:\Data\postgresql\testeofast';

CREATE TABLE logs_sales2016 PARTITION OF logs_sales
    FOR VALUES FROM  ( '2016-01-01' )  TO  ('2016-12-31' )
    TABLESPACE TesteoFast;

CREATE TABLE logs_sales2017 PARTITION OF logs_sales
    FOR VALUES FROM  ( '2017-01-01' )  TO  ('2017-12-31' )
    WITH (parallel_workers = 4)
    TABLESPACE TesteoFast;

CREATE  INDEX  ON  logs_sales2015 (created);
CREATE  INDEX  ON  logs_sales2016 (created);
CREATE  INDEX  ON  logs_sales2017 (created);

CREATE TABLE logs_sales2018  PARTITION OF  logs_sales
    FOR VALUES FROM ( '2018-01-01' )  TO  ('2018-12-31' )
    PARTITION BY LIST (country);


CREATE TABLE logs_sales2018_Spain PARTITION OF logs_sales2018
    FOR VALUES IN ( 1 )
    TABLESPACE TesteoFast;
   
CREATE TABLE logs_sales2018_NorthEurope PARTITION OF logs_sales2018
    FOR VALUES IN  ( 40, 41, 42, 43, 44, 45 )
    TABLESPACE TesteoFast;  


DROP TABLE logs_sales2015;
ALTER TABLE logs_sales DETACH PARTITION logs_sales2016;

CREATE TABLE logs_sales2019 (
    LIKE logs_sales INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
  TABLESPACE TesteoFast;

ALTER TABLE logs_sales2019 ADD CONSTRAINT logs_sales2019_created_check
       CHECK ( created >= DATE '2019-01-01' AND created <= DATE '2019-12-31' );   
-- una vez insertados y tratados los datos de la tabla, aplicamos el mecanismo de herencia para convertirla en partición
ALTER TABLE  logs_sales ATTACH PARTITION  logs_sales2019
    FOR VALUES FROM ( '2019-01-01' )  TO  ('2019-12-31'); 
ALTER TABLE logs_sales2019 DROP CONSTRAINT logs_sales2019_created_check;


insert into logs_sales values (1, 12, '01-02-2015', ARRAY[22,115], 14);  --fallara si lo habeis eliminado
insert into logs_sales values (1, 15, '02-02-2017', ARRAY[25,101], 2);
insert into logs_sales values (1, 15, '02-02-2016', ARRAY[25,101], 2);

insert into logs_sales values (1, 15, '02-02-2012', ARRAY[25,101], 2);  --fallara porque no hay particion para 2012

select * from only logs_sales;   -- presentara nada porque no se guarda ninguna fila en esta tabla


select * from logs_sales; --vemos como por la forma en que hemos insertado valores la sequencia del serial no se ha aplicado
--y hemos repetido codigos de log que deberian ser unicos


-- pero si modificamos la forma de realizar el insert comprobamos que la sequencia se implanta correctamente
insert into logs_sales values (default, 12, '01-02-2015', ARRAY[22,115], 14);
insert into logs_sales values (default, 12, '01-02-2015', ARRAY[22,115], 14);
insert into logs_sales values (default, 12, '01-02-2017', ARRAY[22,115], 14);
insert into logs_sales values (default, 12, '01-02-2017', ARRAY[22,115], 14);

select * from logs_sales;

select * from logs_sales2015;
select * from logs_sales2017;

--COMPROBAR CON EXPLAIN LA EFICIENCIA SEGUN EL CRITERIO DE BUSQUEDA
select * from logs_sales where id = 1;
select * from logs_sales where created between '01-01-2016' and '01-03-2016'



-- PARTICIONAMIENTO A PARTIR DE HERENCIA

-- TABLA PRINCIPAL. EN ESTE MODELO LA CONSIDERAMOS LA TABLA PARTICIONADA

CREATE TABLE logs_salesh (
 	id serial primary key,
	client integer not null,
	created timestamp NOT NULL DEFAULT current_timestamp,
	products integer[],
	country integer 
);

insert into logs_salesh values (1, 12, '01-02-2015', ARRAY[22,115], 14);  --fallara si lo habeis eliminado
insert into logs_salesh values (1, 15, '02-02-2017', ARRAY[25,101], 2);
insert into logs_salesh values (1, 15, '02-02-2016', ARRAY[25,101], 2);

insert into logs_salesh values (default, 12, '01-02-2015', ARRAY[22,115], 14);
insert into logs_salesh values (default, 12, '01-02-2015', ARRAY[22,115], 14);
insert into logs_salesh values (default, 12, '01-02-2017', ARRAY[22,115], 14);
insert into logs_salesh values (default, 12, '01-02-2017', ARRAY[22,115], 14);
insert into logs_salesh values (default, 12, '01-02-2018', ARRAY[22,115], 14);



--TABLAS DERIVADAS, PODRIAN APORTAR SUS PROPIOS CAMPOS SI FUESE NECESARIO
CREATE TABLE logs_salesh2015 ( 
    CHECK ( created >= DATE '2015-01-01' AND created <= DATE '2015-12-31' ) 
) INHERITS (logs_salesh);

CREATE TABLE logs_salesh2016 ( 
    CHECK ( created >= DATE '2016-01-01' AND created <= DATE '2016-12-31' ) 
) INHERITS (logs_salesh);

CREATE TABLE logs_salesh2017 ( 
    jsondate jsonb,
    CHECK ( created >= DATE '2017-01-01' AND created <= DATE '2017-12-31' ) 
) INHERITS (logs_salesh);

-- PODREMOS CREAR LOS INDICES NECESARIOS EN CADA TABLA DERIVADA O PARTICION
CREATE INDEX logs_salesh2015_created ON logs_salesh2015 (created);
CREATE INDEX logs_salesh2016_created ON logs_salesh2016 (created);
CREATE INDEX logs_salesh2017_created ON logs_salesh2017 (created);

-- CONVERSION DE UNA TABLA REGULAR EN PARTICION A TRAVES DE LA HERENCIA
CREATE TABLE logs_salesh2018 (
    LIKE logs_salesh INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
    
ALTER TABLE logs_salesh2018 ADD CONSTRAINT logs_sales2018_created_check
       CHECK ( created >= DATE '2018-01-01' AND created <= DATE '2018-12-31' );   
-- UNA VEZ INSERTADOS Y TRATADOS LOS DATOS DE LA TABLA LA INCORPORAMOS AL MECANISMO
-- DE HERENCIA PARA CONVERTIRLA EN UNA PARTICION
ALTER TABLE logs_salesh2018  INHERIT logs_salesh;



-- Implementacion del sistema de particionado a través de reglas 
-- (mayor coste de procesamiento si la inserción es fila a fila)
CREATE OR REPLACE RULE Insert_logs_sales_2015_rule AS ON INSERT TO logs_salesh
WHERE (created >= DATE '2015-01-01' AND created <= DATE '2015-12-31' )
DO INSTEAD INSERT INTO logs_salesh2015 VALUES ( NEW.* );

CREATE OR REPLACE RULE Insert_logs_sales_2016_rule AS ON INSERT TO logs_salesh
WHERE (created >= DATE '2016-01-01' AND created <= DATE '2016-12-31' )
DO INSTEAD INSERT INTO logs_salesh2016 VALUES ( NEW.* );

CREATE OR REPLACE RULE Insert_logs_sales_2017_rule AS ON INSERT TO logs_salesh
WHERE (created >= DATE '2017-01-01' AND created <= DATE '2017-12-31' )
DO INSTEAD INSERT INTO logs_salesh2017 VALUES ( NEW.* );


-- Implementacion del sistema de particionado a través de triggers 
-- (comprueba fila a fila, en inserciones masivas puede ralentizarlas)
CREATE OR REPLACE FUNCTION fn_InsertsalesByYear()  RETURNS TRIGGER AS $$
BEGIN
     IF ( NEW.created >= DATE '2015-01-01' AND NEW.created <= DATE '2015-12-31' ) THEN
           INSERT INTO logs_salesh2015  VALUES (NEW.*);
     ELSIF ( NEW.created >= DATE '2016-01-01' AND NEW.created <= DATE '2016-12-31' ) THEN
           INSERT INTO logs_salesh2016  VALUES (NEW.*);
     ELSIF ( NEW.created >= DATE '2017-01-01' AND NEW.created <= DATE '2017-12-31' ) THEN
           INSERT INTO logs_salesh2017 VALUES (NEW.*);
     END IF;
     RETURN NULL;
END;
$$  LANGUAGE plpgsql;
--drop function fn_InsertsalesByYear() cascade;  --para borrar funcion y trigger asociado

CREATE TRIGGER Insert_sales_Trigger
     BEFORE INSERT ON logs_salesh
FOR EACH ROW EXECUTE PROCEDURE fn_InsertsalesByYear();


explain analyze insert into logs_salesh values (1, 12, '01-02-2015', ARRAY[22,115], 14);
explain analyze insert into logs_salesh values (1, 15, '02-02-2017', ARRAY[25,101], 2);

select * from logs_salesh;
select * from only logs_salesh;
select * from logs_salesh2015;
select * from logs_salesh2017;