set search_path to partitions;

CREATE TABLE logs_sales (
 	id SERIAL,
	client integer,
	created timestamp NOT NULL DEFAULT current_timestamp,
	products integer[],
	country integer 
)  PARTITION BY RANGE (created);

CREATE TABLE logs_sales2015 PARTITION OF logs_sales
    FOR VALUES FROM  ( '2015-01-01 00:00:00' )  TO  ('2015-12-31 23:59:59' );

CREATE TABLE logs_sales2016 PARTITION OF logs_sales
    FOR VALUES FROM  ( '2016-01-01  00:00:00' ) TO ('2016-12-31 23:59:59' );

CREATE TABLE logs_sales2017 PARTITION OF logs_sales
    FOR VALUES FROM  ( '2017-01-01  00:00:00' ) TO ('2017-12-31 23:59:59' );

CREATE TABLE logs_sales2018  PARTITION OF  logs_sales
    FOR VALUES FROM ( '2018-01-01  00:00:00' )  TO  ('2018-12-31 23:59:59' );

CREATE  INDEX  ON  logs_sales2015 (created);
CREATE  INDEX  ON  logs_sales2016 (created);
CREATE  INDEX  ON  logs_sales2017 (created);
CREATE  INDEX  ON  logs_sales2018 (created);



-- PARTICIONAMIENTO A PARTIR DE HERENCIA

-- TABLA PRINCIPAL. EN ESTE MODELO LA CONSIDERAMOS LA TABLA PARTICIONADA
CREATE TABLE logs_salesh (
    id serial primary key,
    client integer not null,
    created timestamp NOT NULL DEFAULT current_timestamp,
    products integer[],
    country integer 
);

--TABLAS DERIVADAS, PODRIAN APORTAR SUS PROPIOS CAMPOS SI FUESE NECESARIO
CREATE TABLE logs_salesh2015 (
    CHECK ( created >= '2015-01-01 00:00:00' AND created <= '2015-12-31 23:59:59' ) 
) INHERITS (logs_salesh);

CREATE TABLE logs_salesh2016 ( 
    CHECK ( created >= '2016-01-01 00:00:00' AND created <= '2016-12-31 23:59:59' ) 
) INHERITS (logs_salesh);

CREATE TABLE logs_salesh2017 ( 
    jsondate jsonb,
    CHECK ( created >= '2017-01-01 00:00:00' AND created <= '2017-12-31 23:59:59' ) 
) INHERITS (logs_salesh);

-- CONVERSION DE UNA TABLA REGULAR EN PARTICION A TRAVES DE LA HERENCIA
CREATE TABLE logs_salesh2018 (LIKE logs_salesh INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
    
ALTER TABLE logs_salesh2018 ADD CONSTRAINT logs_sales2018_created_check
       CHECK ( created >= '2018-01-01 00:00:00' AND created <= '2018-12-31 23:59:59' );   
-- UNA VEZ INSERTADOS Y TRATADOS LOS DATOS DE LA TABLA LA INCORPORAMOS AL MECANISMO
-- DE HERENCIA PARA CONVERTIRLA EN UNA PARTICION
ALTER TABLE logs_salesh2018  INHERIT logs_salesh;

-- PODREMOS CREAR LOS INDICES NECESARIOS EN CADA TABLA DERIVADA O PARTICION
CREATE INDEX logs_salesh2015_created ON logs_salesh2015 (created);
CREATE INDEX logs_salesh2016_created ON logs_salesh2016 (created);
CREATE INDEX logs_salesh2017_created ON logs_salesh2017 (created);
CREATE INDEX logs_salesh2018_created ON logs_salesh2018 (created);



-- Implementacion del sistema de particionado a través de triggers 
-- (comprueba fila a fila, en inserciones masivas puede ralentizarlas)
-- FUNCION QUE REALIZA DINAMICAMENTE EL INSERT EN FUNCION DE LA FECHA ESTABLECIDA
CREATE OR REPLACE FUNCTION fn_InsertsalesByYear()  RETURNS TRIGGER AS $$
declare year varchar;
BEGIN
     select extract (YEAR from NEW.created) into year; 
     EXECUTE FORMAT ('INSERT INTO logs_salesh%s VALUES ($1.*);', year) USING NEW;    
     RETURN NULL;   --EN LUGAR DE RETURN NULL PARA CONFIRMAR OPERACION
END;
$$  LANGUAGE plpgsql;

CREATE TRIGGER Insert_sales_Trigger BEFORE INSERT ON logs_salesh
FOR EACH ROW EXECUTE PROCEDURE fn_InsertsalesByYear();



-- la funcion anterior fallará cuando no encuentre una particion donde guardar datos, pero como un fallo
-- como puede suceder por diferentes motivos, mi recomendacion es no actuar sobre la BD creando particiones
-- cuando no sabemos si este es el problema, y hacer comprobaciones extra ralentizaria el uso de la tabla
-- por tanto, se preparara un procedimiento para generar particiones cuando la aplicacion lo considere 
CREATE OR REPLACE FUNCTION fn_createPartLogs(year int)  RETURNS VOID AS $$
BEGIN
     execute format ('CREATE TABLE logs_salesh%s 
                    ( 
                        CONSTRAINT pk_salesh%s primary key (id),
                        CHECK ( created >= ''%s-01-01 00:00:00'' AND created <= ''%s-12-31 23:59:59'' ) 
                    ) INHERITS (logs_salesh);',  year,year,year,year);
END;
$$  LANGUAGE plpgsql;

select fn_createpartlogs('2040');



-- PRUEBAS DE RENDIMIENTO ENTRE AMBOS SISTEMAS Y EL SISTEMA DE TABLA CLASICA

-- LLENADO DE TABLA CON PARTICIONAMIENTO DECLARADO
CREATE OR REPLACE FUNCTION  fill_logs_partdec (numSales int, delay int, numClients int) RETURNS integer AS $$
declare clogs int := 1;
declare clientaleat int;
declare created timestamp := '2015-01-01 00:00:00';  
declare timeinterval1 varchar;
BEGIN    
    WHILE clogs <= numSales LOOP    
        
        timeinterval1 := ( (random()*delay)::int)::text || ' seconds';
        clientaleat := 1 + random() * (numClients-1);  
        created := created + timeinterval1::interval; 

        insert into  logs_sales (client, created, products, country) 
        values ( clientaleat, created, ARRAY[25,101], 10);
        clogs := clogs + 1;
    END LOOP;    
    RETURN clogs;
END;
$$ LANGUAGE plpgsql;

-- LLENADO DE LA TABLA CON PARTICIONAMIENTO POR HERENCIA
CREATE OR REPLACE FUNCTION  fill_logs_parther (numSales int, delay int, numClients int) RETURNS integer AS $$
declare clogs int := 1;
declare clientaleat int;
declare created timestamp := '2015-01-01 00:00:00';  
declare timeinterval1 varchar;
BEGIN    
    WHILE clogs <= numSales LOOP    
        
        timeinterval1 := ( (random()*delay)::int)::text || ' seconds';
        clientaleat := 1 + random() * (numClients-1);  
        created := created + timeinterval1::interval; 

        insert into  logs_salesh (client, created, products, country) 
        values ( clientaleat, created, ARRAY[25,101], 10);
        clogs := clogs + 1;
    END LOOP;    
    RETURN clogs;
END;
$$ LANGUAGE plpgsql;


-- TABLA CLASICA
CREATE TABLE logs_sales_clasic (
    id serial primary key,
    client integer not null,
    created timestamp NOT NULL DEFAULT current_timestamp,
    products integer[],
    country integer 
);
-- LLENADO DE LA TABLA SIN PARTICIONAMIENTO
CREATE OR REPLACE FUNCTION  fill_logs_nopart (numSales int, delay int, numClients int) RETURNS integer AS $$
declare clogs int := 1;
declare clientaleat int;
declare created timestamp := '2015-01-01 00:00:00';  
declare timeinterval1 varchar;
BEGIN    
    WHILE clogs <= numSales LOOP    
        
        timeinterval1 := ( (random()*delay)::int)::text || ' seconds';
        clientaleat := 1 + random() * (numClients-1);  
        created := created + timeinterval1::interval; 

        insert into  logs_sales_clasic (client, created, products, country) 
        values ( clientaleat, created, ARRAY[25,101], 10);
        clogs := clogs + 1;
    END LOOP;    
    RETURN clogs;
END;
$$ LANGUAGE plpgsql;

-- VACIADO, LLENADO Y CONSULTA DE LA TABLA CON PARTICIONAMIENTO DECLARATIVO
TRUNCATE TABLE logs_sales RESTART IDENTITY;  
select fill_logs_partdec (1000, 300, 100);
select * from logs_sales;
select * from logs_sales2015;
select * from logs_sales2016;
select * from logs_sales2017;
select * from logs_sales2018;

-- VACIADO, LLENADO Y CONSULTA DE LA TABLA CON PARTICIONAMIENTO POR HERENCIA
TRUNCATE TABLE logs_salesh RESTART IDENTITY;  
select fill_logs_parther (10000000, 300, 100);
select * from logs_salesh;
select * from logs_salesh2015;
select * from logs_salesh2016;
select * from logs_salesh2017;
select * from logs_salesh2018;

-- VACIADO, LLENADO Y CONSULTA DE LA TABLA STANDARD
TRUNCATE TABLE logs_sales_clasic RESTART IDENTITY; 
select fill_logs_nopart (1000, 300, 100);
select * from logs_sales_clasic;