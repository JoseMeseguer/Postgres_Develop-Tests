-- CREACION DE UNA VISTA CLASICA
create or replace view sales_enero as
	select * from sales as s inner join saledetails as sd  on s.id = sd.saleid
where begindate between '2015-01-01' and '2015-01-31';

-- UTILIZAMOS LA VISTA COMO UNA TABLA MAS DEL SISTEMA
select * from sales_enero where client = 1000;

-- CREACION DE UNA VISTA MATERIALIZADA
CREATE MATERIALIZED VIEW mv_sales_enero as
select * from sales as s inner join saledetails as sd  on s.id = sd.saleid
where begindate between '2015-01-01' and '2015-01-31';

-- TAMBIEN PODEMOS USAR ESTA VISTA COMO UNA TABLA MAS, DE HECHO SE GUARDA EN DISCO IGUAL QUE UNA TABLA
-- Y POR TANTO PODEMOS AÑADIRLE INDICES PARA ACELERAR AUN MAS SU RENDIMIENTO
create index mv_iclient on mv_sales_enero (client);

-- SE PUEDE OBSERVAR UN CLARO RENDIMIENTO RESPECTO A UNA VISTA CLASICA
select * from mv_sales_enero where client = 1000;

-- Y EN EL PERIODO QUE QUEDE ESTABLECIDO UN PROCESO CON PERMISOS DEBE EJECUTAR ESTE COMANDO PARA ACTUALIZAR DATOS
REFRESH MATERIALIZED VIEW  mv_sales_enero;

-- UNA POLITICA DE SEGURIDAD SERIA LA DE PERMITIR SOLO EL ACCESO A VISTAS
-- YA SEAN MATERIALIZADAS O NO. PARA ELLO CREAMOS UN USARIO USERPRIV Y SOLO LE ASIGNAMOS
-- PERMISOS SOBRE NUESTRO PAR DE VISTAS
CREATE USER userpriv WITH
	LOGIN
	NOSUPERUSER
	NOCREATEDB
	NOCREATEROLE
	INHERIT
	NOREPLICATION
	CONNECTION LIMIT -1
	PASSWORD 'userpriv';
	
-- IMPORTANTE: COMO TENEMOS LAS VISTAS EN EL ESQUEMA TESTS Y NO EN PUBLIC TENEMOS QUE DAR
-- PERMISO A ESTE USUARIO PARA UTILIZAR UN ESQUEMA QUE NO ES EL QUE SE CARGA POR DEFECTO
GRANT USAGE ON SCHEMA tests TO userpriv;

-- UNA VEZ PODEMOS UTILIZAR EL ESQUEMA ELEGIMOS SOBRE QUE OBJETOS DAMOS PERMISOS Y CUALES
GRANT SELECT ON TABLE tests.mv_sales_enero TO userpriv;
GRANT SELECT ON TABLE tests.sales_enero TO userpriv;

-- AL CONECTARNOS COMO USERPRIV A LA BASE DE DATOS LO PRIMERO QUE HEMOS DE HACER ES SITUARNOS
-- EN EL ESQUEMAS TEST (SI LAS VISTAS LAS TENEMOS EN PUBLIC NO SERIA NECESARIO)
-- Y AL PROBAR A REALIZAR SELECTS NO PODRIAMOS EXCEPTO CON NUESTRO PAR DE CONSULTAS
-- NOTA: AL NO SER PROPIETARIOS DE UNA VISTA MATERIALIZADA NO TENEMOS PERMISO PARA REFRESCARLA


-- Tabla Sale con los SaleDetails integrados en uno de sus campos
create type saledetail as (
    productid   int,
    name        varchar(50),
    units       int,
    unitprice   float 
);

create table isales (
    id          serial     primary key, 
    beginDate   timestamptz,
    endDate     timestamptz,
    price       float,
    client      int, 
    details 	saledetail[]
);
create index idx_isale_vipclient on iSales (client) where client <=5000;
create index idx_isale_begindate on iSales (begindate DESC);
create index idx_isale_price     on iSales (price) ;
create index idx_isale_enddate   on iSales (enddate DESC NULLS FIRST);
create index idx_isale_clientpricebdate on iSales (client ASC, begindate DESC, price DESC);


CREATE OR REPLACE FUNCTION  filliSales(numSales int, delay int, numClients int, numProducts int ) RETURNS integer AS $$
declare csales int := 1;
declare clientaleat int;
declare bdate timestamp := '2015-01-01 00:00:00';  
declare timeinterval1 varchar;
declare timeinterval2 varchar;
declare edate timestamp;
declare result float;
BEGIN    
    WHILE csales <= numSales LOOP        
        timeinterval1 := (random()*delay)::text || ' seconds';
        clientaleat := 1 + random() * (numClients-1);  
              
        bdate := bdate + timeinterval1::interval; 
        timeinterval2 := (1 + random()*10)::text || ' days';          
        edate := bdate + timeinterval2::interval;

        insert into  iSales  (begindate, enddate, price, client) 
        values ( bdate, edate, 0.0, clientaleat);
        
        result :=  filliSaleDetails (csales, 10, numProducts);  -- POR DEFECTO SE FIJA A 10 LINEAS DE COMPRA
        update   iSales  SET price = result where id = csales;
        csales := csales + 1;
    END LOOP;    
    RETURN csales;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION  filliSaleDetails (numSale int, numDetails int, numProducts int) RETURNS float AS $$
declare productAleat int;
declare unitsAleat int;
declare unitPrice float;
declare linePrice float;
declare subtotal float;
declare productname varchar;
declare detailsAleat int;
declare cdetails int;
BEGIN              
        detailsAleat := 1 + random()*numDetails;
        cdetails := 1; -- INSERTAMOS EN LA PRIMERA POSICION
        linePrice := 0;
        WHILE cdetails <= detailsAleat LOOP        
            productAleat := 1 + random() * (numProducts-1); 
            productname := 'product_' || productAleat::text;  
            unitPrice := 4.99 + (random()*100);
            unitsAleat := 1 + random()*10;
            subtotal := unitPrice * unitsAleat;
            linePrice = linePrice + subtotal;

            update iSales  set details[cdetails] = (productAleat, productname, unitsAleat, unitPrice)::saledetail
            where id = numSale;
            cdetails := cdetails + 1;
        END LOOP;    
        select TRUNC( CAST(lineprice as NUMERIC),2) into lineprice;
    RETURN linePrice;
END;
$$ LANGUAGE plpgsql;


-- PODRIAMOS PREGUNTAR POR UNA POSICION EN CONCRETO PARA SABER QUIENES HAN COMPRADO UN UNICO PRODUCTO
select * from isales where details[2] is null;

-- PARA VER CUANTAS LINEAS DE VENTAS TENEMOS EN TODAS AQUELLAS QUE SUPEREN LAS 5 LINEAS
select isales.*, array_length(details, 1) as numdetails from isales where array_length(details, 1)  > 5;


-- POR DEFECTO LAS LINEAS DE VENTA VAN LLENANDO EL VECTOR DINAMICO A PARTIR DE LA POSICION 1
insert into isales values (default, '2018-01-01', '2018-02-02', 3452.23, 1234, 
						ARRAY[(120, 'product120', 3, 10.99)::saledetail,
							(125, 'product125', 13, 19.99)::saledetail,
							(130, 'product130', 13, 1.99)::saledetail,
							(20, 'product20', 30, 99.99)::saledetail]
						);
						
select * from isales where id = 1000;

-- PERO PODEMOS COMPROBAR COMO NOS PERMITE PONER DATOS EN LA POSICION 0 SI QUEREMOS COLOCAR UNA LINEA DE PRODUCTO LA PRIMERA
update isales set details[0]= (1, 'product1', 3, 99.99)::saledetail
where id = 1002;


-- EL PROBLEMA NO SERIA TAN GRAVE SINO FUERA PORQUE TAMBIEN PERMITE REFERENCIAS NEGATIVAS
update isales set details[-2]= (102, 'product100', 23, 199.99)::saledetail
where id = 1002;

-- Y YA PUESTOS SI AÑADIMOS UNA LINEA VARIAS POSICIONES POR ENCIMA DEL MAXIMO LO PERMITE RELLENANDO CON LINEAS A NULL
update isales set details[10]= (100, 'product100', 23, 199.99)::saledetail
where id = 1002;

SELECT details from isales where id = 1002;

-- LA GESTION DE ESTA FLEXIBILIDAD PUEDE RESULTAR MUY COMPLEJA POR LO QUE MEJOR SEGUIR LAS RECOMENDACIONES DE POSTGRESQL
-- Y NO APROVECHARNOS DE ESTAS OPCIONES
-- POR TANTO, INSERTAMOS EN FORMA NORMAL COMO EN EL EJEMPLO DEL ARRAY, CON LO QUE COMENZAREMOS POR EL 1
-- Y A LA HORA DE AÑADIR CON UPDATE SEGUIMOS LAS RECOMENDACIONES
update isales set details= (10, 'product10', 23, 199.99)::saledetail || details where id = 1002;
-- QUE NOS PERMITE PONER UNA LINEA LA PRIMERA DE LA LISTA DESPLAZANDO AL RESTO Y EMPEZANDO POR LA PRIMERA
-- O BIEN AÑADIRLA AL FINAL DE LA LISTA DIRECTAMENTE CON :
update isales set details= details || (13, 'product13', 23, 199.99)::saledetail  where id = 1002;

-- CON ESTA BUENA PRACTICA Y NO ABUSANDO DE LAS POSIBILIDADES DE GESTION DE LISTAS PODREMOS ASUMIR QUE RECORREREMOS
-- SIEMPRE LA LISTA DESDE LA POSICION 1 A LA QUE NOS FACILITE ARRAY_LENGTH
-- Y ADEMAS EVITAMOS ENTREGAR "INEXPERADAMENTE" UN NUEVO FORMATO DE CADENA A LAS APLICACIONES CLIENTES

-- DENTRO DE LA LISTA ANTERIOR PODEMOS SELECCIONAR POR RANGOS 
SELECT details FROM isales WHERE id = 999;
SELECT details[7].name FROM isales WHERE id = 999;
SELECT details[:] FROM isales WHERE id = 999;
SELECT details[:4] FROM isales WHERE id = 999;
SELECT details[5:] FROM isales WHERE id = 999;
SELECT details[4:8] FROM isales WHERE id = 999;
SELECT details[10:15] FROM isales WHERE id = 999;
SELECT details[-2:] FROM isales WHERE id = 1002;


-- PRUEBAS CON LA UNLOGGED TABLE, CLIENT_PREFERENCES
insert into client_preferences values (1, 3, ARRAY[1,2,3,4], ARRAY[1000, 1001, 1002, 1003], 'carrer kalea 3');
insert into client_preferences values (2, 3, ARRAY[1,2,7,8], ARRAY[1000, 1003, 1005, 1006], 'carrer kalea 4');
insert into client_preferences values (3, 3, ARRAY[1,5,7,8], ARRAY[1000, 1003, 1005, 1007], 'carrer kalea 5');

--PARA LA ACTUALIZACION DE LOS VALORES DEL ARRAY USAMOS EL OPERADOR || QUE NOS SIMPLIFICA LA GESTION DE LOS INDICES DE FILA
-- SIEMPRE EMPEZAMOS POR EL 1
update client_preferences set wish_list = wish_list || 1009 where clientcode = 1;
update client_preferences set wish_list = 999 || wish_list  where clientcode = 1;

-- AL TRATARSE DE ARRAYS DE DATOS PRIMITIVOS TENEMOS MAYORES POSIBILIDADES PARA LA CONSULTA DE SUS VALORES
-- EN ESTE CASO HACIENDO USO DE OPERADORES MUY ESPECIFICOS QUE VOLVEREMOS A ENCONTRAR CON JSONB 
select * from client_preferences where clientcode = 3;
select * from client_preferences where categories @> ARRAY[1,8];  --CLIENTES INTERESADOS EN LAS CATERIAS 1 Y 8
select * from client_preferences where wish_list @> ARRAY[1007];  --CLIENTE INTERESADOS EN EL PRODUCTO 1007

