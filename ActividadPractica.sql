-- PROCESO A DESARROLLAR

-- UTILIZAREMOS COMO CARRITO UNA TABLA TEMPORAL, QUE TENDRA UN TRIGGER PARA RESERVAR UNIDADES TEMPORALMENTE EN LA TABLA DE STOCK
-- COMPROBANDO SIEMPRE QUE TENGAMOS EXISTENCIAS NO RESERVADAS. CON UNA FUNCION CONFIRMAREMOS LA OPERACION DE VENTA CREANDO UNA 
-- NUEVA VENTA, DONDE UN TRIGGER EN LA TABLA SALE DETAILS SE ENCARGARA DE ACTUALIZAR LA TABLA DE STOCK, QUE A SU VEZ TENDRA UN
-- TRIGGER QUE ACTUALIZARA LA VISTA MATERIALIZADA CUANDO ALGUN PRODUCTO MARCADO COMO CHECKED QUE SIN EXISTENCIAS

set search_path to schematesteo

-- creacion de la funcion que sera llamada por un trigger, no depende de uno en concreto directamente
-- pero cuidado con las comprobaciones sobre NEW y OLD si la intentamos hacer comun a varios triggers
CREATE OR REPLACE FUNCTION refreshview()  RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.units = 0 )
        THEN refresh materialized view promos_today;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- creacion del trigger sobre stock, pero cuidado que solo salte ante actualizaciones del campo units
-- y siempre que esa fila haya sido "marcada" para su vigilancia. Comprobara cada fila que se actualice por separado
-- y llamara la funcion necesaria
CREATE TRIGGER trg_check_stock AFTER UPDATE OF units ON stock
FOR EACH ROW WHEN (OLD.checked_row = true)
EXECUTE PROCEDURE refreshview();

-- COMPROBACION
update stock set units = 0 where prod = 42;


-- UNA ALTERNATIVA: El trigger anterior saltaba cada vez que las units quedaban a 0, pero eso no significa que no tenga stock
-- pues el reservado quizas sea devuelto. Como el coste de refrescar hay que tenerlo en cuenta, quizas nos pidan cambiar las 
-- reglas de negocio para que solo se penalice el rendimiento del sistema cuando realmente estemos sin unidades

DROP TRIGGER trg_check_stock ON stock;  --borramos el trigger

-- rediseñamos la funcion para que pueda ser llamada por cualquier trigger
CREATE OR REPLACE FUNCTION refreshview()  RETURNS TRIGGER AS $$
BEGIN
    refresh materialized view promos_today;
    RETURN NULL;
END;
$$  LANGUAGE plpgsql;

-- rediseñamos el trigger para que solo salte cuando realmente estemos sin stock real de un producto
CREATE TRIGGER trg_check_stock AFTER UPDATE OF units ON stock
FOR EACH ROW  WHEN (OLD.checked_row = true AND NEW.units=0 AND NEW.uncheck_units=0)
EXECUTE PROCEDURE refreshview();

-- comprobacion no se actualiza la vista materializada. Con lo que podemos establecer algunas simples reglas de negocio
-- directamente en la BD independientemente del tipo de aplicacion con la que conecten a la misma, que en 
-- ocasiones simplifica mucho el desarrollo
update stock set uncheck_units=2, units=0 where prod = 46;


-- CREAREMOS PROCEDIMIENTOS SOBRE LOS QUE EL USUARIO USERPRIV TENGA PERMISOS DE EJECUCION 

-- procedimiento que crea la tabla temporal para guardar el carrito de compra
-- debido a que el manejo de arrays implica mayor gasto de recursos nos sale mas a cuenta
-- implementar una tabla con formato clasico

-- hemos de diseñar un triiger especifico a la tabla como el siguiente, especificando el codigo de cliente
CREATE TRIGGER trg_check_stock AFTER UPDATE ON current_cart_XXXX  FOR EACH ROW EXECUTE PROCEDURE cart_reserve_units();

-- Y LO GENERAREMOS DINAMICAMENTE JUNTO A LA TABLA TEMPORAL ESPECIFICA DONDE GUARDAREMOS LA COMPRA DEL CLIENTE
--DROP FUNCTION prepareCart( int)
CREATE OR REPLACE FUNCTION prepareCart(codeuser int)  RETURNS void  AS $$
declare myquery varchar := 'create temp table current_cart_';
BEGIN
    myquery := myquery || codeuser || ' ( 	ts timestamptz, 
    										product integer, 
    										units integer );';
    execute myquery;  
    myquery := 'CREATE TRIGGER trg_check_stock AFTER UPDATE OR INSERT OR DELETE ON current_cart_' || codeuser || 
                ' FOR EACH ROW EXECUTE PROCEDURE cart_reserve_units();';
    execute myquery;  
END; 
$$ LANGUAGE plpgsql;


-- PROTOTIPO DE LA FUNCION 
CREATE OR REPLACE FUNCTION cart_reserve_units()  RETURNS TRIGGER AS $$
BEGIN
	

    RETURN NULL;
END; 
$$ LANGUAGE plpgsql;

-- PRUEBAS
drop table current_cart_2000
select prepareCart(2000); -- ejecutamos el procedimiento para crear una tabla temporal current_cart_2000
select * from current_cart_2000;  -- comprobamos directamente antes de realizar una vista especifica


-- ANTES DE DESARROLLAR cart_reserve_units()  SIGUIENDO UN PATRON SIMILAR A LA FUNCION DESARROLLADA
-- PARA EL TRIGGER DE LA TABLA DE DETALLES PODEMOS DESARROLLAR FUNCIONES MAS SENCILLAS PARA LA GESTION
-- DE LA TABLA TEMPORAL DE COMPRAS

-- NECESITAMOS PROCEDIENTOS PARAMETRIZABLES PARA EL CRUD SOBRE LA TABLA TEMPORAL ESPECIFICA DEL USUARIO
-- (algun procedimiento contendra acciones logicas de verificacion que en sistemas con alta demanda habria que 
-- delegar en la aplicacion y estos procedimientos centrarse solo en su funcion basica)
-- EN CAMBIO, LAS OPERACIONES DE ACTUALIZACION SOBRE EL STOCK SI TIENEN SENTIDO QUE SE REALIZEN AUTOMATICAMENTE
-- (todo proceso de actualizacion retorna las unidades que componen el carrito, pero podrian no retornar nada)

-- procedimiento para retornar el carrito como una vista
CREATE OR REPLACE FUNCTION showCart(codeuser int)  RETURNS TABLE (daterow timestamptz, prod integer, units int)  AS $$
BEGIN
    RETURN QUERY execute 'select ts, product, units from current_cart_' || codeuser;
END; 
$$  LANGUAGE plpgsql;

--procedimiento para añadir lineas de compra al carrito (comprobando si ya existe el producto para en ese caso solo añadir cantidad)
CREATE OR REPLACE FUNCTION addtoCart(codeuser int, codeprod int, units int)  RETURNS int  AS $$
declare sumunits int;
BEGIN
	execute format ('insert into current_cart_%s values (current_timestamp, $1, $2);', codeuser) USING codeprod, units;
	execute format ('SELECT sum(units) FROM current_cart_%s', codeuser) INTO sumunits;  -- deberiamos ahorrar este calculo
    --y que el numero de unidades maximas, como regla de negocio fuera comprobado por la APP
    RETURN sumunits;
END; 
$$  LANGUAGE plpgsql;

-- comprobaciones
select addtoCart (2000, 100, 3)
select showCart(2000)
SELECT sum(units) FROM current_cart_2000 


-- Procedimiento para modificar SOLAMENTE cantidades de un producto del carrito
CREATE OR REPLACE FUNCTION modifyUnitsCart(codeuser int, codeprod int, units int)  RETURNS int  AS $$
BEGIN
    execute format ('update current_cart_%s set units = $1 where product = $2', codeuser) USING codeprod, units;
    RETURN 1;
END; 
$$  LANGUAGE plpgsql;

select modifyUnitsCart (2000, 5, 8); --comprobacion

-- Procedimiento para eliminar productos del carrito
CREATE OR REPLACE FUNCTION takeoutCart(codeuser int, codeprod int)  RETURNS int  AS $$
BEGIN
    execute format ('delete from current_cart_%s where product = $1', codeuser) USING codeprod;
    RETURN 1;
END; 
$$  LANGUAGE plpgsql;

select takeoutCart(2000, 5); -- comprobacion

--prueba cargando el carrito con varias compras
select addtoCart (2000, 100, 3);
select addtoCart (2000, 101, 2);
select addtoCart (2000, 102, 5);
select addtoCart (2000, 103, 1);
select addtoCart (2000, 104, 2);


-- Procedimiento que guarda el carrito para una posterior revision, se cancelan las unidades reservadas
-- si por reglas de negocio interesa guardar las cantidades de cada producto aunque sea sin reserva, se implementa
CREATE OR REPLACE FUNCTION saveCart(codeuser int, name varchar)  RETURNS int  AS $$
declare v_prods int[];
declare v_units int[];
BEGIN
    execute format ('select array_agg(product), array_agg(units) from current_cart_%s', codeuser) 
    INTO v_prods, v_units;

    execute format ('insert into client_saved_carts values ($1, null, current_timestamp, $2, $3);') 
    USING codeuser, v_prods, v_units; 
    RETURN 0;
END; 
$$ LANGUAGE plpgsql;

insert into client_saved_carts values (2000, null, current_timestamp, null, null);

-- comprobacion
select saveCart(2000, null);
select * from client_saved_carts;
truncate client_saved_carts;

--recordemos la estructura de la tabla que guarda el carrito del usuario temporalmente
CREATE UNLOGGED TABLE  client_saved_carts (
        clientcode  int PRIMARY KEY,
        cartname    varchar (50),
        session_ts  timestamptz,
        products    integer[], 
        units       integer[]
    );

-- procedimiento para cargar uno de los carritos guardados como el actual
CREATE OR REPLACE FUNCTION loadCart(codeuser int, name varchar)  RETURNS int  AS $$
BEGIN
    -- bucle que recorra los array
	RETURN 0;
END; 
$$  LANGUAGE plpgsql;

-- procedimiento para validar la compra a partir del carrito actual
CREATE OR REPLACE FUNCTION acceptCart(codeuser int)  RETURNS int  AS $$
BEGIN
    -- cursor que recorra la tabla temporal y realice inserciones en sale y saledetails
    RETURN 0;
END; 
$$  LANGUAGE plpgsql;

