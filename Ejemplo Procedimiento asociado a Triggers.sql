-- TRIGGER MAS FUNCION ASOCIADA PARA LA TABLA DE DETALLES DE VENTAS
-- CONTROLA LAS OPERACIONES DE ESCRITURA PARA SINCRONIZAR EL ESTADO DE LA TABLA DE STOCK

-- ORDEN DE COMPROBACION: PRIMERO DEBE ESTAR INSERTADO, SE QUITAN LAS UNIDADES RESERVADAS SIEMPRE QUE NO SEAN INFERIORES A LAS UNIDADES A RESTAR
-- DESPUES SE PODRA BORRAR DONDE SE DEVUELVEN LAS UNIDADES DIRECTAMENTE A UNITS YA QUE DESAPARECIERON DE LAS RESERVADAS AL INSERTAR
-- O BIEN MODIFICAR DONDE LAS CANTIDADES DEBERIAN SALIR DIRECTAMENTE DE UNITS YA QUE NO HAY NADA RESERVADO 
-- ESTAS 2 ULTIMAS ACCIONES NO SON LAS USUALES DE NEGOCIO, IMPLICA QUE ALGUIEN CON PERMISOS LAS REALICE MANUALMENTE
-- PERO AUN ASI DEBERIAN VERIFICARSE
CREATE OR REPLACE FUNCTION fn_update_stock() RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') 
            THEN 
                RAISE NOTICE 'quitando unidades reservadas';
                update stock set uncheck_units= uncheck_units - NEW.units 
                where prod= NEW.productid and (uncheck_units - NEW.units) >=0;
                IF NOT FOUND
                THEN    RAISE EXCEPTION 'product % not found, or no units enough', NEW.productid;
                END IF;
        ELSIF (TG_OP = 'DELETE')   -- se quitan unidades directamente de units si es posible
            THEN 
            RAISE NOTICE 'retornando unidades eliminadas';
            update stock set units = units + OLD.units  where prod= OLD.productid;
            IF NOT FOUND 
                THEN RAISE EXCEPTION 'product % not found', NEW.productid;
            END IF;
        ELSIF (TG_OP = 'UPDATE') --la actualizacion puede añadir o quitar unidades
            THEN 
                    IF ( NEW.units > OLD.units )  -- si han modificado para pedir mas producto se descuentan de units si es posible
                        THEN 
                            RAISE NOTICE 'tomando nuevas unidades';
                            update stock set units= units - (NEW.units - OLD.units)
                            where prod= OLD.productid and units - (NEW.units - OLD.units) >=0;
                            IF NOT FOUND 
                            THEN RAISE EXCEPTION 'product % not found, or no units enough', OLD.productid;
                            END IF; 
                        ELSIF ( NEW.units < OLD.units ) -- si se toman menos unidades se devuelven directamente a units teniendo en cuenta el signo de diff
                                THEN 
                                RAISE NOTICE 'retornando unidades devueltas';
                                update stock set units= units + (OLD.units - NEW.units) where prod=OLD.productid;
                                IF NOT FOUND 
                                THEN RAISE EXCEPTION 'product % not found', OLD.productid;
                                END IF; 
                    END IF; 
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER trg_idupdate_stock ON saledetails
CREATE TRIGGER trg_idupdate_stock AFTER INSERT OR DELETE  ON saledetails
FOR EACH ROW   
EXECUTE PROCEDURE fn_update_stock();

-- AFINAMOS AL MAXIMO EL CONTROL DEL DISPARO DEL TRIGGER 
DROP TRIGGER trg_update_stock ON saledetails
CREATE TRIGGER trg_update_stock AFTER UPDATE OF units ON saledetails
FOR EACH ROW WHEN (NEW.units IS DISTINCT FROM OLD.units)  
EXECUTE PROCEDURE fn_update_stock();


-- COMPROBACIONES
insert into stock values (15, 3, 3);
select * from stock where prod = 15;
insert into saledetails values(10000000, 15, null, 2, 0)
insert into saledetails values(10000001, 15, null, 2, 0)
update saledetails set units = 1 where saleid =10000000 and productid = 15
update saledetails set units = 3 where saleid =10000000 and productid = 15
select * from saledetails where saleid =10000000 and productid = 15
delete from saledetails where saleid =10000000 and productid = 15
