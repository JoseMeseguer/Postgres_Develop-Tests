set search_path to tests;

-- DOS TABLAS CON INFORMACION SOBRE CLIENTES PERO QUE PUEDEN CONSULTARSE INDEPENDIENTEMENTE
--drop  table userweb cascade
create table userweb (
    id  bigserial primary key,      --tipo especial, autonumerico de 8 bytes
    nick    varchar(20) not null,
    password varchar(25) not null,
    email   varchar(35) not null,
    dateuser timestamptz not null,
	status int	-- 0 = registered, 1 = member, 2 = premium
);
create unique index uniquenick on userweb(nick);
-- CREAMOS UN INDICE PARA QUE NO SE REPITA EL NICK DEL USUARIO

--drop table client
create table client (
    id  	bigint primary key,      --int64
    name    varchar(20) not null,
    phone varchar(25) not null,
    address   varchar(35) not null
);

-- VISTA AUTOMATICAMENTE ACTUALIZABLE SOBRE USERWEB
create or replace view usersweb as 
select nick, password, email, dateuser, status
from userweb;

-- VISTA QUE "FUSIONA" AMBAS TABLAS PERO QUE NO ES AUTOMATICAMENTE ACTUALIZABLE
create or replace view users as 
select name, nick, password, dateuser, phone, address, email, status
from userweb left join client USING (id);

-- CONJUNTO DE SELECTS SOBRE TABLAS Y VISTAS
select * from userweb;
select * from client;
select * from usersweb;
select * from users;

-- OPERACIONES DE ESCRITURA DIRECTAMENTE SOBRE LA VISTA ACTUALIZABLE
insert into usersweb values ('john2', '1234', 'john@gmail.com', current_timestamp, 0);
insert into usersweb values ('jim', '1234', 'jim@gmail.com', current_timestamp, 0);
update usersweb set status = 1 where nick = 'john2';
delete from usersweb where nick = 'jim';


-- VISTA SOBRE USERWEB CON UNA CONDICION, PERO QUE SIN CREAR UNA REGLA PARA COMPROBAR DICHA CONDICION
create or replace view vipsusers as 
select nick, password, email, dateuser, status
from userweb where status <> 0;

select * from vipsusers;
-- PERMITE INSERTAR Y MODIFICAR REGISTROS QUE DESPUES YA NO PODREMOS VER
insert into vipsusers values ('luke3', '1234', 'luke@gmail.com', current_timestamp, 0);
update vipsusers set status = 0 where nick = 'john2';
update vipsusers set status = 1 where nick = 'john';   -- NO NOS PERMITIRA MODIFICAR REGISTROS QUE NO APARECEN EN LA VISTA


-- OBLIGAMOS A QUE LA VISTA COMPRUEBE LA CONDICION
create or replace view vipsusers as 
select nick, password, email, dateuser, status
from userweb where status <> 0
WITH CASCADED CHECK OPTION;
-- YA NO SE NOS PERMITIRAN MODIFICACIONES QUE IMPLICARAN QUE LOS DATOS QUEDEN FUERA DE LA VISTA



select * from users;
-- COMPROBACION DE QUE LA VISTA USERS NO ES AUTOMATICAMENTE ACTUALIZABLE
insert into users values ('luke', '1234', 'luke@gmail.com', current_timestamp, 0);
update users set status = 0 where nick = 'john';



-- PARA IMPLEMENTAR QUE SEA ACTUALIZABLE CREAMOS LA FUNCION A LA QUE LLAMARA EL 
-- TRIGGER INSTEAD OF
CREATE OR REPLACE FUNCTION updateusers()  RETURNS TRIGGER AS $$
DECLARE ref bigint;
BEGIN
    IF (TG_OP = 'INSERT') 
    THEN 
        insert into userweb values (default, new.nick, new.password, new.email,
								new.dateuser, new.status) returning id into ref;
		insert into client values (ref, new.name, new.phone, new.address);
    ELSIF (TG_OP = 'DELETE')   
    THEN 
        delete from userweb where nick = OLD.nick returning id into ref;
		delete from client where id= ref;
		return old;
    ELSIF (TG_OP = 'UPDATE') 
    THEN 
		update userweb set nick = new.nick, password = new.password, 
							email = new.email, dateuser = new.dateuser, status = new.status
        where nick= new.nick returning id into ref;
		update client set name = new.name, phone = new.phone, address = new.address
        where id= ref;
    END IF;
    RETURN new;
END;
$$ LANGUAGE plpgsql;

-- TRIGGER QUE LLAMARA A LA FUNCION PARA QUE EJECUTE SUS COMANDO EN LUGAR DE LOS QUE 
-- HA REALIZADO EL USUARIO
--DROP TRIGGER  trg_updateusers  ON users
CREATE TRIGGER trg_updateusers INSTEAD OF INSERT OR UPDATE OR DELETE ON users
FOR EACH ROW EXECUTE PROCEDURE updateusers();


-- COMPROBACION DE QUE LA VISTA YA ES ACTUALIZABLE A TRAVES DEL TRIGGER
select * from users;
insert into users values ('Lucas Martinez', 'luke', '1234', current_timestamp, '666666666',
						'street 6', 'luke@gmail.com', 0 );
update users set status = 1 where nick = 'luke';
delete from users where nick = 'luke';
