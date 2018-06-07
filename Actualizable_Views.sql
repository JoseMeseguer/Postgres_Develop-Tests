set search_path to tests;

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

--drop table client
create table client (
    id  	bigint primary key,      --int64
    name    varchar(20) not null,
    phone varchar(25) not null,
    address   varchar(35) not null
);

create or replace view usersweb as 
select nick, password, email, dateuser, status
from userweb;

create or replace view users as 
select name, nick, password, dateuser, phone, address, email, status
from userweb left join client USING (id);

select * from userweb;
select * from client;
select * from usersweb;
select * from users;

insert into usersweb values ('john2', '1234', 'john@gmail.com', current_timestamp, 0);
insert into usersweb values ('jim', '1234', 'jim@gmail.com', current_timestamp, 0);
update usersweb set status = 1 where nick = 'john2';
delete from usersweb where nick = 'jim';

create or replace view vipsusers as 
select nick, password, email, dateuser, status
from userweb where status <> 0;

select * from vipsusers;
insert into vipsusers values ('luke3', '1234', 'luke@gmail.com', current_timestamp, 1);
update vipsusers set status = 0 where nick = 'john2';
update vipsusers set status = 1 where nick = 'john';   -- NO NOS PERMITIRA MODIFICAR REGISTROS QUE NO APARECEN EN LA VISTA

create or replace view vipsusers as 
select nick, password, email, dateuser, status
from userweb where status <> 0
WITH CASCADED CHECK OPTION;


select * from users;
insert into users values ('luke', '1234', 'luke@gmail.com', current_timestamp, 0);
update users set status = 0 where nick = 'john';


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

--DROP TRIGGER  trg_updateusers  ON users
CREATE TRIGGER trg_updateusers INSTEAD OF INSERT OR UPDATE OR DELETE ON users
FOR EACH ROW EXECUTE PROCEDURE updateusers();

select * from users;
insert into users values ('Lucas Martinez', 'luke', '1234', current_timestamp, '666666666',
						'street 6', 'luke@gmail.com', 0 );
update users set status = 1 where nick = 'luke';
delete from users where nick = 'luke';
