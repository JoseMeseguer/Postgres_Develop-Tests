set search_path to tests;

--drop  table userweb cascade
create table userweb (
    id  bigserial,      --tipo especial, autonumerico de 8 bytes
    nick    varchar(20) not null,
    password varchar(25) not null,
    email   varchar(35) not null,
    dateuser timestamptz not null,
	status int	-- 0 = registered, 1 = member, 2 = premium
);

--drop table client
create table client (
    id  	bigint,      --int64
    name    varchar(20) not null,
    phone varchar(25) not null,
    address   varchar(35) not null
);

create or replace view usersweb as 
select nick, password, email, dateuser, status
from userweb;

create or replace view users as 
select name, nick, password, dateuser, phone, address, email, status
from userweb inner join client using (id);

select * from userweb;
select * from client;
select * from usersweb;
select * from users;

insert into usersweb values ('john', '1234', 'john@gmail.com', current_timestamp, 0);
insert into usersweb values ('jim', '1234', 'jim@gmail.com', current_timestamp, 0);
update usersweb set status = 1 where nick = 'john';
delete from usersweb where nick = 'jim';

create or replace view vipsusers as 
select nick, password, email, dateuser, status
from userweb where status <> 0;

select * from vipsusers;
insert into vipsusers values ('luke', '1234', 'luke@gmail.com', current_timestamp, 0);
update vipsusers set status = 0 where nick = 'john';
update vipsusers set status = 1 where nick = 'john';

create or replace view vipsusers as 
select nick, password, email, dateuser, status
from userweb where status <> 0
WITH CASCADE CHECK OPTION;


select * from users;
insert into users values ('luke', '1234', 'luke@gmail.com', current_timestamp, 0);
update users set status = 0 where nick = 'john';


CREATE OR REPLACE FUNCTION updateusers()  RETURNS TRIGGER AS $$
DECLARE ref integer;
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


CREATE TRIGGER trg_updateusers INSTEAD OF INSERT OR UPDATE OR DELETE ON users
FOR EACH ROW EXECUTE PROCEDURE updateusers();

select * from users;
insert into users values ('Lucas Martinez', 'luke', '1234', current_timestamp, '666666666',
						'street 6', 'luke@gmail.com', 0 );
update users set status = 1 where nick = 'luke';
delete from users where nick = 'luke';
