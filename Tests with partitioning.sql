    set search_path to partitions

    create table   Productsp  (
        id          serial     primary key,
        name        varchar(50),
        description varchar(200),
        prod_categ integer
    );
    create table   Salesp  (
        id          serial     primary key, 
        beginDate   timestamptz,
        endDate     timestamptz,
        price       float,
        client      int    
    );
    create table   SaleDetailsp  (
        saleid      int,
        productid   int,
        name        varchar(50),
        units       int,
        unitprice   float    
    ); 