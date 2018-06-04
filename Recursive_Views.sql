set search_path to schematesteo

select * from categories;

WITH RECURSIVE parentcategories AS (
SELECT cat_id, cat_name, cat_name::text AS parent_categories
FROM categories WHERE cat_parent_id = 0 -- parte recursiva de la consulta
UNION ALL -- unión con la parte no recursiva
SELECT c.cat_id, c.cat_name, (pc.parent_categories || ' -> ' || c.cat_name)
FROM categories c INNER JOIN parentcategories pc ON c.cat_parent_id = pc.cat_id
)
SELECT cat_id, cat_name, parent_categories FROM parentcategories -- llamamos a la consulta anterior
ORDER BY cat_id;

-- PODEMOS CREAR UNA VISTA PARA QUE SEA CONSULTADA COMO SI FUERA UNA TABLA
CREATE or REPLACE RECURSIVE VIEW parentcategories (cat_id, cat_name, parent_categories) AS (
SELECT cat_id, cat_name, cat_name::text 
FROM categories WHERE cat_parent_id = 0 
UNION ALL
SELECT c.cat_id, c.cat_name, ( pc.parent_categories || ' -> ' || c.cat_name )
FROM categories c INNER JOIN parentcategories pc ON c.cat_parent_id = pc.cat_id
);
select * from parentcategories;

-- CON UNA VARIANTE POR SI DESEAN QUE EL ORDEN DE PRESENTANCION SUCEDA A LA INVERSA
CREATE or REPLACE RECURSIVE VIEW parentcategoriesinv (cat_id, cat_name, parent_categorie) AS (
SELECT cat_id, cat_name, cat_name::text
FROM categories WHERE cat_parent_id = 0
UNION ALL
SELECT c.cat_id, c.cat_name, ( c.cat_name || ' -> ' || pc.parent_categorie )
FROM categories c INNER JOIN parentcategoriesinv pc ON c.cat_parent_id = pc.cat_id
);
select * from parentcategoriesinv;


-- SI NOS DEMANDAN LOS PRODUCTOS DE UNA DETERMINADA CATEGORIA BASE
-- ESTA OPCION SERIA POCO OPTIMA PERO RAPIDA DE FACILITAR
select * from parentcategories where parent_categories like '%informatica%';


-- LA SOLUCION MÁS OPTIMA EN ESTE CASO ES PARAMETRIZAR UNA FUNCION QUE CREE LA VISTA AD-HOC
create or replace function getcategfamily (id integer) 
returns table (code integer, name varchar, cat_families text) AS
$$
BEGIN
	RETURN QUERY WITH RECURSIVE parentcategories AS (
	SELECT cat_id, cat_name, cat_name::text AS parent_categories
	FROM categories WHERE cat_parent_id = 0 AND cat_id = $1-- parte recursiva de la consulta
	UNION ALL -- unión con la parte no recursiva
	SELECT c.cat_id, c.cat_name, (pc.parent_categories || ' -> ' || c.cat_name)
	FROM categories c INNER JOIN parentcategories pc ON c.cat_parent_id = pc.cat_id
	)
	SELECT cat_id, cat_name, parent_categories FROM parentcategories;

END;
$$ LANGUAGE plpgsql;

select * from getcategfamily(2);