set search_path to tests

-- drop table infogames
-- truncate table infogames
CREATE TABLE infogames (
    id  int  PRIMARY KEY,
    name    varchar(75),
    jsondata    json,
    jsonbdata jsonb
);
CREATE INDEX idxgin     ON infogames USING GIN (jsonbdata);
CREATE INDEX idxginpo   ON infogames USING GIN (jsonbdata jsonb_path_ops);

insert into infogames VALUES 
(1, 'name1', '{"xbox": "one x",  "nintendo": {"3DS":"xl"},  "PlayStation": ["PS3","PS4"]}'::json,
  '{"xbox": "one x",  "nintendo": {"3DS":"xl"},  "PlayStation": ["PS3","PS4"]}'::jsonb),
( 2,'name2',
  to_json(('{"xbox":"one x", "nintendo":{"3DS":["xl","new"],"Wii":"U"}, "PlayStation":"PS3"}')::json),
  to_jsonb(('{"xbox":"one x", "nintendo":{"3DS":["xl","new"],"Wii":"U"},"PlayStation":"PS3"}')::jsonb));

SELECT * from infogames;

-- operador ->> para retornar los valores como text o int y no como un elemento JSON
SELECT id, jsonbdata->'nintendo'->'3DS' as Nin3DS, jsonbdata->'xbox' as XBOX , jsonbdata->>'PlayStation' as PS
FROM infogames
WHERE jsonbdata->'xbox' ? 'one x';

SELECT * FROM infogames WHERE jsonbdata @> '{"xbox": "one x"}';
--presenta el mismo resultado que  
SELECT * FROM infogames WHERE jsonbdata->>'xbox' = 'one x';

SELECT * FROM infogames WHERE jsonbdata->'nintendo'->>'Wii' = 'U';
--presenta el mismo resultado que:
SELECT * FROM infogames WHERE jsonbdata @> '{"nintendo":{"Wii": "U"}}';

SELECT id, jsonbdata->'nintendo'->'3DS' as Nin3DS, jsonbdata->'xbox' as XBOX, jsonbdata->>'PlayStation' as PS
FROM infogames
WHERE jsonbdata->'PlayStation' ? 'PS3' and jsonbdata->'nintendo' @> '{"3DS":["xl","new"],"Wii":"U"}';


-- comprobando si una clave contiene ciertos valores
SELECT '["Fiction", "Thriller", "Horror"]'::jsonb @> '["Fiction", "Horror"]'::jsonb;  --returns true
SELECT '["Fiction", "Horror"]'::jsonb @> '["Fiction", "Thriller", "Horror"]'::jsonb;  -- returns false

--aplicandolo a nuestro ejemplo
SELECT jsonbdata->'PlayStation' FROM infogames WHERE jsonbdata->'PlayStation' @> '["PS3", "PS4"]'::jsonb;  

-- o si contiene ciertos elementos clave
SELECT COUNT(*) FROM infogames WHERE jsonbdata->'nintendo' ? 'Wii';  

-- gran incremento del rendimiento sobre ciertas claves definiendoles un indice
CREATE INDEX idx_nintendogames ON infogames USING GIN ((jsonbdata->'nintendo'));  

-- ejemplos de selects sobre los datos, el ultimo 
SELECT * FROM infogames WHERE jsonbdata->>'xbox' = 'one x';
SELECT jsonbdata->>'nintendo' AS Nintendo FROM infogames;
SELECT jsonbdata->'nintendo'->>'3DS' as Nintendo3DS FROM infogames ORDER BY Nintendo3DS;
SELECT jsonbdata->>'PlayStation' AS customer FROM infogames WHERE jsonbdata->'nintendo'->>'3DS' = 'xl'
SELECT * FROM infogames WHERE jsonbdata->'nintendo' ? 'Wii';
SELECT jsonbdata->'nintendo' ? 'Wii' AS Nintendo_Wii FROM infogames; --indica si existe la llave 'Wii'
SELECT COUNT(*) FROM infogames WHERE jsonbdata->'nintendo' ? 'Wii'; --cuenta cuantos registros cumplen
SELECT * FROM infogames WHERE jsonbdata->'nintendo'->>'Wii' IS NOT NULL; --presenta sus datos

--Buscando valores en un array:
SELECT * FROM infogames WHERE jsonbdata->'nintendo' @> '{"3DS": ["xl"]}';
SELECT * FROM infogames WHERE jsonbdata->>'PlayStation' IN ('PS3','PS4');

--jsonb_each y jsonb_each_text permiten presentar las parejas clave-valor del objeto JSON
SELECT jsonb_each (jsonbdata) FROM infogames WHERE id = 2;
SELECT jsonb_each_text (jsonbdata) FROM infogames WHERE id = 2;

--json_object_keys permite presentar solo las claves del objeto JSON 
SELECT jsonb_object_keys (jsonbdata) FROM infogames WHERE id = 2;

--json_typeof retorna el type de clada clave (el valor puede ser: number, boolean, null, object, array, o string).
SELECT jsonb_typeof (jsonbdata) FROM infogames;

--jsonb_strip_nulls(jsonbdata)  permite presentar los datos del JSON omitiendo los valores nulos
--select array_to_json('{1,2}'::int[])  -- para convertir arrays en array json

--Updating data:
UPDATE infogames 
SET jsonbdata = '{"xbox": "one x", "nintendo":{"2DS":"xl"}, "PlayStation":["PS3","PS4"]}'
WHERE id = 1;

-- añadiendo una clave
UPDATE infogames SET jsonbdata = jsonbdata || '{"alienware": "SteamMachine"}' WHERE id = 1;

--cambiando el valor de una clave
UPDATE infogames SET jsonbdata = jsonbdata - 'xbox' || '{"xbox":"360E"}'
WHERE jsonbdata->'nintendo'->>'2DS' = 'xl';
-- Es igual que ejecutar
UPDATE infogames SET jsonbdata = jsonb_set(jsonbdata, '{xbox}', '"360E"') 
WHERE jsonbdata->'nintendo'->>'2DS' = 'xl';

--añadiendo valores a un array de valores de clave
UPDATE infogames SET jsonbdata = jsonb_set(jsonbdata, '{PlayStation}', (jsonbdata->'PlayStation'||'"PS4_Pro"')::jsonb )
WHERE jsonbdata->'nintendo'->>'2DS' = 'xl';

-- Mas opciones:
--Quitar una llave o un valor con el operador - 
SELECT jsonb '{"a":1,"b":2}' - 'a', -- queda '{"b":2}'
       jsonb '["a",1,"b",2]' - 1    -- queda '["a","b",2]'

-- eliminamos una clave
UPDATE infogames SET jsonbdata = jsonbdata - 'alienware';

---quitar un valor, especificando profundidad en la jerarquia del JSON con el operador #- 
SELECT '{"a":[null,{"b":[5.99]}]}' #- '{a,1,b,0}' -- queda '{"a":[null,{"b":[]}]}'
SELECT '{"a":[null,{"b":[5.99]}]}' #- '{a,1}' -- queda '{"a":[null]}'

-- eliminamos un valor de un array de valores de una clave
UPDATE infogames SET jsonbdata = jsonbdata #- '{PlayStation, 2}'
WHERE jsonbdata->'nintendo'->>'2DS' = 'xl'; 

SELECT * from infogames;


-- ejemplo de modificacion del nombre de una de las claves del documento
--primero añadimos una clave compleja
--luego cambiamos su nombre por una version con un par de majusculas
UPDATE infogames SET jsonbdata = jsonbdata || 
'{"alienware":[{"SteamMachine1":"v1.0"}, {"SteamMachine2":"v2.0"}, {"SteamMachine3":"v2.1"}]}' 
WHERE id = 1;

UPDATE infogames SET jsonbdata = jsonbdata - 'alienware' || jsonb_build_object('AlienWare', jsonbdata->'alienware')
where jsonbdata ? 'alienware';


-- encontrar claves repetidas con vistas recursivas
WITH RECURSIVE find_keys_recursive(key, value) AS (
  	SELECT i.key, i.value 
	FROM infogames, jsonb_each(infogames.jsonbdata) AS i 
	where id = 1	--podemos filtrar el registro del cual queremos evaluar el json
  	UNION ALL
  	SELECT f.key, f.value 
	FROM find_keys_recursive, jsonb_each(CASE 
										WHEN jsonb_typeof(find_keys_recursive.value) <> 'object' 
										THEN '{}'::jsonb
										ELSE find_keys_recursive.value
										END) AS f
	)
SELECT * FROM find_keys_recursive
WHERE jsonb_typeof(find_keys_recursive.value) <> 'object';
