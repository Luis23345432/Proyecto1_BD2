CREATE TABLE restaurantes (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100),
    categoria VARCHAR(50),
    latitud DOUBLE PRECISION,
    longitud DOUBLE PRECISION,
    calificacion NUMERIC(2,1),
    precio_promedio INTEGER
);

ALTER TABLE restaurantes
ADD COLUMN ubicacion geometry(Point, 4326);


UPDATE restaurantes
SET ubicacion = ST_SetSRID(ST_MakePoint(longitud, latitud), 4326);

CREATE INDEX idx_restaurantes_ubicacion 
ON restaurantes 
USING GIST(ubicacion);


-- BÚSQUEDA POR RANGO (rangeSearch)
-- Centro: -12.0464, -77.0428, Radio: 2 km
SELECT 
    id,
    nombre,
    categoria,
    latitud,
    longitud,
    ST_Distance(
        ubicacion::geography,
        ST_SetSRID(ST_MakePoint(-77.0428, -12.0464), 4326)::geography
    ) / 1000 AS distancia_km
FROM restaurantes
WHERE ST_DWithin(
    ubicacion::geography,
    ST_SetSRID(ST_MakePoint(-77.0428, -12.0464), 4326)::geography,
    2000  -- 2 km en metros
)
ORDER BY distancia_km;

-- Funcionando
-- BÚSQUEDA K-NN (kNN)
-- Los 10 restaurantes más cercanos
SELECT 
    id,
    nombre,
    categoria,
    ST_Distance(
        ubicacion::geography,
        ST_SetSRID(ST_MakePoint(-77.0428, -12.0464), 4326)::geography
    ) / 1000 AS distancia_km
FROM restaurantes
ORDER BY ubicacion <-> ST_SetSRID(ST_MakePoint(-77.0428, -12.0464), 4326)
LIMIT 10;


-- BÚSQUEDA EXACTA
SELECT *
FROM restaurantes
WHERE ST_Equals(
    ubicacion,
    ST_SetSRID(ST_MakePoint(-77.064041, -12.069376), 4326)
);


-- INSERCIÓN
INSERT INTO restaurantes (nombre, categoria, latitud, longitud, calificacion, precio_promedio, ubicacion)
VALUES (
    'Nuevo Restaurante',
    'Peruana',
    -12.0500,
    -77.0450,
    4.5,
    80,
    ST_SetSRID(ST_MakePoint(-77.0450, -12.0500), 4326)
);



-- ELIMINACIÓN
DELETE FROM restaurantes
WHERE id = 102;


-- Listar Restaurantes
SELECT *
FROM restaurantes
ORDER BY id ASC;




-- Crear tabla 3D
CREATE TABLE almacen (
  id      TEXT,
  codigo  TEXT,
  tipo    TEXT,
  x       TEXT,
  y       TEXT,
  z       TEXT,
  peso_kg TEXT
);


ALTER TABLE almacen
ADD COLUMN posicion geometry(PointZ, 0);

-- 1) Coma decimal y espacios 
UPDATE almacen SET
  x = REPLACE(TRIM(x::text), ',', '.')::text,
  y = REPLACE(TRIM(y::text), ',', '.')::text,
  z = REPLACE(TRIM(z::text), ',', '.')::text,
  peso_kg = REPLACE(TRIM(peso_kg::text), ',', '.')::text;

-- 2) Casteos de tipos 
ALTER TABLE almacen
  ALTER COLUMN id       TYPE integer USING NULLIF(TRIM(id::text),'')::integer,
  ALTER COLUMN codigo   TYPE varchar(20) USING NULLIF(TRIM(codigo::text),'')::varchar(20),
  ALTER COLUMN tipo     TYPE varchar(50) USING NULLIF(TRIM(tipo::text),'')::varchar(50),
  ALTER COLUMN x        TYPE double precision USING NULLIF(REPLACE(TRIM(x::text),',','.'),'')::double precision,
  ALTER COLUMN y        TYPE double precision USING NULLIF(REPLACE(TRIM(y::text),',','.'),'')::double precision,
  ALTER COLUMN z        TYPE double precision USING NULLIF(REPLACE(TRIM(z::text),',','.'),'')::double precision,
  ALTER COLUMN peso_kg  TYPE numeric(10,2) USING NULLIF(REPLACE(TRIM(peso_kg::text),',','.'), '')::numeric(10,2);


UPDATE almacen
SET posicion = ST_SetSRID(ST_MakePoint(x, y, z), 0);


-- Crear índice 3D
CREATE INDEX idx_almacen_posicion   
ON almacen 
USING GIST(posicion);



-- Búsqueda por rango 3D
SELECT 
    codigo,
    tipo,
    x, y, z,
    ST_3DDistance(
        posicion,
        ST_SetSRID(ST_MakePoint(25.0, 15.0, 5.0), 0)
    ) AS distancia_3d
FROM almacen
WHERE ST_3DDistance(
    posicion,
    ST_SetSRID(ST_MakePoint(25.0, 15.0, 5.0), 0)
) <= 10.0
ORDER BY distancia_3d;





