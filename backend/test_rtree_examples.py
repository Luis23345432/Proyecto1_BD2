"""
test_rtree_examples.py
Tests completos y ejemplos de validación del R-Tree.

Este archivo contiene:
1. Tests unitarios
2. Tests con datos reales (CSV)
3. Ejemplos 2D y 3D
4. Comparación con PostgreSQL/PostGIS (queries de referencia)

"""

import os
import csv
import time
import math
import random
from RTreeSpatialIndex import RTreeSpatialIndex
from RTreeWrapper import RTreeWrapper


# =============================================================================
# GENERACIÓN DE DATOS DE PRUEBA
# =============================================================================

def generar_csv_restaurantes_2d(filename='test_restaurantes_2d.csv', n=100):
    """
    Genera un CSV con restaurantes 2D (latitud, longitud).
    Simula restaurantes en Lima, Perú.
    """
    print(f"\n📝 Generando {filename} con {n} registros...")
    
    # Centro de Lima: -12.0464, -77.0428
    centro_lat, centro_lon = -12.0464, -77.0428
    
    categorias = ['Peruana', 'Italiana', 'China', 'Japonesa', 'Fast Food', 
                  'Mexicana', 'Vegetariana', 'Mariscos', 'Parrillas', 'Postres']
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['id', 'nombre', 'categoria', 'latitud', 'longitud', 
                     'calificacion', 'precio_promedio']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for i in range(1, n + 1):
            # Generar coordenadas aleatorias (radio ~5km)
            offset_lat = random.uniform(-0.05, 0.05)
            offset_lon = random.uniform(-0.05, 0.05)
            
            writer.writerow({
                'id': i,
                'nombre': f'Restaurante {i}',
                'categoria': random.choice(categorias),
                'latitud': round(centro_lat + offset_lat, 6),
                'longitud': round(centro_lon + offset_lon, 6),
                'calificacion': round(random.uniform(3.0, 5.0), 1),
                'precio_promedio': random.randint(20, 150)
            })
    
    print(f"✓ Archivo {filename} generado")
    return filename


def generar_csv_almacen_3d(filename='test_almacen_3d.csv', n=50):
    """
    Genera un CSV con posiciones 3D de cajas en un almacén.
    Coordenadas: x, y, z (metros)
    """
    print(f"\n📝 Generando {filename} con {n} registros...")
    
    tipos = ['Electronica', 'Ropa', 'Alimentos', 'Libros', 'Juguetes']
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['id', 'codigo', 'tipo', 'x', 'y', 'z', 'peso_kg']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for i in range(1, n + 1):
            writer.writerow({
                'id': i,
                'codigo': f'BOX{i:04d}',
                'tipo': random.choice(tipos),
                'x': round(random.uniform(0, 50), 2),
                'y': round(random.uniform(0, 30), 2),
                'z': round(random.uniform(0, 10), 2),
                'peso_kg': round(random.uniform(1, 100), 2)
            })
    
    print(f"✓ Archivo {filename} generado")
    return filename


# =============================================================================
# TESTS 2D
# =============================================================================

def test_2d_basico():
    """Test básico con índice 2D"""
    print("\n" + "="*80)
    print("TEST 1: OPERACIONES BÁSICAS 2D")
    print("="*80)
    
    rtree = RTreeSpatialIndex('test_2d_basic', dimensions=2)
    rtree.clear()
    
    # 1. Inserción
    print("\n1. INSERCIÓN")
    print("-"*80)
    points = [
        ([10.0, 20.0], {"name": "A", "type": "restaurant"}),
        ([10.5, 20.3], {"name": "B", "type": "hotel"}),
        ([15.0, 25.0], {"name": "C", "type": "restaurant"}),
        ([10.2, 20.1], {"name": "D", "type": "cafe"}),
        ([10.8, 20.5], {"name": "E", "type": "restaurant"}),
    ]
    
    inserted_ids = []
    for coords, data in points:
        point_id = rtree.add(coords, data)
        inserted_ids.append(point_id)
        print(f"  ✓ Insertado: {data['name']} en {coords} (ID: {point_id})")
    
    print(f"\nTotal insertados: {len(rtree)}")
    assert len(rtree) == 5, "Debería haber 5 puntos"
    
    # 2. Búsqueda por rango
    print("\n2. BÚSQUEDA POR RANGO")
    print("-"*80)
    center = [10.0, 20.0]
    radius = 1.0
    print(f"Centro: {center}, Radio: {radius}")
    
    results = rtree.rangeSearch(center, radius)
    print(f"Encontrados: {len(results)} puntos")
    for r in results:
        print(f"  - {r['data']['name']}: coord={r['coordinates']}, dist={r['distance']:.4f}")
    
    assert len(results) >= 3, "Debería encontrar al menos 3 puntos"
    
    # 3. Búsqueda KNN
    print("\n3. BÚSQUEDA K-NN (k=3)")
    print("-"*80)
    knn_results = rtree.kNN(center, k=3)
    print(f"Los 3 más cercanos a {center}:")
    for i, r in enumerate(knn_results, 1):
        print(f"  {i}. {r['data']['name']}: dist={r['distance']:.4f}")
    
    assert len(knn_results) == 3, "Debería retornar exactamente 3"
    assert knn_results[0]['distance'] <= knn_results[1]['distance'], "Debe estar ordenado"
    
    # 4. Eliminación
    print("\n4. ELIMINACIÓN")
    print("-"*80)
    id_to_remove = inserted_ids[0]
    print(f"Eliminando ID: {id_to_remove}")
    
    removed = rtree.remove(id_to_remove)
    assert removed == True, "Debería eliminar exitosamente"
    print(f"  ✓ Eliminado")
    print(f"  Total restante: {len(rtree)}")
    
    assert len(rtree) == 4, "Debería quedar 4 puntos"
    
    # 5. Búsqueda del punto eliminado
    print("\n5. VERIFICAR ELIMINACIÓN")
    print("-"*80)
    results_after = rtree.rangeSearch(points[0][0], 0.01)
    print(f"Buscando punto eliminado: {len(results_after)} encontrados")
    assert len(results_after) == 0, "No debería encontrar el punto eliminado"
    print("  ✓ Punto correctamente eliminado")
    
    rtree.clear()
    print("\n✅ TEST 2D BÁSICO: PASADO")


def test_2d_con_csv():
    """Test 2D cargando desde CSV"""
    print("\n" + "="*80)
    print("TEST 2: CARGA DESDE CSV 2D")
    print("="*80)
    
    # Generar CSV
    csv_file = generar_csv_restaurantes_2d('test_rest.csv', n=100)
    
    # Cargar en R-Tree
    print("\n📂 Cargando CSV en R-Tree...")
    rtree = RTreeSpatialIndex('test_2d_csv', dimensions=2)
    rtree.clear()
    
    start = time.time()
    count = rtree.load_from_csv(csv_file, ['latitud', 'longitud'])
    elapsed = time.time() - start
    
    print(f"✓ {count} registros cargados en {elapsed:.4f}s")
    print(f"  Promedio: {(elapsed/count)*1000:.2f}ms por registro")
    
    # Estadísticas
    print("\n📊 ESTADÍSTICAS DEL ÍNDICE")
    print("-"*80)
    stats = rtree.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Pruebas de búsqueda
    print("\n🔍 PRUEBAS DE BÚSQUEDA")
    print("-"*80)
    
    # Centro de Lima
    mi_ubicacion = [-12.0464, -77.0428]
    
    # Búsqueda por rango (2 km)
    print(f"\n1. Búsqueda por rango (2 km desde {mi_ubicacion})")
    radio_km = 2.0
    radio_grados = radio_km / 111.0
    
    start = time.time()
    range_results = rtree.rangeSearch(mi_ubicacion, radio_grados)
    elapsed = time.time() - start
    
    print(f"   Encontrados: {len(range_results)} restaurantes")
    print(f"   Tiempo: {elapsed*1000:.2f}ms")
    print(f"   Primeros 5:")
    for r in range_results[:5]:
        dist_km = r['distance'] * 111.0
        print(f"     - {r['data']['nombre']}: {dist_km:.2f} km")
    
    # Búsqueda KNN
    print(f"\n2. Búsqueda 10-NN desde {mi_ubicacion}")
    
    start = time.time()
    knn_results = rtree.kNN(mi_ubicacion, k=10)
    elapsed = time.time() - start
    
    print(f"   Tiempo: {elapsed*1000:.2f}ms")
    print(f"   Los 10 más cercanos:")
    for i, r in enumerate(knn_results, 1):
        dist_km = r['distance'] * 111.0
        print(f"     {i}. {r['data']['nombre']}: {dist_km:.2f} km "
              f"({r['data']['categoria']}, ⭐{r['data']['calificacion']})")
    
    # Limpiar
    #os.remove(csv_file)
    rtree.clear()
    print("\n✅ TEST CSV 2D: PASADO")


# =============================================================================
# TESTS 3D
# =============================================================================

def test_3d_basico():
    """Test básico con índice 3D"""
    print("\n" + "="*80)
    print("TEST 3: OPERACIONES BÁSICAS 3D")
    print("="*80)
    
    rtree = RTreeSpatialIndex('test_3d_basic', dimensions=3)
    rtree.clear()
    
    # 1. Inserción 3D
    print("\n1. INSERCIÓN 3D (almacén)")
    print("-"*80)
    boxes = [
        ([10.0, 20.0, 5.0], {"codigo": "BOX001", "tipo": "Electronica"}),
        ([10.5, 20.3, 5.2], {"codigo": "BOX002", "tipo": "Ropa"}),
        ([15.0, 25.0, 8.0], {"codigo": "BOX003", "tipo": "Alimentos"}),
        ([10.2, 20.1, 5.1], {"codigo": "BOX004", "tipo": "Libros"}),
    ]
    
    for coords, data in boxes:
        point_id = rtree.add(coords, data)
        print(f"  ✓ Caja {data['codigo']} en {coords} (ID: {point_id})")
    
    print(f"\nTotal: {len(rtree)} cajas")
    assert len(rtree) == 4
    
    # 2. Búsqueda por rango 3D
    print("\n2. BÚSQUEDA POR RANGO 3D")
    print("-"*80)
    center = [10.0, 20.0, 5.0]
    radius = 1.0
    print(f"Buscando cajas dentro de {radius}m desde {center}")
    
    results = rtree.rangeSearch(center, radius)
    print(f"Encontradas: {len(results)} cajas")
    for r in results:
        print(f"  - {r['data']['codigo']}: {r['coordinates']}, dist={r['distance']:.3f}m")
    
    # 3. KNN 3D
    print("\n3. BÚSQUEDA K-NN 3D (k=2)")
    print("-"*80)
    knn_results = rtree.kNN(center, k=2)
    for i, r in enumerate(knn_results, 1):
        print(f"  {i}. {r['data']['codigo']}: dist={r['distance']:.3f}m")
    
    assert len(knn_results) == 2
    
    rtree.clear()
    print("\n✅ TEST 3D BÁSICO: PASADO")


def test_3d_con_csv():
    """Test 3D cargando desde CSV"""
    print("\n" + "="*80)
    print("TEST 4: CARGA DESDE CSV 3D")
    print("="*80)
    
    # Generar CSV
    csv_file = generar_csv_almacen_3d('test_warehouse.csv', n=50)
    
    # Cargar en R-Tree
    print("\n📂 Cargando CSV 3D en R-Tree...")
    rtree = RTreeSpatialIndex('test_3d_csv', dimensions=3)
    rtree.clear()
    
    count = rtree.load_from_csv(csv_file, ['x', 'y', 'z'])
    print(f"✓ {count} cajas cargadas")
    
    # Búsqueda en una sección del almacén
    print("\n🔍 BÚSQUEDA EN SECCIÓN DEL ALMACÉN")
    print("-"*80)
    seccion_centro = [25.0, 15.0, 5.0]
    radio = 10.0
    
    print(f"Buscando cajas en radio de {radio}m desde {seccion_centro}")
    results = rtree.rangeSearch(seccion_centro, radio)
    print(f"Encontradas: {len(results)} cajas en esta sección")
    
    for r in results[:5]:
        print(f"  - {r['data']['codigo']}: {r['coordinates']}, "
              f"tipo={r['data']['tipo']}, dist={r['distance']:.2f}m")
    
    # Limpiar
    #os.remove(csv_file)
    rtree.clear()
    print("\n✅ TEST CSV 3D: PASADO")



# =============================================================================
# QUERIES DE REFERENCIA PARA POSTGRESQL/POSTGIS
# =============================================================================

def generar_queries_postgis():
    """
    Genera las queries SQL equivalentes para PostgreSQL/PostGIS
    que se pueden usar para validar los resultados.
    """
    print("\n" + "="*80)
    print("QUERIES DE REFERENCIA PARA POSTGRESQL/POSTGIS")
    print("="*80)
    
    queries = """
-- ============================================================================
-- SETUP: Crear tabla e índice en PostgreSQL
-- ============================================================================

-- 1. Crear extensión PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- 2. Crear tabla
CREATE TABLE restaurantes (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100),
    categoria VARCHAR(50),
    latitud DOUBLE PRECISION,
    longitud DOUBLE PRECISION,
    calificacion NUMERIC(2,1),
    precio_promedio INTEGER,
    ubicacion GEOMETRY(Point, 4326)
);

-- 3. Cargar datos desde CSV (ajustar ruta)
COPY restaurantes(id, nombre, categoria, latitud, longitud, calificacion, precio_promedio)
FROM '/path/to/test_restaurantes_2d.csv'
DELIMITER ','
CSV HEADER;

-- 4. Actualizar columna geometry
UPDATE restaurantes
SET ubicacion = ST_SetSRID(ST_MakePoint(longitud, latitud), 4326);

-- 5. Crear índice espacial GIST (equivalente al R-Tree)
CREATE INDEX idx_restaurantes_ubicacion 
ON restaurantes 
USING GIST(ubicacion);

-- ============================================================================
-- QUERIES DE BÚSQUEDA (para comparar con tu implementación)
-- ============================================================================

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
    ST_SetSRID(ST_MakePoint(-77.0428, -12.0464), 4326)
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
WHERE id = 5;

-- ============================================================================
-- QUERIES PARA DATOS 3D
-- ============================================================================

-- Crear tabla 3D
CREATE TABLE almacen (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(20),
    tipo VARCHAR(50),
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,
    z DOUBLE PRECISION,
    peso_kg NUMERIC(10,2),
    posicion GEOMETRY(PointZ, 0)  -- Sistema de coordenadas local
);

-- Actualizar geometría 3D
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

    """
    
    # Guardar queries en archivo
    with open('postgis_validation_queries.sql', 'w', encoding='utf-8') as f:
        f.write(queries)
    
    print(queries)
    print("\n✓ Queries guardadas en 'postgis_validation_queries.sql'")
    print("\nPara validar:")
    print("1. Ejecuta estas queries en PostgreSQL/PostGIS")
    print("2. Compara los resultados con los de tu implementación Python")
    print("3. Los resultados deben ser idénticos (o muy similares)")


# =============================================================================
# MAIN - EJECUTAR TODOS LOS TESTS
# =============================================================================

def run_all_tests():
    """Ejecuta todos los tests"""
    print("\n" + "="*80)
    print(" " * 20 + "SUITE DE TESTS COMPLETA")
    print("="*80)
    
    try:
        # Tests 2D
        test_2d_basico()
        test_2d_con_csv()
        
        # Tests 3D
        test_3d_basico()
        test_3d_con_csv()
        
        
        # Generar queries de validación
        generar_queries_postgis()
        
        print("\n" + "="*80)
        print(" " * 20 + "✅ TODOS LOS TESTS PASARON")
        print("="*80)
        
    except AssertionError as e:
        print(f"\n❌ TEST FALLIDO: {e}")
        raise
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        raise


if __name__ == "__main__":
    run_all_tests()