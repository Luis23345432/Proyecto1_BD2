"""
Suite de pruebas unitarias para R-Tree
Ejecutar con: python test_rtree.py
"""

import unittest
import math
import os
from RTree import RTree, Point, MBR, RTreeNode
from RTreeIndex import RTreeIndex


class TestPoint(unittest.TestCase):
    """Pruebas para la clase Point"""
    
    def test_point_creation(self):
        """Verificar creación de puntos"""
        p = Point([1.0, 2.0], data="Test")
        self.assertEqual(len(p), 2)
        self.assertEqual(p[0], 1.0)
        self.assertEqual(p[1], 2.0)
        self.assertEqual(p.data, "Test")
    
    def test_point_indexing(self):
        """Verificar indexación de coordenadas"""
        p = Point([3.5, 4.2, 1.8])
        self.assertEqual(p[0], 3.5)
        self.assertEqual(p[1], 4.2)
        self.assertEqual(p[2], 1.8)


class TestMBR(unittest.TestCase):
    """Pruebas para la clase MBR"""
    
    def test_mbr_creation(self):
        """Verificar creación de MBR"""
        mbr = MBR([0, 0], [10, 10])
        self.assertEqual(mbr.lower, [0, 0])
        self.assertEqual(mbr.upper, [10, 10])
        self.assertEqual(mbr.dimensions, 2)
    
    def test_mbr_area(self):
        """Verificar cálculo de área"""
        mbr = MBR([0, 0], [5, 4])
        self.assertEqual(mbr.area(), 20.0)
        
        mbr_3d = MBR([0, 0, 0], [2, 3, 4])
        self.assertEqual(mbr_3d.area(), 24.0)
    
    def test_mbr_contains_point(self):
        """Verificar si MBR contiene punto"""
        mbr = MBR([0, 0], [10, 10])
        
        # Punto dentro
        p1 = Point([5, 5])
        self.assertTrue(mbr.contains_point(p1))
        
        # Punto en el borde
        p2 = Point([10, 10])
        self.assertTrue(mbr.contains_point(p2))
        
        # Punto fuera
        p3 = Point([11, 5])
        self.assertFalse(mbr.contains_point(p3))
    
    def test_mbr_intersects(self):
        """Verificar intersección entre MBRs"""
        mbr1 = MBR([0, 0], [5, 5])
        mbr2 = MBR([3, 3], [8, 8])
        mbr3 = MBR([6, 6], [10, 10])
        
        # MBR1 y MBR2 se intersectan
        self.assertTrue(mbr1.intersects(mbr2))
        
        # MBR1 y MBR3 no se intersectan
        self.assertFalse(mbr1.intersects(mbr3))
    
    def test_mbr_union(self):
        """Verificar unión de MBRs"""
        mbr1 = MBR([0, 0], [5, 5])
        mbr2 = MBR([3, 3], [8, 8])
        
        union = mbr1.union(mbr2)
        self.assertEqual(union.lower, [0, 0])
        self.assertEqual(union.upper, [8, 8])
    
    def test_mbr_from_point(self):
        """Verificar creación de MBR desde un punto"""
        p = Point([3, 4])
        mbr = MBR.from_point(p)
        
        self.assertEqual(mbr.lower, [3, 4])
        self.assertEqual(mbr.upper, [3, 4])
        self.assertEqual(mbr.area(), 0.0)
    
    def test_mbr_from_points(self):
        """Verificar creación de MBR desde múltiples puntos"""
        points = [
            Point([1, 2]),
            Point([5, 6]),
            Point([3, 1])
        ]
        
        mbr = MBR.from_points(points)
        self.assertEqual(mbr.lower, [1, 1])
        self.assertEqual(mbr.upper, [5, 6])


class TestRTree(unittest.TestCase):
    """Pruebas para la clase RTree"""
    
    def setUp(self):
        """Configuración antes de cada prueba"""
        self.rtree = RTree(max_entries=4, min_entries=2, dimensions=2)
    
    def test_rtree_creation(self):
        """Verificar creación del R-Tree"""
        self.assertEqual(len(self.rtree), 0)
        self.assertEqual(self.rtree.max_entries, 4)
        self.assertEqual(self.rtree.min_entries, 2)
        self.assertEqual(self.rtree.dimensions, 2)
    
    def test_add_single_point(self):
        """Verificar inserción de un punto"""
        p = Point([3, 4], data="Point A")
        self.rtree.add(p)
        
        self.assertEqual(len(self.rtree), 1)
        self.assertTrue(self.rtree.root.is_leaf)
    
    def test_add_multiple_points(self):
        """Verificar inserción de múltiples puntos"""
        points = [
            Point([1, 2], data="A"),
            Point([3, 4], data="B"),
            Point([5, 6], data="C"),
            Point([7, 8], data="D"),
        ]
        
        for p in points:
            self.rtree.add(p)
        
        self.assertEqual(len(self.rtree), 4)
    
    def test_add_triggers_split(self):
        """Verificar que la inserción causa split cuando el nodo está lleno"""
        points = [
            Point([1, 2]),
            Point([3, 4]),
            Point([5, 6]),
            Point([7, 8]),
            Point([9, 10]),  # Este debería causar un split
        ]
        
        for p in points:
            self.rtree.add(p)
        
        self.assertEqual(len(self.rtree), 5)
        # Después del split, la raíz no debería ser hoja
        self.assertFalse(self.rtree.root.is_leaf)
    
    def test_search_exact(self):
        """Verificar búsqueda exacta"""
        p1 = Point([3, 4], data="Target")
        p2 = Point([5, 6], data="Other")
        
        self.rtree.add(p1)
        self.rtree.add(p2)
        
        results = self.rtree.search(Point([3, 4]))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].data, "Target")
        
        # Buscar punto no existente
        results = self.rtree.search(Point([10, 10]))
        self.assertEqual(len(results), 0)
    
    def test_range_search(self):
        """Verificar búsqueda por rango"""
        points = [
            Point([0, 0], data="A"),
            Point([1, 1], data="B"),
            Point([2, 2], data="C"),
            Point([10, 10], data="D"),  # Lejos
        ]
        
        for p in points:
            self.rtree.add(p)
        
        # Buscar puntos cerca de (0, 0) con radio 3
        query = Point([0, 0])
        results = self.rtree.rangeSearch(query, 3.0)
        
        self.assertGreaterEqual(len(results), 2)
        
        # Verificar que el punto lejano no está incluido
        data_values = [p.data for p in results]
        self.assertNotIn("D", data_values)
    
    def test_knn_search(self):
        """Verificar búsqueda KNN"""
        points = [
            Point([0, 0], data="A"),
            Point([1, 1], data="B"),
            Point([2, 2], data="C"),
            Point([3, 3], data="D"),
            Point([10, 10], data="E"),
        ]
        
        for p in points:
            self.rtree.add(p)
        
        # Buscar los 3 vecinos más cercanos a (0, 0)
        query = Point([0, 0])
        results = self.rtree.knnSearch(query, 3)
        
        self.assertEqual(len(results), 3)
        
        # Verificar que están ordenados por distancia
        prev_dist = 0
        for point, dist in results:
            self.assertGreaterEqual(dist, prev_dist)
            prev_dist = dist
        
        # El punto más cercano debería ser (0, 0)
        self.assertEqual(results[0][0].data, "A")
        self.assertAlmostEqual(results[0][1], 0.0)
    
    def test_remove_point(self):
        """Verificar eliminación de punto"""
        p1 = Point([3, 4], data="A")
        p2 = Point([5, 6], data="B")
        
        self.rtree.add(p1)
        self.rtree.add(p2)
        
        self.assertEqual(len(self.rtree), 2)
        
        # Eliminar p1
        removed = self.rtree.remove(p1)
        self.assertTrue(removed)
        self.assertEqual(len(self.rtree), 1)
        
        # Verificar que ya no existe
        results = self.rtree.search(p1)
        self.assertEqual(len(results), 0)
        
        # Intentar eliminar punto no existente
        removed = self.rtree.remove(Point([100, 100]))
        self.assertFalse(removed)
    
    def test_mindist(self):
        """Verificar cálculo de MINDIST"""
        rtree = RTree()
        
        # Punto fuera del MBR a la izquierda
        point = Point([1, 2])
        mbr = MBR([5, 1], [9, 7])
        mindist = rtree._mindist(point, mbr)
        self.assertEqual(mindist, 4.0)
        
        # Punto dentro del MBR
        point = Point([6, 3])
        mindist = rtree._mindist(point, mbr)
        self.assertEqual(mindist, 0.0)
    
    def test_get_all_points(self):
        """Verificar obtención de todos los puntos"""
        points = [
            Point([1, 2], data="A"),
            Point([3, 4], data="B"),
            Point([5, 6], data="C"),
        ]
        
        for p in points:
            self.rtree.add(p)
        
        all_points = self.rtree.get_all_points()
        self.assertEqual(len(all_points), 3)
    
    def test_statistics(self):
        """Verificar estadísticas del árbol"""
        for i in range(10):
            self.rtree.add(Point([i, i], data=f"Point {i}"))
        
        stats = self.rtree.get_statistics()
        
        self.assertEqual(stats['total_points'], 10)
        self.assertGreater(stats['height'], 0)
        self.assertGreater(stats['total_nodes'], 0)


class TestRTreePersistence(unittest.TestCase):
    """Pruebas de persistencia"""
    
    def setUp(self):
        """Configuración antes de cada prueba"""
        self.test_file = "test_rtree.bin"
    
    def tearDown(self):
        """Limpieza después de cada prueba"""
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
    
    def test_save_and_load(self):
        """Verificar guardado y carga del índice"""
        # Crear y poblar R-Tree
        rtree = RTree(max_entries=4, min_entries=2, dimensions=2)
        points = [
            Point([1, 2], data="A"),
            Point([3, 4], data="B"),
            Point([5, 6], data="C"),
        ]
        
        for p in points:
            rtree.add(p)
        
        original_size = len(rtree)
        
        # Guardar
        rtree.save_to_file(self.test_file)
        self.assertTrue(os.path.exists(self.test_file))
        
        # Cargar
        loaded_rtree = RTree.load_from_file(self.test_file)
        
        # Verificar
        self.assertEqual(len(loaded_rtree), original_size)
        self.assertEqual(loaded_rtree.max_entries, 4)
        self.assertEqual(loaded_rtree.min_entries, 2)
        
        # Verificar que los puntos están presentes
        all_points = loaded_rtree.get_all_points()
        self.assertEqual(len(all_points), 3)


class TestRTreeIndex(unittest.TestCase):
    """Pruebas para RTreeIndex wrapper"""
    
    def setUp(self):
        """Configuración antes de cada prueba"""
        self.index_file = "test_index.bin"
        self.index = RTreeIndex(self.index_file, dimensions=2)
    
    def tearDown(self):
        """Limpieza después de cada prueba"""
        if os.path.exists(self.index_file):
            os.remove(self.index_file)
    
    def test_add_record(self):
        """Verificar inserción de registros"""
        record = {"id": 1, "name": "Test", "value": 100}
        success = self.index.add([10.5, 20.3], record)
        
        self.assertTrue(success)
        self.assertEqual(len(self.index), 1)
    
    def test_search_record(self):
        """Verificar búsqueda de registros"""
        record = {"id": 1, "name": "Test"}
        coords = [10.5, 20.3]
        
        self.index.add(coords, record)
        results = self.index.search(coords)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], 1)
    
    def test_range_search_with_wrapper(self):
        """Verificar búsqueda por rango con wrapper"""
        records = [
            {"id": 1, "name": "A"},
            {"id": 2, "name": "B"},
            {"id": 3, "name": "C"},
        ]
        coords = [
            [0, 0],
            [1, 1],
            [10, 10],  # Lejos
        ]
        
        for i, record in enumerate(records):
            self.index.add(coords[i], record)
        
        # Buscar cerca de (0, 0)
        results = self.index.rangeSearch([0, 0], radius=2.0)
        
        # Debería encontrar al menos 2 (los cercanos)
        self.assertGreaterEqual(len(results), 2)
    
    def test_knn_search_with_wrapper(self):
        """Verificar búsqueda KNN con wrapper"""
        for i in range(5):
            record = {"id": i, "value": i * 10}
            self.index.add([i, i], record)
        
        results = self.index.knnSearch([0, 0], k=3)
        
        self.assertEqual(len(results), 3)
        # Verificar orden por distancia
        self.assertEqual(results[0][0]["id"], 0)  # Más cercano
    
    def test_remove_record(self):
        """Verificar eliminación de registros"""
        coords = [5.5, 6.6]
        record = {"id": 1, "name": "Test"}
        
        self.index.add(coords, record)
        self.assertEqual(len(self.index), 1)
        
        removed = self.index.remove(coords)
        self.assertTrue(removed)
        self.assertEqual(len(self.index), 0)
    
    def test_save_and_load_index(self):
        """Verificar persistencia del índice wrapper"""
        # Agregar datos
        for i in range(3):
            self.index.add([i, i], {"id": i})
        
        # Guardar
        self.index.save()
        
        # Cargar en nueva instancia
        new_index = RTreeIndex(self.index_file, dimensions=2)
        
        self.assertEqual(len(new_index), 3)
    
    def test_get_statistics(self):
        """Verificar estadísticas del índice"""
        for i in range(5):
            self.index.add([i, i], {"id": i})
        
        stats = self.index.get_statistics()
        
        self.assertEqual(stats["total_points"], 5)
        self.assertEqual(stats["dimensions"], 2)
        self.assertIn("index_file", stats)


class TestRTreePerformance(unittest.TestCase):
    """Pruebas de rendimiento"""
    
    def test_insertion_performance(self):
        """Verificar rendimiento de inserciones"""
        import time
        
        rtree = RTree(max_entries=50, min_entries=25, dimensions=2)
        n_points = 1000
        
        start = time.time()
        for i in range(n_points):
            x = i % 100
            y = i // 100
            rtree.add(Point([x, y], data=f"Point {i}"))
        end = time.time()
        
        elapsed = end - start
        print(f"\nInsertando {n_points} puntos: {elapsed:.4f}s")
        print(f"Tiempo promedio por inserción: {(elapsed/n_points)*1000:.4f}ms")
        
        self.assertEqual(len(rtree), n_points)
        self.assertLess(elapsed, 5.0)  # Debe completarse en menos de 5 segundos
    
    def test_search_performance(self):
        """Verificar rendimiento de búsquedas"""
        import time
        
        rtree = RTree(max_entries=50, min_entries=25, dimensions=2)
        
        # Insertar puntos
        for i in range(1000):
            rtree.add(Point([i % 100, i // 100], data=i))
        
        # Medir búsquedas por rango
        n_searches = 100
        start = time.time()
        for i in range(n_searches):
            query = Point([i % 50, i // 50])
            results = rtree.rangeSearch(query, 5.0)
        end = time.time()
        
        elapsed = end - start
        print(f"\n{n_searches} búsquedas por rango: {elapsed:.4f}s")
        print(f"Tiempo promedio por búsqueda: {(elapsed/n_searches)*1000:.4f}ms")
        
        self.assertLess(elapsed, 2.0)  # Debe completarse en menos de 2 segundos
    
    def test_knn_performance(self):
        """Verificar rendimiento de búsquedas KNN"""
        import time
        
        rtree = RTree(max_entries=50, min_entries=25, dimensions=2)
        
        # Insertar puntos
        for i in range(1000):
            rtree.add(Point([i % 100, i // 100], data=i))
        
        # Medir búsquedas KNN
        n_searches = 100
        k = 10
        start = time.time()
        for i in range(n_searches):
            query = Point([i % 50, i // 50])
            results = rtree.knnSearch(query, k)
        end = time.time()
        
        elapsed = end - start
        print(f"\n{n_searches} búsquedas KNN (k={k}): {elapsed:.4f}s")
        print(f"Tiempo promedio por búsqueda: {(elapsed/n_searches)*1000:.4f}ms")
        
        self.assertLess(elapsed, 3.0)  # Debe completarse en menos de 3 segundos


class TestRTreeEdgeCases(unittest.TestCase):
    """Pruebas de casos extremos"""
    
    def test_empty_tree_operations(self):
        """Verificar operaciones en árbol vacío"""
        rtree = RTree()
        
        # Búsqueda en árbol vacío
        results = rtree.rangeSearch(Point([0, 0]), 10.0)
        self.assertEqual(len(results), 0)
        
        # KNN en árbol vacío
        results = rtree.knnSearch(Point([0, 0]), 5)
        self.assertEqual(len(results), 0)
        
        # Eliminar de árbol vacío
        removed = rtree.remove(Point([0, 0]))
        self.assertFalse(removed)
    
    def test_single_point_operations(self):
        """Verificar operaciones con un solo punto"""
        rtree = RTree()
        p = Point([5, 5], data="Single")
        rtree.add(p)
        
        # KNN con k mayor al número de puntos
        results = rtree.knnSearch(Point([0, 0]), k=10)
        self.assertEqual(len(results), 1)
        
        # Búsqueda por rango que contiene el punto
        results = rtree.rangeSearch(Point([5, 5]), 1.0)
        self.assertEqual(len(results), 1)
    
    def test_duplicate_coordinates(self):
        """Verificar manejo de coordenadas duplicadas"""
        rtree = RTree()
        
        p1 = Point([5, 5], data="First")
        p2 = Point([5, 5], data="Second")
        
        rtree.add(p1)
        rtree.add(p2)
        
        self.assertEqual(len(rtree), 2)
        
        # Buscar puntos en esa ubicación
        results = rtree.search(Point([5, 5]))
        # Debería encontrar al menos uno
        self.assertGreaterEqual(len(results), 1)
    
    def test_negative_coordinates(self):
        """Verificar manejo de coordenadas negativas"""
        rtree = RTree()
        
        points = [
            Point([-10, -10], data="A"),
            Point([-5, 5], data="B"),
            Point([5, -5], data="C"),
        ]
        
        for p in points:
            rtree.add(p)
        
        self.assertEqual(len(rtree), 3)
        
        # Buscar cerca del origen
        results = rtree.rangeSearch(Point([0, 0]), 10.0)
        self.assertGreater(len(results), 0)
    
    def test_large_coordinates(self):
        """Verificar manejo de coordenadas grandes"""
        rtree = RTree()
        
        p = Point([1000000, 2000000], data="Large")
        rtree.add(p)
        
        results = rtree.search(Point([1000000, 2000000]))
        self.assertEqual(len(results), 1)
    
    def test_high_dimensional_space(self):
        """Verificar funcionamiento en espacios de alta dimensión"""
        rtree = RTree(dimensions=5)
        
        points = [
            Point([1, 2, 3, 4, 5], data="A"),
            Point([2, 3, 4, 5, 6], data="B"),
            Point([10, 11, 12, 13, 14], data="C"),
        ]
        
        for p in points:
            rtree.add(p)
        
        self.assertEqual(len(rtree), 3)
        
        # Búsqueda KNN en 5D
        results = rtree.knnSearch(Point([1, 2, 3, 4, 5]), k=2)
        self.assertEqual(len(results), 2)


class TestRTreeCorrectness(unittest.TestCase):
    """Pruebas de corrección de resultados"""
    
    def test_range_search_correctness(self):
        """Verificar que rangeSearch retorna todos los puntos correctos"""
        rtree = RTree()
        
        # Crear grid de puntos
        points = []
        for i in range(10):
            for j in range(10):
                p = Point([i, j], data=f"{i},{j}")
                points.append(p)
                rtree.add(p)
        
        # Buscar cerca de (5, 5) con radio 2
        center = Point([5, 5])
        radius = 2.0
        results = rtree.rangeSearch(center, radius)
        
        # Verificar manualmente cuántos puntos deberían estar
        expected_count = 0
        for p in points:
            dist = math.sqrt((p[0] - 5)**2 + (p[1] - 5)**2)
            if dist <= radius:
                expected_count += 1
        
        self.assertEqual(len(results), expected_count)
    
    def test_knn_search_correctness(self):
        """Verificar que KNN retorna los vecinos correctos"""
        rtree = RTree()
        
        # Puntos con distancias conocidas desde el origen
        points = [
            Point([1, 0], data="1"),    # dist = 1
            Point([0, 2], data="2"),    # dist = 2
            Point([2, 2], data="2.83"), # dist = 2.83
            Point([3, 0], data="3"),    # dist = 3
            Point([0, 4], data="4"),    # dist = 4
        ]
        
        for p in points:
            rtree.add(p)
        
        # Buscar los 3 más cercanos al origen
        results = rtree.knnSearch(Point([0, 0]), k=3)
        
        self.assertEqual(len(results), 3)
        
        # Verificar que están ordenados por distancia
        self.assertEqual(results[0][0].data, "1")
        self.assertEqual(results[1][0].data, "2")
        self.assertEqual(results[2][0].data, "2.83")
        
        # Verificar distancias
        self.assertAlmostEqual(results[0][1], 1.0, places=2)
        self.assertAlmostEqual(results[1][1], 2.0, places=2)
        self.assertAlmostEqual(results[2][1], 2.83, places=2)


def run_tests():
    """Ejecuta todas las pruebas y muestra un resumen"""
    
    print("=" * 80)
    print("SUITE DE PRUEBAS PARA R-TREE")
    print("=" * 80)
    
    # Crear suite de pruebas
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Agregar todas las clases de prueba
    suite.addTests(loader.loadTestsFromTestCase(TestPoint))
    suite.addTests(loader.loadTestsFromTestCase(TestMBR))
    suite.addTests(loader.loadTestsFromTestCase(TestRTree))
    suite.addTests(loader.loadTestsFromTestCase(TestRTreePersistence))
    suite.addTests(loader.loadTestsFromTestCase(TestRTreeIndex))
    suite.addTests(loader.loadTestsFromTestCase(TestRTreePerformance))
    suite.addTests(loader.loadTestsFromTestCase(TestRTreeEdgeCases))
    suite.addTests(loader.loadTestsFromTestCase(TestRTreeCorrectness))
    
    # Ejecutar pruebas
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Resumen
    print("\n" + "=" * 80)
    print("RESUMEN DE PRUEBAS")
    print("=" * 80)
    print(f"Total de pruebas ejecutadas: {result.testsRun}")
    print(f"✓ Exitosas: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"✗ Fallidas: {len(result.failures)}")
    print(f"⚠ Errores: {len(result.errors)}")
    
    if result.wasSuccessful():
        print("\n🎉 ¡TODAS LAS PRUEBAS PASARON EXITOSAMENTE!")
    else:
        print("\n⚠️  Algunas pruebas fallaron. Revisa los errores arriba.")
    
    print("=" * 80)
    
    return result


if __name__ == "__main__":
    import sys
    result = run_tests()
    
    # Salir con código de error si las pruebas fallaron
    sys.exit(0 if result.wasSuccessful() else 1)