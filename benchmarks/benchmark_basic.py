"""Script de benchmark básico para el motor de base de datos.

Evalúa el rendimiento de inserciones y consultas en tablas con índices BTREE y AVL.
Genera métricas de tiempo y estadísticas de I/O que se exportan a CSV y JSON.
"""

import os
import sys
import random
import string
from time import perf_counter

ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from parser import run_sql
from metrics import stats

USER = "bench"
DB = "bench_db"


def reset_db():
    """Elimina la base de datos de prueba si existe."""
    db_dir = os.path.join(ROOT, "data", "users", USER, "databases", DB)
    if os.path.exists(db_dir):
        import shutil
        shutil.rmtree(db_dir, ignore_errors=True)


def rand_name(n=8):
    """Genera un nombre aleatorio de n caracteres alfabéticos."""
    return ''.join(random.choice(string.ascii_letters) for _ in range(n))


def run_benchmark(n_rows: int = 2000, n_queries: int = 200):
    """Ejecuta el benchmark completo de inserciones y consultas.
    
    Args:
        n_rows: Número de filas a insertar
        n_queries: Número de consultas a ejecutar
    """
    reset_db()
    stats.reset()
    stats.set_meta("n_rows", n_rows)
    stats.set_meta("n_queries", n_queries)

    run_sql(ROOT, USER, DB, 'CREATE TABLE employees USING INDEX btree(id), avl(name)')

    t0 = perf_counter()
    for i in range(n_rows):
        name = rand_name()
        score = round(random.uniform(5.0, 10.0), 2)
        run_sql(ROOT, USER, DB, f'INSERT INTO employees VALUES (id={i}, name="{name}", score={score})')
    t_insert = perf_counter() - t0

    t0 = perf_counter()
    for _ in range(n_queries):
        if random.random() < 0.7:
            k = random.randint(0, n_rows - 1)
            run_sql(ROOT, USER, DB, f'SELECT * FROM employees WHERE id = {k}')
        else:
            a = rand_name(3)
            b = rand_name(3)
            lo, hi = sorted([a, b])
            run_sql(ROOT, USER, DB, f'SELECT * FROM employees WHERE name BETWEEN "{lo}" AND "{hi}"')
    t_queries = perf_counter() - t0

    snap = stats.snapshot()
    print("Benchmark summary:")
    print(f"rows inserted: {n_rows} in {t_insert:.4f}s")
    print(f"queries run:   {n_queries} in {t_queries:.4f}s")

    out_dir = os.path.join(ROOT, "benchmarks", "out")
    os.makedirs(out_dir, exist_ok=True)
    stats.save_csv(os.path.join(out_dir, "metrics.csv"))
    stats.save_json(os.path.join(out_dir, "metrics.json"))

    try:
        import matplotlib.pyplot as plt
        keys = [
            "io.read_page.calls", "io.write_page.calls", "io.append_page.calls",
            "index.btree.add", "index.btree.search", "index.avl.add", "index.avl.search",
            "table.insert.calls", "table.search.calls"
        ]
        vals = [snap["counters"].get(k, 0) for k in keys]
        plt.figure(figsize=(10, 4))
        plt.bar(keys, vals)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "counters.png"))
        plt.close()
    except Exception as e:
        pass


if __name__ == "__main__":
    run_benchmark()
