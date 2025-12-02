# 📚 Explicación del Sistema de Búsqueda de Texto Completo (Full-Text Search)

## 🎯 ¿Qué es este sistema?

Es una implementación de un **motor de búsqueda** similar a Google, pero para buscar en documentos de tu base de datos. Utiliza técnicas avanzadas como:
- **Índice Invertido**: estructura de datos que mapea palabras → documentos
- **TF-IDF**: algoritmo para calcular la relevancia de documentos
- **Similitud de Coseno**: métrica para rankear resultados
- **SPIMI**: técnica para construir índices grandes que no caben en RAM

---

## 📁 Arquitectura de Archivos

### 1. **`preprocessor.py`** - Preprocesador de Texto
**¿Qué hace?**
Prepara el texto antes de indexarlo o buscarlo, realizando 4 pasos:

```
Texto original: "The quick DOGS are running!"
                    ↓
1. Tokenización: ["the", "quick", "dogs", "are", "running"]
2. Minúsculas:   ["the", "quick", "dogs", "are", "running"]
3. Stopwords:    ["quick", "dogs", "running"]  (elimina "the", "are")
4. Stemming:     ["quick", "dog", "run"]       (raíces de palabras)
```

**Componentes principales:**
- `tokenize()`: divide texto en palabras
- `remove_stopwords()`: elimina palabras comunes sin significado ("el", "la", "de")
- `apply_stemming()`: reduce palabras a su raíz ("corriendo" → "corr")
- `preprocess()`: ejecuta todo el pipeline

**Ejemplo de uso:**
```python
preprocessor = TextPreprocessor(language='english')
tokens = preprocessor.preprocess("Machine Learning is amazing!")
# Resultado: ['machin', 'learn', 'amaz']
```

---

### 2. **`inverted_index.py`** - Índice Invertido en Memoria (RAM)
**¿Qué hace?**
Crea una estructura de datos en RAM que permite búsqueda rápida:

```
Estructura del Índice Invertido:
{
    'python': {
        'doc1': {'tf': 5, 'positions': [0, 10, 25, 30, 45]},
        'doc3': {'tf': 2, 'positions': [8, 20]}
    },
    'learning': {
        'doc1': {'tf': 3, 'positions': [5, 15, 35]},
        'doc2': {'tf': 1, 'positions': [12]}
    }
}
```

**Componentes principales:**
- `add_document()`: agrega un documento al índice
- `calculate_tf_idf()`: calcula pesos TF-IDF
- `search()`: busca documentos relevantes
- `save()` / `load()`: guarda/carga el índice desde disco

**Limitación:** Solo funciona para datasets pequeños que caben en RAM (< 500 MB)

---

### 3. **`spimi.py`** - SPIMI (Single-Pass In-Memory Indexing)
**¿Qué hace?**
Construye índices invertidos para **datasets grandes** que NO caben en RAM.

**Algoritmo SPIMI en 3 fases:**

#### **Fase 1: Construcción de Bloques**
```
Documents → [Block 1] [Block 2] [Block 3] ... [Block N]
                ↓         ↓         ↓             ↓
            RAM llena  RAM llena RAM llena    RAM llena
            → Guardar  → Guardar → Guardar  → Guardar
```

Divide los documentos en bloques que sí caben en RAM:
- Lee documentos uno por uno
- Construye índice en RAM hasta llenarla (ej: 100 MB)
- Guarda el bloque en disco (`block_0.pkl`, `block_1.pkl`, etc.)
- Limpia RAM y repite con siguientes documentos

#### **Fase 2: Merge de Bloques**
```
[Block 1] + [Block 2] + [Block 3] + ... + [Block N]
                    ↓
            [Índice Merged Final]
```

Combina todos los bloques en un índice unificado usando **merge sort**:
- Usa heap (cola de prioridad) para procesar términos en orden
- No carga todos los bloques en RAM a la vez
- Genera el índice final combinado

#### **Fase 3: Cálculo TF-IDF y Normalización**
```
Índice Merged → Calcular IDF → Calcular TF-IDF → Normalizar → Guardar bloques finales
```

- **IDF**: `log10(N / df)` donde N = total docs, df = docs con término
- **TF-IDF**: `(1 + log10(tf)) * IDF`
- **Normalización**: divide por norma euclidiana del documento

**Componentes principales:**
- `build_index()`: ejecuta las 3 fases
- `_build_blocks()`: fase 1
- `_merge_blocks()`: fase 2
- `_calculate_tfidf()`: fase 3

**Archivos generados:**
```
data/spimi_blocks/
├── block_0.pkl          # Bloque 0 del índice normalizado
├── block_1.pkl          # Bloque 1 del índice normalizado
├── ...
├── doc_norms.pkl        # Normas euclidianas de documentos
├── doc_ids.pkl          # Mapeo ID numérico → ID original
├── idf_scores.pkl       # Scores IDF de cada término
├── term_to_block.pkl    # Mapeo término → archivo de bloque
└── index_info.pkl       # Metadatos del índice
```

---

### 4. **`spimi_helpers.py`** - Funciones Auxiliares para SPIMI
**¿Qué hace?**
Contiene las funciones matemáticas y de procesamiento para finalizar el índice SPIMI.

**Funciones principales:**
- `compute_idf_scores()`: calcula IDF = log10(N / df)
- `compute_tfidf_weights()`: calcula TF-IDF = (1 + log10(tf)) * IDF
- `compute_document_norms()`: norma = sqrt(Σ weight²)
- `normalize_index_by_doc_norms()`: weight / norma
- `save_blocks_to_disk()`: guarda índice en bloques
- `create_term_to_block_mapping()`: mapea términos a bloques

---

### 5. **`cosine_search.py`** - Motor de Búsqueda
**¿Qué hace?**
Busca documentos relevantes usando **similitud de coseno**.

**Algoritmo:**
```
1. Usuario escribe: "machine learning python"
2. Preprocesar consulta: ['machin', 'learn', 'python']
3. Calcular vector de consulta (TF-IDF de términos)
4. Para cada documento:
   - Cargar posting list del disco (solo términos de consulta)
   - Calcular producto punto: q · d
   - Acumular score
5. Retornar Top-K documentos con mayor score
```

**Similitud de Coseno:**
```
cos(θ) = (q · d) / (||q|| × ||d||)

Donde:
- q = vector de consulta
- d = vector de documento
- ||v|| = norma euclidiana del vector
```

**Componentes principales:**
- `search()`: busca documentos relevantes
- `_load_posting_list_from_disk()`: carga solo los términos necesarios
- `_calculate_query_vector()`: vectoriza la consulta
- `_rank_documents()`: rankea por score descendente

**Optimización clave:** Solo carga del disco los bloques que contienen términos de la consulta (no todo el índice)

---

### 6. **`test_complete_search.py`** - Script de Pruebas
**¿Qué hace?**
Sistema de testing completo con menú interactivo.

**Opciones del menú:**

#### **1. Consulta individual**
Prueba una consulta específica:
```
Consulta: "machine learning"
Top-K: 10
→ Muestra los 10 documentos más relevantes con sus scores
```

**Salida:**
```
🔍 CONSULTA: 'machine learning'
📝 Tokens: ['machin', 'learn']
⏱️  Tiempo de búsqueda: 15.32 ms
📊 Resultados encontrados: 10

📄 Top-10 Documentos:
  1. Doc: 12345
     Score: 0.8523 (85.23%)
  2. Doc: 67890
     Score: 0.7891 (78.91%)
  ...
```

#### **2. Múltiples consultas de prueba**
Ejecuta varias consultas predefinidas y muestra estadísticas:
```python
queries = [
    ("inteligencia artificial", 10),
    ("base de datos", 5),
    ("machine learning", 8),
]
```

**Salida:**
```
📊 ESTADÍSTICAS GENERALES
Total de consultas: 3
Tiempo total: 45.67 ms
Tiempo promedio: 15.22 ms
Consulta más rápida: 12.45 ms
Consulta más lenta: 18.90 ms
```

#### **3. Análisis de rendimiento (diferentes K)**
Prueba la misma consulta con diferentes valores de K (Top-5, Top-10, Top-20, etc.):

**Salida:**
```
⚡ ANÁLISIS DE RENDIMIENTO POR K
Consulta: 'python programming'

K          Tiempo (ms)     Resultados      ms/resultado
--------------------------------------------------------------
5          10.23           5               2.046
10         12.45           10              1.245
20         15.67           20              0.784
50         23.89           50              0.478
100        35.12           100             0.351
```

**Observación:** Más resultados = más tiempo, pero no lineal (optimizado)

#### **4. Pruebas de casos borde**
Prueba casos extremos:
- Consulta vacía: ""
- Términos inexistentes: "xyz123qweasd"
- Solo stopwords: "el la de"
- Una letra: "a"
- Consulta muy larga

**Utilidad:** Verificar que el sistema no falla en casos raros

#### **5. Modo interactivo**
Permite hacer búsquedas en tiempo real:
```
🔍 Consulta: python machine learning
📊 Top-K (default 10): 5

[Muestra resultados]

🔍 Consulta: base de datos
📊 Top-K (default 10): 10

[Muestra resultados]

🔍 Consulta: exit
👋 ¡Hasta luego!
```

Comandos especiales:
- `stats`: muestra estadísticas del índice
- `exit` o `quit`: salir

#### **6. Comparar con búsqueda lineal**
Benchmark teórico vs PostgreSQL:
```
📊 Índice Invertido:
   Tiempo: 15.32 ms
   Resultados: 10

📊 Búsqueda Lineal (PostgreSQL):
   Tiempo: [Ejecutar benchmark con PostgreSQL]
   
💡 Para comparación completa:
   1. Carga el mismo dataset en PostgreSQL
   2. Crea índice GIN sobre tsvector
   3. Ejecuta consulta equivalente
   4. Compara tiempos y resultados
```

#### **7. Mostrar estadísticas del índice**
Muestra información del índice construido:
```
📊 ESTADÍSTICAS DEL ÍNDICE
Total de documentos: 50,000
Total de términos: 125,000
Longitud promedio de doc: 150.23

Archivos en data/spimi_blocks:
  • block_0.pkl                    15.32 MB
  • block_1.pkl                    14.87 MB
  • doc_norms.pkl                   0.45 MB
  • doc_ids.pkl                     0.32 MB
  • idf_scores.pkl                  1.23 MB
  • term_to_block.pkl               0.78 MB
```

---

## 🚀 Cómo Probar el Sistema

### **Paso 1: Construir el Índice SPIMI**

Primero necesitas tener datos. Opciones:

**Opción A: Usar dataset de lyrics (canciones)**
```powershell
# Asegúrate de que existan archivos en datasets/lyrics/
python text_search/spimi.py
```

**Opción B: Importar datos desde tu base de datos**
```python
# Ejemplo: leer desde tu tabla
from text_search.spimi import SPIMIIndexer
from text_search.preprocessor import TextPreprocessor

preprocessor = TextPreprocessor()
indexer = SPIMIIndexer(output_dir='data/spimi_blocks', block_size_mb=100)

def document_generator():
    # Aquí conectas a tu BD y lees documentos
    for doc_id, text in tu_consulta_sql():
        tokens = preprocessor.preprocess(text)
        yield (doc_id, tokens)

indexer.build_index(document_generator())
```

**Salida esperada:**
```
============================================================
🔨 INICIANDO CONSTRUCCIÓN SPIMI
============================================================
🧹 Limpiando archivos anteriores...

📦 FASE 1: Construcción de Bloques
  → Bloque 0: 10,000 docs procesados...
  → Bloque 1: 10,000 docs procesados...
  ...
  ✓ Total: 5 bloques creados

🔀 FASE 2: Merge de Bloques
  → Merging 5 bloques...
  ✓ Índice merged creado

📊 FASE 3: Cálculo TF-IDF
  → Calculando IDF...
  → Calculando TF-IDF...
  → Normalizando vectores...
  ✓ Índice optimizado

✅ Índice SPIMI construido exitosamente
============================================================
```

**Tiempo estimado:** 
- 10,000 docs: ~30 segundos
- 100,000 docs: ~5 minutos
- 1,000,000 docs: ~30-40 minutos

---

### **Paso 2: Ejecutar el Sistema de Pruebas**

```powershell
python text_search/test_complete_search.py
```

**Aparecerá el menú:**
```
============================================================
MENÚ DE PRUEBAS
============================================================
1. Consulta individual
2. Múltiples consultas de prueba
3. Análisis de rendimiento (diferentes K)
4. Pruebas de casos borde
5. Modo interactivo
6. Comparar con búsqueda lineal
7. Mostrar estadísticas del índice
8. Salir
============================================================

Selecciona una opción: 
```

---

### **Paso 3: Ejemplos de Uso**

#### **Ejemplo 1: Búsqueda rápida (Opción 1)**
```
Selecciona una opción: 1

🔍 Ingresa tu consulta: machine learning algorithms
📊 Top-K (default 10): 5

[Muestra los 5 documentos más relevantes]
```

#### **Ejemplo 2: Modo interactivo (Opción 5)**
```
Selecciona una opción: 5

🔎 MODO INTERACTIVO
Comandos:
  - Escribe tu consulta y presiona Enter
  - 'exit' o 'quit' para salir
  - 'stats' para ver estadísticas del índice

🔍 Consulta: python programming
📊 Top-K (default 10): 10

[Resultados...]

🔍 Consulta: deep learning neural networks
📊 Top-K (default 10): 5

[Resultados...]

🔍 Consulta: exit
👋 ¡Hasta luego!
```

#### **Ejemplo 3: Benchmark (Opción 3)**
```
Selecciona una opción: 3

🔍 Ingresa tu consulta: artificial intelligence

[Muestra tabla de rendimiento por K]
```

---

## 🔬 Entendiendo los Resultados

### **Score de Similitud**
```
Score: 0.8523 (85.23%)
```
- **0.0 - 0.3**: Relevancia baja (documento apenas relacionado)
- **0.3 - 0.6**: Relevancia media (documento algo relacionado)
- **0.6 - 0.8**: Relevancia alta (documento muy relacionado)
- **0.8 - 1.0**: Relevancia muy alta (documento extremadamente relevante)

### **Tiempo de Búsqueda**
```
⏱️  Tiempo de búsqueda: 15.32 ms
```
- **< 20 ms**: Excelente (Google ~50-200 ms)
- **20-100 ms**: Bueno
- **100-500 ms**: Aceptable
- **> 500 ms**: Necesita optimización

### **Interpretación de Tokens**
```
Consulta original: "The running dogs are fast!"
📝 Tokens: ['run', 'dog', 'fast']
```
- Se eliminaron stopwords: "The", "are"
- Se aplicó stemming: "running" → "run", "dogs" → "dog"
- Esto permite encontrar variaciones: "dog", "dogs", "running", "run", "runs"

---

## ⚙️ Ajustes y Configuración

### **Cambiar idioma:**
```python
# En preprocessor.py
preprocessor = TextPreprocessor(language='spanish')  # o 'english', 'french', etc.
```

### **Desactivar stemming:**
```python
preprocessor = TextPreprocessor(use_stemming=False)
```

### **Ajustar tamaño de bloques SPIMI:**
```python
# Más pequeño = menos RAM, más bloques
indexer = SPIMIIndexer(output_dir='...', block_size_mb=50)

# Más grande = más RAM, menos bloques (más rápido)
indexer = SPIMIIndexer(output_dir='...', block_size_mb=200)
```

---

## 🐛 Solución de Problemas

### **Error: "Índice no encontrado"**
```
❌ No se encontró el directorio del índice
```
**Solución:** Construye el índice primero:
```powershell
python text_search/spimi.py
```

### **Error: "Import nltk could not be resolved"**
```
Import "nltk" could not be resolved
```
**Solución:** Instala nltk en el entorno correcto:
```powershell
conda install nltk
# o
pip install nltk
```

### **Consulta sin resultados**
```
📊 Resultados encontrados: 0
```
**Causas posibles:**
1. Términos demasiado específicos o inexistentes
2. Todos los términos son stopwords
3. El índice no contiene documentos relevantes

**Solución:** Prueba con consultas más generales

### **Tiempo de búsqueda muy lento (> 1 segundo)**
**Causas posibles:**
1. Índice muy grande en disco lento
2. Muchos términos en la consulta
3. Disco duro mecánico en vez de SSD

**Solución:**
- Usa SSD
- Reduce el número de términos en la consulta
- Aumenta `block_size_mb` al construir el índice

---

## 📊 Comparación: RAM vs SPIMI

| Característica | InvertedIndex (RAM) | SPIMI (Disco) |
|----------------|---------------------|---------------|
| Dataset pequeño (< 10K docs) | ⚡ Muy rápido | 🐢 Innecesario |
| Dataset mediano (10K-100K) | ⚠️ Puede funcionar | ✅ Recomendado |
| Dataset grande (> 100K) | ❌ No cabe en RAM | ✅ Funciona bien |
| Tiempo construcción | Rápido | Moderado |
| Tiempo búsqueda | Muy rápido | Rápido |
| Uso de RAM | Alto | Bajo |

---

## 🎓 Conceptos Clave

### **TF-IDF (Term Frequency - Inverse Document Frequency)**
Mide qué tan importante es una palabra para un documento:
- **TF**: ¿Cuántas veces aparece en el documento?
- **IDF**: ¿Qué tan rara es la palabra en todos los documentos?

Ejemplo:
- "python" aparece 10 veces en doc1 → TF alto
- "python" aparece en 5,000 de 50,000 docs → IDF medio
- Palabra común "el" aparece en 49,000 docs → IDF muy bajo (filtrada)

### **Similitud de Coseno**
Mide el ángulo entre dos vectores:
```
cos(θ) = 0   → Documentos completamente diferentes
cos(θ) = 0.5 → Documentos algo relacionados
cos(θ) = 1.0 → Documentos idénticos
```

### **Índice Invertido**
Estructura de datos invertida:
```
Normal: Doc1 → ["python", "programming", "tutorial"]
Invertido: "python" → [Doc1, Doc5, Doc10]
           "programming" → [Doc1, Doc3, Doc8]
```

Permite búsqueda rápida: O(términos en consulta) en vez de O(todos los documentos)

---

## 🎯 Conclusión

Este sistema implementa un motor de búsqueda profesional con:
- ✅ Preprocesamiento de texto (tokenización, stemming, stopwords)
- ✅ Índice invertido optimizado (SPIMI para grandes datasets)
- ✅ Ranking por relevancia (TF-IDF + Similitud de Coseno)
- ✅ Búsqueda eficiente en disco (solo carga lo necesario)
- ✅ Sistema de pruebas completo

**Casos de uso:**
- Búsqueda en documentos legales
- Motor de búsqueda de productos
- Búsqueda de artículos científicos
- Sistema de recomendación basado en texto
- Análisis de sentimientos en reviews
- Búsqueda en base de conocimiento

¡Ahora puedes buscar en millones de documentos en milisegundos! 🚀
