# 🚀 GUÍA RÁPIDA: Cargar CSV y Probar el Sistema

## Paso a Paso Completo

### **PASO 1: Ejecutar el Constructor de Índice**

```powershell
python text_search/build_index_from_csv.py
```

El script te mostrará un menú interactivo:

```
============================================================
🔍 CONSTRUCTOR DE ÍNDICE SPIMI DESDE CSV
============================================================

📂 Buscando archivos CSV...

✓ 4 archivo(s) CSV encontrado(s):
  1. postman/isam.csv (0.02 MB)
  2. postman/restaurantes.csv (0.15 MB)
  3. postman/sample_people.csv (0.01 MB)
  4. postman/two-stars-michelin-restaurants.csv (0.25 MB)

📁 Selecciona un archivo CSV:
Número: _
```

---

### **PASO 2: Seleccionar el CSV**

Escribe el número del CSV que quieres usar. Por ejemplo:

```
Número: 4
```

El script te mostrará un preview:

```
📄 Preview de: postman/two-stars-michelin-restaurants.csv
============================================================
📋 Columnas: name, address, city, country, cuisine, description

📊 Primeras 5 filas:

Fila 1:
  name: Le Bernardin
  address: 155 West 51st Street
  city: New York
  country: USA
  cuisine: French
  description: Renowned seafood restaurant with elegant atmosphere...

...

📊 Total de filas en el archivo: 500
```

---

### **PASO 3: Seleccionar Columnas de Texto**

Elige qué columnas quieres indexar (las que contienen texto para buscar):

```
📝 Selecciona las columnas que contienen TEXTO para indexar:
   (Puedes seleccionar múltiples columnas, se concatenarán)
------------------------------------------------------------
  1. name
  2. address
  3. city
  4. country
  5. cuisine
  6. description

💡 Ejemplo: Si quieres usar columna 1 y 6, escribe: 1,6

Columnas a usar (separadas por coma): 1,6
```

**Recomendación:** Selecciona las columnas con más texto relevante. Por ejemplo:
- Para restaurantes: `1,5,6` (name, cuisine, description)
- Para personas: nombre, biografía, descripción
- Para productos: nombre, descripción, categoría

---

### **PASO 4: Configuración Adicional**

#### **4.1 Idioma**
```
🌍 Idioma para stopwords:
  1. English (inglés)
  2. Spanish (español)
Selecciona (default: 1): 1
```

**Importante:** Selecciona el idioma del texto de tu CSV
- Si el CSV está en inglés → `1`
- Si está en español → `2`

---

#### **4.2 Número de Documentos**
```
📊 ¿Cuántos documentos procesar?
  • Presiona Enter para procesar TODOS
  • O escribe un número (ej: 1000 para prueba rápida)
Número de documentos: 1000
```

**Recomendación:**
- Primera vez / Prueba rápida: `1000` (toma ~30 segundos)
- Producción: Presiona Enter (procesa todos)

---

#### **4.3 Tamaño de Bloque**
```
💾 Tamaño de bloque en RAM:
  • 50 MB - Para RAM limitada
  • 100 MB - Recomendado (default)
  • 200 MB - Para más velocidad
MB (default: 100): [Enter]
```

**Recomendación:** Deja el default (presiona Enter)

---

### **PASO 5: Confirmación y Construcción**

```
============================================================
📋 RESUMEN:
============================================================
Archivo: postman/two-stars-michelin-restaurants.csv
Columnas: name, description
Idioma: english
Documentos: 1000
Tamaño bloque: 100 MB
============================================================

¿Proceder con la construcción? (s/n): s
```

Escribe `s` y presiona Enter.

**El proceso iniciará:**

```
============================================================
🚀 INICIANDO CONSTRUCCIÓN DEL ÍNDICE
============================================================
📁 Archivo: postman/two-stars-michelin-restaurants.csv
📝 Columnas: name, description
💾 Salida: data/spimi_blocks
🌍 Idioma: english
📦 Tamaño de bloque: 100 MB
============================================================

📖 Leyendo documentos...
  ✓ 1000 documentos procesados...

✅ Total procesado: 1000 documentos

============================================================
🔨 INICIANDO CONSTRUCCIÓN SPIMI
============================================================
🧹 Limpiando archivos anteriores...

📦 FASE 1: Construcción de Bloques
  → Bloque 0: 1000 docs, 5000 términos
  ✓ 1 bloques creados en 2.34 segundos

🔀 FASE 2: Merge de Bloques
  → Merging 1 bloques...
  ✓ Índice merged creado

📊 FASE 3: Cálculo TF-IDF
  📊 Calculando IDF scores...
     ✓ 5000 términos procesados
  📊 Calculando pesos TF-IDF...
     ✓ Pesos TF-IDF calculados
  📊 Calculando normas de documentos...
     ✓ 1000 normas calculadas
  📊 Normalizando índice...
     ✓ Índice normalizado
  💾 Guardando bloques finales...
     ✓ 5 bloques guardados

✅ Índice SPIMI construido exitosamente
============================================================

============================================================
✅ ÍNDICE CONSTRUIDO EXITOSAMENTE
============================================================
📁 Ubicación: data/spimi_blocks

📋 Archivos generados:
  ✓ doc_norms.pkl              0.01 MB
  ✓ doc_ids.pkl                0.02 MB
  ✓ idf_scores.pkl             0.05 MB
  ✓ term_to_block.pkl          0.03 MB
  ✓ index_info.pkl             0.00 MB
  ✓ Bloques: 5 archivos (2.50 MB)

📊 Tamaño total del índice: 2.61 MB

============================================================
🎯 PRÓXIMOS PASOS:
============================================================
1. Ejecuta el sistema de pruebas:
   python text_search/test_complete_search.py

2. O prueba una búsqueda rápida:
   from text_search.cosine_search import CosineSearch
   searcher = CosineSearch(index_dir='data/spimi_blocks')
   results = searcher.search(['tu', 'consulta'])
============================================================
```

---

### **PASO 6: Probar el Sistema de Búsqueda**

Una vez construido el índice, ejecuta:

```powershell
python text_search/test_complete_search.py
```

Aparecerá el menú:

```
============================================================
MENÚ DE PRUEBAS
============================================================
1. Consulta individual
2. Múltiples consultas de prueba
3. Análisis de rendimiento (diferentes K)
4. Pruebas de casos borde
5. Modo interactivo              ← RECOMENDADO PARA EMPEZAR
6. Comparar con búsqueda lineal
7. Mostrar estadísticas del índice
8. Salir
============================================================

Selecciona una opción: 5
```

---

### **PASO 7: Modo Interactivo (Opción 5)**

```
🔎 MODO INTERACTIVO
============================================================
Comandos:
  - Escribe tu consulta y presiona Enter
  - 'exit' o 'quit' para salir
  - 'stats' para ver estadísticas del índice
============================================================

🔍 Consulta: french seafood restaurant
📊 Top-K (default 10): 5

============================================================
🔍 CONSULTA: 'french seafood restaurant'
============================================================
📝 Tokens: ['french', 'seafood', 'restaur']

⏱️  Tiempo de búsqueda: 12.45 ms
📊 Resultados encontrados: 5

📄 Top-5 Documentos:
  1. Doc: doc_1
     Score: 0.8523 (85.23%)
  2. Doc: doc_42
     Score: 0.7891 (78.91%)
  3. Doc: doc_156
     Score: 0.7234 (72.34%)
  4. Doc: doc_89
     Score: 0.6890 (68.90%)
  5. Doc: doc_203
     Score: 0.6512 (65.12%)

🔍 Consulta: exit
👋 ¡Hasta luego!
```

---

## 💡 Ejemplos de Consultas

Dependiendo de tu CSV:

### **Para Restaurantes:**
```
- "italian pizza pasta"
- "french michelin star"
- "seafood sushi japanese"
- "vegan vegetarian healthy"
```

### **Para Personas:**
```
- "engineer software developer"
- "manager sales marketing"
- "doctor medical healthcare"
```

### **Para Productos:**
```
- "laptop computer gaming"
- "phone mobile android"
- "camera photography professional"
```

---

## 🔧 Solución de Problemas

### **Error: "No se encontraron archivos CSV"**
**Solución:** Copia tu CSV a la carpeta `postman/` o `datasets/`

### **Error: "Error leyendo CSV"**
**Causas posibles:**
1. CSV mal formado (sin header)
2. Encoding incorrecto (no UTF-8)

**Solución:** Abre el CSV en Excel/LibreOffice y guárdalo como UTF-8

### **Búsqueda sin resultados**
**Causas posibles:**
1. Idioma incorrecto (CSV en español, pero seleccionaste inglés)
2. Todos los términos son stopwords
3. Términos muy específicos

**Solución:** 
- Verifica el idioma
- Usa términos más generales
- Revisa que las columnas seleccionadas contengan texto

### **Proceso muy lento**
**Solución:** Limita los documentos en el PASO 4.2:
- Primera prueba: `100` documentos
- Prueba media: `1000` documentos
- Producción: todos

---

## 📊 Interpretación de Resultados

### **Score de Similitud:**
- **0.8 - 1.0**: Extremadamente relevante (match casi perfecto)
- **0.6 - 0.8**: Muy relevante (buen match)
- **0.4 - 0.6**: Relevante (match moderado)
- **0.2 - 0.4**: Poco relevante (match débil)
- **< 0.2**: Casi irrelevante

### **Tiempo de Búsqueda:**
- **< 50 ms**: Excelente
- **50-200 ms**: Bueno
- **200-500 ms**: Aceptable
- **> 500 ms**: Necesita optimización

---

## 🎯 Resumen de Comandos

```powershell
# Paso 1: Construir índice
python text_search/build_index_from_csv.py

# Paso 2: Probar búsquedas
python text_search/test_complete_search.py
```

---

## ✅ Checklist

- [ ] CSV copiado a `postman/` o `datasets/`
- [ ] Ejecutado `build_index_from_csv.py`
- [ ] Seleccionado CSV y columnas correctas
- [ ] Configurado idioma correcto
- [ ] Índice construido exitosamente
- [ ] Ejecutado `test_complete_search.py`
- [ ] Probado búsquedas en modo interactivo

---

¡Listo! Ahora tienes un motor de búsqueda funcionando con tu CSV 🎉
