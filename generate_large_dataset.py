"""
Script para generar dataset de 2000 restaurantes
Amplía el CSV original con variaciones realistas
"""

import csv
import random

# Plantillas base de los 20 restaurantes originales
restaurantes_base = [
    ("La Casa", "Restaurante tradicional de la casa: platos caseros, pollo a la brasa y guisos al estilo familiar. Ambiente acogedor."),
    ("El Puerto", "Mariscos frescos, ceviche, camarones y pescado a la plancha. Vista al mar y horario para cenas."),
    ("Doña Lupita", "Comida mexicana auténtica: tacos al pastor, mole poblano y salsas caseras. Excelente para familias."),
    ("Parrilla Norte", "Parrilla y carnes: cortes importados, parrilladas para dos y servicio de cerveza artesanal."),
    ("Sakura Sushi", "Sushi y cocina japonesa: nigiri, maki, ramen y menú vegetariano. Ideal para cenas formales."),
    ("Pizza Roma", "Pizzas al horno de leña, masa fina, ingredientes italianos y opciones vegetarianas. Entrega a domicilio."),
    ("El Rincón Vegetal", "Opciones veganas y vegetarianas, bowls, smoothies y postres saludables. Ambiente moderno."),
    ("Café Central", "Desayunos, brunch y café de especialidad. Tostadas, croissants y pastelería artesanal. Wifi gratis."),
    ("Tacos El Rey", "Tacos y antojitos: pastor, bistec, suadero y salsa picante. Comida rápida tradicional."),
    ("La Tasca", "Tapas y platos españoles: paella, jamón ibérico, sangría y menú para compartir. Música en vivo fines de semana."),
    ("Mar y Tierra", "Fusión mediterránea: platos con mariscos y carnes. Recomendado: risotto de camarón."),
    ("Bistro Provenza", "Cocina francesa y platillos europeos: quiches, crepes y menú ejecutivo al mediodía."),
    ("El Mercado", "Ingredientes locales y mercado-cocina: platillos del día con productos de temporada."),
    ("La Esquina", "Comida rápida y antojitos: hamburguesas gourmet, papas fritas y batidos. Excelente para niños."),
    ("Brasa y Sazón", "Pollo rostizado, adobos caseros y guarniciones tradicionales. Servicio familiar y para llevar."),
    ("Sabor a Mar", "Especialidad en mariscos y pescados: cócteles de mariscos, filete a la mantequilla."),
    ("Noche de Tapas", "Bar de tapas y vinos, ambiente nocturno y música en vivo. Ideal para grupos."),
    ("Panadería Dulce Hogar", "Panadería y repostería: pan fresco, pasteles personalizados y café por la mañana."),
    ("La Cantina", "Cantina mexicana clásica: botanas, tequilas, mezcales y platos fuertes estilo casero."),
    ("Sabor Asiático", "Cocina asiática variada: wok, dim sum, sushi y platos picantes. Servicio rápido."),
]

# Variaciones para nombres
prefijos = ["El", "La", "Los", "Las", "Don", "Doña", "Casa", "Restaurante"]
sufijos_lugar = ["Norte", "Sur", "Este", "Oeste", "Centro", "Del Valle", "Colonial", "Moderno"]
sufijos_tema = ["Real", "Imperial", "Tradicional", "Gourmet", "Express", "Premium", "Clásico"]

# Variaciones para descripciones
adjetivos_comida = ["deliciosa", "auténtica", "tradicional", "casera", "gourmet", "fresca", "selecta", "artesanal"]
adjetivos_ambiente = ["acogedor", "elegante", "moderno", "rústico", "familiar", "íntimo", "amplio", "luminoso"]
servicios_extra = [
    "Wifi gratis", "Estacionamiento", "Terraza", "Delivery", "Reservas online",
    "Música en vivo", "Para llevar", "Buffet", "Menú infantil", "Pet friendly"
]

ciudades = [
    "Lima", "Cusco", "Arequipa", "Trujillo", "Chiclayo", "Piura", "Iquitos", "Tacna",
    "Huancayo", "Ayacucho", "Puno", "Moquegua", "Tumbes", "Cajamarca", "Chimbote"
]

def generar_variacion_nombre(nombre_base, indice):
    """Genera una variación del nombre"""
    nombre_limpio = nombre_base.replace("El ", "").replace("La ", "").replace("Los ", "").replace("Las ", "")
    
    if indice % 10 == 0:
        return nombre_base  # Cada 10, mantener el original
    elif indice % 5 == 0:
        # Agregar ciudad
        return f"{nombre_base} {random.choice(ciudades)}"
    elif indice % 3 == 0:
        # Agregar sufijo de lugar
        return f"{nombre_base} {random.choice(sufijos_lugar)}"
    else:
        # Agregar sufijo temático
        return f"{nombre_base} {random.choice(sufijos_tema)}"

def generar_variacion_descripcion(desc_base):
    """Genera una variación de la descripción"""
    # Agregar un adjetivo aleatorio
    adj_comida = random.choice(adjetivos_comida)
    adj_ambiente = random.choice(adjetivos_ambiente)
    servicio = random.choice(servicios_extra)
    
    # Insertar variaciones en la descripción
    variaciones = [
        f"Comida {adj_comida}. {desc_base}",
        f"{desc_base} Ambiente {adj_ambiente}.",
        f"{desc_base} {servicio} disponible.",
        f"{adj_comida.capitalize()} y {adj_ambiente}. {desc_base}",
        f"{desc_base} Ofrecemos {servicio.lower()}.",
    ]
    
    return random.choice(variaciones)

def generar_dataset_ampliado(num_restaurantes=2000, archivo_salida='postman/restaurantes2_extended.csv'):
    """Genera el dataset ampliado"""
    print(f"🔨 Generando dataset de {num_restaurantes} restaurantes...")
    
    restaurantes_generados = []
    
    # Generar restaurantes
    for i in range(num_restaurantes):
        # Seleccionar un restaurante base de forma cíclica
        idx_base = i % len(restaurantes_base)
        nombre_base, desc_base = restaurantes_base[idx_base]
        
        # Generar variaciones
        nombre_nuevo = generar_variacion_nombre(nombre_base, i)
        desc_nueva = generar_variacion_descripcion(desc_base)
        
        restaurantes_generados.append({
            'id': i + 1,
            'name': nombre_nuevo,
            'description': desc_nueva
        })
        
        # Progreso
        if (i + 1) % 200 == 0:
            print(f"  ✓ {i + 1} restaurantes generados...")
    
    # Guardar en CSV
    print(f"\n💾 Guardando en {archivo_salida}...")
    with open(archivo_salida, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['id', 'name', 'description'])
        writer.writeheader()
        writer.writerows(restaurantes_generados)
    
    print(f"✅ Dataset generado exitosamente!")
    print(f"📊 Total de restaurantes: {len(restaurantes_generados)}")
    print(f"📁 Archivo: {archivo_salida}")
    
    # Estadísticas
    nombres_unicos = len(set(r['name'] for r in restaurantes_generados))
    print(f"📝 Nombres únicos: {nombres_unicos}")
    
    return archivo_salida

if __name__ == "__main__":
    import sys
    
    # Permitir especificar número de restaurantes
    num = 2000
    if len(sys.argv) > 1:
        try:
            num = int(sys.argv[1])
        except:
            pass
    
    archivo = generar_dataset_ampliado(num)
    
    print(f"\n🎯 Próximos pasos:")
    print(f"1. Construir índice:")
    print(f"   python text_search/build_index_from_csv.py")
    print(f"2. Seleccionar: {archivo}")
    print(f"3. Probar búsquedas y ver scores más altos!")
