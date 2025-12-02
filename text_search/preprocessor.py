"""
Preprocesador de texto para Full-Text Search
Implementa: Tokenización, Stopwords, Stemming
"""

import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, SnowballStemmer
from typing import List, Set

# Descargar recursos de NLTK (ejecutar una vez)
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)


class TextPreprocessor:
    """
    Preprocesa texto para indexación:
    1. Tokenización: divide el texto en palabras
    2. Normalización: convierte a minúsculas, elimina puntuación
    3. Filtrado: elimina stopwords
    4. Stemming: reduce palabras a su raíz
    """
    
    def __init__(self, language: str = 'english', use_stemming: bool = True):
        """
        Args:
            language: Idioma para stopwords ('english', 'spanish', etc.)
            use_stemming: Si aplica stemming o no
        """
        self.language = language
        self.use_stemming = use_stemming
        
        # Cargar stopwords según idioma
        try:
            self.stopwords: Set[str] = set(stopwords.words(language))
        except Exception as e:
            print(f"⚠️ Error cargando stopwords para '{language}': {e}")
            self.stopwords = set()
        
        # Inicializar stemmer
        if self.use_stemming:
            if language == 'english':
                self.stemmer = PorterStemmer()
            else:
                # Snowball soporta múltiples idiomas
                self.stemmer = SnowballStemmer(language)
        else:
            self.stemmer = None
    
    def tokenize(self, text: str) -> List[str]:
        """
        Tokeniza el texto en palabras individuales
        
        Args:
            text: Texto a tokenizar
            
        Returns:
            Lista de tokens (palabras)
        """
        # Convertir a minúsculas
        text = text.lower()
        
        # Eliminar signos de puntuación y caracteres especiales
        # Mantener solo letras, números y espacios
        text = re.sub(r'[^a-záéíóúñ0-9\s]', ' ', text)
        
        # Dividir por espacios y filtrar tokens vacíos
        tokens = [token for token in text.split() if token]
        
        return tokens
    
    def remove_stopwords(self, tokens: List[str]) -> List[str]:
        """
        Filtra stopwords de la lista de tokens
        
        Args:
            tokens: Lista de tokens
            
        Returns:
            Tokens sin stopwords
        """
        return [token for token in tokens if token not in self.stopwords]
    
    def stem(self, tokens: List[str]) -> List[str]:
        """
        Aplica stemming a los tokens
        
        Args:
            tokens: Lista de tokens
            
        Returns:
            Tokens con stemming aplicado
        """
        if not self.stemmer:
            return tokens
        
        return [self.stemmer.stem(token) for token in tokens]
    
    def preprocess(self, text: str) -> List[str]:
        """
        Aplica todo el pipeline de preprocesamiento
        
        Args:
            text: Texto original
            
        Returns:
            Lista de tokens procesados
        """
        # 1. Tokenización
        tokens = self.tokenize(text)
        
        # 2. Eliminar stopwords
        tokens = self.remove_stopwords(tokens)
        
        # 3. Stemming
        if self.use_stemming:
            tokens = self.stem(tokens)
        
        return tokens
    
    def preprocess_documents(self, documents: List[str]) -> List[List[str]]:
        """
        Preprocesa una lista de documentos
        
        Args:
            documents: Lista de documentos (strings)
            
        Returns:
            Lista de listas de tokens procesados
        """
        return [self.preprocess(doc) for doc in documents]


# Función auxiliar para uso rápido
def preprocess_text(text: str, language: str = 'english', use_stemming: bool = True) -> List[str]:
    """
    Función helper para preprocesar un texto rápidamente
    
    Args:
        text: Texto a procesar
        language: Idioma
        use_stemming: Aplicar stemming
        
    Returns:
        Lista de tokens procesados
    """
    preprocessor = TextPreprocessor(language=language, use_stemming=use_stemming)
    return preprocessor.preprocess(text)


if __name__ == "__main__":
    # Ejemplos de uso
    print("=" * 60)
    print("PRUEBA DEL PREPROCESADOR DE TEXTO")
    print("=" * 60)
    
    # Ejemplo 1: Inglés
    text_en = "The cats are running quickly in the beautiful garden!"
    print(f"\n📄 Texto original (inglés):\n{text_en}")
    
    preprocessor_en = TextPreprocessor(language='english', use_stemming=True)
    tokens_en = preprocessor_en.preprocess(text_en)
    print(f"\n🔍 Tokens procesados:\n{tokens_en}")
    
    # Ejemplo 2: Español
    text_es = "Los gatos corren rápidamente en el hermoso jardín. ¡Qué lindos son!"
    print(f"\n📄 Texto original (español):\n{text_es}")
    
    preprocessor_es = TextPreprocessor(language='spanish', use_stemming=True)
    tokens_es = preprocessor_es.preprocess(text_es)
    print(f"\n🔍 Tokens procesados:\n{tokens_es}")
    
    # Ejemplo 3: Comparación con/sin stemming
    text = "Running runners ran quickly"
    print(f"\n📄 Texto original:\n{text}")
    
    print("\n--- Con stemming ---")
    tokens_with = preprocess_text(text, use_stemming=True)
    print(f"Tokens: {tokens_with}")
    
    print("\n--- Sin stemming ---")
    tokens_without = preprocess_text(text, use_stemming=False)
    print(f"Tokens: {tokens_without}")
    
    print("\n" + "=" * 60)