"""Índice invertido para búsqueda de texto completo.

Implementa tokenización, filtrado de stopwords y stemming opcional:
- Tokeniza texto con soporte para español e inglés.
- Filtra palabras vacías (stopwords) en ambos idiomas.
- Aplica stemming opcional usando snowballstemmer.
- Mantiene mapeo de términos a conjuntos de RIDs.
- Soporta persistencia en JSON.
"""
from __future__ import annotations

import json
import os
import re
import unicodedata
from typing import Dict, Set, Tuple, List, Iterable, Any, Optional

Rid = Tuple[int, int]

STOPWORDS = {
    "the","a","an","and","or","in","on","at","to","of","for","is","are","was","were","be","been","by","with","from","as","that","this","these","those","it","its","into","about","over","under","than","then","there","here","up","down","out","off","so","but","not",
    "el","la","los","las","un","una","unos","unas","y","o","u","en","de","del","al","a","por","para","con","sin","sobre","entre","tras","durante","segun","según","contra","como","que","qué","se","su","sus","tu","tus","mi","mis","nuestro","nuestra","nuestros","nuestras","vuestro","vuestra","vuestros","vuestras","lo","le","les","ya","muy","más","menos","tambien","también","pero","porque","cuando","donde","dónde","cual","cuál","cuales","cuáles","quien","quién","quienes","quiénes","esto","eso","aquello","aqui","aquí","alli","allí","allá","hoy","ayer","mañana","si","sí","no","ni","cada","casi","tal","tales","otro","otros","otra","otras","donde","desde","hasta","sino","e","ademas","además","pues","ante","bajo","cabe","era","eran","es","son","ser","será","serán"
}

TOKEN_RE = re.compile(r"\w+", flags=re.UNICODE)

try:
    from snowballstemmer import stemmer as SnowballStemmer
    _STEMMER = SnowballStemmer("spanish")
except Exception:
    _STEMMER = None


def tokenize(text: Any, *, do_stem: bool = False, normalize: bool = True) -> List[str]:
    """Tokeniza texto en palabras, filtrando stopwords y aplicando stemming opcional.
    
    Args:
        text: Texto a tokenizar.
        do_stem: Si es True, aplica stemming a los tokens.
        normalize: Si es True, elimina acentos para búsqueda insensible a acentos.
    
    Returns:
        Lista de tokens procesados.
    """
    if text is None:
        return []
    s = str(text)
    if normalize:
        try:
            s = unicodedata.normalize('NFKD', s)
            s = ''.join(ch for ch in s if not unicodedata.combining(ch))
        except Exception:
            s = str(text)
    s = s.lower()
    tokens = TOKEN_RE.findall(s)
    tokens = [t for t in tokens if t and t not in STOPWORDS and len(t) > 1]
    if do_stem:
        if _STEMMER is not None:
            try:
                return _STEMMER.stemWords(tokens)
            except Exception:
                return [t.rstrip('s') for t in tokens]
        else:
            return [t.rstrip('s') for t in tokens]
    return tokens


class InvertedIndex:
    """Índice invertido simple con persistencia en JSON.

    Mantiene un mapeo de términos a conjuntos de RIDs (tuplas page, slot).
    Soporta construcción incremental, búsqueda con semántica AND,
    y persistencia en disco.
    """

    def __init__(self, *, do_stem: bool = False):
        self.index: Dict[str, Set[Rid]] = {}
        self.do_stem: bool = bool(do_stem)

    def add(self, text: Any, rid: Rid) -> None:
        """Agrega un documento al índice invertido."""
        terms = tokenize(text, do_stem=self.do_stem)
        for t in terms:
            s = self.index.setdefault(t, set())
            s.add(tuple(rid))

    def build_from_pairs(self, pairs: Iterable[Tuple[Any, Rid]]) -> None:
        """Construye el índice desde pares (texto, rid)."""
        self.index = {}
        for text, rid in pairs:
            self.add(text, rid)

    def remove(self, key: Any) -> None:
        """Elimina entradas del índice.
        
        Si key es un RID (tupla de 2 enteros), lo elimina de todas las postings.
        Si key es texto, elimina los términos derivados de ese texto.
        """
        if isinstance(key, (list, tuple)) and len(key) == 2:
            rid = tuple(key)
            removed_terms = []
            for t, s in list(self.index.items()):
                if rid in s:
                    s.discard(rid)
                    if not s:
                        removed_terms.append(t)
            for t in removed_terms:
                del self.index[t]
            return

        terms = tokenize(key, do_stem=self.do_stem)
        for t in terms:
            if t in self.index:
                del self.index[t]

    def search(self, query: Any) -> List[Rid]:
        """Busca documentos que contengan todos los términos de la consulta (semántica AND).
        
        Aplica múltiples estrategias de fallback para compatibilidad con diferentes
        configuraciones de stemming y normalización.
        """
        terms = tokenize(query, do_stem=self.do_stem, normalize=True)
        if not terms:
            return []

        def postings_for_terms(term_list: List[str]) -> Optional[Set[Rid]]:
            postings: List[Set[Rid]] = []
            for t in term_list:
                s = self.index.get(t)
                if not s:
                    return None
                postings.append(s)
            return set.intersection(*postings) if postings else set()

        # Try primary terms first
        res = postings_for_terms(terms)
        if res:
            return sorted(list(res))

        terms_no_norm = tokenize(query, do_stem=self.do_stem, normalize=False)
        if terms_no_norm and terms_no_norm != terms:
            res = postings_for_terms(terms_no_norm)
            if res:
                return sorted(list(res))

        def stem_list(tokens: List[str]) -> List[str]:
            if _STEMMER is not None:
                try:
                    return _STEMMER.stemWords(tokens)
                except Exception:
                    return [t.rstrip('s') for t in tokens]
            return [t.rstrip('s') for t in tokens]

        if not self.do_stem:
            stemmed = stem_list(terms)
            if stemmed and stemmed != terms:
                res = postings_for_terms(stemmed)
                if res:
                    return sorted(list(res))

            stemmed_no_norm = stem_list(terms_no_norm) if terms_no_norm else []
            if stemmed_no_norm and stemmed_no_norm != terms_no_norm:
                res = postings_for_terms(stemmed_no_norm)
                if res:
                    return sorted(list(res))

        if self.do_stem:
            unstemmed = tokenize(query, do_stem=False, normalize=True)
            if unstemmed and unstemmed != terms:
                res = postings_for_terms(unstemmed)
                if res:
                    return sorted(list(res))

            unstemmed_no_norm = tokenize(query, do_stem=False, normalize=False)
            if unstemmed_no_norm and unstemmed_no_norm != unstemmed:
                res = postings_for_terms(unstemmed_no_norm)
                if res:
                    return sorted(list(res))

        return []

    def get_terms(self) -> List[str]:
        """Retorna lista ordenada de todos los términos en el índice."""
        return sorted(self.index.keys())

    def save_idx(self, path: str) -> None:
        """Guarda el índice invertido en un archivo JSON."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        serializable = {t: [[r[0], r[1]] for r in sorted(list(s))] for t, s in self.index.items()}
        payload = {
            "_meta": {"do_stem": bool(self.do_stem)},
            "terms": serializable,
        }
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        try:
            os.replace(tmp, path)
        except PermissionError:
            try:
                if os.path.exists(path):
                    os.remove(path)
                os.replace(tmp, path)
            finally:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass

    @classmethod
    def load_idx(cls, path: str) -> "InvertedIndex":
        """Carga el índice invertido desde un archivo JSON."""
        inst = cls()
        if not os.path.exists(path):
            return inst
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict) and "terms" in data:
            meta = data.get("_meta", {}) or {}
            inst.do_stem = bool(meta.get("do_stem", False))
            terms = data.get("terms", {})
            for t, lst in terms.items():
                inst.index[t] = {tuple(x) for x in lst}
            return inst

        for t, lst in data.items():
            inst.index[t] = {tuple(x) for x in lst}
        return inst

    def __repr__(self) -> str:
        return f"<InvertedIndex terms={len(self.index)}>"
