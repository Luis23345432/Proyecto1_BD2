from __future__ import annotations

import json
import os
import re
import unicodedata
from typing import Dict, Set, Tuple, List, Iterable, Any, Optional

Rid = Tuple[int, int]

# Very small stopword list; can be extended
STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "in",
    "on",
    "at",
    "to",
    "of",
    "for",
}

TOKEN_RE = re.compile(r"\w+", flags=re.UNICODE)

# Try to import snowballstemmer for robust stemming
_STEMMER = None
try:
    from snowballstemmer import stemmer as SnowballStemmer
    _STEMMER = SnowballStemmer("spanish")
except Exception:
    _STEMMER = None


def tokenize(text: Any, *, do_stem: bool = False, normalize: bool = True) -> List[str]:
    if text is None:
        return []
    s = str(text)
    if normalize:
        # Normalize to NFKD and strip diacritics so search is accent-insensitive
        try:
            s = unicodedata.normalize('NFKD', s)
            s = ''.join(ch for ch in s if not unicodedata.combining(ch))
        except Exception:
            s = str(text)
    s = s.lower()
    tokens = TOKEN_RE.findall(s)
    # filter stopwords and short tokens
    tokens = [t for t in tokens if t and t not in STOPWORDS and len(t) > 1]
    if do_stem:
        if _STEMMER is not None:
            try:
                return _STEMMER.stemWords(tokens)
            except Exception:
                # fallback to naive heuristic if stemmer fails
                return [t.rstrip('s') for t in tokens]
        else:
            # fallback lightweight heuristic (remove plural 's')
            return [t.rstrip('s') for t in tokens]
    return tokens


class InvertedIndex:
    """Simple on-disk inverted index.

    Data model: a mapping term -> set of RIDs
    Persisted as JSON: {term: [[page, slot], ...], ...}
    """

    def __init__(self, *, do_stem: bool = False):
        # term -> set of rid tuples
        self.index: Dict[str, Set[Rid]] = {}
        # whether to apply stemming (uses snowballstemmer if available)
        self.do_stem: bool = bool(do_stem)

    # -- mutation API -------------------------------------------------
    def add(self, text: Any, rid: Rid) -> None:
        terms = tokenize(text, do_stem=self.do_stem)
        for t in terms:
            s = self.index.setdefault(t, set())
            s.add(tuple(rid))

    def build_from_pairs(self, pairs: Iterable[Tuple[Any, Rid]]) -> None:
        self.index = {}
        for text, rid in pairs:
            self.add(text, rid)

    def remove(self, key: Any) -> None:
        """Remove by text term or by rid.

        If `key` is a string, remove all RIDs associated to that text (i.e. remove mapping for its tokens).
        If `key` is a tuple/list of two ints, treat as rid and remove it from all term postings.
        """
        # remove by rid
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

        # otherwise treat as text: remove terms derived from text
        terms = tokenize(key, do_stem=self.do_stem)
        for t in terms:
            if t in self.index:
                del self.index[t]

    # -- query API ----------------------------------------------------
    def search(self, query: Any) -> List[Rid]:
        """Simple AND semantics: all terms in query must be present.

        If query has multiple terms, return intersection of postings.
        If no terms, return empty list.
        """
        # Primary tokenization (use stored do_stem and normalize)
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

        # Fallbacks to preserve compatibility with older .idx formats
        # 1) Try without normalizing accents (preserve accents from query)
        terms_no_norm = tokenize(query, do_stem=self.do_stem, normalize=False)
        if terms_no_norm and terms_no_norm != terms:
            res = postings_for_terms(terms_no_norm)
            if res:
                return sorted(list(res))

        # 2) If index wasn't stemmed, try stemmed variants of tokens (if stemmer available)
        #    This helps when index was built without stemming but we receive a stemmed query or viceversa.
        def stem_list(tokens: List[str]) -> List[str]:
            if _STEMMER is not None:
                try:
                    return _STEMMER.stemWords(tokens)
                except Exception:
                    return [t.rstrip('s') for t in tokens]
            return [t.rstrip('s') for t in tokens]

        # If the index is not stemmed, try stemming the query tokens
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

        # 3) If index *is* stemmed but we couldn't match, attempt unstemmed tokens (query without stemming)
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

        # No matches
        return []

    def get_terms(self) -> List[str]:
        return sorted(self.index.keys())

    # -- persistence --------------------------------------------------
    def save_idx(self, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        serializable = {t: [[r[0], r[1]] for r in sorted(list(s))] for t, s in self.index.items()}
        payload = {
            "_meta": {"do_stem": bool(self.do_stem)},
            "terms": serializable,
        }
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, path)

    @classmethod
    def load_idx(cls, path: str) -> "InvertedIndex":
        inst = cls()
        if not os.path.exists(path):
            return inst
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # support new format with metadata
        if isinstance(data, dict) and "terms" in data:
            meta = data.get("_meta", {}) or {}
            inst.do_stem = bool(meta.get("do_stem", False))
            terms = data.get("terms", {})
            for t, lst in terms.items():
                inst.index[t] = {tuple(x) for x in lst}
            return inst

        # fallback to old format (term -> [[p,s],...])
        for t, lst in data.items():
            inst.index[t] = {tuple(x) for x in lst}
        return inst

    def __repr__(self) -> str:
        return f"<InvertedIndex terms={len(self.index)}>"
