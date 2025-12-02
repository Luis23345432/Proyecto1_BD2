"""Tokenizador para sentencias SQL.

Convierte texto SQL en una secuencia de tokens identificando:
- Palabras clave (CREATE, SELECT, INSERT, etc.).
- Identificadores de tablas y columnas.
- Literales numéricos y de cadena.
- Operadores (=, @@, <, >, etc.).
- Puntuación (paréntesis, comas, corchetes).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional


KEYWORDS = {
    "CREATE", "TABLE", "FROM", "FILE", "USING", "INDEX", "SELECT", "INSERT",
    "INTO", "VALUES", "DELETE", "WHERE", "BETWEEN", "AND", "OR", "NOT", "AS",
    "FLOAT", "INT", "VARCHAR", "DATE", "ARRAY", "PRIMARY", "KEY", "UNIQUE", "*",
    "NEAR", "RADIUS", "KNN", "K",
}


@dataclass
class Token:
    """Representa un token con su tipo y valor."""
    type: str
    value: str


class SQLTokenizer:
    """Tokenizador de sentencias SQL.
    
    Procesa una cadena SQL y la divide en tokens individuales,
    normalizando comillas y reconociendo operadores especiales.
    """
    def __init__(self, sql: str):
        self.sql = (
            sql.replace('\u2018', "'")
               .replace('\u2019', "'")
               .replace('\u201C', '"')
               .replace('\u201D', '"')
        )

    def tokenize(self) -> List[Token]:
        """Tokeniza la cadena SQL en una lista de tokens."""
        s = self.sql
        tokens: List[Token] = []
        i = 0
        N = len(s)
        while i < N:
            ch = s[i]
            if ch.isspace():
                i += 1
                continue
            if ch in ",()=;*[]":
                tokens.append(Token("PUNC", ch))
                i += 1
                continue
            if ch in "+-":
                tokens.append(Token("OP", ch))
                i += 1
                continue
            if ch in "<>":
                if i + 1 < N and s[i+1] in "=>":
                    tokens.append(Token("OP", ch + s[i+1]))
                    i += 2
                else:
                    tokens.append(Token("OP", ch))
                    i += 1
                continue
            if ch == '"' or ch == "'":
                quote = ch
                j = i + 1
                val = []
                while j < N and s[j] != quote:
                    if s[j] == "\\" and j + 1 < N:
                        val.append(s[j+1])
                        j += 2
                    else:
                        val.append(s[j])
                        j += 1
                tokens.append(Token("STRING", ''.join(val)))
                i = j + 1
                continue
            if ch == '@':
                if i + 1 < N and s[i+1] == '@':
                    tokens.append(Token('OP', '@@'))
                    i += 2
                    continue
            if ch.isdigit() or (ch == '.' and i + 1 < N and s[i+1].isdigit()):
                j = i
                has_dot = False
                while j < N and (s[j].isdigit() or (s[j] == '.' and not has_dot)):
                    if s[j] == '.':
                        has_dot = True
                    j += 1
                tokens.append(Token("NUMBER", s[i:j]))
                i = j
                continue
            if ch.isalpha() or ch == '_' or ch == '"':
                j = i
                while j < N and (s[j].isalnum() or s[j] in ['_', '.']):
                    j += 1
                word = s[i:j]
                up = word.upper()
                if up in KEYWORDS:
                    tokens.append(Token("KW", up))
                else:
                    tokens.append(Token("IDENT", word))
                i = j
                continue
            tokens.append(Token("CHAR", ch))
            i += 1
        return tokens