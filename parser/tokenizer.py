from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional


KEYWORDS = {
    "CREATE", "TABLE", "FROM", "FILE", "USING", "INDEX", "SELECT", "INSERT",
    "INTO", "VALUES", "DELETE", "WHERE", "BETWEEN", "AND", "OR", "NOT", "AS",
    "FLOAT", "INT", "VARCHAR", "DATE", "ARRAY", "PRIMARY", "KEY", "UNIQUE", "*",
    # Spatial/extended
    "NEAR", "RADIUS", "KNN", "K",
}


@dataclass
class Token:
    type: str
    value: str


class SQLTokenizer:
    def __init__(self, sql: str):
        self.sql = sql

    def tokenize(self) -> List[Token]:
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
            # ← AGREGAR ESTA SECCIÓN PARA OPERADORES +, -, <, >
            if ch in "+-":
                tokens.append(Token("OP", ch))
                i += 1
                continue
            if ch in "<>":
                # simple operators: <, >, <=, >=, <>
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
            # number literal
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
            # identifier / keyword
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
            # fallback one-char token
            tokens.append(Token("CHAR", ch))
            i += 1
        return tokens