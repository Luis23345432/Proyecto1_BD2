from __future__ import annotations

from typing import List, Optional, Tuple, Dict, Any
from .tokenizer import SQLTokenizer, Token
from .ast import (
    CreateTableStmt, SelectStmt, InsertStmt, DeleteStmt, Condition
)


class SQLParser:
    def __init__(self, sql: str):
        self.tokens: List[Token] = SQLTokenizer(sql).tokenize()
        self.i = 0

    def _peek(self) -> Optional[Token]:
        return self.tokens[self.i] if self.i < len(self.tokens) else None

    def _eat(self, expected_type: Optional[str] = None, expected_val: Optional[str] = None) -> Token:
        t = self._peek()
        if t is None:
            raise ValueError("Unexpected end of input")
        if expected_type and t.type != expected_type:
            raise ValueError(f"Expected {expected_type}, got {t.type}")
        if expected_val and t.value.upper() != expected_val.upper():
            raise ValueError(f"Expected {expected_val}, got {t.value}")
        self.i += 1
        return t

    def parse(self):
        t = self._peek()
        if not t:
            raise ValueError("Empty SQL")
        if t.type == 'KW' and t.value == 'CREATE':
            return self._parse_create_table()
        if t.type == 'KW' and t.value == 'SELECT':
            return self._parse_select()
        if t.type == 'KW' and t.value == 'INSERT':
            return self._parse_insert()
        if t.type == 'KW' and t.value == 'DELETE':
            return self._parse_delete()
        raise ValueError(f"Unsupported SQL starting with {t.value}")

    def _parse_create_table(self) -> CreateTableStmt:
        self._eat('KW', 'CREATE')
        self._eat('KW', 'TABLE')
        name = self._eat('IDENT').value
        csv_path = None
        indexes: List[Tuple[str, str]] = []
        columns: Optional[List[CreateTableStmt.ColumnDecl]] = None
        t = self._peek()
        # Optional DDL-style columns in parentheses
        if t and t.type == 'PUNC' and t.value == '(':
            self._eat('PUNC', '(')
            columns = []
            while True:
                col_name = self._eat('IDENT').value
                # parse type: e.g., INT | FLOAT | DATE | VARCHAR[20] | ARRAY[FLOAT]
                typ_tok = self._eat()
                type_name = typ_tok.value.upper()
                length: Optional[int] = None
                is_array = False
                inner_type: Optional[str] = None
                if type_name == 'VARCHAR':
                    # optional [n]
                    tlen = self._peek()
                    if tlen and tlen.type == 'PUNC' and tlen.value == '[':
                        self._eat('PUNC', '[')
                        length_tok = self._eat('NUMBER')
                        length = int(length_tok.value)
                        self._eat('PUNC', ']')
                elif type_name == 'ARRAY':
                    # expect [FLOAT] or [INT]
                    is_array = True
                    self._eat('PUNC', '[')
                    inner_tok = self._eat()
                    inner_type = inner_tok.value.upper()
                    self._eat('PUNC', ']')
                # optional PRIMARY KEY
                primary_key = False
                t2 = self._peek()
                if t2 and t2.type == 'KW' and t2.value in ('PRIMARY', 'KEY'):
                    # accept 'PRIMARY KEY' or 'KEY'
                    if t2.value == 'PRIMARY':
                        self._eat('KW', 'PRIMARY')
                        self._eat('KW', 'KEY')
                    else:
                        self._eat('KW', 'KEY')
                    primary_key = True
                # optional INDEX type name
                index_type: Optional[str] = None
                t3 = self._peek()
                if t3 and t3.type == 'KW' and t3.value == 'INDEX':
                    self._eat('KW', 'INDEX')
                    # next token can be IDENT or KW for type
                    tt = self._eat()
                    index_type = tt.value
                columns.append(CreateTableStmt.ColumnDecl(
                    name=col_name,
                    type_name=type_name,
                    length=length,
                    is_array=is_array,
                    inner_type=inner_type,
                    primary_key=primary_key,
                    index_type=index_type,
                ))
                t = self._peek()
                if t and t.type == 'PUNC' and t.value == ',':
                    self._eat('PUNC', ',')
                    continue
                break
            self._eat('PUNC', ')')
            t = self._peek()
        if t and t.type == 'KW' and t.value == 'FROM':
            self._eat('KW', 'FROM')
            self._eat('KW', 'FILE')
            csv_path = self._eat('STRING').value
        t = self._peek()
        if t and t.type == 'KW' and t.value == 'USING':
            self._eat('KW', 'USING')
            self._eat('KW', 'INDEX')
            # Accept: tipo("col")[, tipo("col") ...]
            while True:
                t = self._eat()
                # allow IDENT or KW for index type names
                idx_type = t.value
                self._eat('PUNC', '(')
                col = self._eat('IDENT').value
                self._eat('PUNC', ')')
                indexes.append((idx_type, col))
                t = self._peek()
                if t and t.type == 'PUNC' and t.value == ',':
                    self._eat('PUNC', ',')
                    continue
                break
        return CreateTableStmt(name=name, csv_path=csv_path, indexes=indexes, columns=columns)

    def _parse_select(self) -> SelectStmt:
        self._eat('KW', 'SELECT')
        cols: List[str] = []
        t = self._peek()
        if t and t.type == 'PUNC' and t.value == '*':
            self._eat('PUNC', '*')
            cols = ['*']
        else:
            while True:
                cols.append(self._eat('IDENT').value)
                t = self._peek()
                if t and t.type == 'PUNC' and t.value == ',':
                    self._eat('PUNC', ',')
                    continue
                break
        self._eat('KW', 'FROM')
        table = self._eat('IDENT').value
        condition = None
        t = self._peek()
        if t and t.type == 'KW' and t.value == 'WHERE':
            self._eat('KW', 'WHERE')
            # clause: col = value  OR col BETWEEN x AND y
            col = self._eat('IDENT').value
            t2 = self._peek()
            if t2 and t2.type == 'KW' and t2.value == 'BETWEEN':
                self._eat('KW', 'BETWEEN')
                v1 = self._parse_value()
                self._eat('KW', 'AND')
                v2 = self._parse_value()
                condition = Condition(column=col, op='BETWEEN', value=v1, value2=v2)
            else:
                self._eat('OP') if (t2 and t2.type == 'OP') else self._eat('PUNC', '=')
                v = self._parse_value()
                condition = Condition(column=col, op='=', value=v)
        return SelectStmt(table=table, columns=cols, condition=condition)

    def _parse_insert(self) -> InsertStmt:
        self._eat('KW', 'INSERT')
        self._eat('KW', 'INTO')
        table = self._eat('IDENT').value
        self._eat('KW', 'VALUES')
        self._eat('PUNC', '(')

        # Parse positional values (standard SQL syntax)
        positional_values = []
        while True:
            positional_values.append(self._parse_value())

            t = self._peek()
            if t and t.type == 'PUNC' and t.value == ',':
                self._eat('PUNC', ',')
                continue
            break

        self._eat('PUNC', ')')

        # Store as positional for executor to map to column names
        values = {"__positional__": positional_values}

        return InsertStmt(table=table, values=values)

    def _parse_delete(self) -> DeleteStmt:
        self._eat('KW', 'DELETE')
        self._eat('KW', 'FROM')
        table = self._eat('IDENT').value
        condition = None
        t = self._peek()
        if t and t.type == 'KW' and t.value == 'WHERE':
            self._eat('KW', 'WHERE')
            col = self._eat('IDENT').value
            self._eat('PUNC', '=')
            v = self._parse_value()
            condition = Condition(column=col, op='=', value=v)
        return DeleteStmt(table=table, condition=condition)

    def _parse_value(self):
        t = self._peek()

        # Handle arrays [...]
        if t and t.type == 'PUNC' and t.value == '[':
            self._eat('PUNC', '[')
            array_values = []

            # Empty array
            t = self._peek()
            if t and t.type == 'PUNC' and t.value == ']':
                self._eat('PUNC', ']')
                return array_values

            # Parse array elements
            while True:
                elem_t = self._peek()

                # Handle negative/positive numbers with explicit sign
                if elem_t and elem_t.type == 'OP' and elem_t.value in ('+', '-'):
                    sign = self._eat('OP').value
                    num_t = self._eat('NUMBER')
                    num_val = float(num_t.value) if '.' in num_t.value else int(num_t.value)
                    if sign == '-':
                        num_val = -num_val
                    array_values.append(num_val)
                elif elem_t and elem_t.type == 'NUMBER':
                    num_t = self._eat('NUMBER')
                    num_val = float(num_t.value) if '.' in num_t.value else int(num_t.value)
                    array_values.append(num_val)
                else:
                    raise ValueError(f"Unexpected token in array: {elem_t}")

                t = self._peek()
                if t and t.type == 'PUNC' and t.value == ',':
                    self._eat('PUNC', ',')
                    continue
                break

            self._eat('PUNC', ']')
            return array_values

        # Handle negative/positive numbers outside arrays
        if t and t.type == 'OP' and t.value in ('+', '-'):
            sign = self._eat('OP').value
            num_t = self._eat('NUMBER')
            num_val = float(num_t.value) if '.' in num_t.value else int(num_t.value)
            if sign == '-':
                num_val = -num_val
            return num_val

        # Original value parsing
        t = self._eat()
        if t.type == 'NUMBER':
            return int(t.value) if t.value.isdigit() else float(t.value)
        if t.type == 'STRING':
            return t.value
        if t.type == 'IDENT':
            return t.value
        raise ValueError(f"Unexpected value token {t}")