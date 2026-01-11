#!/usr/bin/env python3
"""
Módulo para parsear archivos SQL y extraer información
"""

import re
from typing import Optional, Tuple


def get_table_name_from_sql(content: str, schema: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    """
    Extrae el nombre de la tabla del primer INSERT statement
    
    Args:
        content: Contenido del archivo SQL
        schema: Schema por defecto si no se encuentra en el SQL
    
    Returns:
        Tuple[Optional[str], Optional[str]]: (table_schema, table_name)
    """
    # Buscar INSERT INTO con posibles esquemas y nombres de tabla entre comillas
    # Patrones: INSERT INTO "SCHEMA"."TABLE" o INSERT INTO TABLE o INSERT INTO DB_TABLE
    patterns = [
        r'INSERT\s+INTO\s+"([^"]+)"\s*\.\s*"([^"]+)"',  # "schema"."table"
        r'INSERT\s+INTO\s+(\w+)\s*\.\s*(\w+)',  # schema.table
        r'INSERT\s+INTO\s+"?(\w+)"?',  # table o "table"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            if len(match.groups()) == 2:
                # Tiene schema y tabla
                table_schema = match.group(1)
                table_name = match.group(2)
                return table_schema, table_name
            elif len(match.groups()) == 1:
                # Solo tabla (sin schema explícito)
                table_name = match.group(1)
                # Verificar si es DB_* (necesita schema)
                if table_name.upper().startswith('DB_'):
                    return schema, table_name
                return None, table_name
    
    return None, None


def count_insert_statements(content: str) -> int:
    """
    Cuenta cuántos INSERT statements hay en el contenido
    
    Args:
        content: Contenido del archivo SQL
    
    Returns:
        int: Número de INSERT statements
    """
    lines = content.split('\n')
    count = 0
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith('--') and re.search(r'INSERT\s+INTO', stripped, re.IGNORECASE):
            count += 1
    return count


def prepare_sql_content(content: str, schema: Optional[str] = None) -> str:
    """
    Prepara el contenido SQL reemplazando referencias a tablas DB_* con schema completo
    
    Args:
        content: Contenido del archivo SQL
        schema: Nombre del schema
    
    Returns:
        str: Contenido SQL preparado
    """
    if schema:
        # Reemplazar INSERT INTO DB_TABLE con INSERT INTO "schema"."DB_TABLE"
        content = re.sub(
            r'(INSERT\s+INTO)\s+(DB_\w+)',
            rf'\1 "{schema}"."\2"',
            content,
            flags=re.IGNORECASE
        )
    return content
