#!/usr/bin/env python3
"""
Utilidades compartidas para los scripts
"""

import os
import tarfile
from pathlib import Path


def detect_schema_from_tar(tar_path):
    """
    Detecta automáticamente el nombre del schema desde el export.tar.gz
    Busca en la estructura index/SCHEMA_NAME/
    """
    try:
        with tarfile.open(tar_path, 'r:gz') as tar:
            for member in tar.getmembers():
                if member.name.startswith('index/') and '/' in member.name[6:]:
                    # Extraer schema de la ruta: index/SCHEMA_NAME/...
                    parts = member.name.split('/')
                    if len(parts) >= 2:
                        schema_name = parts[1]
                        if schema_name and schema_name != 'index':
                            return schema_name
    except Exception:
        pass
    return None


def detect_schema_from_extracted(extract_dir):
    """
    Detecta el schema desde archivos ya descomprimidos
    """
    index_dir = extract_dir / "index"
    if index_dir.exists():
        # Buscar el primer subdirectorio en index/
        for item in index_dir.iterdir():
            if item.is_dir():
                return item.name
    return None


def get_schema_name(config=None, tar_path=None, extract_dir=None):
    """
    Obtiene el nombre del schema en este orden:
    1. Variable de entorno SCHEMA
    2. Configuración en hana_config.conf (SCHEMA)
    3. Auto-detección desde export.tar.gz
    4. Auto-detección desde archivos descomprimidos
    """
    # 1. Variable de entorno
    schema = os.environ.get('SCHEMA')
    if schema:
        return schema
    
    # 2. Configuración
    if config and 'SCHEMA' in config:
        return config['SCHEMA']
    
    # 3. Auto-detección desde tar.gz
    if tar_path and Path(tar_path).exists():
        schema = detect_schema_from_tar(tar_path)
        if schema:
            return schema
    
    # 4. Auto-detección desde archivos descomprimidos
    if extract_dir:
        schema = detect_schema_from_extracted(extract_dir)
        if schema:
            return schema
    
    return None
