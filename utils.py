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


def load_config_file(script_dir=None):
    """
    Carga la configuración desde hana_config.conf
    Retorna un diccionario con todas las configuraciones
    """
    if script_dir is None:
        script_dir = Path(__file__).parent
    
    config = {}
    config_file = script_dir / "hana_config.conf"
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip().strip('"').strip("'")
    
    return config


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


def get_cap_project_dir(script_dir=None):
    """
    Obtiene el nombre del directorio del proyecto CAP en este orden:
    1. Variable de entorno CAP_PROJECT_DIR
    2. Configuración en hana_config.conf (CAP_PROJECT_DIR)
    3. Valor por defecto: cap_project
    """
    # 1. Variable de entorno
    cap_dir = os.environ.get('CAP_PROJECT_DIR')
    if cap_dir:
        return cap_dir
    
    # 2. Configuración desde archivo
    if script_dir is None:
        script_dir = Path(__file__).parent
    
    config = load_config_file(script_dir)
    if 'CAP_PROJECT_DIR' in config:
        return config['CAP_PROJECT_DIR']
    
    # 3. Valor por defecto
    return 'cap_project'
