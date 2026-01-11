#!/usr/bin/env python3
"""
Módulo para manejar la conexión a SAP HANA
Proporciona funciones para cargar configuración y usar hdbsql
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional


class Colors:
    """Colores para output en terminal"""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color


def load_config(require_config=True, show_messages=True):
    """
    Carga la configuración desde hana_config.conf o variables de entorno
    
    Args:
        require_config: Si True, sale del programa si no hay configuración
        show_messages: Si True, muestra mensajes informativos
    
    Returns:
        dict: Diccionario con la configuración o None si no se encuentra
    """
    config = {}
    
    # Intentar cargar desde archivo de configuración en schema_to_cap
    script_dir = Path(__file__).parent
    config_file = script_dir / "hana_config.conf"
    
    if config_file.exists():
        if show_messages:
            print(f"{Colors.BLUE}Usando configuración desde hana_config.conf{Colors.NC}")
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip().strip('"').strip("'")
    else:
        # Intentar desde variables de entorno
        if show_messages:
            print(f"{Colors.BLUE}Intentando cargar desde variables de entorno...{Colors.NC}")
        env_vars = ['HANA_HOST', 'HANA_PORT', 'HANA_DATABASE', 'HANA_USER', 'HANA_PASSWORD']
        for var in env_vars:
            value = os.environ.get(var)
            if value:
                config[var] = value
        
        if len(config) == len(env_vars):
            return config
        elif require_config:
            if show_messages:
                print(f"{Colors.RED}Error: No se encontró el archivo hana_config.conf ni variables de entorno{Colors.NC}")
                print("Por favor, crea el archivo de configuración primero.")
            sys.exit(1)
        else:
            return None
    
    required_keys = ['HANA_HOST', 'HANA_PORT', 'HANA_DATABASE', 'HANA_USER', 'HANA_PASSWORD']
    for key in required_keys:
        if key not in config:
            if require_config:
                if show_messages:
                    print(f"{Colors.RED}Error: Falta la configuración {key} en {config_file}{Colors.NC}")
                sys.exit(1)
            else:
                return None
    
    # Agregar configuraciones opcionales con valores por defecto
    # Primero desde el archivo, luego desde variables de entorno, luego valores por defecto
    optional_configs = {
        'SQL_TIMEOUT': os.environ.get('SQL_TIMEOUT', None),
        'HANA_CLIENT_PATH': os.environ.get('HANA_CLIENT_PATH', None),
        'SCHEMA': os.environ.get('SCHEMA', None),
        'CAP_PROJECT_DIR': os.environ.get('CAP_PROJECT_DIR', 'cap_project'),
        'EXPORT_TAR_FILE': os.environ.get('EXPORT_TAR_FILE', 'export.tar.gz'),
        'SQL_DIR': os.environ.get('SQL_DIR', 'data_insert_sql'),
        'EXTRACT_DIR': os.environ.get('EXTRACT_DIR', 'temp_extract'),
        'LOG_DIR': os.environ.get('LOG_DIR', 'logs'),
        'CREATED_DIR': os.environ.get('CREATED_DIR', 'created'),
        'PROJECT_BASE_DIR': os.environ.get('PROJECT_BASE_DIR', None),
    }
    
    for key, default_value in optional_configs.items():
        if key not in config:
            config[key] = default_value
    
    return config


def get_config_value(config: Dict[str, str], key: str, default: Any = None, 
                     env_key: Optional[str] = None) -> Any:
    """
    Obtiene un valor de configuración con fallback a variable de entorno y valor por defecto
    
    Args:
        config: Diccionario de configuración
        key: Clave en el diccionario de configuración
        default: Valor por defecto si no se encuentra
        env_key: Nombre de la variable de entorno (si es diferente de key)
    
    Returns:
        Valor de configuración
    """
    if config and key in config and config[key]:
        return config[key]
    
    env_key = env_key or key
    env_value = os.environ.get(env_key)
    if env_value:
        return env_value
    
    return default


def extract_schema_from_user(user):
    """
    Extrae el nombre del schema desde el usuario de HANA
    
    Formatos soportados:
    - SCHEMA_XXXXX_RT -> SCHEMA
    - SCHEMA_XXXXX -> SCHEMA
    - SCHEMA -> SCHEMA
    
    Args:
        user: Nombre de usuario de HANA
    
    Returns:
        str: Nombre del schema o None si no se puede extraer
    """
    if not user or '_' not in user:
        return user if user else None
    
    parts = user.split('_')
    if len(parts) >= 3:
        # Formato: SCHEMA_XXXXX_RT -> tomar SCHEMA
        return '_'.join(parts[:-2])
    elif len(parts) == 2:
        # Formato: SCHEMA_XXXXX -> tomar SCHEMA
        return parts[0]
    else:
        return parts[0]


def _get_common_hana_client_paths():
    """
    Retorna una lista de rutas comunes donde se puede encontrar el cliente HANA
    después de extraer hanaclient-latest-linux-x64.tar.gz
    
    Returns:
        list: Lista de rutas Path a verificar
    """
    script_dir = Path(__file__).parent
    base_dir = script_dir.parent
    home_dir = Path.home()
    
    common_paths = [
        # Rutas relativas al script
        script_dir / "client" / "hdbclient" / "bin" / "hdbsql",
        script_dir / "client" / "hana_client" / "bin" / "hdbsql",
        script_dir / "hdbclient" / "bin" / "hdbsql",
        script_dir / "hana_client" / "bin" / "hdbsql",
        
        # Rutas en el directorio base
        base_dir / "client" / "hdbclient" / "bin" / "hdbsql",
        base_dir / "client" / "hana_client" / "bin" / "hdbsql",
        base_dir / "hdbclient" / "bin" / "hdbsql",
        base_dir / "hana_client" / "bin" / "hdbsql",
        
        # Rutas en home
        home_dir / "hana_client" / "bin" / "hdbsql",
        home_dir / "hdbclient" / "bin" / "hdbsql",
        home_dir / ".hana-client" / "bin" / "hdbsql",
        
        # Rutas comunes del sistema
        Path("/usr/sap/hdbclient/bin/hdbsql"),
        Path("/opt/sap/hdbclient/bin/hdbsql"),
        Path("/usr/local/hana_client/bin/hdbsql"),
    ]
    
    return common_paths


def find_hdbsql_path(config=None):
    """
    Encuentra la ruta al binario hdbsql en este orden:
    1. PATH del sistema (shutil.which)
    2. HANA_CLIENT_PATH del config (archivo o directorio)
    3. Variable de entorno HANA_CLIENT_PATH
    4. Rutas comunes donde se instala el cliente HANA
    5. Búsqueda recursiva en directorios comunes
    
    Args:
        config: Diccionario con la configuración (opcional)
    
    Returns:
        str: Ruta al binario hdbsql o None si no se encuentra
    """
    import shutil
    
    # 1. Intentar encontrar en PATH del sistema
    hdbsql_path = shutil.which('hdbsql')
    if hdbsql_path:
        return hdbsql_path
    
    def _check_path(path):
        """Verifica si un path existe y es ejecutable"""
        if path.exists() and path.is_file():
            # Verificar si es ejecutable
            if os.access(path, os.X_OK):
                return str(path)
        return None
    
    # 2. Si no está en PATH, usar HANA_CLIENT_PATH del config
    if config and config.get('HANA_CLIENT_PATH'):
        client_path = Path(config['HANA_CLIENT_PATH'])
        # Si es un archivo directo
        result = _check_path(client_path)
        if result:
            return result
        # Si es un directorio, buscar hdbsql dentro
        if client_path.is_dir():
            # Buscar directamente
            hdbsql = client_path / "hdbsql"
            result = _check_path(hdbsql)
            if result:
                return result
            # Buscar en bin/
            hdbsql = client_path / "bin" / "hdbsql"
            result = _check_path(hdbsql)
            if result:
                return result
    
    # 3. Intentar desde variable de entorno
    env_path = os.environ.get('HANA_CLIENT_PATH')
    if env_path:
        client_path = Path(env_path)
        result = _check_path(client_path)
        if result:
            return result
        if client_path.is_dir():
            hdbsql = client_path / "hdbsql"
            result = _check_path(hdbsql)
            if result:
                return result
            hdbsql = client_path / "bin" / "hdbsql"
            result = _check_path(hdbsql)
            if result:
                return result
    
    # 4. Buscar en rutas comunes
    for common_path in _get_common_hana_client_paths():
        result = _check_path(common_path)
        if result:
            return result
    
    # 5. Búsqueda recursiva en directorios comunes (solo si no se encontró)
    script_dir = Path(__file__).parent
    base_dir = script_dir.parent
    search_dirs = [
        script_dir / "client",
        base_dir / "client",
        Path.home() / "hana_client",
        Path.home() / "hdbclient",
    ]
    
    for search_dir in search_dirs:
        if search_dir.exists() and search_dir.is_dir():
            # Buscar recursivamente hdbsql
            for root, dirs, files in os.walk(search_dir):
                if 'hdbsql' in files:
                    hdbsql_path = Path(root) / 'hdbsql'
                    result = _check_path(hdbsql_path)
                    if result:
                        return result
    
    return None


def get_existing_records(hdbsql_path, config, schema, table_name, columns):
    """
    Obtiene todos los registros existentes de una tabla en HANA usando hdbsql
    
    Args:
        hdbsql_path: Ruta al binario hdbsql
        config: Diccionario con la configuración de HANA
        schema: Nombre del schema
        table_name: Nombre de la tabla (sin el prefijo DB_)
        columns: Lista de nombres de columnas
    
    Returns:
        set: Conjunto de tuplas normalizadas con los registros existentes
             Retorna conjunto vacío si hay error o no hay conexión
    """
    if not hdbsql_path or not config or not columns:
        return set()
    
    if not schema:
        # Si no hay schema, no podemos hacer la query
        return set()
    
    try:
        import subprocess
        
        # Construir query SELECT con todas las columnas
        columns_str = ', '.join([f'"{col}"' for col in columns])
        table_full_name = f'"{schema}"."DB_{table_name}"'
        query = f'SELECT {columns_str} FROM {table_full_name};'
        
        # Construir comando hdbsql
        host_port = f"{config['HANA_HOST']}:{config['HANA_PORT']}"
        cmd = [
            hdbsql_path,
            '-n', host_port,
            '-u', config['HANA_USER'],
            '-p', config['HANA_PASSWORD'],
            '-attemptencrypt',
            '-quiet'
        ]
        
        # Ejecutar query con timeout
        result = subprocess.run(cmd, input=query, capture_output=True, text=True, timeout=300)
        
        # Si hay error en la ejecución, retornar conjunto vacío
        if result.returncode != 0:
            # Verificar si es un error de tabla no encontrada (normal)
            stderr_lower = result.stderr.lower() if result.stderr else ''
            if 'table' in stderr_lower and ('not found' in stderr_lower or 'does not exist' in stderr_lower or 'invalid table' in stderr_lower):
                return set()
            # Para otros errores, retornar conjunto vacío (no lanzar excepción)
            return set()
        
        # Parsear la salida de hdbsql
        # Formato: CSV con comillas
        # USERID,APPROVALDATE,BYWHOM
        # "01614934","2024-08-21 18:47:51","01614934"
        # "01614934","2020-12-01 10:30:00","01614934"
        # N rows selected
        
        if not result.stdout:
            return set()
        
        import csv
        from io import StringIO
        
        # Usar csv.reader para parsear correctamente
        csv_reader = csv.reader(StringIO(result.stdout))
        
        existing_records = set()
        header_skipped = False
        
        for row in csv_reader:
            if not row:
                continue
            
            # Saltar la primera línea (encabezado)
            if not header_skipped:
                header_skipped = True
                continue
            
            # Saltar líneas que contengan "rows selected"
            row_str = ' '.join(row).lower()
            if 'rows selected' in row_str or 'row selected' in row_str:
                continue
            
            # Asegurar que tenemos suficientes valores
            while len(row) < len(columns):
                row.append('')
            
            # Tomar solo los valores que corresponden a las columnas
            values = row[:len(columns)]
            
            # Normalizar valores para comparación (eliminar comillas si las hay)
            normalized_row = tuple(
                str(val).strip().strip('"').strip("'") if val else '' 
                for val in values
            )
            existing_records.add(normalized_row)
        
        return existing_records
    except subprocess.TimeoutExpired:
        # Timeout, retornar conjunto vacío
        return set()
    except Exception as e:
        # Cualquier otro error, retornar conjunto vacío
        return set()
