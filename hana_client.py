#!/usr/bin/env python3
"""
Módulo para ejecutar comandos en SAP HANA usando hdbsql
Proporciona funciones reutilizables para interactuar con HANA
"""

import os
import subprocess
import csv
from io import StringIO
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from hana_connection import find_hdbsql_path, load_config, extract_schema_from_user


class HanaClientError(Exception):
    """Excepción personalizada para errores del cliente HANA"""
    pass


class HanaClient:
    """Cliente para ejecutar comandos en SAP HANA usando hdbsql"""
    
    def __init__(self, config: Optional[Dict[str, str]] = None, hdbsql_path: Optional[str] = None):
        """
        Inicializa el cliente HANA
        
        Args:
            config: Diccionario con configuración de HANA (si es None, se carga automáticamente)
            hdbsql_path: Ruta al binario hdbsql (si es None, se busca automáticamente)
        """
        self.config = config or load_config(require_config=True, show_messages=False)
        self.hdbsql_path = hdbsql_path or find_hdbsql_path(self.config)
        
        if not self.hdbsql_path:
            raise HanaClientError(
                "No se encontró el cliente HANA (hdbsql). "
                "Por favor, instala el cliente HANA o configura HANA_CLIENT_PATH."
            )
        
        if not Path(self.hdbsql_path).exists():
            raise HanaClientError(f"El binario hdbsql no existe en: {self.hdbsql_path}")
        
        # Validar configuración requerida
        required_keys = ['HANA_HOST', 'HANA_PORT', 'HANA_USER', 'HANA_PASSWORD']
        for key in required_keys:
            if key not in self.config:
                raise HanaClientError(f"Falta la configuración requerida: {key}")
    
    def _build_base_command(self) -> List[str]:
        """Construye el comando base de hdbsql"""
        host_port = f"{self.config['HANA_HOST']}:{self.config['HANA_PORT']}"
        return [
            self.hdbsql_path,
            '-n', host_port,
            '-u', self.config['HANA_USER'],
            '-p', self.config['HANA_PASSWORD'],
            '-attemptencrypt',
            '-quiet'
        ]
    
    def execute_query(self, query: str, timeout: Optional[int] = None) -> Tuple[int, str, str]:
        """
        Ejecuta una query SQL y retorna el resultado
        
        Args:
            query: Query SQL a ejecutar
            timeout: Timeout en segundos (None = sin timeout)
        
        Returns:
            Tuple[int, str, str]: (returncode, stdout, stderr)
        """
        cmd = self._build_base_command()
        
        try:
            result = subprocess.run(
                cmd,
                input=query,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            raise HanaClientError(f"Timeout ejecutando query (timeout: {timeout}s)")
        except Exception as e:
            raise HanaClientError(f"Error ejecutando query: {str(e)}")
    
    def execute_sql_file(self, sql_file_path: Path, timeout: Optional[int] = None) -> Tuple[int, str, str]:
        """
        Ejecuta un archivo SQL y retorna el resultado
        
        Args:
            sql_file_path: Ruta al archivo SQL
            timeout: Timeout en segundos (None = sin timeout, usa SQL_TIMEOUT del config)
        
        Returns:
            Tuple[int, str, str]: (returncode, stdout, stderr)
        """
        if not sql_file_path.exists():
            raise HanaClientError(f"El archivo SQL no existe: {sql_file_path}")
        
        # Usar timeout del config si no se especifica
        if timeout is None:
            timeout_str = self.config.get('SQL_TIMEOUT')
            if timeout_str:
                try:
                    timeout = int(timeout_str)
                except (ValueError, TypeError):
                    timeout = None
        
        cmd = self._build_base_command()
        cmd.extend(['-I', str(sql_file_path)])
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            raise HanaClientError(f"Timeout ejecutando archivo SQL (timeout: {timeout}s)")
        except Exception as e:
            raise HanaClientError(f"Error ejecutando archivo SQL: {str(e)}")
    
    def count_table_records(self, schema: str, table_name: str, timeout: int = 60) -> Optional[int]:
        """
        Cuenta los registros en una tabla
        
        Args:
            schema: Nombre del schema
            table_name: Nombre de la tabla
            timeout: Timeout en segundos (por defecto 60)
        
        Returns:
            int: Número de registros o None si hay error
        """
        query = f'SELECT COUNT(*) FROM "{schema}"."{table_name}";'
        
        try:
            returncode, stdout, stderr = self.execute_query(query, timeout=timeout)
            if returncode == 0 and stdout:
                # Parsear el resultado: "COUNT(*)\n12345\n1 row selected"
                lines = [
                    l.strip() 
                    for l in stdout.strip().split('\n') 
                    if l.strip() and not l.strip().startswith('COUNT')
                ]
                if lines:
                    try:
                        return int(lines[0])
                    except (ValueError, TypeError):
                        pass
        except Exception:
            pass
        
        return None
    
    def get_table_records_paginated(self, schema: str, table_name: str, columns: List[str],
                                    offset: int, limit: int, timeout: int = 300) -> List[Tuple[str, ...]]:
        """
        Obtiene registros de una tabla con paginación (OFFSET/LIMIT)
        
        Args:
            schema: Nombre del schema
            table_name: Nombre de la tabla
            columns: Lista de nombres de columnas
            offset: Número de registros a saltar
            limit: Número máximo de registros a obtener
            timeout: Timeout en segundos (por defecto 300)
        
        Returns:
            List[Tuple]: Lista de tuplas con los registros
        """
        if not columns:
            return []
        
        columns_str = ', '.join([f'"{col}"' for col in columns])
        table_full_name = f'"{schema}"."{table_name}"'
        query = f'SELECT {columns_str} FROM {table_full_name} LIMIT {limit} OFFSET {offset};'
        
        try:
            returncode, stdout, stderr = self.execute_query(query, timeout=timeout)
            
            if returncode != 0:
                stderr_lower = stderr.lower() if stderr else ''
                if 'table' in stderr_lower and ('not found' in stderr_lower or 
                                                'does not exist' in stderr_lower or 
                                                'invalid table' in stderr_lower):
                    return []
                return []
            
            if not stdout:
                return []
            
            # Parsear la salida de hdbsql usando CSV
            csv_reader = csv.reader(StringIO(stdout))
            records = []
            header_skipped = False
            
            for row in csv_reader:
                if not row:
                    continue
                
                if not header_skipped:
                    header_skipped = True
                    continue
                
                row_str = ' '.join(row).lower()
                if 'rows selected' in row_str or 'row selected' in row_str:
                    continue
                
                while len(row) < len(columns):
                    row.append('')
                
                values = row[:len(columns)]
                normalized_row = tuple(
                    str(val).strip().strip('"').strip("'") if val else '' 
                    for val in values
                )
                records.append(normalized_row)
            
            return records
        except Exception:
            return []
    
    def get_table_records(self, schema: str, table_name: str, columns: List[str], 
                         timeout: int = 300) -> List[Tuple[str, ...]]:
        """
        Obtiene todos los registros de una tabla
        
        Args:
            schema: Nombre del schema
            table_name: Nombre de la tabla
            columns: Lista de nombres de columnas
            timeout: Timeout en segundos (por defecto 300)
        
        Returns:
            List[Tuple]: Lista de tuplas con los registros
        """
        if not columns:
            return []
        
        columns_str = ', '.join([f'"{col}"' for col in columns])
        table_full_name = f'"{schema}"."{table_name}"'
        query = f'SELECT {columns_str} FROM {table_full_name};'
        
        try:
            returncode, stdout, stderr = self.execute_query(query, timeout=timeout)
            
            if returncode != 0:
                # Verificar si es un error de tabla no encontrada (normal)
                stderr_lower = stderr.lower() if stderr else ''
                if 'table' in stderr_lower and ('not found' in stderr_lower or 
                                                'does not exist' in stderr_lower or 
                                                'invalid table' in stderr_lower):
                    return []
                return []
            
            if not stdout:
                return []
            
            # Parsear la salida de hdbsql usando CSV
            csv_reader = csv.reader(StringIO(stdout))
            records = []
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
                
                # Normalizar valores (eliminar comillas si las hay)
                normalized_row = tuple(
                    str(val).strip().strip('"').strip("'") if val else '' 
                    for val in values
                )
                records.append(normalized_row)
            
            return records
        except Exception:
            return []
    
    def test_connection(self) -> bool:
        """
        Prueba la conexión con HANA ejecutando una query simple
        
        Returns:
            bool: True si la conexión es exitosa, False en caso contrario
        """
        try:
            returncode, stdout, stderr = self.execute_query("SELECT 1 FROM DUMMY;", timeout=10)
            return returncode == 0
        except Exception:
            return False
    
    def get_schema(self) -> Optional[str]:
        """
        Obtiene el nombre del schema desde el usuario configurado
        
        Returns:
            str: Nombre del schema o None
        """
        return extract_schema_from_user(self.config.get('HANA_USER', ''))
