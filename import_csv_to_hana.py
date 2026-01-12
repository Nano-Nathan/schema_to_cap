#!/usr/bin/env python3
"""
Script optimizado para importar datos CSV directamente a SAP HANA
Lee CSV, verifica duplicados y ejecuta INSERT statements sin crear archivos intermedios
"""

import csv
import re
import sys
import time
import tarfile
from io import StringIO
from pathlib import Path
from typing import Optional, Set, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from hana_connection import load_config, Colors, get_config_value
from hana_client import HanaClient, HanaClientError
from utils import get_schema_name


def extract_column_names(create_sql_content: str) -> Optional[List[str]]:
    """Extrae nombres de columnas del CREATE TABLE"""
    match = re.search(r'CREATE\s+COLUMN\s+TABLE\s+[^(]+\((.+)\)', create_sql_content, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    
    columns_section = re.split(r'\b(PRIMARY\s+KEY|UNLOAD|AUTO|MERGE)', match.group(1), flags=re.IGNORECASE)[0]
    column_pattern = r'["\']?([A-Z_$][A-Z0-9_$]*?)["\']?\s+(NVARCHAR|VARCHAR|INTEGER|INT|BIGINT|DECIMAL|DOUBLE|REAL|SECONDDATE|TIMESTAMP|DATE|TIME|BINARY|VARBINARY|BOOLEAN|TINYINT|SMALLINT|CLOB|NCLOB|BLOB)'
    
    columns = []
    for match in re.finditer(column_pattern, columns_section, re.IGNORECASE):
        col_name = match.group(1)
        if not col_name.startswith('$') and col_name.upper() not in ['PRIMARY', 'KEY', 'INVERTED', 'VALUE', 'UNLOAD', 'PRIORITY', 'AUTO', 'MERGE']:
            columns.append(col_name)
    
    return columns if columns else None


def extract_primary_key_from_create_sql(create_sql_content: str) -> Optional[List[str]]:
    """Extrae las columnas de la clave primaria del CREATE TABLE statement"""
    # Buscar PRIMARY KEY (col1, col2, ...)
    # Patrón: PRIMARY KEY (col1, col2) o PRIMARY KEY INVERTED VALUE (col1, col2)
    match = re.search(r'PRIMARY\s+KEY\s*(?:INVERTED\s+VALUE\s*)?\(([^)]+)\)', create_sql_content, re.IGNORECASE)
    if not match:
        return None
    
    pk_section = match.group(1)
    # Extraer nombres de columnas (pueden estar entre comillas y separadas por comas)
    # Patrón mejorado: busca palabras completas entre comillas o sin comillas
    pk_columns = []
    # Dividir por comas primero
    parts = [p.strip() for p in pk_section.split(',')]
    for part in parts:
        # Extraer nombre de columna (puede estar entre comillas)
        col_match = re.search(r'["\']?([A-Z_$][A-Z0-9_$]+)["\']?', part, re.IGNORECASE)
        if col_match:
            col_name = col_match.group(1)
            if col_name.upper() not in ['PRIMARY', 'KEY', 'INVERTED', 'VALUE']:
                pk_columns.append(col_name)
    
    return pk_columns if pk_columns else None


def get_table_name_from_path(path: str) -> Optional[str]:
    """Extrae nombre de tabla desde ruta: index/SCHEMA/EN/TABLENAME/data.csv"""
    parts = path.split('/')
    return parts[3] if len(parts) >= 4 else None


def read_file_content(tar_path: Path, file_path: str, extract_dir: Path) -> Optional[str]:
    """Lee archivo desde descomprimido o tar.gz"""
    # Intentar desde descomprimido
    full_path = extract_dir / file_path
    if full_path.exists():
        return full_path.read_text(encoding='utf-8', errors='ignore')
    
    # Leer desde tar.gz
    try:
        with tarfile.open(tar_path, 'r:gz') as tar:
            member = tar.getmember(file_path)
            if member:
                file_obj = tar.extractfile(member)
                if file_obj:
                    return file_obj.read().decode('utf-8', errors='ignore')
    except Exception:
        pass
    return None


def escape_sql_value(value) -> str:
    """Escapa valor para SQL"""
    if value is None:
        return 'NULL'
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def normalize_value(val) -> str:
    """Normaliza un valor para comparación (exactamente igual que en hana_client.get_table_records)"""
    # Debe ser idéntico a: str(val).strip().strip('"').strip("'") if val else ''
    return str(val).strip().strip('"').strip("'") if val else ''

def generate_and_execute_inserts(
    table_name: str,
    columns: List[str],
    csv_content: str,
    client: HanaClient,
    schema: str,
    error_log_dir: Path,
    create_sql_content: Optional[str] = None
) -> Tuple[int, int, List[str]]:
    """
    Genera y ejecuta INSERT statements con comparación optimizada en paralelo
    Usa clave primaria para comparación (más eficiente y correcto)
    Retorna (insertados, omitidos, errores)
    """
    # Determinar columnas para comparación (clave primaria si está disponible)
    pk_columns = None
    if create_sql_content:
        pk_columns = extract_primary_key_from_create_sql(create_sql_content)
    
    if pk_columns:
        # Verificar que todas las PK columns estén en columns
        pk_indices = []
        for pk_col in pk_columns:
            if pk_col in columns:
                pk_indices.append(columns.index(pk_col))
            else:
                pk_columns = None  # Si alguna PK no está, usar todas las columnas
                break
        
        if pk_columns and pk_indices:
            print(f"  {Colors.BLUE}Usando clave primaria para comparación: {pk_columns}{Colors.NC}")
            use_pk = True
        else:
            use_pk = False
    else:
        use_pk = False
    
    # Paso 1: Obtener todos los registros del CSV
    print(f"  {Colors.BLUE}Leyendo registros del CSV...{Colors.NC}")
    csv_reader = csv.reader(StringIO(csv_content))
    csv_records = {}  # Dict: normalized_tuple -> (row_idx, original_values)
    
    for row_idx, row in enumerate(csv_reader, 1):
        if not row:
            continue
        
        # Asegurar suficientes valores
        while len(row) < len(columns):
            row.append('')
        
        values = row[:len(columns)]
        
        # Normalizar usando PK o todas las columnas
        if use_pk:
            pk_values = tuple(normalize_value(values[i]) for i in pk_indices)
            normalized = pk_values
        else:
            normalized = tuple(normalize_value(val) for val in values)
        
        csv_records[normalized] = (row_idx, values)
    
    total_csv_records = len(csv_records)
    print(f"  {Colors.BLUE}Total registros en CSV: {total_csv_records:,}{Colors.NC}")
    
    # Paso 2: Obtener registros de HANA en lotes y comparar en paralelo
    table_full_name = f"DB_{table_name}"
    count = client.count_table_records(schema, table_full_name)
    
    if count is None or count == 0:
        print(f"  {Colors.BLUE}Tabla vacía o no existe, todos los registros son nuevos{Colors.NC}")
        csv_records_to_insert = csv_records
        skipped = 0
    else:
        print(f"  {Colors.BLUE}Registros en HANA: {count:,}{Colors.NC}")
        print(f"  {Colors.BLUE}Comparando en paralelo (5 threads, lotes de 1000)...{Colors.NC}")
        
        # Estructuras thread-safe para la comparación
        csv_lock = Lock()
        skipped_count = [0]  # Lista para poder modificar desde threads (thread-safe)
        
        def compare_batch(offset: int) -> int:
            """
            Obtiene un lote de HANA y compara con CSV de forma thread-safe
            Retorna cantidad de duplicados encontrados en este lote
            """
            batch_skipped = 0
            try:
                # Obtener lote de 1000 registros de HANA
                hana_batch = client.get_table_records_paginated(
                    schema, table_full_name, columns, offset, 1000
                )
                
                if not hana_batch:
                    return 0
                
                # Normalizar registros de HANA usando PK o todas las columnas
                hana_normalized = []
                for hana_record in hana_batch:
                    if use_pk:
                        pk_values = tuple(normalize_value(hana_record[i]) for i in pk_indices)
                        hana_normalized.append(pk_values)
                    else:
                        hana_normalized.append(hana_record)
                
                # Primero identificar duplicados (sin lock para mejor rendimiento)
                duplicates_found = []
                for hana_key in hana_normalized:
                    # Verificar si existe (lectura, no necesita lock todavía)
                    if hana_key in csv_records:
                        duplicates_found.append(hana_key)
                
                # Eliminar duplicados de forma thread-safe (escritura necesita lock)
                if duplicates_found:
                    with csv_lock:
                        for hana_key in duplicates_found:
                            # Verificar de nuevo dentro del lock (double-check)
                            if hana_key in csv_records:
                                del csv_records[hana_key]
                                batch_skipped += 1
                
                return batch_skipped
            except Exception as e:
                print(f"  {Colors.YELLOW}⚠ Error en lote offset {offset}: {e}{Colors.NC}")
                return 0
        
        # Ejecutar comparación en paralelo: 5 threads procesando 5000 registros a la vez
        batch_size = 1000
        comparison_threads = 5
        total_batches = (count + batch_size - 1) // batch_size
        
        with ThreadPoolExecutor(max_workers=comparison_threads) as executor:
            futures = []
            for batch_num in range(total_batches):
                offset = batch_num * batch_size
                future = executor.submit(compare_batch, offset)
                futures.append(future)
            
            # Esperar resultados y contar duplicados
            for future in as_completed(futures):
                batch_skipped = future.result()
                skipped_count[0] += batch_skipped
        
        skipped = skipped_count[0]
        csv_records_to_insert = csv_records
        print(f"  {Colors.BLUE}Duplicados encontrados: {skipped:,}{Colors.NC}")
        print(f"  {Colors.BLUE}Registros nuevos a insertar: {len(csv_records_to_insert):,}{Colors.NC}")
    
    if not csv_records_to_insert:
        return 0, skipped, []
    
    # Paso 3: Preparar INSERTs para los registros que quedan
    inserts_to_execute = []
    failed_inserts = []
    columns_str = ', '.join([f'"{col}"' for col in columns])
    table_full_name = f'"{schema}"."DB_{table_name}"'
    
    for normalized, (row_idx, values) in csv_records_to_insert.items():
        escaped_values = [escape_sql_value(val) for val in values]
        values_str = ', '.join(escaped_values)
        inserts_to_execute.append((row_idx, values_str))
    
    if not inserts_to_execute:
        return 0, skipped, []
    
    # Mostrar cuántos se van a ejecutar
    if skipped > 0:
        print(f"  {Colors.BLUE}Ejecutando {len(inserts_to_execute):,} registros (omitiendo {skipped:,} duplicados) con threading...{Colors.NC}")
    else:
        print(f"  {Colors.BLUE}Ejecutando {len(inserts_to_execute):,} registros con threading...{Colors.NC}")
    
    # Ejecutar INSERTs individuales en paralelo con threading
    # HANA no soporta múltiples VALUES, así que ejecutamos uno por uno pero en paralelo
    inserted = 0
    insert_lock = Lock()
    error_lock = Lock()
    
    def execute_single_insert(row_idx: int, values_str: str) -> Tuple[int, Optional[str]]:
        """
        Ejecuta un solo INSERT statement
        Returns: (status, error_msg)
        status: 1=insertado, 0=duplicado (skip), -1=error
        """
        try:
            single_stmt = f"INSERT INTO {table_full_name} ({columns_str}) VALUES ({values_str});"
            returncode, stdout, stderr = client.execute_query(single_stmt)
            if returncode == 0:
                return 1, None  # Insertado exitosamente
            else:
                error_msg = stderr.strip() if stderr else 'Error desconocido'
                # Ignorar errores de unique constraint (duplicados) - es normal
                if 'unique constraint' in error_msg.lower() or 'duplicate' in error_msg.lower():
                    return 0, None  # Duplicado, contar como skipped
                return -1, f"Línea {row_idx}: {error_msg}"  # Error real
        except Exception as e:
            return -1, f"Línea {row_idx}: {str(e)}"
    
    # Ejecutar con ThreadPoolExecutor (10 threads en paralelo)
    max_workers = 10
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Enviar todos los INSERTs al pool
        future_to_insert = {
            executor.submit(execute_single_insert, row_idx, values_str): (row_idx, values_str)
            for row_idx, values_str in inserts_to_execute
        }
        
        # Procesar resultados conforme se completan
        for future in as_completed(future_to_insert):
            status, error_msg = future.result()
            with insert_lock:
                if status == 1:
                    inserted += 1
                elif status == 0:
                    skipped += 1  # Duplicado detectado durante inserción
                elif error_msg:
                    with error_lock:
                        failed_inserts.append(error_msg)
    
    # Guardar errores si los hay
    if failed_inserts:
        error_file = error_log_dir / f"{table_name}.err"
        with open(error_file, 'w', encoding='utf-8') as f:
            f.write(f"Errores al insertar en tabla {table_name}:\n\n")
            for error in failed_inserts:
                f.write(f"{error}\n")
    
    return inserted, skipped, failed_inserts


def process_table(
    tar_path: Path,
    table_path: str,
    extract_dir: Path,
    client: HanaClient,
    schema: str,
    error_log_dir: Path
) -> Tuple[bool, int, int, int]:
    """Procesa una tabla: lee CSV, verifica duplicados y ejecuta INSERTs"""
    table_name = get_table_name_from_path(table_path)
    if not table_name:
        return False, 0, 0, 0
    
    # Leer create.sql
    create_sql_path = table_path.replace('/data.csv', '/create.sql')
    create_sql_content = read_file_content(tar_path, create_sql_path, extract_dir)
    if not create_sql_content:
        print(f"  {Colors.YELLOW}⚠ No se encontró create.sql{Colors.NC}")
        return False, 0, 0, 0
    
    # Extraer columnas
    columns = extract_column_names(create_sql_content)
    if not columns:
        print(f"  {Colors.YELLOW}⚠ No se pudieron extraer columnas{Colors.NC}")
        return False, 0, 0, 0
    
    # Leer CSV
    csv_content = read_file_content(tar_path, table_path, extract_dir)
    if not csv_content:
        print(f"  {Colors.YELLOW}⚠ No se encontró CSV{Colors.NC}")
        return False, 0, 0, 0
    
    # Generar y ejecutar INSERTs (pasar create_sql_content para extraer PK)
    inserted, skipped, errors = generate_and_execute_inserts(
        table_name, columns, csv_content, client, schema, error_log_dir, create_sql_content
    )
    
    success = len(errors) == 0
    return success, inserted, skipped, len(errors)


def get_csv_files(tar_path: Path, extract_dir: Path, schema_name: str) -> List[str]:
    """Obtiene lista de archivos CSV desde descomprimido o tar.gz"""
    # Intentar desde descomprimido
    index_dir = extract_dir / "index" / schema_name
    if index_dir.exists():
        csv_files = []
        for csv_file in index_dir.rglob("data.csv"):
            rel_path = csv_file.relative_to(extract_dir)
            csv_files.append(str(rel_path))
        if csv_files:
            return csv_files
    
    # Leer desde tar.gz
    csv_files = []
    schema_path = f'index/{schema_name}/'
    try:
        with tarfile.open(tar_path, 'r:gz') as tar:
            for member in tar.getmembers():
                if schema_path in member.name and member.name.endswith('/data.csv'):
                    csv_files.append(member.name)
    except Exception:
        pass
    
    return csv_files


def main():
    """Función principal"""
    script_dir = Path(__file__).parent
    config = load_config(require_config=True, show_messages=True)
    
    # Rutas configurables
    tar_filename = get_config_value(config, 'EXPORT_TAR_FILE', 'export.tar.gz')
    extract_dir_name = get_config_value(config, 'EXTRACT_DIR', 'temp_extract')
    error_log_dir_name = get_config_value(config, 'ERROR_LOG_DIR', 'error_logs')
    
    tar_path = script_dir / tar_filename
    extract_dir = script_dir / extract_dir_name
    error_log_dir = script_dir / error_log_dir_name
    error_log_dir.mkdir(exist_ok=True)
    
    if not tar_path.exists():
        print(f"{Colors.RED}Error: No se encontró {tar_path}{Colors.NC}")
        sys.exit(1)
    
    # Obtener schema
    schema_name = get_schema_name(config=config, tar_path=tar_path, extract_dir=extract_dir)
    if not schema_name:
        print(f"{Colors.RED}Error: No se pudo detectar el schema{Colors.NC}")
        sys.exit(1)
    
    print(f"{Colors.YELLOW}=== Importando CSV a SAP HANA ==={Colors.NC}")
    print(f"Schema: {schema_name}\n")
    
    # Inicializar cliente HANA
    try:
        client = HanaClient(config)
        schema = client.get_schema()
        if not client.test_connection():
            print(f"{Colors.RED}Error: No se pudo conectar con HANA{Colors.NC}")
            sys.exit(1)
        print(f"{Colors.GREEN}✓ Conectado a HANA{Colors.NC}\n")
    except HanaClientError as e:
        print(f"{Colors.RED}Error: {str(e)}{Colors.NC}")
        sys.exit(1)
    
    # Obtener archivos CSV
    csv_files = get_csv_files(tar_path, extract_dir, schema_name)
    if not csv_files:
        print(f"{Colors.RED}No se encontraron archivos CSV{Colors.NC}")
        sys.exit(1)
    
    print(f"{Colors.BLUE}Encontradas {len(csv_files)} tablas{Colors.NC}\n")
    
    # Procesar cada tabla
    success_count = 0
    error_count = 0
    total_inserted = 0
    total_skipped = 0
    total_errors = 0
    start_time = time.time()
    
    for idx, table_path in enumerate(sorted(csv_files), 1):
        table_name = get_table_name_from_path(table_path)
        table_start = time.time()
        print(f"{Colors.YELLOW}[{idx}/{len(csv_files)}] {table_name}{Colors.NC}")
        
        success, inserted, skipped, errors = process_table(
            tar_path, table_path, extract_dir, client, schema, error_log_dir
        )
        
        table_duration = int(time.time() - table_start)
        
        if success:
            success_count += 1
            msg = f"  {Colors.GREEN}✓ {inserted:,} insertados"
            if skipped > 0:
                msg += f", {skipped:,} omitidos"
            msg += f" ({table_duration}s){Colors.NC}"
            print(msg)
        else:
            error_count += 1
            msg = f"  {Colors.RED}✗ {inserted:,} insertados"
            if skipped > 0:
                msg += f", {skipped:,} omitidos"
            if errors > 0:
                msg += f", {errors} errores (ver {table_name}.err)"
            msg += f" ({table_duration}s){Colors.NC}"
            print(msg)
        
        total_inserted += inserted
        total_skipped += skipped
        total_errors += errors
    
    # Resumen
    total_duration = int(time.time() - start_time)
    print(f"\n{Colors.YELLOW}=== Resumen ==={Colors.NC}")
    print(f"Total tablas: {len(csv_files)}")
    print(f"{Colors.GREEN}Exitosas: {success_count}{Colors.NC}")
    print(f"{Colors.RED}Con errores: {error_count}{Colors.NC}")
    print(f"Total insertados: {total_inserted:,}")
    if total_skipped > 0:
        print(f"Total omitidos: {total_skipped:,}")
    if total_errors > 0:
        print(f"{Colors.RED}Total errores: {total_errors}{Colors.NC}")
        print(f"Logs de errores en: {error_log_dir}/")
    print(f"\n{Colors.BLUE}Tiempo total: {total_duration}s ({total_duration//60}m {total_duration%60}s){Colors.NC}")
    if total_inserted > 0:
        rate = total_inserted / total_duration if total_duration > 0 else 0
        print(f"{Colors.BLUE}Velocidad: {rate:,.0f} registros/segundo{Colors.NC}")


if __name__ == "__main__":
    main()
