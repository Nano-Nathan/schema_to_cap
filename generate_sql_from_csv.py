#!/usr/bin/env python3
"""
Script para generar archivos SQL con INSERT statements desde los CSV del export.tar.gz
Lee los datos de index/SCHEMA_NAME/ y genera archivos SQL compatibles con execute_sql.py
"""

import os
import sys
import csv
import re
import tarfile
import tempfile
from pathlib import Path
from io import StringIO
from utils import get_schema_name
from hana_connection import load_config, extract_schema_from_user, get_existing_records, find_hdbsql_path


class Colors:
    """Colores para output en terminal"""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color


def extract_column_names_from_create_sql(create_sql_content):
    """Extrae los nombres de columnas del CREATE TABLE statement"""
    # Buscar el patrón: "COLUMN_NAME" TYPE o COLUMN_NAME TYPE
    # Ejemplo: CREATE COLUMN TABLE "SCHEMA"."TABLENAME" ("COLUMN" NVARCHAR(500), ...)
    
    # Remover la parte del CREATE TABLE hasta el primer paréntesis
    match = re.search(r'CREATE\s+COLUMN\s+TABLE\s+[^(]+\((.+)\)', create_sql_content, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    
    columns_section = match.group(1)
    
    # Remover PRIMARY KEY y otras cláusulas al final
    # Buscar hasta PRIMARY KEY, UNLOAD, AUTO, etc.
    columns_section = re.split(r'\b(PRIMARY\s+KEY|UNLOAD|AUTO|MERGE)', columns_section, flags=re.IGNORECASE)[0]
    
    # Dividir por comas y procesar cada definición de columna
    columns = []
    # Patrón más preciso: "COLUMN_NAME" o COLUMN_NAME seguido de tipo de datos
    # Tipos comunes: NVARCHAR, INTEGER, SECONDDATE, TIMESTAMP, etc.
    column_pattern = r'["\']?([A-Z_$][A-Z0-9_$]*?)["\']?\s+(NVARCHAR|VARCHAR|INTEGER|INT|BIGINT|DECIMAL|DOUBLE|REAL|SECONDDATE|TIMESTAMP|DATE|TIME|BINARY|VARBINARY|BOOLEAN|TINYINT|SMALLINT|CLOB|NCLOB|BLOB)'
    
    matches = re.finditer(column_pattern, columns_section, re.IGNORECASE)
    for match in matches:
        col_name = match.group(1)
        # Filtrar columnas del sistema y palabras reservadas
        if not col_name.startswith('$') and col_name.upper() not in ['PRIMARY', 'KEY', 'INVERTED', 'VALUE', 'UNLOAD', 'PRIORITY', 'AUTO', 'MERGE']:
            columns.append(col_name)
    
    return columns if columns else None


def get_table_name_from_path(path):
    """Extrae el nombre de la tabla desde la ruta"""
    # Ejemplo: index/SCHEMA/EN/ENABLEDUSER/data.csv -> ENABLEDUSER
    parts = path.split('/')
    if len(parts) >= 4:
        return parts[3]  # El nombre de la tabla está en la posición 3
    return None


def read_csv_from_tar(tar_path, csv_path, extract_dir):
    """Lee un archivo CSV desde archivos descomprimidos o desde tar.gz"""
    # Primero intentar leer desde archivos descomprimidos
    csv_content = read_file_from_extracted(extract_dir, csv_path)
    if csv_content is not None:
        return csv_content
    
    # Si no está descomprimido, leer desde tar.gz
    try:
        with tarfile.open(tar_path, 'r:gz') as tar:
            member = tar.getmember(csv_path)
            if member:
                file_obj = tar.extractfile(member)
                if file_obj:
                    # Leer como texto
                    content = file_obj.read().decode('utf-8', errors='ignore')
                    return content
    except Exception as e:
        # Si el archivo no existe en el tar, puede ser que el CSV esté vacío
        pass
    return None


def check_files_already_extracted(extract_dir, schema_name):
    """Verifica si los archivos CSV ya están descomprimidos"""
    # Verificar si existe el directorio base
    index_dir = extract_dir / "index" / schema_name
    if not index_dir.exists():
        return False
    
    # Contar cuántos archivos data.csv deberían existir
    csv_files = list(index_dir.rglob("data.csv"))
    
    # Si hay menos de 100 archivos, probablemente no están todos
    if len(csv_files) < 100:
        return False
    
    return True


def read_file_from_tar(tar_path, file_path):
    """Lee un archivo desde un tar.gz"""
    try:
        with tarfile.open(tar_path, 'r:gz') as tar:
            member = tar.getmember(file_path)
            if member:
                file_obj = tar.extractfile(member)
                if file_obj:
                    return file_obj.read().decode('utf-8', errors='ignore')
    except Exception as e:
        pass
    return None


def read_file_from_extracted(extract_dir, file_path):
    """Lee un archivo desde el directorio descomprimido"""
    full_path = extract_dir / file_path
    if full_path.exists():
        return full_path.read_text(encoding='utf-8', errors='ignore')
    return None


def escape_sql_value(value):
    """Escapa un valor para SQL (reemplaza comillas simples)"""
    if value is None:
        return 'NULL'
    # Convertir a string y escapar comillas simples
    str_value = str(value)
    # Reemplazar comillas simples por dos comillas simples (SQL escape)
    str_value = str_value.replace("'", "''")
    return f"'{str_value}'"






def extract_primary_key_from_create_sql(create_sql_content):
    """Extrae las columnas de la clave primaria del CREATE TABLE statement"""
    # Buscar PRIMARY KEY (col1, col2, ...)
    match = re.search(r'PRIMARY\s+KEY\s*\(([^)]+)\)', create_sql_content, re.IGNORECASE)
    if not match:
        return None
    
    pk_section = match.group(1)
    # Extraer nombres de columnas (pueden estar entre comillas)
    pk_columns = []
    for col_match in re.finditer(r'["\']?([A-Z_$][A-Z0-9_$]*?)["\']?', pk_section, re.IGNORECASE):
        col_name = col_match.group(1)
        if col_name.upper() not in ['PRIMARY', 'KEY']:
            pk_columns.append(col_name)
    
    return pk_columns if pk_columns else None


def generate_insert_statements(table_name, columns, csv_content, existing_records=None):
    """Genera INSERT statements desde el contenido CSV, filtrando registros que ya existen"""
    if not columns:
        return None
    
    # Leer CSV
    csv_reader = csv.reader(StringIO(csv_content))
    
    # Generar INSERT statements
    insert_statements = []
    insert_statements.append(f"-- Script SQL generado automáticamente")
    insert_statements.append(f"-- Tabla: DB_{table_name}")
    insert_statements.append(f"-- Archivo CSV origen: {table_name}.csv")
    if existing_records is not None and len(existing_records) > 0:
        insert_statements.append(f"-- Filtrando registros existentes en HANA")
    elif existing_records is not None:
        insert_statements.append(f"-- Tabla verificada en HANA (vacía o no existe)")
    insert_statements.append("")
    
    row_count = 0
    skipped_count = 0
    
    # Si no hay registros existentes, usar conjunto vacío
    if existing_records is None:
        existing_records = set()
    
    for row in csv_reader:
        if not row:  # Saltar filas vacías
            continue
        
        # Asegurar que tenemos suficientes valores
        while len(row) < len(columns):
            row.append('')
        
        # Tomar solo los valores que corresponden a las columnas
        values = row[:len(columns)]
        
        # Normalizar valores para comparación (igual que en get_existing_records)
        normalized_values = tuple(
            str(val).strip() if val else '' 
            for val in values
        )
        
        # Verificar si el registro ya existe
        if normalized_values in existing_records:
            skipped_count += 1
            continue
        
        # Crear la lista de valores escapados
        escaped_values = [escape_sql_value(val) for val in values]
        
        # Crear el INSERT statement
        # Formato: INSERT INTO DB_TABLENAME ("COL1", "COL2", ...) VALUES ('val1', 'val2', ...);
        columns_str = ', '.join([f'"{col}"' for col in columns])
        values_str = ', '.join(escaped_values)
        
        insert_stmt = f"INSERT INTO DB_{table_name} ({columns_str}) VALUES ({values_str});"
        insert_statements.append(insert_stmt)
        row_count += 1
    
    # Agregar comentario con estadísticas
    if skipped_count > 0:
        insert_statements.insert(4, f"-- Registros omitidos (ya existen): {skipped_count:,}")
        insert_statements.insert(5, "")
    
    return '\n'.join(insert_statements), row_count, skipped_count


def process_table(tar_path, table_path, output_dir, extract_dir, hdbsql_path=None, config=None, schema=None):
    """Procesa una tabla: lee CSV y genera SQL, filtrando registros existentes en HANA"""
    table_name = get_table_name_from_path(table_path)
    if not table_name:
        return None, 0, 0
    
    # Rutas de archivos
    csv_path = table_path
    create_sql_path = table_path.replace('/data.csv', '/create.sql')
    
    # Leer create.sql (desde descomprimido o tar.gz)
    create_sql_content = read_file_from_extracted(extract_dir, create_sql_path)
    if not create_sql_content:
        create_sql_content = read_file_from_tar(tar_path, create_sql_path)
    
    if not create_sql_content:
        print(f"  {Colors.YELLOW}⚠ No se encontró create.sql para {table_name}{Colors.NC}")
        return None, 0, 0
    
    # Extraer nombres de columnas
    columns = extract_column_names_from_create_sql(create_sql_content)
    if not columns:
        print(f"  {Colors.YELLOW}⚠ No se pudieron extraer columnas de {table_name}{Colors.NC}")
        return None, 0, 0
    
    # Leer CSV (desde descomprimido o tar.gz)
    csv_content = read_csv_from_tar(tar_path, csv_path, extract_dir)
    if not csv_content:
        print(f"  {Colors.YELLOW}⚠ No se encontró data.csv para {table_name}{Colors.NC}")
        return None, 0, 0
    
    # Obtener registros existentes en HANA si hay hdbsql y configuración
    existing_records = None
    if hdbsql_path and config and schema:
        try:
            print(f"  {Colors.BLUE}Consultando registros existentes en HANA...{Colors.NC}")
            existing_records = get_existing_records(hdbsql_path, config, schema, table_name, columns)
            if existing_records and len(existing_records) > 0:
                print(f"  {Colors.BLUE}  Encontrados {len(existing_records):,} registros existentes{Colors.NC}")
            else:
                print(f"  {Colors.BLUE}  No hay registros existentes (tabla vacía o no existe){Colors.NC}")
                # Si la tabla está vacía, usar conjunto vacío para que se generen todos los INSERT
                existing_records = set()
        except Exception as e:
            error_msg = str(e)
            print(f"  {Colors.YELLOW}⚠ No se pudieron obtener registros existentes: {error_msg}{Colors.NC}")
            print(f"  {Colors.YELLOW}  Generando todos los INSERT statements{Colors.NC}")
            # Si hay error, no filtrar (generar todos los INSERT)
            existing_records = None
    else:
        # No hay hdbsql o configuración disponible, generar todos los INSERT
        existing_records = None
    
    # Generar INSERT statements (filtrando existentes si hay conexión)
    result = generate_insert_statements(table_name, columns, csv_content, existing_records)
    if result is None:
        return None, 0, 0
    
    sql_content, row_count, skipped_count = result
    
    # Guardar archivo SQL
    output_file = output_dir / f"{table_name}.sql"
    output_file.write_text(sql_content, encoding='utf-8')
    
    return output_file, row_count, skipped_count


def extract_files_from_tar(tar_path, extract_dir, schema_name):
    """Extrae los archivos CSV y create.sql necesarios del tar.gz (si no están ya descomprimidos)"""
    schema_path = f'index/{schema_name}/'
    
    # Verificar si ya están descomprimidos
    if check_files_already_extracted(extract_dir, schema_name):
        print(f"{Colors.BLUE}Archivos ya descomprimidos, usando existentes...{Colors.NC}")
        # Listar archivos CSV existentes
        csv_files = []
        index_dir = extract_dir / "index" / schema_name
        for csv_file in index_dir.rglob("data.csv"):
            rel_path = csv_file.relative_to(extract_dir)
            csv_files.append(str(rel_path))
        print(f"  {Colors.GREEN}✓ Encontrados {len(csv_files)} archivos CSV{Colors.NC}\n")
        return csv_files
    
    print(f"{Colors.BLUE}Extrayendo archivos CSV y create.sql del export.tar.gz...{Colors.NC}")
    
    extracted_files = []
    csv_files = []
    try:
        with tarfile.open(tar_path, 'r:gz') as tar:
            for member in tar.getmembers():
                # Extraer archivos CSV y create.sql de index/SCHEMA_NAME/
                if schema_path in member.name:
                    if member.name.endswith('/data.csv') or member.name.endswith('/create.sql'):
                        # Extraer archivo
                        tar.extract(member, extract_dir)
                        extracted_files.append(member.name)
                        if member.name.endswith('/data.csv'):
                            csv_files.append(member.name)
        print(f"  {Colors.GREEN}✓ Extraídos {len(extracted_files)} archivos ({len(csv_files)} CSV){Colors.NC}\n")
        return csv_files
    except Exception as e:
        print(f"  {Colors.RED}✗ Error extrayendo archivos: {e}{Colors.NC}")
        return []


def main():
    """Función principal"""
    import os
    # Directorio del script (schema_to_cap)
    script_dir = Path(__file__).parent
    # Directorio base (padre de schema_to_cap)
    base_dir = Path(os.environ.get('PROJECT_BASE_DIR', script_dir.parent))
    # Rutas configurables
    tar_filename = os.environ.get('EXPORT_TAR_FILE', 'export.tar.gz')
    sql_output_dir = os.environ.get('SQL_DIR', 'data_insert_sql')
    extract_dir_name = os.environ.get('EXTRACT_DIR', 'temp_extract')
    # El tar.gz y archivos temporales están en schema_to_cap
    tar_path = script_dir / tar_filename
    output_dir = script_dir / sql_output_dir
    extract_dir = script_dir / extract_dir_name
    
    # Validar que existe el tar.gz
    if not tar_path.exists():
        print(f"{Colors.RED}Error: No se encontró {tar_path}{Colors.NC}")
        sys.exit(1)
    
    # Crear directorios
    extract_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    
    print(f"{Colors.YELLOW}=== Generando archivos SQL desde export.tar.gz ==={Colors.NC}")
    print(f"Archivo: {tar_path}")
    print(f"Directorio de salida: {output_dir}")
    print()
    
    # Obtener nombre del schema (auto-detectado o configurado)
    schema_name = get_schema_name(tar_path=tar_path, extract_dir=extract_dir)
    if not schema_name:
        print(f"{Colors.RED}Error: No se pudo detectar el nombre del schema{Colors.NC}")
        print(f"{Colors.YELLOW}Configura SCHEMA en hana_config.conf o como variable de entorno{Colors.NC}")
        sys.exit(1)
    
    print(f"{Colors.BLUE}Usando schema: {schema_name}{Colors.NC}\n")
    
    # Extraer archivos necesarios (o usar existentes)
    table_paths = extract_files_from_tar(tar_path, extract_dir, schema_name)
    
    if not table_paths:
        print(f"{Colors.RED}No se encontraron archivos data.csv en index/{schema_name}/{Colors.NC}")
        sys.exit(1)
    
    print(f"{Colors.BLUE}Encontradas {len(table_paths)} tablas para procesar{Colors.NC}\n")
    
    # Intentar encontrar hdbsql y cargar configuración para filtrar registros existentes
    hdbsql_path = None
    schema = None
    config = load_config(require_config=False, show_messages=False)
    
    if config:
        print(f"{Colors.BLUE}Buscando hdbsql para filtrar registros existentes...{Colors.NC}")
        hdbsql_path = find_hdbsql_path(config)
        if hdbsql_path:
            # Extraer schema del usuario
            schema = extract_schema_from_user(config['HANA_USER'])
            print(f"{Colors.GREEN}✓ hdbsql encontrado: {hdbsql_path}{Colors.NC}")
            print(f"{Colors.GREEN}✓ Schema: {schema}{Colors.NC}")
            print(f"{Colors.BLUE}  Solo se generarán INSERT para registros nuevos{Colors.NC}\n")
        else:
            print(f"{Colors.YELLOW}⚠ No se encontró hdbsql{Colors.NC}")
            print(f"{Colors.YELLOW}  Se generarán todos los INSERT statements{Colors.NC}\n")
    else:
        print(f"{Colors.YELLOW}⚠ No se encontró configuración de HANA{Colors.NC}")
        print(f"{Colors.YELLOW}  Se generarán todos los INSERT statements{Colors.NC}\n")
    
    # Procesar cada tabla
    success_count = 0
    error_count = 0
    total_rows = 0
    total_skipped = 0
    
    for idx, table_path in enumerate(sorted(table_paths), 1):
        table_name = get_table_name_from_path(table_path)
        print(f"{Colors.YELLOW}[{idx}/{len(table_paths)}] Procesando: {table_name}{Colors.NC}")
        
        try:
            result = process_table(tar_path, table_path, output_dir, extract_dir, hdbsql_path, config, schema)
            if result is None:
                error_count += 1
                print(f"  {Colors.RED}✗ Error generando SQL{Colors.NC}")
            else:
                output_file, row_count, skipped_count = result
                if output_file:
                    success_count += 1
                    total_rows += row_count
                    total_skipped += skipped_count
                    msg = f"  {Colors.GREEN}✓ Generado: {output_file.name} ({row_count:,} registros nuevos"
                    if skipped_count > 0:
                        msg += f", {skipped_count:,} omitidos"
                    msg += f"){Colors.NC}"
                    print(msg)
                else:
                    error_count += 1
                    print(f"  {Colors.RED}✗ Error generando SQL{Colors.NC}")
        except Exception as e:
            error_count += 1
            print(f"  {Colors.RED}✗ Error: {str(e)}{Colors.NC}")
    
    
    # Resumen
    print()
    print(f"{Colors.YELLOW}=== Resumen ==={Colors.NC}")
    print(f"Total de tablas: {len(table_paths)}")
    print(f"{Colors.GREEN}Exitosas: {success_count}{Colors.NC}")
    print(f"{Colors.RED}Con errores: {error_count}{Colors.NC}")
    print(f"Total de registros nuevos: {total_rows:,}")
    if total_skipped > 0:
        print(f"Total de registros omitidos (ya existían): {total_skipped:,}")
    print()
    print(f"Archivos SQL generados en: {output_dir}/")
    print(f"\n{Colors.BLUE}Para ejecutar los SQL, usa:{Colors.NC}")
    print(f"  python3 execute_sql.py")


if __name__ == "__main__":
    main()
