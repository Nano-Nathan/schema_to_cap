#!/usr/bin/env python3
"""
Script para clonar la estructura del export.tar.gz al proyecto CAP
Descomprime los archivos necesarios y genera el schema.cds usando table.xml
"""

import os
import re
import tarfile
import xml.etree.ElementTree as ET
from pathlib import Path
from collections import OrderedDict
from utils import get_schema_name


class Colors:
    """Colores para output en terminal"""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color


# Mapeo de tipos HANA (desde create.sql) a tipos CDS
HANA_TYPE_TO_CDS = {
    'NVARCHAR': 'String',
    'VARCHAR': 'String',
    'NCHAR': 'String',
    'CHAR': 'String',
    'SECONDDATE': 'DateTime',
    'LONGDATE': 'DateTime',  # LONGDATE es equivalente a DateTime
    'TIMESTAMP': 'DateTime',
    'DATE': 'Date',
    'TIME': 'Time',
    'BIGINT': 'Integer64',
    'INTEGER': 'Integer',
    'INT': 'Integer',
    'SMALLINT': 'Integer',
    'TINYINT': 'Integer',
    'DECIMAL': 'Decimal',
    'DOUBLE': 'Double',
    'REAL': 'Double',
    'FLOAT': 'Double',
    'BINARY': 'Binary',
    'VARBINARY': 'Binary',
    'BLOB': 'LargeBinary',
    'CLOB': 'LargeString',
    'NCLOB': 'LargeString',
    'BOOLEAN': 'Boolean',
}


def map_hana_type_to_cds(hana_type):
    """Mapea un tipo HANA (string) a un tipo CDS"""
    hana_type_upper = hana_type.upper()
    return HANA_TYPE_TO_CDS.get(hana_type_upper, 'String')


def parse_create_sql(create_sql_content):
    """Parsea un CREATE TABLE statement y extrae información de columnas"""
    columns_info = OrderedDict()
    
    # Extraer la sección de definición de columnas
    # Buscar desde el primer paréntesis hasta PRIMARY KEY
    match = re.search(r'CREATE\s+COLUMN\s+TABLE\s+[^(]+\((.+?)\)\s*(?:PRIMARY|UNLOAD|AUTO|MERGE|$)', create_sql_content, re.IGNORECASE | re.DOTALL)
    if not match:
        return columns_info
    
    columns_section = match.group(1)
    
    # Buscar cada definición de columna: "COLUMN_NAME" TYPE(LENGTH) [NOT NULL] [GENERATED ... AS IDENTITY] [DEFAULT value]
    # Patrón mejorado para capturar nombre, tipo y opciones
    column_pattern = r'["\']([A-Z_$][A-Z0-9_$]*?)["\']\s+([A-Z]+)(?:\(([^)]+)\))?'
    
    matches = list(re.finditer(column_pattern, columns_section, re.IGNORECASE))
    for match in matches:
        col_name = match.group(1).upper()
        col_type = match.group(2).upper()
        col_length = match.group(3) if match.group(3) else None
        
        # Filtrar palabras reservadas
        reserved_words = {'PRIMARY', 'KEY', 'INVERTED', 'VALUE', 'UNLOAD', 'PRIORITY', 'AUTO', 'MERGE'}
        if col_name in reserved_words:
            continue
        
        # Buscar información después de esta columna (hasta la siguiente columna o fin)
        start_pos = match.end()
        next_match_start = len(columns_section)
        if matches.index(match) < len(matches) - 1:
            next_match_start = matches[matches.index(match) + 1].start()
        
        remaining = columns_section[start_pos:next_match_start]
        
        # Verificar si tiene GENERATED ... AS IDENTITY (no debe tener default)
        has_identity = bool(re.search(r'\bGENERATED\s+(?:ALWAYS|BY\s+DEFAULT)\s+AS\s+IDENTITY\b', remaining, re.IGNORECASE))
        
        # Buscar NOT NULL
        is_not_null = bool(re.search(r'\bNOT\s+NULL\b', remaining, re.IGNORECASE))
        
        # Buscar DEFAULT (solo si no es IDENTITY)
        default_value = None
        if not has_identity:
            # Buscar DEFAULT seguido de un valor numérico o string
            # Patrón: DEFAULT seguido de número, string entre comillas, o palabra (pero no "AS")
            default_match = re.search(r'\bDEFAULT\s+([^\s,)]+)(?:\s+(?:NOT\s+NULL|AS))?', remaining, re.IGNORECASE)
            if default_match:
                default_val = default_match.group(1).strip()
                # Filtrar "AS" si está presente
                if default_val.upper() != 'AS':
                    # Remover comillas si las tiene
                    if default_val.startswith("'") and default_val.endswith("'"):
                        default_val = default_val[1:-1]
                    elif default_val.startswith('"') and default_val.endswith('"'):
                        default_val = default_val[1:-1]
                    default_value = default_val
        
        # Mapear tipo
        cds_type = map_hana_type_to_cds(col_type)
        
        columns_info[col_name] = {
            'type': cds_type,
            'not_null': is_not_null,
            'default': default_value,
            'is_identity': has_identity
        }
    
    return columns_info


def parse_table_xml(xml_content):
    """Parsea un archivo table.xml y extrae información adicional (claves primarias, NOT NULL)"""
    try:
        root = ET.fromstring(xml_content)
        
        # Nombre de la tabla
        name_elem = root.find('Name')
        table_name = name_elem.text if name_elem is not None else None
        
        # Claves primarias
        key_attrs = root.find('KeyAttrs')
        primary_keys = []
        if key_attrs is not None:
            for name_elem in key_attrs.findall('Name'):
                primary_keys.append(name_elem.text.upper())
        
        # Información adicional de columnas (NOT NULL principalmente)
        all_attrs = root.find('AllAttrs')
        column_constraints = {}
        
        if all_attrs is not None:
            for field in all_attrs.findall('Field'):
                name_elem = field.find('Name')
                if name_elem is None:
                    continue
                
                col_name = name_elem.text.upper()
                
                # Filtrar columnas del sistema
                if col_name.startswith('$'):
                    continue
                
                # Verificar NOT NULL (Constr)
                constr_elem = field.find('Constr')
                is_not_null = False
                if constr_elem is not None:
                    constr = int(constr_elem.text) if constr_elem.text else 0
                    # Constr 26 (0x1A) o cualquier valor con bit 1 (0x02) indica NOT NULL
                    # También si es parte de la clave primaria, es NOT NULL implícitamente
                    is_not_null = (constr & 2) != 0 or (constr == 26) or col_name in primary_keys
                
                column_constraints[col_name] = {
                    'not_null': is_not_null
                }
        
        return {
            'name': table_name,
            'primary_keys': primary_keys,
            'column_constraints': column_constraints
        }
    except Exception as e:
        print(f"  {Colors.YELLOW}⚠ Error parseando XML: {e}{Colors.NC}")
        return None


def generate_cds_entity(table_info):
    """Genera el código CDS para una entidad"""
    if not table_info or not table_info.get('columns'):
        return None
    
    lines = []
    lines.append(f"entity {table_info['name']} {{")
    
    # Agregar columnas
    for col_name, col_info in table_info['columns'].items():
        indent = "    "
        
        # Clave primaria
        if col_info['is_key']:
            indent += "key "
        
        # Nombre y tipo
        line = f"{indent}{col_name}: {col_info['type']}"
        
        # NOT NULL
        if col_info['not_null']:
            line += " not null"
        
        # DEFAULT
        if col_info['default'] is not None:
            default_val = col_info['default']
            line += f" default {default_val}"
        
        line += ";"
        lines.append(line)
    
    lines.append("}")
    
    return "\n".join(lines)


def check_files_exist(extract_dir, required_files):
    """Verifica si todos los archivos requeridos ya existen"""
    if not extract_dir.exists():
        return False
    
    missing_files = []
    for file_path in required_files:
        full_path = extract_dir / file_path
        if not full_path.exists():
            missing_files.append(file_path)
    
    return len(missing_files) == 0


def extract_files_from_tar(tar_path, extract_dir, schema_name):
    """Extrae todos los archivos necesarios del tar.gz (excepto CSV)"""
    # Primero, obtener lista de archivos requeridos
    required_files = []
    schema_path = f'index/{schema_name}/'
    try:
        with tarfile.open(tar_path, 'r:gz') as tar:
            for member in tar.getmembers():
                if schema_path in member.name:
                    if not member.name.endswith('/data.csv'):
                        required_files.append(member.name)
    except Exception as e:
        print(f"  {Colors.RED}✗ Error leyendo tar.gz: {e}{Colors.NC}")
        return []
    
    # Verificar si ya existen todos los archivos
    if check_files_exist(extract_dir, required_files):
        print(f"{Colors.BLUE}Archivos ya descomprimidos, usando existentes...{Colors.NC}")
        print(f"  {Colors.GREEN}✓ {len(required_files)} archivos encontrados{Colors.NC}\n")
        return required_files
    
    # Si no existen, extraer
    print(f"{Colors.BLUE}Extrayendo archivos del export.tar.gz...{Colors.NC}")
    extracted_files = []
    try:
        with tarfile.open(tar_path, 'r:gz') as tar:
            for member in tar.getmembers():
                if member.name in required_files:
                    # Extraer archivo
                    tar.extract(member, extract_dir)
                    extracted_files.append(member.name)
        print(f"  {Colors.GREEN}✓ Extraídos {len(extracted_files)} archivos{Colors.NC}\n")
        return extracted_files
    except Exception as e:
        print(f"  {Colors.RED}✗ Error extrayendo archivos: {e}{Colors.NC}")
        return []


def main():
    """Función principal"""
    import os
    # Directorio del script (schema_to_cap)
    script_dir = Path(__file__).parent
    # Directorio base (padre de schema_to_cap, donde está cap_project)
    base_dir = Path(os.environ.get('PROJECT_BASE_DIR', script_dir.parent))
    # Rutas configurables
    tar_filename = os.environ.get('EXPORT_TAR_FILE', 'export.tar.gz')
    cap_project_dir = os.environ.get('CAP_PROJECT_DIR', 'cap_project')
    # El proyecto CAP está al mismo nivel que schema_to_cap
    schema_file = base_dir / cap_project_dir / "db" / "schema.cds"
    # Los archivos temporales y el tar.gz están en schema_to_cap
    extract_dir = script_dir / os.environ.get('EXTRACT_DIR', 'temp_extract')
    tar_path = script_dir / tar_filename
    
    print(f"{Colors.YELLOW}=== Clonando estructura del export.tar.gz al proyecto CAP ==={Colors.NC}\n")
    
    # Validar archivos
    if not tar_path.exists():
        print(f"{Colors.RED}Error: No se encontró {tar_path}{Colors.NC}")
        return 1
    
    # Crear directorio de extracción
    extract_dir.mkdir(exist_ok=True)
    
    # Obtener nombre del schema (auto-detectado o configurado)
    schema_name = get_schema_name(tar_path=tar_path, extract_dir=extract_dir)
    if not schema_name:
        print(f"{Colors.RED}Error: No se pudo detectar el nombre del schema{Colors.NC}")
        print(f"{Colors.YELLOW}Configura SCHEMA en hana_config.conf o como variable de entorno{Colors.NC}")
        return 1
    
    print(f"{Colors.BLUE}Usando schema: {schema_name}{Colors.NC}\n")
    
    # Crear backup del schema.cds si existe
    if schema_file.exists():
        backup_file = schema_file.with_suffix('.cds.backup')
        print(f"{Colors.BLUE}Creando backup de schema.cds...{Colors.NC}")
        backup_file.write_text(schema_file.read_text(encoding='utf-8'), encoding='utf-8')
        print(f"  {Colors.GREEN}✓ Backup creado: {backup_file}{Colors.NC}\n")
    
    # Extraer archivos necesarios
    extracted_files = extract_files_from_tar(tar_path, extract_dir, schema_name)
    
    if not extracted_files:
        print(f"{Colors.RED}No se encontraron archivos para extraer{Colors.NC}")
        return 1
    
    # Obtener lista de tablas (desde los table.xml)
    table_xml_files = [f for f in extracted_files if f.endswith('/table.xml')]
    table_xml_files.sort()
    
    print(f"{Colors.BLUE}Procesando {len(table_xml_files)} tablas...{Colors.NC}\n")
    
    # Procesar cada tabla
    entities = []
    success_count = 0
    error_count = 0
    
    for idx, xml_file_path in enumerate(table_xml_files, 1):
        table_name = xml_file_path.split('/')[3]  # Extraer nombre de tabla
        print(f"{Colors.YELLOW}[{idx}/{len(table_xml_files)}] Procesando: {table_name}{Colors.NC}")
        
        try:
            # Leer table.xml y create.sql
            xml_file = extract_dir / xml_file_path
            create_sql_file = extract_dir / xml_file_path.replace('/table.xml', '/create.sql')
            
            if not xml_file.exists():
                print(f"  {Colors.YELLOW}⚠ No se encontró table.xml{Colors.NC}")
                error_count += 1
                continue
            
            if not create_sql_file.exists():
                print(f"  {Colors.YELLOW}⚠ No se encontró create.sql{Colors.NC}")
                error_count += 1
                continue
            
            xml_content = xml_file.read_text(encoding='utf-8', errors='ignore')
            create_sql_content = create_sql_file.read_text(encoding='utf-8', errors='ignore')
            
            # Parsear create.sql para obtener tipos de datos
            columns_from_sql = parse_create_sql(create_sql_content)
            
            # Parsear XML para obtener información adicional (claves primarias, NOT NULL)
            xml_info = parse_table_xml(xml_content)
            if not xml_info:
                print(f"  {Colors.YELLOW}⚠ No se pudo parsear table.xml{Colors.NC}")
                error_count += 1
                continue
            
            # Combinar información: tipos desde SQL, constraints desde XML
            combined_columns = OrderedDict()
            primary_keys = xml_info['primary_keys']
            
            for col_name, col_info in columns_from_sql.items():
                # Obtener constraints desde XML si existen
                xml_constraints = xml_info['column_constraints'].get(col_name, {})
                
                # Si es IDENTITY, no debe tener default (HANA lo maneja automáticamente)
                # Para otros campos, mapear el default si existe (especialmente DEFAULT 0)
                default_value = None
                if not col_info.get('is_identity', False):
                    default_value = col_info.get('default')
                
                combined_columns[col_name] = {
                    'type': col_info['type'],
                    'not_null': col_info['not_null'] or xml_constraints.get('not_null', False) or (col_name in primary_keys),
                    'default': default_value,
                    'is_key': col_name in primary_keys
                }
            
            # Crear table_info combinado
            table_info = {
                'name': xml_info['name'],
                'columns': combined_columns,
                'primary_keys': primary_keys
            }
            
            # Generar entidad CDS
            cds_entity = generate_cds_entity(table_info)
            if not cds_entity:
                print(f"  {Colors.YELLOW}⚠ No se pudo generar entidad CDS{Colors.NC}")
                error_count += 1
                continue
            
            entities.append(cds_entity)
            
            success_count += 1
            print(f"  {Colors.GREEN}✓ Procesada ({len(table_info['columns'])} columnas, {len(table_info['primary_keys'])} keys){Colors.NC}")
            
        except Exception as e:
            error_count += 1
            print(f"  {Colors.RED}✗ Error: {str(e)}{Colors.NC}")
    
    # Generar schema.cds completo
    print(f"\n{Colors.BLUE}Generando schema.cds...{Colors.NC}")
    
    schema_content = []
    schema_content.append("namespace db;")
    schema_content.append("")
    schema_content.append("using {cuid} from '@sap/cds/common';")
    schema_content.append("")
    
    # Agregar todas las entidades
    for entity in entities:
        schema_content.append(entity)
        schema_content.append("")
    
    # Escribir archivo
    schema_file.parent.mkdir(parents=True, exist_ok=True)
    schema_file.write_text("\n".join(schema_content), encoding='utf-8')
    
    print(f"  {Colors.GREEN}✓ Schema generado: {schema_file}{Colors.NC}")
    
    # Resumen
    print()
    print(f"{Colors.YELLOW}=== Resumen ==={Colors.NC}")
    print(f"Total de tablas: {len(table_xml_files)}")
    print(f"{Colors.GREEN}Exitosas: {success_count}{Colors.NC}")
    print(f"{Colors.RED}Con errores: {error_count}{Colors.NC}")
    print(f"Entidades generadas: {len(entities)}")
    print()
    print(f"Archivos descomprimidos en: {extract_dir}")
    print(f"Schema generado en: {schema_file}")
    
    if error_count == 0:
        print(f"\n{Colors.GREEN}✓ Estructura clonada correctamente{Colors.NC}")
        return 0
    else:
        print(f"\n{Colors.YELLOW}⚠ Estructura clonada con algunos errores{Colors.NC}")
        return 1


if __name__ == "__main__":
    exit(main())
