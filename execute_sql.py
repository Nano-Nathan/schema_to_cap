#!/usr/bin/env python3
"""
Script para ejecutar archivos SQL en SAP HANA usando hdbsql
Uso: python3 execute_sql.py [archivo.sql]
Si no se especifica archivo, ejecuta todos los archivos .sql del directorio
"""

import os
import sys
import time
import glob
import threading
from datetime import datetime
from pathlib import Path
from hana_connection import load_config, extract_schema_from_user, find_hdbsql_path

class Colors:
    """Colores para output en terminal"""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color


def get_table_name_from_sql(content, schema):
    """Extrae el nombre de la tabla del primer INSERT statement"""
    import re
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


def count_table_records(hdbsql_path, config, schema, table_name):
    """Cuenta los registros en una tabla usando hdbsql"""
    if not table_name:
        return None
    try:
        import subprocess
        host_port = f"{config['HANA_HOST']}:{config['HANA_PORT']}"
        query = f'SELECT COUNT(*) FROM "{schema}"."{table_name}";'
        cmd = [
            hdbsql_path,
            '-n', host_port,
            '-u', config['HANA_USER'],
            '-p', config['HANA_PASSWORD'],
            '-attemptencrypt',
            '-quiet'
        ]
        # Timeout para queries de conteo (más corto)
        result = subprocess.run(cmd, input=query, capture_output=True, text=True, timeout=60)
        if result.returncode == 0 and result.stdout:
            # Parsear el resultado: "COUNT(*)\n12345\n1 row selected"
            lines = [l.strip() for l in result.stdout.strip().split('\n') if l.strip() and not l.strip().startswith('COUNT')]
            if lines:
                try:
                    return int(lines[0])
                except:
                    pass
    except:
        pass
    return None


def count_insert_statements(content):
    """Cuenta cuántos INSERT statements hay en el contenido"""
    import re
    # Contar líneas que contienen INSERT INTO (no comentarios)
    lines = content.split('\n')
    count = 0
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith('--') and re.search(r'INSERT\s+INTO', stripped, re.IGNORECASE):
            count += 1
    return count


def show_progress(current_count, initial_count, total_inserts):
    """Muestra el progreso de forma clara"""
    if total_inserts == 0:
        return f"  Progreso: {current_count:,} registros en tabla"
    
    inserted = current_count - initial_count
    percent = min(100, (inserted / total_inserts * 100)) if total_inserts > 0 else 0
    return f"  Progreso: {inserted:,}/{total_inserts:,} insertados ({percent:.1f}%)"


def monitor_progress(hdbsql_path, config, schema, table_name, initial_count, total_inserts, stop_event):
    """Monitorea el progreso de inserción en un thread separado"""
    last_count = initial_count
    update_interval = 0.5  # Actualizar cada medio segundo
    
    while not stop_event.is_set():
        try:
            current_count = count_table_records(hdbsql_path, config, schema, table_name)
            if current_count is not None:
                # Actualizar siempre que cambie el conteo
                if current_count != last_count:
                    progress = show_progress(current_count, initial_count, total_inserts)
                    # Actualizar la línea de progreso en la parte inferior
                    sys.stdout.write(f"\r{progress}")
                    sys.stdout.flush()
                    last_count = current_count
            # Esperar antes de la siguiente verificación
            if stop_event.wait(timeout=update_interval):
                break
        except Exception:
            # Si hay error al contar, continuar intentando
            if stop_event.wait(timeout=1):
                break
    
    # Mostrar progreso final (última actualización)
    try:
        final_count = count_table_records(hdbsql_path, config, schema, table_name)
        if final_count is not None:
            progress = show_progress(final_count, initial_count, total_inserts)
            sys.stdout.write(f"\r{progress}\n")
            sys.stdout.flush()
        else:
            # Si no se puede obtener el conteo final, al menos limpiar la línea
            sys.stdout.write("\n")
            sys.stdout.flush()
    except Exception:
        # Si hay error, limpiar la línea
        sys.stdout.write("\n")
        sys.stdout.flush()


def execute_sql_file(sql_file_path, log_dir, config=None):
    """Ejecuta un archivo SQL y retorna el resultado"""
    import subprocess
    import tempfile
    import os
    
    filename = os.path.basename(sql_file_path)
    error_log_path = log_dir / f"{filename}.err"
    output_log_path = log_dir / f"{filename}.out"
    
    # Encontrar hdbsql
    hdbsql_path = find_hdbsql_path(config)
    
    if hdbsql_path and config:
        # Usar hdbsql (más confiable para HANA Cloud)
        # Lógica idéntica al script temporal que funciona
        try:
            import re
            
            # Extraer schema del usuario
            schema = extract_schema_from_user(config['HANA_USER'])
            
            # Leer contenido del archivo SQL original
            with open(sql_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Contar INSERT statements y obtener nombre de tabla para progreso
            total_inserts = count_insert_statements(content)
            table_schema, table_name = get_table_name_from_sql(content, schema)
            
            # Contar registros antes de insertar
            records_before = None
            if table_name and hdbsql_path:
                records_before = count_table_records(hdbsql_path, config, schema, table_name)
                if records_before is not None:
                    print(f"  {Colors.BLUE}Registros antes: {records_before:,}{Colors.NC}")
                    if total_inserts > 0:
                        print(f"  {Colors.BLUE}INSERT statements a ejecutar: {total_inserts:,}{Colors.NC}")
            
            # Si tenemos schema, reemplazar referencias a tablas DB_* con schema completo
            if schema:
                content = re.sub(
                    r'(INSERT\s+INTO)\s+(DB_\w+)',
                    rf'\1 "{schema}"."\2"',
                    content,
                    flags=re.IGNORECASE
                )
            
            # Crear archivo temporal
            temp_sql = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8')
            temp_sql.write(content)
            temp_sql.flush()
            temp_sql.close()
            sql_file_to_use = temp_sql.name
            
            # Construir comando hdbsql
            host_port = f"{config['HANA_HOST']}:{config['HANA_PORT']}"
            cmd = [
                hdbsql_path,
                '-n', host_port,
                '-u', config['HANA_USER'],
                '-p', config['HANA_PASSWORD'],
                '-attemptencrypt',
                '-I', sql_file_to_use,
                '-quiet'
            ]
            
            # Ejecutar y capturar tanto stdout como stderr
            # Iniciar monitoreo de progreso en thread separado
            stop_event = threading.Event()
            progress_thread = None
            if table_name and records_before is not None:
                print(f"  {Colors.BLUE}Ejecutando INSERT statements...{Colors.NC}")
                # Mostrar progreso inicial en nueva línea (parte inferior)
                initial_progress = show_progress(records_before, records_before, total_inserts)
                sys.stdout.write(initial_progress)
                sys.stdout.flush()
                if total_inserts > 0:
                    progress_thread = threading.Thread(
                        target=monitor_progress,
                        args=(hdbsql_path, config, schema, table_name, records_before, total_inserts, stop_event),
                        daemon=True
                    )
                    progress_thread.start()
            else:
                print(f"  {Colors.BLUE}Ejecutando INSERT statements...{Colors.NC}")
            
            # Obtener timeout desde configuración o usar None (sin timeout)
            timeout_seconds = config.get('SQL_TIMEOUT')
            if timeout_seconds:
                try:
                    timeout_seconds = int(timeout_seconds)
                except:
                    timeout_seconds = None
            else:
                timeout_seconds = None  # Sin timeout por defecto
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds)
            
            # Detener el monitoreo de progreso
            if progress_thread:
                stop_event.set()
                progress_thread.join(timeout=2)
                # Limpiar la línea de progreso si el thread no lo hizo
                sys.stdout.write("\n")
                sys.stdout.flush()
            
            # Contar registros después de insertar (siempre, incluso si hubo errores)
            records_after = None
            if table_name and hdbsql_path and records_before is not None:
                records_after = count_table_records(hdbsql_path, config, schema, table_name)
                if records_after is not None:
                    inserted = records_after - records_before
                    print(f"  {Colors.BLUE}Registros después: {records_after:,}{Colors.NC}")
                    if inserted > 0:
                        print(f"  {Colors.GREEN}✓ Registros insertados: {inserted:,}{Colors.NC}")
                    elif inserted < 0:
                        print(f"  {Colors.YELLOW}⚠ Diferencia: {inserted:,} (posibles duplicados eliminados){Colors.NC}")
                    elif inserted == 0 and total_inserts > 0:
                        print(f"  {Colors.YELLOW}⚠ No se insertaron nuevos registros (posiblemente ya existían){Colors.NC}")
            
            # Guardar output
            with open(output_log_path, 'w', encoding='utf-8') as out_file:
                if result.stdout:
                    out_file.write(result.stdout)
                # Agregar información de conteo
                if records_before is not None and records_after is not None:
                    out_file.write(f"\n--- Estadísticas de inserción ---\n")
                    out_file.write(f"Registros antes: {records_before:,}\n")
                    out_file.write(f"Registros después: {records_after:,}\n")
                    out_file.write(f"Registros insertados: {records_after - records_before:,}\n")
                    out_file.write(f"INSERT statements en archivo: {total_inserts:,}\n")
            
            # Limpiar archivo temporal
            try:
                if os.path.exists(sql_file_to_use):
                    os.unlink(sql_file_to_use)
            except:
                pass
            
            # Verificar si hay errores de constraint única (datos duplicados)
            stderr_lower = result.stderr.lower() if result.stderr else ''
            
            unique_constraint_count = stderr_lower.count('unique constraint violated')
            
            if result.returncode == 0:
                result_dict = {
                    'success': True,
                    'executed': 1,
                    'total': 1
                }
                if records_before is not None and records_after is not None:
                    result_dict['records_before'] = records_before
                    result_dict['records_after'] = records_after
                    result_dict['records_inserted'] = records_after - records_before
                return result_dict
            elif unique_constraint_count > 0:
                # Errores de constraint única son aceptables (datos duplicados)
                with open(error_log_path, 'w', encoding='utf-8') as err_file:
                    err_file.write(f"Advertencia: {unique_constraint_count} registros ya existían (unique constraint)\n")
                    err_file.write("El script se ejecutó correctamente, pero algunos datos eran duplicados.\n")
                    if result.stderr:
                        err_file.write("\n--- Detalles de errores ---\n")
                        err_file.write(result.stderr)
                return {
                    'success': True,
                    'executed': 1,
                    'total': 1,
                    'warning': f'{unique_constraint_count} registros duplicados fueron omitidos'
                }
            else:
                with open(error_log_path, 'w', encoding='utf-8') as err_file:
                    if result.stderr:
                        err_file.write(result.stderr)
                    if result.stdout:
                        err_file.write('\n--- STDOUT ---\n')
                        err_file.write(result.stdout)
                return {
                    'success': False,
                    'error': f'hdbsql error (código: {result.returncode})'
                }
        except subprocess.TimeoutExpired:
            return {'success': False, 'error': 'Timeout ejecutando hdbsql'}
        except Exception as e:
            return {'success': False, 'error': f'Error ejecutando hdbsql: {str(e)}'}
    
    # Si no hay hdbsql, retornar error
    return {'success': False, 'error': 'hdbsql no está disponible'}


def move_to_created(file_path, script_dir):
    """Mueve un archivo a la carpeta created/"""
    import os
    created_dir_name = os.environ.get('CREATED_DIR', 'created')
    created_dir = script_dir / created_dir_name
    created_dir.mkdir(exist_ok=True)
    
    dest_path = created_dir / file_path.name
    file_path.rename(dest_path)
    return dest_path


def main():
    """Función principal"""
    import os
    # Directorio del script (schema_to_cap)
    script_dir = Path(__file__).parent
    # Directorio base (padre de schema_to_cap)
    base_dir = Path(os.environ.get('PROJECT_BASE_DIR', script_dir.parent))
    # Directorio de archivos SQL (configurable, dentro de schema_to_cap)
    sql_dir_name = os.environ.get('SQL_DIR', 'data_insert_sql')
    sql_dir = script_dir / sql_dir_name
    
    if not sql_dir.exists():
        print(f"{Colors.RED}Error: El directorio {sql_dir} no existe{Colors.NC}")
        sys.exit(1)
    
    if len(sys.argv) > 1:
        # Ejecutar archivo específico
        sql_file = Path(sys.argv[1])
        if not sql_file.is_absolute():
            sql_file = sql_dir / sql_file
        
        if not sql_file.exists():
            print(f"{Colors.RED}Error: El archivo {sql_file} no existe{Colors.NC}")
            sys.exit(1)
        
        sql_files = [sql_file]
        single_file = True
    else:
        # Ejecutar todos los archivos SQL en data_insert_sql/
        sql_files = sorted([f for f in sql_dir.glob("*.sql")])
        single_file = False
    
    if not sql_files:
        print(f"{Colors.RED}No se encontraron archivos SQL para ejecutar en {sql_dir}{Colors.NC}")
        sys.exit(1)
    
    print(f"{Colors.YELLOW}=== Iniciando ejecución de scripts SQL en SAP HANA ==={Colors.NC}")
    
    # Cargar configuración
    config = load_config()
    print(f"Servidor: {config['HANA_HOST']}:{config['HANA_PORT']}")
    print(f"Base de datos: {config['HANA_DATABASE']}")
    print(f"Usuario: {config['HANA_USER']}")
    print()
    
    # Directorios de logs y created (configurables, dentro de schema_to_cap)
    log_dir_name = os.environ.get('LOG_DIR', 'logs')
    created_dir_name = os.environ.get('CREATED_DIR', 'created')
    log_dir = script_dir / log_dir_name
    log_dir.mkdir(exist_ok=True)
    
    error_log = log_dir / "errors.log"
    success_log = log_dir / "success.log"
    execution_log = log_dir / "execution.log"
    
    # Limpiar logs anteriores
    for log_file in [error_log, success_log, execution_log]:
        if log_file.exists():
            log_file.write_text("")
    
    # Verificar si usamos hdbsql primero (preferido para HANA Cloud)
    hdbsql_path = find_hdbsql_path(config)
    
    # Si no se encuentra hdbsql, mostrar error claro
    if not hdbsql_path:
        print(f"{Colors.RED}Error: No se encontró el cliente HANA (hdbsql){Colors.NC}")
        print(f"\n{Colors.YELLOW}El cliente HANA es requerido para ejecutar los scripts SQL.{Colors.NC}")
        print(f"\n{Colors.BLUE}Opciones:{Colors.NC}")
        print(f"  1. Agregar hdbsql al PATH del sistema")
        print(f"  2. Configurar HANA_CLIENT_PATH en hana_config.conf apuntando al binario hdbsql")
        sys.exit(1)
    
    print(f"{Colors.GREEN}✓ Usando hdbsql para ejecución: {hdbsql_path}{Colors.NC}\n")
    
    
    # Contadores
    total_files = len(sql_files)
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    # Procesar cada archivo
    for idx, sql_file in enumerate(sql_files, 1):
        filename = sql_file.name
        print(f"{Colors.YELLOW}[{idx}/{total_files}] Procesando: {filename}{Colors.NC}")
        
        start_time = time.time()
        result = execute_sql_file(sql_file, log_dir, config)
        duration = int(time.time() - start_time)
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if result.get('skipped'):
            skipped_count += 1
            print(f"  {Colors.YELLOW}⚠ Omitido: {result.get('error', '')}{Colors.NC}")
            with open(execution_log, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] SKIPPED: {filename} - {result.get('error', '')}\n")
        elif result['success']:
            success_count += 1
            stats = f"({result.get('executed', 0)} statements)"
            if 'records_inserted' in result:
                stats += f" - {result['records_inserted']:,} registros insertados"
            print(f"  {Colors.GREEN}✓ Éxito {stats} ({duration}s){Colors.NC}")
            with open(success_log, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] SUCCESS: {filename} - {stats} - {duration}s\n")
            
            # Si es un solo archivo y fue exitoso, moverlo a created/
            if single_file:
                try:
                    moved_to = move_to_created(sql_file, script_dir)
                    print(f"  {Colors.GREEN}✓ Movido a: {moved_to}{Colors.NC}")
                except Exception as e:
                    print(f"  {Colors.YELLOW}⚠ No se pudo mover a created/: {e}{Colors.NC}")
        else:
            error_count += 1
            print(f"  {Colors.RED}✗ Error: {result.get('error', '')} ({duration}s){Colors.NC}")
            with open(error_log, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] ERROR: {filename} - {result.get('error', '')} - {duration}s\n")
        
        with open(execution_log, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] {filename} - {result.get('error', 'SUCCESS')} - {duration}s\n")
    
    # Resumen final
    print()
    print(f"{Colors.YELLOW}=== Resumen de ejecución ==={Colors.NC}")
    print(f"Total de archivos: {total_files}")
    print(f"{Colors.GREEN}Exitosos: {success_count}{Colors.NC}")
    print(f"{Colors.RED}Con errores: {error_count}{Colors.NC}")
    print(f"{Colors.YELLOW}Omitidos: {skipped_count}{Colors.NC}")
    print()
    print(f"Logs guardados en: {log_dir}/")
    print(f"  - errors.log: Archivos con errores")
    print(f"  - success.log: Archivos ejecutados correctamente")
    print(f"  - execution.log: Log completo de ejecución")
    
    if error_count > 0:
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
