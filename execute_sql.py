#!/usr/bin/env python3
"""
Script para ejecutar archivos SQL en SAP HANA usando hdbsql
Uso: python3 execute_sql.py [archivo.sql]
Si no se especifica archivo, ejecuta todos los archivos .sql del directorio
"""

import os
import sys
import time
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from hana_connection import load_config, get_config_value, Colors
from hana_client import HanaClient, HanaClientError
from sql_parser import get_table_name_from_sql, count_insert_statements, prepare_sql_content
from sql_progress import ProgressMonitor


def execute_sql_file(sql_file_path: Path, log_dir: Path, client: HanaClient) -> Dict[str, Any]:
    """
    Ejecuta un archivo SQL y retorna el resultado
    
    Args:
        sql_file_path: Ruta al archivo SQL
        log_dir: Directorio para guardar logs
        client: Cliente HANA
    
    Returns:
        dict: Resultado de la ejecución con keys: success, executed, total, etc.
    """
    filename = sql_file_path.name
    error_log_path = log_dir / f"{filename}.err"
    output_log_path = log_dir / f"{filename}.out"
    
    try:
        # Leer contenido del archivo SQL
        with open(sql_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Obtener schema del cliente
        schema = client.get_schema()
        
        # Contar INSERT statements y obtener nombre de tabla para progreso
        total_inserts = count_insert_statements(content)
        table_schema, table_name = get_table_name_from_sql(content, schema)
        
        # Usar schema del cliente si no se encontró en el SQL
        if not table_schema and schema:
            table_schema = schema
        
        # Contar registros antes de insertar
        records_before = None
        if table_name and table_schema:
            records_before = client.count_table_records(table_schema, table_name)
            if records_before is not None:
                print(f"  {Colors.BLUE}Registros antes: {records_before:,}{Colors.NC}")
                if total_inserts > 0:
                    print(f"  {Colors.BLUE}INSERT statements a ejecutar: {total_inserts:,}{Colors.NC}")
        
        # Preparar contenido SQL (reemplazar referencias DB_* con schema completo)
        prepared_content = prepare_sql_content(content, schema)
        
        # Crear archivo temporal con el contenido preparado
        temp_sql = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8')
        temp_sql.write(prepared_content)
        temp_sql.flush()
        temp_sql.close()
        sql_file_to_use = Path(temp_sql.name)
        
        # Iniciar monitoreo de progreso
        progress_monitor = None
        if table_name and table_schema and records_before is not None:
            print(f"  {Colors.BLUE}Ejecutando INSERT statements...{Colors.NC}")
            progress_monitor = ProgressMonitor(
                client, 
                table_schema, 
                table_name, 
                records_before, 
                total_inserts
            )
            progress_monitor.start()
        
        # Ejecutar archivo SQL
        try:
            returncode, stdout, stderr = client.execute_sql_file(sql_file_to_use)
        finally:
            # Detener monitoreo de progreso
            if progress_monitor:
                progress_monitor.stop()
            
            # Limpiar archivo temporal
            try:
                if sql_file_to_use.exists():
                    sql_file_to_use.unlink()
            except Exception:
                pass
        
        # Contar registros después de insertar
        records_after = None
        if table_name and table_schema and records_before is not None:
            records_after = client.count_table_records(table_schema, table_name)
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
            if stdout:
                out_file.write(stdout)
            # Agregar información de conteo
            if records_before is not None and records_after is not None:
                out_file.write(f"\n--- Estadísticas de inserción ---\n")
                out_file.write(f"Registros antes: {records_before:,}\n")
                out_file.write(f"Registros después: {records_after:,}\n")
                out_file.write(f"Registros insertados: {records_after - records_before:,}\n")
                out_file.write(f"INSERT statements en archivo: {total_inserts:,}\n")
        
        # Verificar si hay errores de constraint única (datos duplicados)
        stderr_lower = stderr.lower() if stderr else ''
        unique_constraint_count = stderr_lower.count('unique constraint violated')
        
        if returncode == 0:
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
                if stderr:
                    err_file.write("\n--- Detalles de errores ---\n")
                    err_file.write(stderr)
            return {
                'success': True,
                'executed': 1,
                'total': 1,
                'warning': f'{unique_constraint_count} registros duplicados fueron omitidos'
            }
        else:
            with open(error_log_path, 'w', encoding='utf-8') as err_file:
                if stderr:
                    err_file.write(stderr)
                if stdout:
                    err_file.write('\n--- STDOUT ---\n')
                    err_file.write(stdout)
            return {
                'success': False,
                'error': f'hdbsql error (código: {returncode})'
            }
    
    except HanaClientError as e:
        with open(error_log_path, 'w', encoding='utf-8') as err_file:
            err_file.write(f"Error del cliente HANA: {str(e)}\n")
        return {'success': False, 'error': str(e)}
    except Exception as e:
        with open(error_log_path, 'w', encoding='utf-8') as err_file:
            err_file.write(f"Error inesperado: {str(e)}\n")
        return {'success': False, 'error': f'Error ejecutando SQL: {str(e)}'}


def move_to_created(file_path: Path, script_dir: Path, config: Dict[str, str]) -> Path:
    """
    Mueve un archivo a la carpeta created/
    
    Args:
        file_path: Archivo a mover
        script_dir: Directorio del script
        config: Configuración para obtener CREATED_DIR
    
    Returns:
        Path: Ruta del archivo movido
    """
    created_dir_name = get_config_value(config, 'CREATED_DIR', 'created')
    created_dir = script_dir / created_dir_name
    created_dir.mkdir(exist_ok=True)
    
    dest_path = created_dir / file_path.name
    file_path.rename(dest_path)
    return dest_path


def main():
    """Función principal"""
    # Directorio del script (schema_to_cap)
    script_dir = Path(__file__).parent
    
    # Cargar configuración
    config = load_config(require_config=True, show_messages=True)
    
    # Directorio de archivos SQL (configurable, dentro de schema_to_cap)
    sql_dir_name = get_config_value(config, 'SQL_DIR', 'data_insert_sql')
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
    print(f"Servidor: {config['HANA_HOST']}:{config['HANA_PORT']}")
    print(f"Base de datos: {config['HANA_DATABASE']}")
    print(f"Usuario: {config['HANA_USER']}")
    print()
    
    # Crear cliente HANA
    try:
        client = HanaClient(config)
        print(f"{Colors.GREEN}✓ Cliente HANA inicializado: {client.hdbsql_path}{Colors.NC}")
        
        # Probar conexión
        print(f"{Colors.BLUE}Probando conexión con HANA...{Colors.NC}")
        if not client.test_connection():
            print(f"{Colors.RED}Error: No se pudo conectar con HANA{Colors.NC}")
            print(f"{Colors.YELLOW}Verifica tus credenciales en hana_config.conf{Colors.NC}")
            sys.exit(1)
        print(f"{Colors.GREEN}✓ Conexión exitosa{Colors.NC}\n")
        
    except HanaClientError as e:
        print(f"{Colors.RED}Error: {str(e)}{Colors.NC}")
        print(f"\n{Colors.YELLOW}El cliente HANA es requerido para ejecutar los scripts SQL.{Colors.NC}")
        print(f"\n{Colors.BLUE}Opciones:{Colors.NC}")
        print(f"  1. Agregar hdbsql al PATH del sistema")
        print(f"  2. Configurar HANA_CLIENT_PATH en hana_config.conf apuntando al binario hdbsql")
        print(f"  3. Instalar el cliente HANA en una ubicación común")
        sys.exit(1)
    except Exception as e:
        print(f"{Colors.RED}Error inesperado: {str(e)}{Colors.NC}")
        sys.exit(1)
    
    # Directorios de logs y created (configurables, dentro de schema_to_cap)
    log_dir_name = get_config_value(config, 'LOG_DIR', 'logs')
    created_dir_name = get_config_value(config, 'CREATED_DIR', 'created')
    log_dir = script_dir / log_dir_name
    log_dir.mkdir(exist_ok=True)
    
    error_log = log_dir / "errors.log"
    success_log = log_dir / "success.log"
    execution_log = log_dir / "execution.log"
    
    # Limpiar logs anteriores
    for log_file in [error_log, success_log, execution_log]:
        if log_file.exists():
            log_file.write_text("")
    
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
        result = execute_sql_file(sql_file, log_dir, client)
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
                    moved_to = move_to_created(sql_file, script_dir, config)
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
