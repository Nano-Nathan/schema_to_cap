#!/usr/bin/env python3
"""
Script para ejecutar archivos SQL en SAP HANA usando hdbcli
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

try:
    from hdbcli import dbapi
except ImportError:
    print("Error: hdbcli no está instalado.")
    print("Instalándolo automáticamente...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "hdbcli", "--quiet"])
    from hdbcli import dbapi


class Colors:
    """Colores para output en terminal"""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color


def load_config():
    """Carga la configuración desde hana_config.conf o variables de entorno"""
    import os
    config = {}
    
    # Intentar cargar desde archivo de configuración en schema_to_cap
    script_dir = Path(__file__).parent
    config_file = script_dir / "hana_config.conf"
    
    if config_file.exists():
        print(f"{Colors.BLUE}Usando configuración desde hana_config.conf{Colors.NC}")
    else:
        # Intentar desde variables de entorno
        print(f"{Colors.BLUE}Intentando cargar desde variables de entorno...{Colors.NC}")
        env_vars = ['HANA_HOST', 'HANA_PORT', 'HANA_DATABASE', 'HANA_USER', 'HANA_PASSWORD']
        for var in env_vars:
            value = os.environ.get(var)
            if value:
                config[var] = value
        
        if len(config) == len(env_vars):
            return config
        else:
            print(f"{Colors.RED}Error: No se encontró el archivo hana_config.conf ni variables de entorno{Colors.NC}")
            print("Por favor, crea el archivo de configuración primero.")
            sys.exit(1)
    
    with open(config_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip().strip('"').strip("'")
    
    required_keys = ['HANA_HOST', 'HANA_PORT', 'HANA_DATABASE', 'HANA_USER', 'HANA_PASSWORD']
    for key in required_keys:
        if key not in config:
            print(f"{Colors.RED}Error: Falta la configuración {key} en {config_file}{Colors.NC}")
            sys.exit(1)
    
    # Agregar configuraciones opcionales con valores por defecto
    if 'SQL_TIMEOUT' not in config:
        config['SQL_TIMEOUT'] = os.environ.get('SQL_TIMEOUT', None)  # None = sin timeout
    
    return config


def connect_to_hana(config):
    """Establece conexión con SAP HANA usando hdbsql o hdbcli"""
    # Verificar si hdbsql está disponible
    import shutil
    hdbsql_path = shutil.which('hdbsql')
    
    if hdbsql_path:
        # Usar hdbsql si está disponible (más confiable para HANA Cloud)
        return None  # Retornar None indica que usaremos hdbsql directamente
    
    # Si no hay hdbsql, usar hdbcli
    try:
        port = int(config['HANA_PORT'])
        
        # Para SAP HANA Cloud con puerto 443, usar SSL con validación
        if port == 443:
            # Extraer schema del usuario si está disponible (formato: SCHEMA_USER)
            user = config['HANA_USER']
            schema = None
            if '_' in user:
                # El schema es la parte antes del último guión bajo
                parts = user.rsplit('_', 1)
                if len(parts) == 2:
                    schema = parts[0]
            
            # Intentar con validación de certificado primero (recomendado para producción)
            try:
                conn_params = {
                    'address': config['HANA_HOST'],
                    'port': port,
                    'user': user,
                    'password': config['HANA_PASSWORD'],
                    'encrypt': True,
                    'sslValidateCertificate': True,
                    'sslHostNameInCertificate': config['HANA_HOST']
                }
                
                # Agregar databaseName solo si está especificado y no es vacío
                if 'HANA_DATABASE' in config and config['HANA_DATABASE']:
                    conn_params['databaseName'] = config['HANA_DATABASE']
                
                conn = dbapi.connect(**conn_params)
                
                # Si tenemos schema, establecerlo después de conectar
                if schema:
                    try:
                        cursor = conn.cursor()
                        cursor.execute(f'SET SCHEMA "{schema}"')
                        cursor.close()
                    except:
                        pass  # Si falla, continuar sin schema
                
                return conn
            except Exception as ssl_error:
                # Si falla con validación, intentar sin validación (solo para desarrollo)
                print(f"{Colors.YELLOW}Advertencia: Falló con validación SSL, intentando sin validación...{Colors.NC}")
                conn_params = {
                    'address': config['HANA_HOST'],
                    'port': port,
                    'user': user,
                    'password': config['HANA_PASSWORD'],
                    'encrypt': True,
                    'sslValidateCertificate': False,
                    'sslHostNameInCertificate': config['HANA_HOST']
                }
                
                if 'HANA_DATABASE' in config and config['HANA_DATABASE']:
                    conn_params['databaseName'] = config['HANA_DATABASE']
                
                conn = dbapi.connect(**conn_params)
                
                if schema:
                    try:
                        cursor = conn.cursor()
                        cursor.execute(f'SET SCHEMA "{schema}"')
                        cursor.close()
                    except:
                        pass
                
                return conn
        else:
            # Para conexiones normales (puerto 30015)
            conn = dbapi.connect(
                address=config['HANA_HOST'],
                port=port,
                databaseName=config.get('HANA_DATABASE', ''),
                user=config['HANA_USER'],
                password=config['HANA_PASSWORD']
            )
            return conn
    except Exception as e:
        error_msg = str(e)
        print(f"{Colors.RED}Error al conectar con SAP HANA: {error_msg}{Colors.NC}")
        print(f"\n{Colors.YELLOW}Posibles causas:{Colors.NC}")
        print("  1. Problema de conectividad de red/firewall")
        print("  2. Credenciales incorrectas")
        print("  3. Servidor no accesible desde este entorno")
        print("  4. Configuración SSL incorrecta")
        print(f"\n{Colors.YELLOW}Verifica:{Colors.NC}")
        print(f"  - Host: {config['HANA_HOST']}")
        print(f"  - Puerto: {config['HANA_PORT']}")
        print(f"  - Base de datos: {config['HANA_DATABASE']}")
        print(f"  - Usuario: {config['HANA_USER']}")
        print(f"\n{Colors.BLUE}Nota: El script está listo, pero necesita conectividad al servidor HANA.{Colors.NC}")
        sys.exit(1)


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


def show_progress_bar(current, total, bar_length=40):
    """Muestra una barra de progreso simple"""
    if total == 0:
        return ''
    percent = min(100, (current / total) * 100)
    filled = int(bar_length * current / total)
    bar = '█' * filled + '░' * (bar_length - filled)
    return f"  [{bar}] {current:,}/{total:,} ({percent:.1f}%)"


def monitor_progress(hdbsql_path, config, schema, table_name, initial_count, total_inserts, stop_event):
    """Monitorea el progreso de inserción en un thread separado"""
    last_inserted = 0
    while not stop_event.is_set():
        time.sleep(1)  # Verificar cada segundo
        current_count = count_table_records(hdbsql_path, config, schema, table_name)
        if current_count is not None:
            inserted = current_count - initial_count
            if inserted != last_inserted and inserted >= 0:
                progress = show_progress_bar(inserted, total_inserts)
                print(f"\r{progress}", end='', flush=True)
                last_inserted = inserted
        time.sleep(0.5)  # Esperar medio segundo más


def execute_sql_file(conn, sql_file_path, log_dir, config=None):
    """Ejecuta un archivo SQL y retorna el resultado"""
    import subprocess
    import shutil
    import tempfile
    import os
    
    filename = os.path.basename(sql_file_path)
    error_log_path = log_dir / f"{filename}.err"
    output_log_path = log_dir / f"{filename}.out"
    
    # Verificar si hdbsql está disponible
    # Primero buscar en el PATH estándar
    hdbsql_path = shutil.which('hdbsql')
    
    # Si no está en PATH, buscar en la instalación persistente
    if not hdbsql_path:
        persistent_hdbsql = Path(__file__).parent / "client" / "hana_client" / "hdbsql"
        if persistent_hdbsql.exists() and persistent_hdbsql.is_file():
            hdbsql_path = str(persistent_hdbsql)
    
    if hdbsql_path and config:
        # Usar hdbsql (más confiable para HANA Cloud)
        # Lógica idéntica al script temporal que funciona
        try:
            import re
            
            # Extraer schema del usuario
            # El formato es: SCHEMA_XXXXX_RT, necesitamos solo SCHEMA
            user = config['HANA_USER']
            schema = None
            if '_' in user:
                # Dividir por _ y tomar todo excepto los últimos 2 segmentos
                parts = user.split('_')
                if len(parts) >= 3:
                    # Formato: SCHEMA_XXXXX_RT -> tomar SCHEMA
                    schema = '_'.join(parts[:-2])
                elif len(parts) == 2:
                    # Formato: SCHEMA_XXXXX -> tomar SCHEMA
                    schema = parts[0]
                else:
                    schema = parts[0]
            
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
            if table_name and records_before is not None and total_inserts > 0:
                print(f"  {Colors.BLUE}Ejecutando INSERT statements...{Colors.NC}")
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
                print()  # Nueva línea después de la barra de progreso
            
            # Contar registros después de insertar
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
    
    # Usar hdbcli (fallback)
    if conn is None:
        return {'success': False, 'error': 'No hay conexión disponible'}
    
    cursor = conn.cursor()
    
    try:
        # Leer el contenido del archivo SQL
        with open(sql_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            sql_content = f.read()
        
        if not sql_content.strip():
            return {'success': False, 'error': 'Archivo vacío', 'skipped': True}
        
        # Dividir en statements individuales (separados por ;)
        statements = [s.strip() for s in sql_content.split(';') 
                     if s.strip() and not s.strip().startswith('--')]
        
        if not statements:
            return {'success': False, 'error': 'No se encontraron statements SQL válidos', 'skipped': True}
        
        executed_count = 0
        errors = []
        output_lines = []
        
        for idx, statement in enumerate(statements, 1):
            try:
                cursor.execute(statement)
                executed_count += 1
                try:
                    results = cursor.fetchall()
                    if results:
                        output_lines.append(f"Statement {idx}: {len(results)} filas afectadas")
                except:
                    output_lines.append(f"Statement {idx}: Ejecutado correctamente")
            except Exception as e:
                error_msg = f"Error en statement {idx}: {str(e)}"
                errors.append(error_msg)
                with open(error_log_path, 'a', encoding='utf-8') as err_file:
                    err_file.write(f"{error_msg}\nStatement: {statement[:200]}...\n\n")
        
        conn.commit()
        
        with open(output_log_path, 'w', encoding='utf-8') as out_file:
            out_file.write('\n'.join(output_lines))
        
        if errors:
            return {
                'success': False,
                'error': f'{len(errors)} errores de {len(statements)} statements',
                'executed': executed_count,
                'total': len(statements)
            }
        
        return {
            'success': True,
            'executed': executed_count,
            'total': len(statements)
        }
        
    except Exception as e:
        with open(error_log_path, 'w', encoding='utf-8') as err_file:
            err_file.write(f"Error fatal: {str(e)}\n")
        return {'success': False, 'error': str(e)}
    finally:
        cursor.close()


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
    import shutil
    hdbsql_path = shutil.which('hdbsql')
    
    # Si no está en PATH, buscar en la instalación persistente (configurable, dentro de schema_to_cap)
    if not hdbsql_path:
        client_dir = os.environ.get('HANA_CLIENT_DIR', str(script_dir / "client" / "hana_client"))
        persistent_hdbsql = Path(client_dir) / "hdbsql"
        if persistent_hdbsql.exists() and persistent_hdbsql.is_file():
            hdbsql_path = str(persistent_hdbsql)
    
    # Intentar conectar con hdbcli solo si hdbsql no está disponible (fallback)
    conn = None
    if not hdbsql_path:
        print(f"{Colors.BLUE}Conectando a SAP HANA (hdbcli)...{Colors.NC}")
        conn = connect_to_hana(config)
        if conn:
            print(f"{Colors.GREEN}✓ Conexión establecida (hdbcli){Colors.NC}\n")
        else:
            print(f"{Colors.RED}Error: No se pudo establecer conexión{Colors.NC}\n")
            sys.exit(1)
    else:
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
        result = execute_sql_file(conn, sql_file, log_dir, config)
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
    
    # Cerrar conexión si existe
    if conn:
        conn.close()
    
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
