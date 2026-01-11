#!/usr/bin/env python3
"""
Módulo para monitorear el progreso de ejecución SQL
"""

import sys
import threading
from typing import Optional
from hana_client import HanaClient


def format_progress(current_count: int, initial_count: int, total_inserts: int) -> str:
    """
    Formatea el mensaje de progreso
    
    Args:
        current_count: Conteo actual de registros
        initial_count: Conteo inicial de registros
        total_inserts: Total de INSERT statements a ejecutar
    
    Returns:
        str: Mensaje de progreso formateado
    """
    if total_inserts == 0:
        return f"  Progreso: {current_count:,} registros en tabla"
    
    inserted = current_count - initial_count
    percent = min(100, (inserted / total_inserts * 100)) if total_inserts > 0 else 0
    return f"  Progreso: {inserted:,}/{total_inserts:,} insertados ({percent:.1f}%)"


class ProgressMonitor:
    """Monitor de progreso para ejecución SQL"""
    
    def __init__(self, client: HanaClient, schema: str, table_name: str, 
                 initial_count: int, total_inserts: int):
        """
        Inicializa el monitor de progreso
        
        Args:
            client: Cliente HANA
            schema: Nombre del schema
            table_name: Nombre de la tabla
            initial_count: Conteo inicial de registros
            total_inserts: Total de INSERT statements a ejecutar
        """
        self.client = client
        self.schema = schema
        self.table_name = table_name
        self.initial_count = initial_count
        self.total_inserts = total_inserts
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.last_count = initial_count
    
    def start(self):
        """Inicia el monitoreo en un thread separado"""
        if self.table_name:
            self.thread = threading.Thread(
                target=self._monitor_loop,
                daemon=True
            )
            self.thread.start()
            # Mostrar progreso inicial
            initial_progress = format_progress(
                self.initial_count, 
                self.initial_count, 
                self.total_inserts
            )
            sys.stdout.write(initial_progress)
            sys.stdout.flush()
    
    def stop(self):
        """Detiene el monitoreo y muestra el progreso final"""
        if self.thread:
            self.stop_event.set()
            self.thread.join(timeout=2)
            
            # Mostrar progreso final
            try:
                final_count = self.client.count_table_records(
                    self.schema, 
                    self.table_name
                )
                if final_count is not None:
                    progress = format_progress(
                        final_count, 
                        self.initial_count, 
                        self.total_inserts
                    )
                    sys.stdout.write(f"\r{progress}\n")
                else:
                    sys.stdout.write("\n")
            except Exception:
                sys.stdout.write("\n")
            finally:
                sys.stdout.flush()
    
    def _monitor_loop(self):
        """Loop de monitoreo que se ejecuta en el thread"""
        update_interval = 0.5  # Actualizar cada medio segundo
        
        while not self.stop_event.is_set():
            try:
                current_count = self.client.count_table_records(
                    self.schema, 
                    self.table_name
                )
                if current_count is not None and current_count != self.last_count:
                    progress = format_progress(
                        current_count, 
                        self.initial_count, 
                        self.total_inserts
                    )
                    sys.stdout.write(f"\r{progress}")
                    sys.stdout.flush()
                    self.last_count = current_count
                
                if self.stop_event.wait(timeout=update_interval):
                    break
            except Exception:
                # Si hay error, continuar intentando
                if self.stop_event.wait(timeout=1):
                    break
