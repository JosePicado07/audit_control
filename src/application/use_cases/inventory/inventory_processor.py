from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
import os
from typing import Dict, Optional, List
from datetime import datetime
import pandas as pd
import logging
from dataclasses import dataclass

from domain.entities.inventory_entity import InventoryMatch, InventoryAgingInfo
from domain.criteria.inventory_criteria import InventoryMatchCriteria

class InventoryProcessor:
    """Handles detailed inventory data processing with enhanced validation"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._inventory_cache = {}
        self.match_criteria = InventoryMatchCriteria()

    @staticmethod 
    def _extract_aging_info(inventory_df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        """
        Extrae y analiza la información de aging del inventario.
        Mantiene la funcionalidad original para compatibilidad.
        """
        aging_info = {}
        aging_patterns = [
            'AGING 0-30', 'AGING 31-60', 'AGING 61-90',
            'AGING 91-120', 'AGING 121-150', 'AGING 151-180',
            'AGING 181-365', 'AGING OVER 365'
        ]
        
        total_quantity = 0
        total_value = 0
        
        for pattern in aging_patterns:
            quantity_col = f"{pattern} QUANTITY"
            value_col = f"{pattern} VALUE"
            
            try:
                if quantity_col in inventory_df.columns and value_col in inventory_df.columns:
                    quantity = float(inventory_df[quantity_col].sum() or 0)
                    value = float(inventory_df[value_col].sum() or 0)
                    
                    if quantity > 0 or value > 0:
                        period = pattern.replace('AGING ', '')
                        aging_info[period] = {
                            'quantity': quantity,
                            'value': value
                        }
                        
                        total_quantity += quantity
                        total_value += value
        
            except Exception as e:
                logging.warning(f"Error processing aging period {pattern}: {str(e)}")
        
        if aging_info:
            aging_info['total'] = {
                'quantity': total_quantity,
                'value': total_value
            }
        
        return aging_info

    def process_inventory_data(self, df: pd.DataFrame) -> List[InventoryMatch]:
        """
        Procesa datos de inventario con validación mejorada utilizando operaciones vectorizadas
        
        Args:
            df: DataFrame con datos de inventario
            
        Returns:
            Lista de InventoryMatch con información procesada y validada
        """
        try:
            
            # Crear una copia para evitar modificar el DataFrame original
            df = df.copy()
            
            # Preprocesar columnas críticas vectorialmente - mucho más rápido que hacerlo fila por fila
            if 'Part Number' in df.columns:
                df['part_number_norm'] = df['Part Number'].astype(str).str.strip().str.upper()
            else:
                df['part_number_norm'] = 'UNKNOWN'
                
            if 'Organization' in df.columns:
                df['org_norm'] = df['Organization'].astype(str).str.strip().str.zfill(2)
            else:
                df['org_norm'] = '00'
            
            # Función para procesar un lote de filas
            def process_batch(batch_df):
                batch_results = []
                
                # Crear diccionarios una vez por lote para menor overhead
                batch_dicts = batch_df.to_dict('records')
                
                for record in batch_dicts:
                    try:
                        # Usar valores normalizados previamente
                        part_number = record.get('part_number_norm', 'UNKNOWN')
                        org = record.get('org_norm', '00')
                        
                        # Verificar caché primero si está disponible
                        cache_key = f"{part_number}_{org}"
                        if cache_key in self._inventory_cache:
                            batch_results.append(self._inventory_cache[cache_key])
                            continue
                        
                        # Crear aging info (solo una vez por registro)
                        aging_info = InventoryAgingInfo()
                        aging_data = {
                            'Aging 0-30 Quantity': record.get('Aging_0_30'),
                            'Aging 31-60 Quantity': record.get('Aging_31_60'),
                            'Aging 61-90 Quantity': record.get('Aging_61_90')
                        }
                        aging_info.update_from_aging_values(aging_data)
                        
                        # Crear match con validación
                        match = InventoryMatch(
                            part_number=part_number,
                            organization=org,
                            has_inventory=False,  # Se actualizará en update_from_raw_data
                            match_criteria=self.match_criteria,
                            aging_info=aging_info
                        )
                        
                        # Actualizar con datos completos
                        match.update_from_raw_data(record)
                        
                        # Guardar en caché para reutilización
                        self._inventory_cache[cache_key] = match
                        
                        batch_results.append(match)
                        
                    except Exception as e:
                        self.logger.error(f"Error processing record: {str(e)}")
                        batch_results.append(self.create_empty_record(
                            part=str(record.get('Part Number', 'UNKNOWN')),
                            org=str(record.get('Organization', '00')),
                            error=str(e)
                        ))
                
                return batch_results
            
            # Determinar tamaño de lote y número de workers óptimos
            total_rows = len(df)
            cpu_count = os.cpu_count() or 4
            workers = min(cpu_count, 8)  # Limitar a 8 threads máximo
            batch_size = max(100, total_rows // (workers * 4))  # Ajustado para equilibrar carga y overhead
            
            # Logging inicial
            self.logger.info(f"Procesando {total_rows} registros de inventario con {workers} workers")
            self.logger.info(f"Tamaño de lote: {batch_size} registros")
            
            results = []
            
            if total_rows < 1000:
                # Para conjuntos pequeños, procesamiento secuencial es más eficiente
                results = process_batch(df)
            else:
                # Para conjuntos grandes, procesamiento paralelo por lotes
                batches = [df.iloc[i:i+batch_size] for i in range(0, total_rows, batch_size)]
                batch_count = len(batches)
                
                self.logger.info(f"Procesando en {batch_count} lotes")
                
                # Procesamiento paralelo de lotes
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    # Enviar lotes para procesamiento
                    futures = {executor.submit(process_batch, batch): i for i, batch in enumerate(batches)}
                    
                    # Recolectar resultados a medida que se completan
                    for i, future in enumerate(as_completed(futures)):
                        batch_idx = futures[future]
                        try:
                            batch_results = future.result()
                            results.extend(batch_results)
                            
                            # Logging de progreso cada 5 lotes o en el último lote
                            if (batch_idx % 5 == 0 or batch_idx == batch_count - 1):
                                self.logger.info(f"Completados {batch_idx + 1}/{batch_count} lotes ({len(results)} registros)")
                                
                            # Liberar memoria periódicamente
                            if i % 10 == 0:
                                gc.collect()
                                
                        except Exception as e:
                            self.logger.error(f"Error en lote {batch_idx}: {str(e)}")
                            # Continuar con los demás lotes a pesar del error
            
            # Limitar tamaño del caché para evitar problemas de memoria
            if len(self._inventory_cache) > 10000:
                # Conservar solo las entradas más recientes
                cache_items = list(self._inventory_cache.items())
                self._inventory_cache = dict(cache_items[-10000:])
            
            self.logger.info(f"Procesamiento completado: {len(results)} registros generados")
            return results
            
        except Exception as e:
            self.logger.error(f"Error en process_inventory_data: {str(e)}")
            return []

    @staticmethod
    def create_empty_record(part: str, org: str, error: Optional[str] = None) -> Dict:
        """Creates an empty inventory record - mantiene compatibilidad"""
        return {
            'part_number': part,
            'organization': org,
            'quantity': 0,
            'value': 0.0,
            'has_stock': False,
            'subinventory': '',
            'warehouse_code': '',
            'serial_numbers': [],
            'description': '',
            'aging_info': {},
            'last_updated': datetime.now().isoformat(),
            'status': 'no_inventory' if not error else 'error',
            'error': error if error else None
        }

    @staticmethod
    def advanced_part_match(inventory_part: str, search_part: str) -> bool:
        """Performs advanced part number matching - mantiene funcionalidad original"""
        inv_part = str(inventory_part).upper().strip()
        search_part = str(search_part).upper().strip()
        
        inv_segments = inv_part.split('.')
        search_segments = search_part.split('.')
        
        if inv_segments[0] == search_segments[0]:
            return True
        
        inv_clean = inv_part.replace('=', '').replace('-', '')
        search_clean = search_part.replace('=', '').replace('-', '')
        
        return (
            search_segments[0] in inv_part or 
            inv_segments[0] in search_part or
            search_clean in inv_clean or
            inv_clean in search_clean
        )

    def generate_inventory_summary(self, matches: List[InventoryMatch]) -> Dict:
        """
        Genera resumen detallado del inventario procesado
        
        Args:
            matches: Lista de InventoryMatch procesados
            
        Returns:
            Dict con estadísticas detalladas
        """
        summary = {
            'total_items': len(matches),
            'items_with_stock': len([m for m in matches if m.has_inventory]),
            'total_quantity': sum(m.quantity for m in matches),
            'total_value': sum(m.value for m in matches),
            'aging_summary': {
                '0-30': sum(m.aging_info.days_0_30 for m in matches),
                '31-60': sum(m.aging_info.days_31_60 for m in matches),
                '61-90': sum(m.aging_info.days_61_90 for m in matches),
                '91+': sum(m.aging_info.days_91_plus for m in matches)
            }
        }
        
        self.logger.info("\n=== INVENTORY SUMMARY ===")
        self.logger.info(f"Total items: {summary['total_items']}")
        self.logger.info(f"Items with stock: {summary['items_with_stock']}")
        self.logger.info(f"Total quantity: {summary['total_quantity']:,.2f}")
        
        return summary