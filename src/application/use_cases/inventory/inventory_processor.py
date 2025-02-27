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
        Procesa datos de inventario con validación mejorada
        
        Args:
            df: DataFrame con datos de inventario
            
        Returns:
            Lista de InventoryMatch con información procesada y validada
        """
        try:
            results = []
            
            for _, row in df.iterrows():
                try:
                    # Extraer y validar datos básicos
                    part_number = str(row.get('Part Number', '')).strip().upper()
                    org = str(row.get('Organization', '')).strip().zfill(2)
                    
                    # Crear aging info
                    aging_info = InventoryAgingInfo()
                    aging_data = {
                        'Aging 0-30 Quantity': row.get('Aging_0_30'),
                        'Aging 31-60 Quantity': row.get('Aging_31_60'),
                        'Aging 61-90 Quantity': row.get('Aging_61_90')
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
                    match.update_from_raw_data(row.to_dict())
                    results.append(match)
                    
                except Exception as e:
                    self.logger.error(f"Error processing row: {str(e)}")
                    results.append(self.create_empty_record(
                        part=str(row.get('Part Number', 'UNKNOWN')),
                        org=str(row.get('Organization', '00')),
                        error=str(e)
                    ))
                    
            return results
            
        except Exception as e:
            self.logger.error(f"Error in process_inventory_data: {str(e)}")
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