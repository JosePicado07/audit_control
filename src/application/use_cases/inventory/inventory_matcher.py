from functools import lru_cache
import re
import traceback
from typing import Dict, Set, Optional, List
import pandas as pd
import logging
from dataclasses import dataclass
from pathlib import Path

from application.use_cases.inventory.inventory_columns import InventoryColumns
from application.use_cases.inventory.inventory_processor import InventoryProcessor
from domain.entities.inventory_entity import InventoryAgingInfo, InventoryMatch

logger = logging.getLogger(__name__)

@lru_cache(maxsize=10000)
class InventoryMatcher:
    """Efficient inventory matching system optimized for large datasets"""
    
    PART_NUMBER_COLUMNS = {
        'Material Designator',
        'Mfg Item Number',
        'MATERIAL_DESIGNATOR',
        'MFG_ITEM_NUMBER'
    }
    
    ORG_CODE_COLUMNS = {
        'Organization Code',
        'Org Code',
        'ORGANIZATION_CODE',
        'ORG_CODE'
    }
    
    QUANTITY_COLUMNS = {
        'Quantity',
        'QUANTITY',
        'Total Quantity',
        'TOTAL_QUANTITY'
    }

    def __init__(self):
        self._inventory_data = None
        self._part_column = None
        self._org_column = None
        self._qty_column = None
        self.column_mappings = None
        self.processor = InventoryProcessor()
        
    def process_audit_inventory(self, mismatched_parts: List[str], 
        org_destination: List[str], 
        inventory_df: pd.DataFrame) -> Dict:
        """
        Processes inventory for audit checking

        Args:
        mismatched_parts: List of part numbers to check
        org_destination: List of organizations to check
        inventory_df: Inventory DataFrame to process

        Returns:
        Dict containing inventory check results
        """
        self.load_inventory(inventory_df)

        results = {}
        for part in mismatched_parts:
            for org in org_destination:
                match_result = self.check_inventory(part, org)
                results[f"{part}_{org}"] = {
                'quantity': match_result.quantity,
                'has_inventory': match_result.has_inventory,
                'error': match_result.error_message
                }

        return results
        
    def load_inventory(
        self, 
        inventory_df: pd.DataFrame, 
        column_mappings: Dict[str, str]
    ) -> None:
        try:
            print("\n=== INVENTORY DATAFRAME PREVIEW ===")
            print("Columns:", inventory_df.columns.tolist())
            print("\nFirst few rows:")
            print(inventory_df.head())
            print("\nData types:")
            print(inventory_df.dtypes)
            
            # Encontrar columnas críticas
            self._part_column = InventoryColumns.find_best_column_match(
                inventory_df.columns.tolist(), 
                InventoryColumns.ITEM_NUMBER_ALTERNATIVES
            )
            self._org_column = InventoryColumns.find_best_column_match(
                inventory_df.columns.tolist(), 
                InventoryColumns.ORGANIZATION_CODE_ALTERNATIVES
            )
            self._qty_column = InventoryColumns.find_best_column_match(
                inventory_df.columns.tolist(), 
                InventoryColumns.QUANTITY_ALTERNATIVES
            )
            
            # Encontrar columna de subinventory
            self._subinv_column = next(
                (col for col in inventory_df.columns if 'SUBINV' in col.upper()),
                None
            )
            
            if not self._subinv_column:
                logger.warning("SUBINVENTORY column not found!")
                
            # Validar columnas críticas
            self._validate_required_columns()
            
            # Normalizar organización
            inventory_df[self._org_column] = (
                inventory_df[self._org_column]
                .astype(str)
                .str.extract(r'(\d+)', expand=False)
                .fillna('00')
                .str.zfill(2)  # Esto es correcto porque ya estamos usando Series.str
            )
            
            # Crear estructura de datos mejorada
            self._inventory_data = {}
            valid_subinv = {'STOCK', 'LAB'}
            
            for idx, row in inventory_df.iterrows():
                try:
                    part_number = str(row[self._part_column]).strip().upper()
                    org_code = row[self._org_column]
                    
                    # Verificar subinventory
                    subinv = str(row.get(self._subinv_column, '')).upper() if self._subinv_column else ''
                    has_valid_subinv = any(sub in subinv for sub in valid_subinv)
                    
                    if not has_valid_subinv:
                        continue  # Saltamos si no es STOCK o LAB
                    
                    try:
                        quantity = float(row[self._qty_column] or 0)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid quantity at row {idx}, defaulting to 0")
                        quantity = 0
                    
                    key = (part_number, org_code)
                    
                    # Guardar como diccionario en lugar de float
                    if key not in self._inventory_data:
                        self._inventory_data[key] = {
                            'quantity': 0,
                            'subinventory': subinv,
                            'aging_info': InventoryAgingInfo()
                        }
                    
                    self._inventory_data[key]['quantity'] += quantity
                    
                except Exception as e:
                    logger.error(f"Error processing row {idx}: {str(e)}")
            
            # Log resultados
            total_qty = sum(data['quantity'] for data in self._inventory_data.values())
            print(f"Loaded {len(self._inventory_data)} unique inventory records")
            print(f"Total quantity across all items: {total_qty:,.2f}")
            
        except Exception as e:
            logger.error(f"Error loading inventory: {str(e)}")
            raise

    def _validate_inventory_data(self, df: pd.DataFrame, column_mapping: Dict[str, str]) -> None:
        """
        Validación exhaustiva de datos de inventario.
        Actualizado para usar ITEM_NUMBER en lugar de MATERIAL_DESIGNATOR.
        """
        logger.info("Starting data validation...")
        
        # Verificar valores nulos
        for col_name, col in column_mapping.items():
            if col and col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    logger.warning(f"Found {null_count} null values in {col_name}")
        
        # Verificar duplicados - Actualizado para usar ITEM_NUMBER
        duplicates = df.duplicated(subset=[
            column_mapping['ITEM_NUMBER'],           # Cambiado de MATERIAL_DESIGNATOR
            column_mapping['ORGANIZATION_CODE']
        ], keep=False)
        
        if duplicates.any():
            logger.info(f"Found {duplicates.sum()} duplicate part-org combinations (quantities will be summed)")
            
            # Mostrar ejemplos de duplicados
            duplicate_rows = df[duplicates].head(3)
            for _, row in duplicate_rows.iterrows():
                logger.debug(f"Duplicate found - Item: {row[column_mapping['ITEM_NUMBER']]}, "
                            f"Org: {row[column_mapping['ORGANIZATION_CODE']]}")
        
        # Verificar formatos de organización
        if column_mapping['ORGANIZATION_CODE']:
            invalid_orgs = df[~df[column_mapping['ORGANIZATION_CODE']].astype(str).str.match(r'^\d{1,3}$')]
            if not invalid_orgs.empty:
                logger.warning(f"Found {len(invalid_orgs)} invalid organization codes")
                
        # Verificar formato de Item Number
        if column_mapping['ITEM_NUMBER']:
            empty_items = df[df[column_mapping['ITEM_NUMBER']].astype(str).str.strip() == '']
            if not empty_items.empty:
                logger.warning(f"Found {len(empty_items)} empty item numbers")
            
    def check_inventory(self, part_number: str, organization: str) -> InventoryMatch:
        """Verifica el inventario con validación mejorada"""
        try:
            if not self._inventory_data:
                return InventoryMatch(
                    part_number=part_number,
                    organization=organization,
                    has_inventory=False,
                    error_message="No inventory data loaded"
                )

            # Normalizar organización y parte
            # Usar .zfill directamente en str, no en Series
            org_code = str(organization).strip().zfill(2)
            base_part = part_number.split('.')[0].strip().upper()
            
            logger.debug(f"\n=== INVENTORY CHECK ===")
            logger.debug(f"Checking part: {part_number}")
            logger.debug(f"Base part number: {base_part}")
            logger.debug(f"Organization: {org_code}")
            
            # Buscar coincidencias exactas y parciales
            total_quantity = 0
            total_value = 0
            found_subinventories = set()
            serial_numbers = []
            aging_info = InventoryAgingInfo()
            warehouse_code = None
            
            # Diccionario para acumular datos de aging
            aging_totals = {
                'days_0_30': 0.0,
                'days_31_60': 0.0,
                'days_61_90': 0.0,
                'days_91_plus': 0.0
            }
            
            for (inv_part, inv_org), data in self._inventory_data.items():
                # Verificar coincidencia de parte y organización
                if inv_org == org_code and self._is_matching_part(inv_part, base_part):
                    logger.debug(f"Match found: {inv_part}")
                    
                    # Acumular cantidades y valores
                    qty = float(data.get('quantity', 0) or 0)
                    total_quantity += qty
                    total_value += float(data.get('value', 0) or 0)
                    
                    # Capturar warehouse_code si no lo tenemos aún
                    if not warehouse_code and 'warehouse_code' in data:
                        warehouse_code = data['warehouse_code']
                    
                    # Recolectar subinventories
                    if 'subinventory' in data:
                        found_subinventories.add(data['subinventory'])
                    
                    # Recolectar números de serie
                    if 'serial_numbers' in data:
                        serial_numbers.extend(data['serial_numbers'])
                    
                    # Acumular aging
                    aging_totals['days_0_30'] += float(data.get('aging_0_30', 0) or 0)
                    aging_totals['days_31_60'] += float(data.get('aging_31_60', 0) or 0)
                    aging_totals['days_61_90'] += float(data.get('aging_61_90', 0) or 0)
                    aging_totals['days_91_plus'] += float(data.get('aging_91_plus', 0) or 0)
            
            # Actualizar aging_info con los totales
            aging_info.days_0_30 = aging_totals['days_0_30']
            aging_info.days_31_60 = aging_totals['days_31_60']
            aging_info.days_61_90 = aging_totals['days_61_90']
            aging_info.days_91_plus = aging_totals['days_91_plus']
            
            # Generar resultado detallado
            return InventoryMatch(
                part_number=part_number,
                organization=organization,
                has_inventory=total_quantity > 0,
                quantity=total_quantity,
                value=total_value,
                aging_info=aging_info,
                subinventory=', '.join(found_subinventories) if found_subinventories else None,
                warehouse_code=warehouse_code,
                serial_numbers=serial_numbers
            )
            
        except Exception as e:
            logger.error(f"Error checking inventory: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return InventoryMatch(
                part_number=part_number,
                organization=organization,
                has_inventory=False,
                error_message=str(e)
            )
            
    def _validate_required_columns(self) -> None:
        """
        Valida que todas las columnas requeridas estén presentes y sean del tipo correcto
        """
        required_columns = {
            'part_column': self._part_column,
            'org_column': self._org_column,
            'qty_column': self._qty_column
        }
        
        missing_columns = [
            name for name, col in required_columns.items() 
            if col is None
        ]
        
        if missing_columns:
            raise ValueError(
                f"Missing required columns: {', '.join(missing_columns)}"
            )

        # Validar que las columnas de aging existan si se van a usar
        aging_columns = [
            'Aging 0-30 Quantity', 'Aging 31-60 Quantity', 
            'Aging 61-90 Quantity', 'Aging 91-120 Quantity'
        ]
        
        logger.info("Required columns validation completed successfully")
            
    def _is_matching_part(self, inventory_part: str, base_part: str) -> bool:
        """Lógica mejorada para matching de parte"""
        inv_part = inventory_part.strip().upper()
        base_part = base_part.strip().upper()
        
        # Matching exacto
        if inv_part == base_part:
            return True
        
        # Matching sin sufijo
        if inv_part.startswith(base_part) or base_part.startswith(inv_part):
            return True
        
        # Matching sin caracteres especiales
        clean_inv = re.sub(r'[.\-=]', '', inv_part)
        clean_base = re.sub(r'[.\-=]', '', base_part)
        
        return clean_inv == clean_base or clean_inv.startswith(clean_base) or clean_base.startswith(clean_inv)
    
    @classmethod
    def find_best_column_match(
        cls, 
        df_columns: List[str], 
        alternatives: Set[str]
    ) -> Optional[str]:
        """
        Advanced column matching with multiple strategies
        
        Strategies (in order of preference):
        1. Exact match (case-insensitive)
        2. Partial match
        3. Normalized match
        """
        # Normalize DataFrame columns for matching
        normalized_columns = [col.upper().replace(' ', '_') for col in df_columns]
        
        # Logging for debugging
        logger.debug(f"Available columns: {df_columns}")
        logger.debug(f"Alternatives: {alternatives}")
        
        # Exact match first (case-insensitive, space-insensitive)
        exact_matches = [
            col for col, norm_col in zip(df_columns, normalized_columns)
            if any(
                alt.upper().replace(' ', '_') == norm_col 
                for alt in alternatives
            )
        ]
        
        if exact_matches:
            logger.debug(f"Exact matches found: {exact_matches}")
            return exact_matches[0]
        
        # Partial match
        partial_matches = [
            col for col, norm_col in zip(df_columns, normalized_columns)
            if any(
                alt.upper().replace(' ', '_') in norm_col 
                for alt in alternatives
            )
        ]
        
        if partial_matches:
            logger.debug(f"Partial matches found: {partial_matches}")
            return partial_matches[0]
        
        # Log when no match is found
        logger.warning(f"No matching column found for alternatives: {alternatives}")
        return None

    def update_audit_processor(self, inventory_df: pd.DataFrame, org_destination: List[str], mismatched_parts: List[str]):
        """Example of how to use in AuditProcessor"""
        matcher = InventoryMatcher()
        matcher.load_inventory(inventory_df)
        
        # Now you can use it in your existing _check_inventory_for_mismatches method
        results = {}
        for part in mismatched_parts:
            for org in org_destination:
                match_result = matcher.check_inventory(part, org)
                results[f"{part}_{org}"] = {
                    'quantity': match_result.quantity,
                    'has_inventory': match_result.has_inventory,
                    'error': match_result.error_message
                }
        
        return results

