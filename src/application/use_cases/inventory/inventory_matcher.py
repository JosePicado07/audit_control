from functools import lru_cache
import re
import time
import traceback
from typing import Dict, Set, Optional, List
import pandas as pd
import polars as pl
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
        column_mappings: Dict[str, str] = None
    ) -> None:
        try:
            print("\n=== INVENTORY DATAFRAME PREVIEW ===")
            print("Columns:", inventory_df.columns.tolist())
            print("\nFirst few rows:")
            print(inventory_df.head())
            print("\nData types:")
            print(inventory_df.dtypes)
            
            # Ampliar los subinventarios válidos
            valid_subinv = {'STOCK', 'LAB', 'NON-CON', 'L2PROD', 'RETURN', 'SHIPPING', 'WIP', 'WIPSUPPLY'}
            print(f"Valid subinventories: {valid_subinv}")
            
            # Encontrar columnas críticas con mejor logging
            self._part_column = InventoryColumns.find_best_column_match(
                inventory_df.columns.tolist(), 
                InventoryColumns.ITEM_NUMBER_ALTERNATIVES
            )
            print(f"Part column identified: {self._part_column}")
            
            self._org_column = InventoryColumns.find_best_column_match(
                inventory_df.columns.tolist(), 
                InventoryColumns.ORGANIZATION_CODE_ALTERNATIVES
            )
            print(f"Organization column identified: {self._org_column}")
            
            self._qty_column = InventoryColumns.find_best_column_match(
                inventory_df.columns.tolist(), 
                InventoryColumns.QUANTITY_ALTERNATIVES
            )
            print(f"Quantity column identified: {self._qty_column}")
            
            # Encontrar columna de subinventory con mejor logging
            self._subinv_column = next(
                (col for col in inventory_df.columns if 'SUBINV' in col.upper()),
                None
            )
            print(f"Subinventory column identified: {self._subinv_column}")
            
            if not self._subinv_column:
                logger.warning("SUBINVENTORY column not found!")
                # Búsqueda alternativa con términos relacionados
                alternative_subinv_terms = ['SUB_INV', 'SUB INV', 'SUBINVENTORY', 'SUB-INV']
                for term in alternative_subinv_terms:
                    for col in inventory_df.columns:
                        if term in col.upper():
                            self._subinv_column = col
                            print(f"Found alternative subinventory column: {col}")
                            break
                    if self._subinv_column:
                        break
                
            # Validar columnas críticas
            self._validate_required_columns()
            
            # Normalizar organización con mejor manejo de excepciones
            try:
                inventory_df[self._org_column] = (
                    inventory_df[self._org_column]
                    .astype(str)
                    .str.extract(r'(\d+)', expand=False)
                    .fillna('00')
                    .str.zfill(2)
                )
            except Exception as e:
                logger.error(f"Error normalizing organization column: {str(e)}")
                # Fallback: normalización más simple
                inventory_df[self._org_column] = inventory_df[self._org_column].astype(str).apply(
                    lambda x: str(x).strip().zfill(2) if x and str(x).strip().isdigit() else '00'
                )
            
            # Crear estructura de datos mejorada
            self._inventory_data = {}
            rows_processed = 0
            rows_skipped = 0
            rows_with_quantity = 0
            
            for idx, row in inventory_df.iterrows():
                try:
                    rows_processed += 1
                    if rows_processed % 5000 == 0:
                        print(f"Processed {rows_processed} rows...")
                    
                    part_number = str(row[self._part_column]).strip().upper()
                    org_code = row[self._org_column]
                    
                    # Verificar subinventory con diagnóstico mejorado
                    subinv = str(row.get(self._subinv_column, '')).upper() if self._subinv_column else ''
                    has_valid_subinv = any(sub in subinv for sub in valid_subinv)
                    
                    if not has_valid_subinv:
                        rows_skipped += 1
                        if rows_skipped <= 5:  # Limitar el logging excesivo
                            print(f"Skipping row {idx}, subinventory '{subinv}' not in valid list")
                        continue  # Saltamos si no es STOCK o LAB
                    
                    try:
                        quantity = float(row[self._qty_column] or 0)
                        if quantity > 0:
                            rows_with_quantity += 1
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
                    logger.error(f"Row content: {row}")
            
            # Log resultados con estadísticas detalladas
            total_qty = sum(data['quantity'] for data in self._inventory_data.values())
            print(f"Loaded {len(self._inventory_data)} unique inventory records")
            print(f"Total quantity across all items: {total_qty:,.2f}")
            print(f"Total rows processed: {rows_processed}")
            print(f"Rows skipped (invalid subinventory): {rows_skipped}")
            print(f"Rows with positive quantity: {rows_with_quantity}")
            
            # Ejemplos de las primeras entradas para depuración
            print("\n=== SAMPLE INVENTORY ENTRIES ===")
            for i, ((part, org), data) in enumerate(list(self._inventory_data.items())[:5]):
                print(f"Entry {i+1}: Part={part}, Org={org}, Quantity={data['quantity']}, Subinv={data['subinventory']}")
            
        except Exception as e:
            logger.error(f"Error loading inventory: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
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
            org_code = str(organization).strip().zfill(2)
            part_norm = str(part_number).strip().upper()
            base_part = part_norm.split('.')[0].strip().upper()
            
            # Clave para búsqueda exacta
            exact_key = (part_norm, org_code)
            
            # 1. VERIFICACIÓN RÁPIDA para coincidencia exacta
            if exact_key in self._inventory_data:
                # Coincidencia exacta encontrada - retornar inmediatamente
                data = self._inventory_data[exact_key]
                return self._create_inventory_match_from_data(
                    part_number, organization, data, [part_norm]
                )
            
            # 2. BÚSQUEDA para coincidencias parciales
            # Ya que no tenemos _part_index, buscamos entre todas las claves
            matching_parts = []
            
            # Verificar todas las claves en _inventory_data
            for inv_part, inv_org in self._inventory_data.keys():
                # Solo procesar registros que coincidan con la organización
                if inv_org == org_code and self._is_matching_part(inv_part, part_norm):
                    matching_parts.append(inv_part)
                    
            # Si no hay coincidencias, retornar resultado vacío
            if not matching_parts:
                return InventoryMatch(
                    part_number=part_number,
                    organization=organization,
                    has_inventory=False,
                    quantity=0,
                    aging_info=InventoryAgingInfo()
                )
                
            # 3. AGREGACIÓN de datos para todas las partes coincidentes
            total_quantity = 0
            total_value = 0
            found_subinventories = set()
            serial_numbers = []
            warehouse_code = None
            
            # Acumuladores de aging
            aging_totals = {
                'days_0_30': 0.0,
                'days_31_60': 0.0,
                'days_61_90': 0.0,
                'days_91_plus': 0.0
            }
            
            # Agregar datos de todas las coincidencias
            for matching_part in matching_parts:
                key = (matching_part, org_code)
                if key in self._inventory_data:
                    data = self._inventory_data[key]
                    
                    # Acumular cantidades y valores
                    total_quantity += float(data.get('quantity', 0) or 0)
                    total_value += float(data.get('value', 0) or 0)
                    
                    # Capturar warehouse_code si no lo tenemos aún
                    if not warehouse_code and 'warehouse_code' in data:
                        warehouse_code = data['warehouse_code']
                    
                    # Recolectar subinventories
                    if 'subinventory' in data:
                        subinv_str = data['subinventory']
                        if isinstance(subinv_str, str):
                            found_subinventories.add(subinv_str.strip())
                    
                    # Recolectar números de serie
                    if 'serial_numbers' in data and isinstance(data['serial_numbers'], list):
                        serial_numbers.extend(data['serial_numbers'])
                    
                    # Acumular aging si existe
                    if 'aging_info' in data:
                        aging_info = data['aging_info']
                        # Verificar si es una instancia de InventoryAgingInfo
                        if isinstance(aging_info, InventoryAgingInfo):
                            aging_totals['days_0_30'] += aging_info.days_0_30
                            aging_totals['days_31_60'] += aging_info.days_31_60
                            aging_totals['days_61_90'] += aging_info.days_61_90
                            aging_totals['days_91_plus'] += aging_info.days_91_plus
            
            # 4. CREAR RESULTADO
            # Crear aging_info con los totales
            aging_info = InventoryAgingInfo()
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
        
    def _create_inventory_match_from_data(
        self, part_number: str, organization: str, 
        data: Dict, matched_parts: List[str]
    ) -> InventoryMatch:
        """
        Helper method to create InventoryMatch from data dictionary
        """
        # Extraer valores del diccionario de datos
        quantity = float(data.get('quantity', 0) or 0)
        value = float(data.get('value', 0) or 0)
        
        # Extraer subinventory
        subinventory = data.get('subinventory')
        if isinstance(subinventory, str) and ',' in subinventory:
            subinventories = set(s.strip() for s in subinventory.split(','))
            subinventory = ', '.join(subinventories)
        
        # Extraer aging info
        aging_info = data.get('aging_info', InventoryAgingInfo())
        
        # Extraer serial numbers
        serial_numbers = data.get('serial_numbers', [])
        
        # Obtener warehouse code
        warehouse_code = data.get('warehouse_code')
        
        # Crear y retornar el objeto InventoryMatch
        return InventoryMatch(
            part_number=part_number,
            organization=organization,
            has_inventory=quantity > 0,
            quantity=quantity,
            value=value,
            aging_info=aging_info,
            subinventory=subinventory,
            warehouse_code=warehouse_code,
            serial_numbers=serial_numbers
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
            
    def _is_matching_part(self, inventory_part: str, part_number: str) -> bool:
        """
        Determina si un número de parte del inventario coincide con un número de parte buscado.
        Utiliza criterios estrictos para evitar falsos positivos.
        
        Args:
            inventory_part: Número de parte en el inventario
            part_number: Número de parte buscado
            
        Returns:
            bool: True si coinciden según criterios definidos, False en caso contrario
        """
        # Convertir a strings y normalizar a mayúsculas
        inv_part = str(inventory_part).strip().upper()
        search_part = str(part_number).strip().upper()
        
        # 1. Coincidencia exacta (caso prioritario)
        if inv_part == search_part:
            return True
        
        # Dividir en componentes para análisis estructurado
        inv_components = inv_part.split('.')
        search_components = search_part.split('.')
        
        # 2. Si la base (antes del primer punto) no coincide, no es match
        if inv_components[0] != search_components[0]:
            return False
        
        # 3. Verificar sufijos críticos que deben coincidir exactamente
        # Si ambas partes tienen el mismo número de componentes, deben ser idénticos
        if len(inv_components) == len(search_components):
            for i in range(1, len(inv_components)):
                # Si componentes como ACTUAL/NC o números de proyecto no coinciden, no es match
                if inv_components[i] != search_components[i]:
                    return False
            return True
        
        # 4. Para variantes con sufijos específicos, no considerar coincidencias
        # Sufijos específicos que indican variantes distintas
        critical_suffixes = ['-3Y', '-5Y', '-L1Y1', '-L2-1YR', '=-N', '/2', 'LR/2']
        
        for suffix in critical_suffixes:
            # Si una parte tiene el sufijo y la otra no, no es match
            if (suffix in inv_part) != (suffix in search_part):
                return False
        
        # 5. Caso especial: si una es .ACTUAL y la otra .NC, no es match
        if ('.ACTUAL.' in inv_part and '.NC.' in search_part) or ('.NC.' in inv_part and '.ACTUAL.' in search_part):
            return False
        
        # 6. Verificación adicional para partes problemáticas conocidas
        problematic_parts = ["83X-16S-104-NAA", "88-CAT4", "C9300-DNA", "GLC-TE", "C9K-PWR-650WAC"]
        for prob_part in problematic_parts:
            if prob_part in inv_part or prob_part in search_part:
                # Para estas partes, exigir coincidencia exacta en primer componente y formato
                if inv_components[0] != search_components[0]:
                    return False
                # Verificar estructura de formato
                if ('=' in inv_part) != ('=' in search_part):
                    return False
        
        # Si ha pasado todas las verificaciones anteriores, podemos considerar match
        # Esto aplica principalmente para partes con misma base pero diferente notación
        return True
    
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

