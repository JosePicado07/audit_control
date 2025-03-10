import re
import traceback
import pandas as pd
from collections import deque
from typing import Dict, List, Optional, Any, Union, Generator
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import logging
from application.use_cases.inventory.inventory_columns import InventoryColumns
from utils.constant import EXCEL_EXTENSIONS
import os
import json
from functools import lru_cache
import openpyxl
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

logger = logging.getLogger(__name__)

class ExcelRepository:
    """Repository for handling Excel file operations."""
    
    def __init__(
        self, 
        base_path: Optional[Union[str, Path]] = None,
        config_path: Optional[Union[str, Path]] = None,
        batch_size: int = 150000  # Nuevo parámetro

    ):
        """Initialize repository with paths."""
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.config_path = Path(config_path) if config_path else self.base_path
        max_workers = min(os.cpu_count(), 12)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.batch_size = batch_size  # Nueva variable de instancia

        
        self.inventory_required_columns = InventoryColumns.get_required_columns()

        
        # Required columns for audit files (case-insensitive)
        self.audit_required_columns = {
            'FULL PART NUMBER': str,
            'PART#': str,
            'COST TYPE': str,
            'MANUFACTURER': str,
            'MFG NUMBER': str,
            'CONTRACT': str,
            'ORGANIZATION CODE': str,
            'ITEM ORG DESTINATION': str,
            'SERIAL NUMBER CONTROL': str,
            'MFG PART NUM': str,
            'VERTEX PRODUCT CLASS': str,
            'DESCRIPTION': str,
            'CATALOG PRODUCT PART': str,
            'CUSTOMER ID': str,
            'CATEGORY NAME': str,
            'CROSS REFERENCE': str,
            'CROSS REFERENCE DESCRIPTION': str,
            'CROSS REFERENCE TYPE': str,
            'CATEGORY SET NAME': str,
            'CREATED BY': str,
            'CREATION DATE': str,
            'LAST UPDATE DATE': str,
            'LAST UPDATED BY': str,
            'UNSPSC CODE': str,
            'UNSPSC DESCRIPTION': str,
            'ITEM STATUS': str,
        }
        
        # Required columns for inventory files (case-insensitive)
        self.inventory_required_columns = {
            # Columnas clave para identificación
            'ITEM NUMBER': str,                    # Campo principal para coincidencia de partes
            'ORGANIZATION CODE': str,              # Código de organización
            'ORG WAREHOUSE CODE': str,             # Código de almacén
            'SUBINVENTORY CODE': str,              # Código de subinventario
            
            # Columnas de aging - fundamentales para el análisis de inventario
            'AGING 0-30 QUANTITY': float,          # Inventario de 0-30 días
            'AGING 31-60 QUANTITY': float,         # Inventario de 31-60 días
            'AGING 61-90 QUANTITY': float,         # Inventario de 61-90 días
            'AGING 91-120 QUANTITY': float,        # Inventario de 91-120 días
            'AGING 121-150 QUANTITY': float,       # Inventario de 121-150 días
            'AGING 151-180 QUANTITY': float,       # Inventario de 151-180 días
            'AGING 181-365 QUANTITY': float,       # Inventario de 181-365 días
            'AGING OVER 365 QUANTITY': float,      # Inventario mayor a 365 días
            
            # Columnas de valor
            'TOTAL VALUE': float,                  # Valor total del inventario
            'QUANTITY': float,                     # Cantidad total
            
            # Información adicional
            'SERIAL NUMBER': str,                  # Número de serie si aplica
            'ITEM DESCRIPTION': str,               # Descripción del ítem
            'MATERIAL DESIGNATOR': str             # Designador de material (aunque no se use para matching)
        }
        
        self.program_requirements_file = self._resolve_requirements_path()
        
        if not os.environ.get('SKIP_VALIDATION'):
            self._validate_environment()

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame column names."""
        df.columns = df.columns.str.strip() \
                            .str.replace('\n', '') \
                            .str.replace('\r', '') \
                            .str.replace('\t', '') \
                            .str.replace('  ', ' ') \
                            .str.upper()
        return df
    
    def _validate_environment(self) -> None:
        """Validate the environment setup."""
        try:
            if not self.program_requirements_file.exists():
                message = (
                    f"Program requirements file not found at: {self.program_requirements_file}\n"
                    f"Please ensure the file exists in the config directory or set "
                    f"PROGRAM_REQUIREMENTS_PATH environment variable.\n"
                    f"Expected config path: {self.config_path}"
                )
                logger.error(message)
                raise FileNotFoundError(message)
                
            logger.debug("Environment validation successful")
            
        except Exception as e:
            logger.error(f"Environment validation failed: {str(e)}")
            raise

    def _is_inventory_file(self, file_path: Path) -> bool:
        """Check if file is an inventory format."""
        try:
            wb = openpyxl.load_workbook(file_path, read_only=True)
            ws = wb.active
            # Verificar primeras 5 filas por si el título está en otra posición
            for row in list(ws.iter_rows(max_row=5)):
                for cell in row:
                    if cell.value and 'WMS' in str(cell.value).upper():
                        wb.close()
                        return True
            wb.close()
            return False
        except Exception as e:
            logger.error(f"Error checking if file is inventory: {str(e)}")
            return False
        
    def read_excel_file_in_batches(
        self, 
        file_path: Path,
        is_inventory: bool = False,
        sheet_name: Optional[str] = None,
        batch_size: int = 150000,
        progress_callback=None
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Lee archivo Excel en lotes usando BFS para optimizar memoria.
        Omite la primera fila (título) y usa la segunda fila como encabezados.
        
        Args:
            file_path: Ruta al archivo Excel
            is_inventory: Si es un archivo de inventario
            sheet_name: Nombre de la hoja (opcional)
            batch_size: Tamaño de cada lote
            progress_callback: Función de callback para actualizar progreso
            
        Returns:
            Generador que produce DataFrames para cada lote
        """
        try:
            logger.info(f"Reading {'inventory' if is_inventory else 'audit'} file in batches: {file_path}")
            
            # Abrir el archivo en modo de solo lectura para optimizar memoria
            wb = openpyxl.load_workbook(file_path, read_only=True)
            
            # Si no se especificó hoja, usar la activa
            if not sheet_name:
                ws = wb.active
            else:
                # Verificar si existe la hoja solicitada
                if sheet_name not in wb.sheetnames:
                    raise ValueError(f"Sheet {sheet_name} not found in {file_path}")
                ws = wb[sheet_name]
            
            # Obtener los encabezados (segunda fila, no la primera)
            rows_iter = ws.iter_rows()
            # Saltamos la primera fila (título)
            next(rows_iter)
            # Tomamos la segunda fila como encabezados
            headers_row = next(rows_iter)
            
            # Extraer y normalizar encabezados
            headers = []
            header_counts = {}  # Para manejar duplicados
            
            for cell in headers_row:
                value = cell.value
                if value is not None:
                    header = str(value).strip().replace('\n', '').replace('\r', '').replace('\t', '').replace('  ', ' ').upper()
                    
                    # Manejar columnas duplicadas añadiendo un sufijo numérico
                    if header in header_counts:
                        header_counts[header] += 1
                        header = f"{header}_{header_counts[header]}"
                    else:
                        header_counts[header] = 0
                        
                    headers.append(header)
                else:
                    # Asignar un nombre predeterminado para columnas sin nombre
                    headers.append(f"COL_{len(headers)}")
                    
            # Verificar que tenemos encabezados
            if not headers:
                raise ValueError("No valid headers found in the Excel file")
                
            logger.debug(f"Found {len(headers)} columns: {headers}")
            
            # Obtener número total de filas para cálculo de progreso
            total_rows = ws.max_row - 2  # Restamos 2 por las filas de título y encabezados
            
            # Inicializar la cola BFS con el rango inicial (comenzando desde la fila 3)
            queue = deque([(3, min(batch_size + 3, ws.max_row + 1))])  # (inicio, fin)
            processed_rows = 0
            
            # Procesar lotes usando BFS
            while queue:
                start_row, end_row = queue.popleft()
                
                # Crear lote actual
                batch_data = []
                # Leer rango de filas actual
                for row in ws.iter_rows(min_row=start_row, max_row=end_row):
                    row_data = [cell.value for cell in row[:len(headers)]]  # Limitamos al número de encabezados
                    # Asegurarnos de que el número de elementos coincida con el número de encabezados
                    while len(row_data) < len(headers):
                        row_data.append(None)  # Rellenar con None si faltan columnas
                    batch_data.append(row_data)
                
                # Convertir a DataFrame
                if batch_data:
                    df_batch = pd.DataFrame(batch_data, columns=headers)
                    
                    # Optimizar DataFrame
                    df_batch = self._optimize_dataframe(df_batch)
                    
                    # Actualizar progreso
                    processed_rows += len(batch_data)
                    progress = min(1.0, processed_rows / max(1, total_rows))
                    
                    if progress_callback:
                        progress_callback(progress, f"Procesando filas: {processed_rows}/{total_rows}")
                    
                    # Retornar el lote procesado
                    yield df_batch
                
                # Agregar siguiente rango a la cola si hay más datos
                if end_row < ws.max_row:
                    next_start = end_row + 1
                    next_end = min(next_start + batch_size - 1, ws.max_row)
                    if next_start <= next_end:
                        queue.append((next_start, next_end))
            
            # Cerrar el libro
            wb.close()
            
        except Exception as e:
            logger.error(f"Error reading Excel file in batches: {str(e)}")
            logger.error(traceback.format_exc())
            raise
        
    def _optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimización ligera de DataFrame con manejo seguro de columnas duplicadas"""
        try:
            # Conversión de tipos para columnas repetitivas
            category_columns = [
                'FULL PART NUMBER', 
                'ORGANIZATION CODE', 
                'SERIAL NUMBER CONTROL',
                'ITEM STATUS'
            ]
            
            for col in category_columns:
                if col in df.columns:
                    # Conversión a categoría solo si hay repeticiones
                    unique_ratio = len(df[col].unique()) / max(len(df), 1)
                    if unique_ratio < 0.5:  # Solo si menos del 50% son valores únicos
                        df[col] = df[col].astype('category')
            
            # Limpieza segura de columnas de texto
            for col in df.select_dtypes(include=['object']).columns:
                if pd.api.types.is_object_dtype(df[col]):
                    df[col] = df[col].astype(str).str.strip()
            
            return df
        except Exception as e:
            logger.warning(f"Error optimizing DataFrame: {str(e)}")
            # Si hay un error, devolver el DataFrame original sin optimizaciones
            return df
        
    def read_excel_file(
        self, 
        file_path: Path,
        is_inventory: bool = False,
        sheet_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Read Excel file with improved header detection.
        Uses header=1 to skip title row.
        
        Args:
            file_path: Path to the Excel file
            is_inventory: Whether the file is an inventory file
            sheet_name: Optional sheet name to read
            
        Returns:
            DataFrame with properly captured column headers
        """
        try:
            logger.debug(f"Reading {'inventory' if is_inventory else 'audit'} file: {file_path}")
            
            # Para compatibilidad con el código existente, y corregir el problema
            # con pd.read_excel, usamos openpyxl directamente para leer el archivo
            
            # Configuración para leer todos los lotes
            all_batches = []
            
            for batch_df in self.read_excel_file_in_batches(file_path, is_inventory, sheet_name):
                all_batches.append(batch_df)
            
            # Combinar todos los lotes en un único DataFrame
            if all_batches:
                df = pd.concat(all_batches, ignore_index=True)
            else:
                # Si no hay datos, crear un DataFrame vacío
                df = pd.DataFrame()
            
            # Log para diagnóstico
            logger.debug(f"Read Excel file successfully: {len(df)} rows, {len(df.columns)} columns")
            logger.debug(f"Columns: {df.columns.tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading Excel file {file_path}: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def _validate_file_basics(self, file_path: Union[str, Path]) -> Path:
        """Validate basic file requirements."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
            
        if path.suffix.lower() not in EXCEL_EXTENSIONS:
            raise ValueError(f"Invalid file format. Must be one of: {EXCEL_EXTENSIONS}")
            
        return path
    
    def validate_input_file(self, file_path: Union[str, Path]) -> bool:
        """
        Validate audit file format and content with improved diagnostics.
        
        Args:
            file_path: Path to the audit file to validate
        """
        try:
            path = self._validate_file_basics(file_path)
            
            # Read and normalize
            df = self.read_excel_file(path)
            
            # Print diagnostic information
            logger.info(f"File validation - Found columns ({len(df.columns)}): {df.columns.tolist()}")
            logger.info(f"File validation - Required columns ({len(self.audit_required_columns)}): {list(self.audit_required_columns.keys())}")
            
            # Create column lookup for case-insensitive comparison
            file_cols_norm = {col.upper().replace(' ', ''): col for col in df.columns}
            req_cols_norm = {col.upper().replace(' ', ''): col for col in self.audit_required_columns.keys()}
            
            # Check for case or format differences
            true_missing = []
            for req_key, req_col in req_cols_norm.items():
                found = False
                # Try exact match first
                if req_key in file_cols_norm:
                    found = True
                # Try partial match (for cases with extra spaces, underscores, etc)
                if not found:
                    for file_key in file_cols_norm:
                        if req_key in file_key or file_key in req_key:
                            logger.info(f"Found similar column: Required '{req_col}' matched with '{file_cols_norm[file_key]}'")
                            found = True
                            break
                if not found:
                    true_missing.append(req_col)
            
            # Verify required columns
            missing_columns = set(self.audit_required_columns.keys()) - set(df.columns)
            
            if missing_columns:
                if true_missing:
                    # If we have true missing columns, provide more specific feedback
                    logger.error(f"Audit file is missing these critical columns: {true_missing}")
                    logger.error("These columns cannot be matched even with flexible comparison")
                else:
                    # If it's just formatting differences, provide guidance
                    logger.warning("Columns may exist but with different formatting")
                    logger.warning("Consider checking for case differences, spacing, or special characters")
                
                # Show a sample of the file's actual columns for diagnosis
                sample_cols = list(df.columns)[:10]
                logger.info(f"Sample of file columns: {sample_cols}")
                
                raise ValueError(f"Audit file missing required columns: {missing_columns}")
            
            return True
            
        except Exception as e:
            logger.error(f"File validation error: {str(e)}")
            raise

    def validate_inventory_file(self, file_path: Union[str, Path]) -> bool:
        """
        Validate inventory file format and content.
        """
        try:
            path = self._validate_file_basics(file_path)
            
            # Leer archivo de inventario con el método específico
            df = self.read_inventory_file(path)
            
            # Verificar columnas requeridas - Convertir todas las columnas a string primero
            df_columns = set(str(col).upper() for col in df.columns)
            required_columns = set(col.upper() for col in self.inventory_required_columns.keys())
            
            missing_columns = required_columns - df_columns
            if missing_columns:
                raise ValueError(f"Inventory file missing required columns: {missing_columns}")
            
            return True
                
        except Exception as e:
            logger.error(f"Inventory file validation error: {str(e)}")
            raise
        
    def save_excel_file(self, df: pd.DataFrame, file_path: Path) -> None:
        """
        Save DataFrame to Excel with consistent formatting.
        
        Args:
            df: DataFrame to save
            file_path: Path where to save the file
        """
        try:
            # Configure default styles for new workbook
            options = {
                'engine': 'openpyxl',
                'index': False,
                # Remover encoding ya que no es soportado por pandas to_excel
            }
            
            # Save with configured options
            df.to_excel(file_path, **options)
            
            # Apply styles
            wb = openpyxl.load_workbook(file_path)
            ws = wb.active
            
            font = Font(name='Calibri', size=11)
            alignment = Alignment(horizontal='general', vertical='bottom')
            
            for row in ws.iter_rows():
                for cell in row:
                    cell.font = font
                    cell.alignment = alignment
            
            wb.save(file_path)
            
        except Exception as e:
            logger.error(f"Error saving Excel file {file_path}: {str(e)}")
            raise

    def get_program_requirements(self, contract: str) -> Dict[str, Any]:
        try:
            if not self.program_requirements_file.exists():
                raise FileNotFoundError(f"Program requirements file not found: {self.program_requirements_file}")
            
            # Define dtypes to force string reading for org columns
            dtypes = {
                'CONTRACT': str,
                'ORG FOR SERIAL CONTROL COMPARISION': str,
                'ORGANIZATION CODE (PHYSICAL ORGS)': str,
                'ORGANIZATION CODE (DROPSHIP ORGS)': str,
                'ITEM ORG DESTINATION THAT CAN BE USED + OTHER ORGS': str,
                'DOES PROGRAM TRANSACT INTERNATIONALLY?': str,
                'STATUS': str
            }
            
            # Read Excel file with specified dtypes
            df = pd.read_excel(self.program_requirements_file, engine='openpyxl', dtype=dtypes)
            df = self._normalize_columns(df)
            
            # Required columns validation
            required_columns = {
                'CONTRACT': 'Contract ID',
                'ORG FOR SERIAL CONTROL COMPARISION': 'Base Organization',
                'ORGANIZATION CODE (PHYSICAL ORGS)': 'Physical Organizations',
                'ORGANIZATION CODE (DROPSHIP ORGS)': 'Dropship Organizations',
                'ITEM ORG DESTINATION THAT CAN BE USED + OTHER ORGS': 'Item Org Destination',
                'DOES PROGRAM TRANSACT INTERNATIONALLY?': 'International Flag',
                'STATUS': 'Status'
            }
            
            missing_columns = set(required_columns.keys()) - set(df.columns)
            if missing_columns:
                raise ValueError(f"Missing required columns in requirements file: {', '.join(missing_columns)}")
            
            # Contract validation and data retrieval
            contract = str(contract).strip().upper()
            contract_mask = df['CONTRACT'].str.upper() == contract
            program_data = df[contract_mask]
            
            if program_data.empty:
                raise ValueError(f"No requirements found for contract: {contract}")
                
            # Extract base org with validation    
            base_org = program_data['ORG FOR SERIAL CONTROL COMPARISION'].iloc[0]
            if pd.isna(base_org):
                raise ValueError(f"Base organization not specified for contract {contract}")
            
            base_org = str(base_org).strip()
            if base_org.endswith('.0'):
                base_org = base_org[:-2]
            base_org = base_org.zfill(2)
            
            # Process organizations
            destination_orgs_raw = program_data['ITEM ORG DESTINATION THAT CAN BE USED + OTHER ORGS'].iloc[0]
            physical_orgs_raw = program_data['ORGANIZATION CODE (PHYSICAL ORGS)'].iloc[0]
            dropship_orgs_raw = program_data['ORGANIZATION CODE (DROPSHIP ORGS)'].iloc[0]
            
            # Add debug log for raw input
            logger.debug(f"Raw input before processing - Destination: {destination_orgs_raw}")
            logger.debug(f"Raw input before processing - Physical: {physical_orgs_raw}")
            logger.debug(f"Raw input before processing - Dropship: {dropship_orgs_raw}")
            
            destination_orgs = self._process_org_codes(destination_orgs_raw)
            physical_orgs = self._process_org_codes(physical_orgs_raw)
            dropship_orgs = self._process_org_codes(dropship_orgs_raw)
            
            all_orgs = destination_orgs + physical_orgs + dropship_orgs
            unique_orgs = sorted(list(set(all_orgs)))
            
            logger.debug(f"Raw destination orgs: {destination_orgs_raw}")
            logger.debug(f"Processed destination orgs: {destination_orgs}")
            logger.debug(f"Raw physical orgs: {physical_orgs_raw}")
            logger.debug(f"Processed physical orgs: {physical_orgs}")
            logger.debug(f"Raw dropship orgs: {dropship_orgs_raw}")
            logger.debug(f"Processed dropship orgs: {dropship_orgs}")
            logger.debug(f"Combined unique orgs: {unique_orgs}")
            
            if not destination_orgs:
                raise ValueError(f"Item Org Destination organizations are required for contract {contract}")
            
            requirements = {
                'contract': contract,
                'base_org': base_org,
                'org_destination': unique_orgs,
                'physical_orgs': physical_orgs,
                'dropship_orgs': dropship_orgs,
                'international': bool(program_data['DOES PROGRAM TRANSACT INTERNATIONALLY?'].iloc[0]),
                'status': program_data['STATUS'].iloc[0]
            }
            
            logger.debug(f"Program requirements loaded for contract {contract}:")
            logger.debug(f"Base org: {requirements['base_org']}")
            logger.debug(f"Org destination: {requirements['org_destination']}")
            logger.debug(f"Physical orgs: {requirements['physical_orgs']}")
            logger.debug(f"Dropship orgs: {requirements['dropship_orgs']}")
            
            return requirements
        
        except Exception as e:
            logger.error(f"Error retrieving program requirements for contract {contract}: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            raise

    def _process_org_codes(self, org_codes: str) -> List[str]:
        """
        Procesa códigos de organización con soporte completo para diferentes patrones, prefijos y variaciones.

        Args:
            org_codes: Cadena que contiene códigos de organización

        Returns:
            List[str]: Lista de códigos de organización validados
        """
        try:
            if pd.isna(org_codes):
                return []
                        
            # Convertir a cadena y limpiar valores flotantes
            org_str = str(org_codes).strip()
            if org_str.endswith('.0'):
                org_str = org_str[:-2]
                        
            if not org_str:
                return []
            
            normalized_orgs = set()

            # Separar usando palabras clave comunes como "and", "add", "&", "+"
            add_parts = re.split(r'\s+and\s+add\s+|\s+and\s+|\s*add\s*|\s*&\s*|\s*\+\s*', org_str, flags=re.IGNORECASE)
            
            # Procesar la parte principal (antes de cualquier "and add" o similar)
            main_part = add_parts[0]
            
            # Extraer números de cualquier prefijo, priorizando contexto de "org"
            org_context = re.search(r'(?:org\s+|in\s+org\s+)(\d+(?:[,-]\d+)*)', main_part, re.IGNORECASE)
            if org_context:
                numbers = re.findall(r'\d+', org_context.group(1))  # Ej: "04,132,40" -> ["04", "132", "40"]
            else:
                numbers = re.findall(r'\d+', main_part)
            for num in numbers:
                normalized_orgs.add(num.zfill(2))

            # Manejar casos con "No ORGs" o "per Shannon"
            if "No ORGs" in org_str:
                no_orgs_numbers = re.findall(r'\d+', org_str)
                for num in no_orgs_numbers:
                    normalized_orgs.add(num.zfill(2))

            if "per shannon" in org_str.lower():
                shannon_numbers = re.findall(r'\d+', org_str)
                for num in shannon_numbers:
                    normalized_orgs.add(num.zfill(2))

            # Procesar los números adicionales después de "and add", "and", "add", "&", "+"
            for part in add_parts[1:]:
                logger.debug(f"Procesando parte adicional: '{part}'")
                org_context = re.search(r'(?:org\s+|in\s+org\s+)(\d+(?:[,-]\d+)*)', part, re.IGNORECASE)
                if org_context:
                    additional_numbers = re.findall(r'\d+', org_context.group(1))
                else:
                    additional_numbers = re.findall(r'\d+', part)
                for num in additional_numbers:
                    # Evitar agregar números que sean combinación de códigos previos
                    if not any(num in existing or existing in num for existing in normalized_orgs):
                        normalized_orgs.add(num.zfill(2))
                        logger.debug(f"Agregada organización adicional: {num.zfill(2)}")

            # Retornar resultados ordenados y únicos
            valid_orgs = sorted(normalized_orgs)
            logger.debug(f"Códigos de organización procesados: Entrada='{org_codes}' Salida={valid_orgs}")
            return valid_orgs
                    
        except Exception as e:
            logger.error(f"Error al procesar códigos de organización: {str(e)}")
            logger.error(f"Valor de entrada: {org_codes}")
            return []
            
    @staticmethod
    def extract_organizations(org_str: str) -> List[str]:
        """
        Extraer organizaciones de manera extremadamente robusta.
        Maneja múltiples formatos y variaciones.
        
        Args:
            org_str: String conteniendo códigos de organización
            
        Returns:
            List[str]: Lista ordenada de códigos de organización únicos
        """
        if pd.isna(org_str):
            return []
        
        # Convertir a string y normalizar
        org_str = str(org_str).lower()
        
        # Patrones de extracción extendidos
        extraction_patterns = [
            r'wwt_orgs_(\d+(?:_\d+)*)',
            r'wwt_sing_orgs_(\d+(?:_\d+)*)',
            r'wwt_uk_orgs_(\d+(?:_\d+)*)',
            r'wwt_ind_orgs_(\d+(?:_\d+)*)',
            r'wwt_br_orgs_(\d+(?:_\d+)*)',
            r'wwt_vn_orgs_(\d+(?:_\d+)*)',
            r'telco_(\d+(?:_\d+)*)',
            r'orgs(?:_|\s*)(\d+(?:[,_\s]\d+)*)',
            # Patrones adicionales para casos comunes
            r'org[s]?\s*[:=-]?\s*(\d+(?:[,_\s]\d+)*)',
            r'location[s]?\s*[:=-]?\s*(\d+(?:[,_\s]\d+)*)'
        ]
        
        additional_patterns = [
            r'and\s*add\s*(\d+(?:[,_\s]\d+)*)',
            r'\+\s*(\d+(?:[,_\s]\d+)*)',
            r'with\s*(\d+(?:[,_\s]\d+)*)',
            r'includes?\s*(\d+(?:[,_\s]\d+)*)'
        ]
        
        organizations = set()
        
        # Intentar extraer con todos los patrones
        all_patterns = extraction_patterns + additional_patterns
        for pattern in all_patterns:
            matches = re.findall(pattern, org_str)
            for match in matches:
                orgs = re.findall(r'\d+', str(match))
                organizations.update(orgs)
        
        # Extracción general si no se encontraron organizaciones
        if not organizations:
            direct_orgs = re.findall(r'\b(\d{2,3})\b', org_str)
            organizations.update(direct_orgs)
        
        # Filtrar y validar
        valid_orgs = sorted(set(
            org for org in organizations 
            if len(org) >= 2 and len(org) <= 3
        ))
        
        return valid_orgs
    
    def _resolve_requirements_path(self) -> Path:
        """
        Find the program requirements file path.
        
        Returns:
            Path: Ruta validada al archivo de requerimientos
            
        Raises:
            FileNotFoundError: Si no se encuentra el archivo en ninguna ubicación
        """
        possible_filenames = [
            "Program Requirements - PDM - NPI for Audit.xlsx",
            "Program Requirements - PDM - NPI  for Audit.xlsx"
        ]
        
        logger.debug("Searching for requirements file in possible locations...")
        
        # Rutas posibles con prioridad
        search_paths = [
            self.config_path,
            self.base_path / "config",
            self.base_path,
            Path(__file__).parent.parent.parent / "config"
        ]
        
        # Buscar en todas las combinaciones posibles
        for filename in possible_filenames:
            for base_path in search_paths:
                path = base_path / filename
                logger.debug(f"Checking path: {path}")
                if path.exists():
                    logger.info(f"Found requirements file at: {path}")
                    return path
        
        # Verificar variable de entorno
        env_path = os.environ.get('PROGRAM_REQUIREMENTS_PATH')
        if env_path:
            path = Path(env_path)
            if path.exists():
                logger.info(f"Using requirements file from environment: {path}")
                return path
            else:
                logger.warning(f"Environment path does not exist: {env_path}")
        
        # Usar ruta por defecto como último recurso
        default_path = self.config_path / "Program Requirements - PDM - NPI for Audit.xlsx"
        logger.warning(f"No requirements file found, defaulting to: {default_path}")
        
        if not default_path.exists():
            error_msg = (
                f"Requirements file not found in any location. "
                f"Searched in: {[str(p) for p in search_paths]}"
            )
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        return default_path

    def cleanup(self) -> None:
        """Clean up repository resources."""
        try:
            if hasattr(self, 'executor'):
                logger.debug("Shutting down executor...")
                self.executor.shutdown(wait=True)
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

    def __del__(self):
        """Ensure cleanup on destruction."""
        self.cleanup()
        
    def read_inventory_file(self, file_path: Path) -> pd.DataFrame:
        """Read and normalize inventory file."""
        try:
            logger.info(f"Reading inventory file: {file_path}")
            
            if not self._is_inventory_file(file_path):
                raise ValueError("Not a valid inventory file format")
            
            # Usar read_excel_file para mantener la consistencia
            df = self.read_excel_file(file_path, is_inventory=True)
            
            return df
                
        except Exception as e:
            logger.error(f"Error reading inventory file: {str(e)}")
            raise