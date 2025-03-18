import re
import traceback
import pandas as pd
from typing import Dict, List, Optional, Any, Union
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
import numpy as np  # Para optimización de tipos de datos
from functools import lru_cache  # Para caché eficiente
import psutil  # Para monitoreo de memoria

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

logger = logging.getLogger(__name__)

class ExcelRepository:
    """Repository for handling Excel file operations."""
    
    def __init__(
        self, 
        base_path: Optional[Union[str, Path]] = None,
        config_path: Optional[Union[str, Path]] = None,
        
    ):
        """Initialize repository with paths."""
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.config_path = Path(config_path) if config_path else self.base_path
        self.executor = ThreadPoolExecutor(max_workers=4)
        self._excel_cache = lru_cache(maxsize=10)(self.read_excel_file)

        
        self._dataframe_cache = {}
        self._file_modification_times = {}
          
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
        
    def read_excel_file(
        self, 
        file_path: Path,
        is_inventory: bool = False,
        sheet_name: Optional[str] = None,
        use_cache: bool = True,
        chunk_size: int = 10000,  # Nuevo parámetro para procesamiento por chunks
        memory_limit: float = 0.7  # Límite de memoria (70% de RAM disponible)
    ) -> pd.DataFrame:
        try:
            # Verificar límite de memoria disponible
            available_memory = psutil.virtual_memory().available / (1024 * 1024 * 1024)  # GB
            max_memory = available_memory * memory_limit
            
            logger.debug(f"Memoria disponible: {available_memory:.2f} GB, Límite: {max_memory:.2f} GB")

            # Leer archivo con opciones optimizadas
            options = {
                'engine': 'openpyxl',
                'skiprows': 1,
                'dtype': self._infer_dtypes(file_path),  # Nuevo método para inferir tipos
                'parse_dates': True,  # Parsear fechas automáticamente
                'low_memory': True  # Habilitar procesamiento de bajo consumo de memoria
            }

            # Si el archivo es muy grande, usar procesamiento por chunks
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # Tamaño en MB
            
            if file_size > 50:  # Umbral para procesamiento por chunks
                logger.info(f"Archivo grande detectado ({file_size:.2f} MB). Procesando por chunks.")
                chunks = []
                for chunk in pd.read_excel(file_path, chunksize=chunk_size, **options):
                    chunk = self._process_chunk(chunk, is_inventory)
                    chunks.append(chunk)
                
                df = pd.concat(chunks, ignore_index=True)
            else:
                # Lectura de archivo pequeño
                df = pd.read_excel(file_path, **options)
                df = self._process_chunk(df, is_inventory)

            return df

        except MemoryError:
            logger.error("Memoria insuficiente para procesar el archivo")
            raise MemoryError("El archivo es demasiado grande para procesarse con la memoria disponible")

    def _infer_dtypes(self, file_path: Path) -> Dict:
        """Inferir tipos de datos de manera eficiente"""
        try:
            # Leer solo primeras 1000 filas para inferir tipos
            sample_df = pd.read_excel(file_path, nrows=1000)
            
            dtype_mapping = {}
            for col in sample_df.columns:
                if sample_df[col].dtype == 'object':
                    # Si es texto, intentar convertir a categoría si pocos valores únicos
                    unique_count = sample_df[col].nunique()
                    if unique_count < len(sample_df) * 0.5:
                        dtype_mapping[col] = 'category'
                    else:
                        dtype_mapping[col] = str
                elif pd.api.types.is_numeric_dtype(sample_df[col]):
                    # Reducir precisión de números flotantes
                    dtype_mapping[col] = np.float32
                elif pd.api.types.is_datetime64_any_dtype(sample_df[col]):
                    dtype_mapping[col] = 'datetime64[ns]'
            
            return dtype_mapping
        except Exception as e:
            logger.warning(f"Error infiriendo tipos de datos: {e}")
            return {}

    def _process_chunk(self, chunk: pd.DataFrame, is_inventory: bool = False) -> pd.DataFrame:
        """Procesar chunk con normalización y limpieza"""
        # Normalizar columnas
        chunk.columns = chunk.columns.str.strip().str.upper()
        
        # Eliminar columnas sin nombre
        chunk = chunk.loc[:, ~chunk.columns.str.contains('^UNNAMED:', na=False)]
        
        # Convertir columnas a tipos más eficientes
        for col in chunk.columns:
            if chunk[col].dtype == 'object':
                # Convertir a categoría si pocas categorías únicas
                unique_count = chunk[col].nunique()
                if unique_count < len(chunk) * 0.5:
                    chunk[col] = chunk[col].astype('category')
        
        return chunk

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
        Validate audit file format and content.
        
        Args:
            file_path: Path to the audit file to validate
        """
        try:
            path = self._validate_file_basics(file_path)
            
            # Read and normalize con opción de no usar caché para validación
            df = self.read_excel_file(path, use_cache=False)
            
            if df is None or df.empty:
                raise ValueError("File read returned empty or None DataFrame")
            
            # Verify required columns
            missing_columns = set(self.audit_required_columns.keys()) - set(df.columns)
            
            if missing_columns:
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
    
    def clear_cache(self, file_path: Optional[Union[str, Path]] = None) -> None:
        """
        Limpiar la caché de DataFrames.
        
        Args:
            file_path: Si se proporciona, solo limpia la caché para ese archivo.
                    Si es None, limpia toda la caché.
        """
        try:
            if file_path is None:
                logger.debug("Clearing all DataFrame caches")
                self._dataframe_cache.clear()
                self._file_modification_times.clear()
            else:
                file_path_str = str(file_path)
                keys_to_remove = [k for k in self._dataframe_cache if k.startswith(file_path_str)]
                for key in keys_to_remove:
                    logger.debug(f"Removing cache for {key}")
                    self._dataframe_cache.pop(key, None)
                self._file_modification_times.pop(file_path_str, None)
        except Exception as e:
            logger.error(f"Error clearing cache: {str(e)}")

    def cleanup(self) -> None:
        """Clean up repository resources."""
        try:
            if hasattr(self, 'executor'):
                logger.debug("Shutting down executor...")
                self.executor.shutdown(wait=True)
            # Limpiar caché
            if hasattr(self, '_dataframe_cache'):
                self.clear_cache()
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