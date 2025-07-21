import gc
import os
from pathlib import Path
import time
import traceback
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
import pandas as pd
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import datetime

from domain.entities.inventory_entity import InventoryAgingInfo
from .inventory.inventory_matcher import InventoryMatcher


from domain.entities.audit_entity import AuditResult, AuditItem, InventoryInfo, SerialControlInfo
from domain.value_objects.audit_values import AuditStatus, AuditType
from domain.interfaces.repository_interface import IAuditRepository
from infrastructure.persistence.excel_repository import ExcelRepository
from infrastructure.logging.logger import get_logger

logger = get_logger(__name__)

@dataclass
class AuditConfig:
    """Configuration for audit processing"""
    column_mapping: Dict[str, str]
    column_order: List[str]
    dtypes: Dict[str, Any]

class AuditProcessor:
    
    INVENTORY_COLUMNS = {
    'MATERIAL_DESIGNATOR': 'MATERIAL DESIGNATOR',
    'ORG_CODE': 'ORGANIZATION CODE',
    'WAREHOUSE_CODE': 'ORG WAREHOUSE CODE',
    'SUBINV_CODE': 'SUBINVENTORY CODE',
    'SERIAL_NUMBER': 'SERIAL NUMBER',
    'QUANTITY': 'QUANTITY',
    'TOTAL_VALUE': 'TOTAL VALUE',
    'ITEM_DESC': 'ITEM DESCRIPTION'
    }
    
    def __init__(
        self,
        repository: Optional[IAuditRepository] = None,
        executor_workers: int = 4
    ):
        """Initialize the AuditProcessor with configuration"""
        self.repository = repository or ExcelRepository()
        self.status = AuditStatus.PENDING
        self.executor = ThreadPoolExecutor(max_workers=executor_workers)
        self._config = self._initialize_config()
        
    def _initialize_config(self) -> AuditConfig:
        """Initialize audit configuration with column mappings and data types"""
        return AuditConfig(
            column_mapping={
                'FULL PART NUMBER': 'Part Number',
                'PART#': 'Part Code',
                'COST TYPE': 'Cost Type',
                'MANUFACTURER': 'Manufacturer',
                'CONTRACT': 'Contract Segment',  
                'ITEM STATUS': 'Status',
                'ORGANIZATION CODE': 'Organization',
                'ITEM ORG DESTINATION': 'Destination Org',
                'SERIAL NUMBER CONTROL': 'Serial Control',
                'MFG PART NUM': 'Manufacturer Part Number',
                'VERTEX PRODUCT CLASS': 'Vertex',
                'DESCRIPTION': 'Description',
                'CATALOG PRODUCT PART': 'Catalog Part',
                'CUSTOMER ID': 'Customer ID',
                'CATEGORY NAME': 'Category',
                'CATEGORY SET NAME': 'Category Set'
            },
            column_order=[
                'Full Part Number', 'Part#', 'Cost Type', 'Manufacturer',
                'Contract','Item Status', 'Organization Code', 'Item Org Destination',
                'Serial Number Control', 'MFG Part Num', 'Vertex Product Class',
                'Description', 'Catalog Product Part', 'Customer ID', 'Category Name',
                'Category Set Name'
            ],
            dtypes={
                'Full Part Number': str,
                'Part#': str,
                'Organization Code': str,
                'Serial Number Control': str,
                'Contract': str,  
                'Customer ID': str,
                'MFG Part Num': str,
                'Item Status': str
            }
        )
        
    def process_audit(self, file_path: str, contract: str, inventory_file: Optional[str] = None) -> AuditResult:
        try:
            # Logging detallado al inicio
            logger.debug(f"Contract: {contract}")
            logger.debug(f"Audit file: {file_path}")
            logger.debug(f"Inventory file: {inventory_file}")

            # Get program requirements
            program_reqs = self.repository.get_program_requirements(contract)
            logger.debug(f"Program requirements: {program_reqs}")
            
            if not program_reqs:
                raise ValueError(f"No program requirements found for contract: {contract}")
            
            # Detalles de requisitos de programa
            base_org = program_reqs.get('base_org')
            org_destination = program_reqs.get('org_destination', [])
            
            logger.debug(f"Base Org: {base_org}")
            logger.debug(f"Org Destination: {org_destination}")
            
            # Read and validate audit file
            audit_df = self._read_audit_file(file_path)
            logger.debug(f"Audit DataFrame shape: {audit_df.shape}")
            logger.debug(f"Audit DataFrame columns: {audit_df.columns.tolist()}")
            
            # Validaciones previas a procesamiento
            if audit_df.empty:
                logger.warning("Audit DataFrame is empty")
                raise ValueError("Audit file contains no data")

            # Validar existencia de columnas críticas
            critical_columns = ['Part Number', 'Organization', 'Serial Control']
            missing_columns = [col for col in critical_columns if col not in audit_df.columns]
            if missing_columns:
                logger.error(f"Missing critical columns: {missing_columns}")
                raise ValueError(f"Missing columns: {missing_columns}")

            # Validar base org y destination orgs
            if not base_org:
                logger.error("No base organization specified")
                raise ValueError("Base organization is required")
            
            if not org_destination:  
                logger.error("No Item Org Destination organizations specified")
                raise ValueError("Item Org Destination organizations are required")

            # Read inventory file if provided
            inventory_df = None
            if inventory_file:
                inventory_df = self._read_inventory_file(inventory_file)
                logger.debug(f"Inventory DataFrame shape: {inventory_df.shape}")
            
            # Process each audit type - con manejo de errores
            try:
                serial_results = self._process_serial_control_audit(
                    audit_df,
                    program_reqs,
                    inventory_df
                )
            except Exception as e:
                logger.error(f"Error in serial control audit: {e}")
                raise ValueError(f"Failed to process serial control audit: {e}")

            try:
                org_results = self._process_org_mismatch_audit(
                    audit_df, 
                    program_reqs
                )
            except Exception as e:
                logger.error(f"Error in org mismatch audit: {e}")
                raise ValueError(f"Failed to process org mismatch audit: {e}")

            try:
                other_results = self._process_other_attributes_audit(
                    audit_df, 
                    program_reqs
                )
            except Exception as e:
                logger.error(f"Error in other attributes audit: {e}")
                raise ValueError(f"Failed to process other attributes audit: {e}")

            # Validaciones finales de resultados
            if (serial_results['data'].empty and 
                org_results['data'].empty and 
                other_results['data'].empty):
                logger.warning("All audit result DataFrames are empty")
                raise ValueError("No valid audit results generated from processing")

            # Combine results
            combined_df = self._combine_audit_results(
                serial_results['data'],
                org_results['data'],
                other_results['data']
            )

            # Generate audit summary
            audit_summary = self.generate_audit_summary(combined_df)

            # Convert results to AuditItems
            audit_items = self._convert_to_audit_items(combined_df, program_reqs.get('org_destination', []), audit_df, inventory_df,existing_inventory_check=serial_results.get('inventory_check'))

            audit_result = AuditResult(
            audit_id=f"AUDIT_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            contract=contract,
            timestamp=datetime.now(),
            items=audit_items,
            summary=audit_summary,
            # Corrección en cómo acceder a los valores del DataFrame
            manufacturer=combined_df['Manufacturer'].iloc[0] if not combined_df.empty and 'Manufacturer' in combined_df.columns else '',
            description=combined_df['Description'].iloc[0] if not combined_df.empty and 'Description' in combined_df.columns else '',
            vertex_class=combined_df['Vertex'].iloc[0] if not combined_df.empty and 'Vertex' in combined_df.columns else '',
            serial_control_results=serial_results,
            org_mismatch_results=org_results,
            report_path=None
        )

            self.status = AuditStatus.COMPLETED
            logger.info("Audit result created successfully")
            return audit_result

        except Exception as e:
            self.status = AuditStatus.FAILED
            logger.error(f"Comprehensive audit processing error: {str(e)}")
            logger.error(f"Detailed stack trace: {traceback.format_exc()}")
            raise ValueError(f"Failed to process audit: {str(e)}")
        
    def _process_serial_control_audit(
        self,
        df: pd.DataFrame,
        program_reqs: Dict,
        inventory_df: Optional[pd.DataFrame] = None
    ) -> Dict:
        """
        Versión altamente optimizada para auditoría de control de serie
        """
        try:
            start_time = time.time()
            logger.info("=== SERIAL CONTROL AUDIT (HIGH PERFORMANCE) ===")
            
            # 1. Extraer parámetros una sola vez (evitar lookups repetidos)
            base_org = program_reqs.get('base_org')
            org_destination = program_reqs.get('org_destination', [])
            
            # Validaciones y defaults optimizados
            if not base_org and org_destination:
                base_org = org_destination[0]
                logger.warning(f"No base org specified, using first destination org: {base_org}")
                
            if not org_destination:
                org_strings = df['Organization'].astype(str).str.strip().unique()
                org_destination = sorted(pd.Series(org_strings).str.zfill(2).tolist())
                logger.warning(f"No destination orgs specified, using all unique orgs: {org_destination}")
            
            # 2. Obtener resultados de comparación (usando método ya optimizado)
            serial_comparison = self._check_serial_control(df, base_org, org_destination)
            
            # 3. Verificar inventario para partes problemáticas usando cache inteligente
            # Usar instancia singleton para evitar recreaciones
            mismatched_parts = serial_comparison.get('mismatched_parts', [])
            
            # Solo procesar inventario para partes problemáticas (eficiencia)
            inventory_check = {}
            if mismatched_parts:
                inventory_check = self._check_inventory_for_mismatches(
                    mismatched_parts,
                    df, 
                    org_destination,
                    inventory_df
                )
            
            # 4. Validar partes non-hardware de manera optimizada
            non_hardware = self._validate_non_hardware_parts(df)
            
            # 5. Crear mapeo de inventario eficiente
            # Usar defaultdict para evitar verificaciones constantes de existencia
            from collections import defaultdict
            inventory_map = defaultdict(dict)
            
            for k, v in inventory_check.items():
                if k != 'summary' and isinstance(v, dict):
                    inventory_map[k] = v
            
            # 6. Crear columnas dinámicas una sola vez
            dynamic_columns = {org: f'{org} Serial Control' for org in org_destination}
            
            # 7. Construir DataFrame de resultados de manera eficiente
            # Preparar lista para DataFrame en lugar de concat repetido
            result_rows = []
            
            for part_data in serial_comparison.get('data', []):
                # Eficiente: obtener datos de una sola vez
                part_number = part_data.get('part_number', '')
                organization = part_data.get('organization', '')
                inventory_key = f"{part_number}_{organization}"
                
                # Obtener datos de inventario (defaultdict previene KeyError)
                inventory_info = inventory_map.get(inventory_key, {})
                
                # Construir fila resultado eficientemente
                row = {
                    'Part Number': part_number,
                    'Organization': organization,
                    'Serial Control': part_data.get('serial_control', ''),
                    'Base Org Serial Control': part_data.get('base_serial', ''),
                    'Status': 'Mismatch' if part_data.get('has_mismatch', False) else 'OK',
                    'Action Required': 'Review Serial Control' if part_data.get('has_mismatch', False) else 'None',
                    'On Hand Quantity': inventory_info.get('quantity', 0),
                    'has_inventory': inventory_info.get('has_inventory', False),
                    'Value': inventory_info.get('value', 0.0),
                    'Subinventory Code': inventory_info.get('subinventory', ''),
                    'Warehouse Code': inventory_info.get('warehouse_code', ''),
                    'Aging_0_30': inventory_info.get('aging_0_30', 0.0),
                    'Aging_31_60': inventory_info.get('aging_31_60', 0.0),
                    'Aging_61_90': inventory_info.get('aging_61_90', 0.0),
                    'Is Hardware': 'No' if part_number in non_hardware.get('non_hardware_parts', []) else 'Yes',
                    'Manufacturer': part_data.get('manufacturer', ''),
                    'Description': part_data.get('description', ''),
                    'Vertex': part_data.get('vertex', '')
                }
                
                # Agregar columnas dinámicas eficientemente
                for org, column_name in dynamic_columns.items():
                    # Usar generators en lugar de comprehensions para mejor memoria
                    org_data = next(
                        (data for data in serial_comparison.get('data', [])
                        if data.get('part_number') == part_number and data.get('organization') == org),
                        {}
                    )
                    row[column_name] = org_data.get('serial_control', 'N/A')
                
                result_rows.append(row)
                
                # Liberar memoria periódicamente para conjuntos grandes
                if len(result_rows) % 10000 == 0:
                    gc.collect()
            
            # Crear DataFrame una sola vez (mucho más eficiente)
            results_df = pd.DataFrame(result_rows)
            
            # 8. Crear resultado final
            inventory_summary = inventory_check.get('summary', {})
            
            result = {
                'data': df,  # Mantener compatibilidad
                'mismatched_parts': serial_comparison.get('mismatched_parts', []),
                'dynamic_columns': list(dynamic_columns.values()),
                'inventory_map': dict(inventory_map),  # Convertir defaultdict a dict
                'program_requirements': program_reqs,
                'summary': {
                    'total_mismatches': len(serial_comparison.get('mismatched_parts', [])),
                    'total_parts': len(df['Part Number'].unique()),
                    'total_with_inventory': inventory_summary.get('parts_with_inventory', 0),
                    'total_inventory_records': inventory_summary.get('total_inventory_records', 0),
                    'total_non_hardware_issues': len(non_hardware.get('non_hardware_parts', [])),
                    'processing_time': time.time() - start_time
                }
            }
            
            logger.info(f"Serial control audit completed in {time.time() - start_time:.2f}s")
            return result

        except Exception as e:
            logger.error(f"Error in serial control audit: {str(e)}")
            raise

    def _check_serial_control(
        self,
        df: pd.DataFrame,
        base_org: str,
        org_destination: List[str]
    ) -> Dict:
        """
        Versión optimizada con Polars y procesamiento paralelo por lotes.
        Identifica discrepancias entre 'Dynamic entry at inventory receipt' y 'No serial number control',
        además de detectar patrones sospechosos donde el control de serie existe en organizaciones limitadas.
        """
        try:
            import os
            import polars as pl
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import time
            from collections import defaultdict
            
            start_time = time.time()
            
            # Variables para resultados globales
            mismatched_parts = []
            comparison_data = []
            pattern_registry = {}
            valid_values = ["Dynamic entry at inventory receipt", "No serial number control"]
            
            # Log inicial
            logger.info("=== SERIAL CONTROL CHECK (OPTIMIZED) ===")
            logger.info(f"Base org: {base_org}")
            logger.info(f"Destination orgs: {org_destination}")
            
            # 1. Preparación con Polars - Conversión y normalización
            try:
                # Convertir a Polars si no lo es ya
                if isinstance(df, pd.DataFrame):
                    df_pl = pl.from_pandas(df)
                else:
                    df_pl = df
                    
                # Normalizamos valores de Serial Control de una vez (vectorizado)
                df_pl = df_pl.with_columns([
                    pl.when(pl.col("Serial Control").str.to_uppercase() == "YES")
                    .then(pl.lit("Dynamic entry at inventory receipt"))
                    .when(pl.col("Serial Control").str.to_uppercase() == "NO")
                    .then(pl.lit("No serial number control"))
                    .otherwise(pl.col("Serial Control"))
                    .alias("Serial Control Normalized")
                ])
                
                # Preparar información básica por parte
                part_info_df = df_pl.group_by("Part Number").agg([
                    pl.first("Manufacturer").alias("Manufacturer"),
                    pl.first("Description").alias("Description"),
                    pl.first("Vertex").alias("Vertex"),
                    pl.first("Item Status").alias("Item Status") if "Item Status" in df_pl.columns else pl.lit("").alias("Item Status")
                ])
                
                # Convertir a diccionario para acceso O(1)
                part_info = {
                    row["Part Number"]: {
                        "manufacturer": row["Manufacturer"],
                        "description": row["Description"],
                        "vertex": row["Vertex"], 
                        "item_status": row["Item Status"]
                    } for row in part_info_df.to_dicts()
                }
                
                # Extraer valores de base_org para todas las partes de una vez
                base_values_df = df_pl.filter(pl.col("Organization") == base_org).select(
                    ["Part Number", "Serial Control Normalized"]
                )
                
                # Crear diccionario de valores base
                base_values = {}
                for row in base_values_df.to_dicts():
                    base_values[row["Part Number"]] = row["Serial Control Normalized"]
                
                # Extraer todos los valores seriales organizados por parte/org
                serial_data_df = df_pl.select(
                    ["Part Number", "Organization", "Serial Control Normalized"]
                )
                
                # Crear estructura para valores seriales
                serial_values = defaultdict(dict)
                for row in serial_data_df.to_dicts():
                    part = row["Part Number"]
                    org = row["Organization"]
                    value = row["Serial Control Normalized"]
                    serial_values[part][org] = value
                    
                # Obtener todas las partes únicas
                unique_parts = df_pl["Part Number"].unique().to_list()
                logger.info(f"Procesando {len(unique_parts)} partes únicas")
                
            except Exception as e:
                # Si falla Polars, usamos la versión pandas tradicional
                logger.warning(f"Error en procesamiento con Polars: {str(e)}")
                logger.warning("Fallback a procesamiento con Pandas")
                
                # Normalizar valores en pandas
                df["Serial Control Normalized"] = df["Serial Control"].apply(
                    lambda x: "Dynamic entry at inventory receipt" if str(x).upper() == "YES" 
                    else "No serial number control" if str(x).upper() == "NO" 
                    else x
                )
                
                # Estructuras equivalentes con pandas
                part_info = {}
                for _, group in df.groupby("Part Number"):
                    part_number = group["Part Number"].iloc[0]
                    part_info[part_number] = {
                        "manufacturer": group["Manufacturer"].iloc[0],
                        "description": group["Description"].iloc[0],
                        "vertex": group.get("Vertex", pd.Series([""])).iloc[0],
                        "item_status": group.get("Item Status", pd.Series([""])).iloc[0]
                    }
                
                # Valores base
                base_values = {}
                for part, group in df[df["Organization"] == base_org].groupby("Part Number"):
                    if not group.empty:
                        base_values[part] = group["Serial Control Normalized"].iloc[0]
                
                # Valores seriales
                serial_values = defaultdict(dict)
                for _, row in df.iterrows():
                    part = row["Part Number"]
                    org = row["Organization"]
                    value = row["Serial Control Normalized"]
                    serial_values[part][org] = value
                    
                # Partes únicas
                unique_parts = df["Part Number"].unique()
                logger.info(f"Procesando {len(unique_parts)} partes únicas")
            
            # 2. Procesamiento por lotes (batching)
            # Calcular tamaño de lote óptimo
            batch_size = min(500, max(50, len(unique_parts) // (os.cpu_count() * 2 or 4)))
            
            # Dividir en lotes
            part_batches = [unique_parts[i:i+batch_size] for i in range(0, len(unique_parts), batch_size)]
            
            logger.info(f"Dividiendo procesamiento en {len(part_batches)} lotes de ~{batch_size} partes")
            
            # 3. Función para procesar un lote
            def process_batch(batch_parts):
                local_mismatched = []
                local_comparison = []
                local_patterns = {}
                
                for part_number in batch_parts:
                    # Obtener valores seriales para esta parte
                    part_values = serial_values.get(part_number, {})
                    base_serial = base_values.get(part_number, "Not found in base org")
                    
                    # Construir patrón para análisis
                    part_pattern = {"base": base_serial}
                    for org in org_destination:
                        if org in part_values:
                            part_pattern[org] = part_values[org]
                        else:
                            part_pattern[org] = "Not found"
                    
                    # Recolectar valores válidos para detectar mismatch
                    valid_part_values = set()
                    for org in org_destination:
                        if org in part_values:
                            value = part_values[org]
                            if value in valid_values:
                                valid_part_values.add(value)
                    
                    # Verificar mismatch
                    has_mismatch = len(valid_part_values) > 1
                    
                    # Registrar patrón para análisis posterior
                    pattern_key = tuple(sorted((org, value) for org, value in part_pattern.items()))
                    
                    if pattern_key not in local_patterns:
                        local_patterns[pattern_key] = {
                            "parts": [],
                            "count": 0
                        }
                    
                    local_patterns[pattern_key]["parts"].append({
                        "part_number": part_number,
                        "info": part_info.get(part_number, {})
                    })
                    local_patterns[pattern_key]["count"] += 1
                    
                    # Si hay mismatch, registrar la parte
                    if has_mismatch:
                        local_mismatched.append(part_number)
                        
                        # Seleccionar una organización representativa
                        rep_org = next((org for org in org_destination if org in part_values), org_destination[0])
                        rep_value = part_values.get(rep_org, "Not found")
                        
                        # Crear registro de comparación
                        local_comparison.append({
                            'part_number': part_number,
                            'organization': rep_org,
                            'serial_control': rep_value,
                            'base_serial': base_serial,
                            'has_mismatch': True,
                            'item_status': part_info.get(part_number, {}).get('item_status', ''),
                            'manufacturer': part_info.get(part_number, {}).get('manufacturer', ''),
                            'description': part_info.get(part_number, {}).get('description', ''),
                            'vertex': part_info.get(part_number, {}).get('vertex', ''),
                            'status': 'Mismatch'
                        })
                
                return local_mismatched, local_comparison, local_patterns
            
            # 4. Procesamiento paralelo de lotes
            with ThreadPoolExecutor(max_workers=min(os.cpu_count() * 2 or 4, 8)) as executor:
                batch_results = list(executor.map(process_batch, part_batches))
            
            # 5. Combinar resultados
            for local_mismatched, local_comparison, local_patterns in batch_results:
                mismatched_parts.extend(local_mismatched)
                comparison_data.extend(local_comparison)
                
                # Combinar patrones
                for pattern, data in local_patterns.items():
                    if pattern not in pattern_registry:
                        pattern_registry[pattern] = {
                            "parts": [],
                            "count": 0
                        }
                    pattern_registry[pattern]["parts"].extend(data["parts"])
                    pattern_registry[pattern]["count"] += data["count"]
            
            # 6. Analizar patrones sospechosos
            suspicious_patterns = []
            logger.info("Analizando patrones...")
            
            for pattern, data in pattern_registry.items():
                pattern_dict = dict(pattern)
                
                # Detectar patrón donde solo una org tiene serial control
                orgs_with_serial = [
                    org for org, value in pattern_dict.items()
                    if value in valid_values and org != "base"
                ]
                
                if len(orgs_with_serial) == 1:
                    # Patrón sospechoso: solo una org tiene control serial
                    suspicious_patterns.append({
                        'pattern': pattern_dict,
                        'affected_parts': data['parts'],
                        'count': data['count']
                    })
                    
                    # Log reducido, solo los patrones más significativos
                    if data['count'] >= 10:
                        logger.info(f"Patrón sospechoso: {data['count']} partes con control serial solo en org {orgs_with_serial[0]}")
            
            # 7. Finalización y medición
            elapsed_time = time.time() - start_time
            
            # Eliminar duplicados en mismatched_parts
            mismatched_parts = list(set(mismatched_parts))
            
            logger.info(f"Procesamiento completado en {elapsed_time:.2f} segundos")
            logger.info(f"Total partes: {len(unique_parts)}, Mismatches: {len(mismatched_parts)}")
            
            return {
                'mismatched_parts': mismatched_parts,
                'data': comparison_data,
                'summary': {
                    'total_parts': len(unique_parts),
                    'total_mismatches': len(mismatched_parts),
                    'suspicious_patterns': suspicious_patterns,
                    'processing_time': elapsed_time
                }
            }

        except Exception as e:
            logger.error(f"Error checking serial control: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            raise
        
    def _check_inventory_for_mismatches(
        self, 
        mismatched_parts: List[str], 
        df: pd.DataFrame,
        org_destination: List[str], 
        inventory_df: Optional[pd.DataFrame]
    ) -> Dict:
        """
        Versión optimizada que utiliza Polars y procesamiento paralelo por lotes.
        
        Args:
            mismatched_parts: Lista de partes con discrepancias
            df: DataFrame principal de auditoría
            org_destination: Lista de organizaciones a verificar
            inventory_df: DataFrame opcional con datos de inventario
        
        Returns:
            Dict con resultados del análisis de inventario
        """
        
        start_time = time.time()
        logger.info("=== INICIO CHECK INVENTORY (OPTIMIZED) ===")
        
        # Validaciones iniciales (igual que antes)
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise ValueError("DataFrame principal vacío o inválido")
            
        if not org_destination:
            raise ValueError("Lista de organizaciones destino vacía")

        # 1. Normalizar datos de entrada
        all_parts = set(df['Part Number'].unique())
        mismatched_set = set(str(part).strip().upper() for part in (mismatched_parts or []))
        normalized_orgs = [str(org).strip().zfill(2) for org in org_destination]
        
        total_parts = len(all_parts)
        logger.info(f"Total de partes en archivo: {total_parts}")
        logger.info(f"Partes con discrepancias: {len(mismatched_set)}")
        logger.info(f"Organizaciones a revisar: {len(normalized_orgs)}")

        # 2. Inicializar matcher y cargar inventario (igual que antes)
        matcher = InventoryMatcher()
        if inventory_df is not None:
            # Normalizar columnas
            column_mappings = {col: col for col in inventory_df.columns}
            inventory_df.columns = [
                col.strip().upper().replace(' ', '_') 
                for col in inventory_df.columns
            ]
            
            # Cargar inventario
            matcher.load_inventory(inventory_df, column_mappings)
        
        # 3. Preparar estructuras para resultados
        results = {}
        parts_with_inventory = set()
        total_inventory_records = 0
        
        # 4. Definir el tamaño de lote óptimo
        # - Usar número de CPUs disponibles para escalar
        # - Limitar tamaño máximo para control de memoria
        cpu_count = os.cpu_count() or 4
        batch_size = min(500, max(50, total_parts // (cpu_count * 2)))
        
        # 5. Dividir partes en lotes para procesamiento paralelo
        part_batches = [list(all_parts)[i:i+batch_size] for i in range(0, total_parts, batch_size)]
        num_batches = len(part_batches)
        
        logger.info(f"Procesando {total_parts} partes en {num_batches} lotes de ~{batch_size} items cada uno")
        
        # 6. Función para procesar un lote de partes
        def process_batch(batch_parts, batch_idx):
            batch_results = {}
            batch_inventory_parts = set()
            batch_inventory_records = 0
            
            for part_idx, part_number in enumerate(batch_parts):
                part_clean = str(part_number).strip().upper()
                is_mismatched = part_clean in mismatched_set
                part_has_inventory = False
                
                # Procesar cada organización para esta parte
                for org in normalized_orgs:
                    key = f"{part_clean}_{org}"
                    
                    # Obtener información de inventario
                    match_result = matcher.check_inventory(part_clean, org)
                    
                    # Crear resultado para esta combinación parte/organización
                    batch_results[key] = {
                        'part_number': part_clean,
                        'organization': org,
                        'quantity': match_result.quantity,
                        'has_inventory': match_result.has_inventory,
                        'has_mismatch': is_mismatched,
                        'value': match_result.value,
                        'subinventory': match_result.subinventory,
                        'warehouse_code': match_result.warehouse_code,
                        'aging_info': {
                            'aging_0_30': match_result.aging_info.days_0_30,
                            'aging_31_60': match_result.aging_info.days_31_60,
                            'aging_61_90': match_result.aging_info.days_61_90
                        }
                    }
                    
                    # Registrar errores si existen
                    if match_result.error_message:
                        batch_results[key]['error'] = match_result.error_message
                    
                    # Actualizar estadísticas si hay inventario
                    if match_result.quantity > 0:
                        part_has_inventory = True
                        batch_inventory_records += 1
                
                # Registrar parte con inventario
                if part_has_inventory:
                    batch_inventory_parts.add(part_clean)
                
                # Log de progreso más espaciado (cada 20% del lote)
                if part_idx % max(1, len(batch_parts) // 5) == 0 and part_idx > 0:
                    logger.debug(f"Lote {batch_idx+1}/{num_batches}: Procesadas {part_idx}/{len(batch_parts)} partes")
            
            gc.collect()
            # Retornar resultados del lote
            return {
                'batch_results': batch_results,
                'batch_inventory_parts': batch_inventory_parts,
                'batch_inventory_records': batch_inventory_records
            }
        
        # 7. Ejecutar procesamiento en paralelo
        # - Usar ThreadPoolExecutor para paralelización controlada
        # - Limitar workers para evitar saturación
        max_workers = min(cpu_count * 2, 16)  # Evitar excesivos threads
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Crear futuros para cada lote
            futures = {
                executor.submit(process_batch, batch, idx): idx 
                for idx, batch in enumerate(part_batches)
            }
            
            # Procesar resultados a medida que se completan
            for future in as_completed(futures):
                batch_idx = futures[future]
                try:
                    batch_data = future.result()
                    
                    # Actualizar resultados globales
                    results.update(batch_data['batch_results'])
                    parts_with_inventory.update(batch_data['batch_inventory_parts'])
                    total_inventory_records += batch_data['batch_inventory_records']
                    
                    # Log de progreso por lote
                    logger.info(f"Completado lote {batch_idx+1}/{num_batches}")
                    
                    # Liberar memoria del batch procesado
                    del batch_data
                    gc.collect()
                    
                except Exception as e:
                    logger.error(f"Error en lote {batch_idx+1}: {str(e)}")
        
        # 8. Generar resumen
        summary = {
            'total_parts': total_parts,
            'parts_with_mismatch': len(mismatched_set),
            'parts_without_mismatch': total_parts - len(mismatched_set),
            'parts_with_inventory': len(parts_with_inventory),
            'total_inventory_records': total_inventory_records,
            'processing_time': time.time() - start_time
        }
        
        logger.info("=== RESUMEN DE INVENTARIO ===")
        for key, value in summary.items():
            logger.info(f"{key}: {value}")
        
        results['summary'] = summary
        return results
        
    def _validate_non_hardware_parts(self, df: pd.DataFrame) -> Dict:
        """Versión optimizada para validar partes non-hardware usando operaciones vectorizadas"""
        try:
            # 1. Filtrar de manera vectorizada
            non_hardware_mask = ~df['Vertex'].str.contains('Hardware', case=False, na=False)
            serial_control_yes = df['Serial Control'].str.upper() == 'YES'
            
            # 2. Combinar filtros en una sola operación
            combined_mask = non_hardware_mask & serial_control_yes
            
            # 3. Extraer partes únicas directamente
            non_hardware_parts = df.loc[combined_mask, 'Part Number'].unique().tolist()

            return {
                'non_hardware_parts': non_hardware_parts,
                'total_issues': len(non_hardware_parts)
            }
        except Exception as e:
            logger.error(f"Error validating non-hardware parts: {str(e)}")
            return {'non_hardware_parts': [], 'total_issues': 0}
    
    def _check_vertex_consistency(self, df: pd.DataFrame) -> Dict:
        """
        Versión optimizada de _check_vertex_consistency usando operaciones vectorizadas
        """
        try:
            issues = []
            
            # Usar groupby más eficiente con Pandas
            vertex_counts = df.groupby('Part Number')['Vertex'].nunique()
            inconsistent_parts = vertex_counts[vertex_counts > 1].index.tolist()
            
            # Solo procesar partes con inconsistencia
            if inconsistent_parts:
                # Procesar en un solo paso para reducir iteraciones
                inconsistent_data = df[df['Part Number'].isin(inconsistent_parts)]
                
                # Agrupar y agregar de manera eficiente
                for part_number, group in inconsistent_data.groupby('Part Number'):
                    vertex_values = group['Vertex'].unique()
                    
                    issues.append({
                        'part_number': part_number,
                        'vertex_values': list(vertex_values),
                        'type': 'Vertex Inconsistency',
                        'description': f"Multiple Vertex values found: {', '.join(vertex_values)}"
                    })

            return {
                'issues': issues,
                'total_issues': len(issues)
            }

        except Exception as e:
            logger.error(f"Error checking vertex consistency: {str(e)}")
            return {'issues': [], 'total_issues': 0}  # Fallback silencioso

    
    def _process_org_mismatch_audit(self, df: pd.DataFrame, program_reqs: Dict) -> Dict:
        """Optimized Organization Mismatch Audit with batch processing and memory optimization"""
        try:
            import gc  # Para limpieza de memoria
            start_time = time.time()
            logger.info("Starting organization mismatch audit (high performance)")
            
            org_destination = program_reqs['org_destination']
            
            # 1. Obtener datos de orgs faltantes una sola vez (ya optimizado)
            missing_orgs_result = self._check_missing_orgs(df, org_destination)
            missing_items = missing_orgs_result['missing_items']
            
            # 2. Pre-calcular capacidad para evitar realocaciones
            estimated_rows = sum(len(item['current_orgs']) + len(item['missing_orgs']) for item in missing_items)
            all_rows = []
            
            # 3. Procesar en batches para conjuntos grandes
            batch_size = 1000
            num_batches = (len(missing_items) + batch_size - 1) // batch_size
            
            for batch_idx in range(num_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(missing_items))
                batch_items = missing_items[start_idx:end_idx]
                
                # Procesar batch completo en una operación
                batch_rows = []
                
                for item in batch_items:
                    # Extraer datos comunes una sola vez
                    part_number = item['part_number']
                    vertex = item.get('vertex_class', '')
                    description = item.get('description', '')
                    missing = item.get('missing_orgs', [])
                    current = item.get('current_orgs', [])
                    org_status = item.get('org_status', {})
                    
                    # Generar registros para orgs existentes
                    for org in current:
                        status = org_status.get(org, 'Active')
                        batch_rows.append({
                            'Part Number': part_number,
                            'Organization': org,
                            'Status': status,
                            'Action Required': 'None' if status == 'Active' else f'Check status in Org {org}',
                            'Vertex': vertex,
                            'Description': description,
                            'Current Orgs': ', '.join(sorted(current)),
                            'Missing Orgs': ', '.join(sorted(missing))
                        })
                    
                    # Generar registros para orgs faltantes
                    for org in missing:
                        batch_rows.append({
                            'Part Number': part_number,
                            'Organization': org,
                            'Status': 'Missing in Org',
                            'Action Required': f"Create in Org {org}",
                            'Vertex': vertex,
                            'Description': description,
                            'Current Orgs': ', '.join(sorted(current)),
                            'Missing Orgs': ', '.join(sorted(missing))
                        })
                
                # Agregar al resultado global
                all_rows.extend(batch_rows)
                
                # Liberar memoria del batch
                del batch_rows
                del batch_items
                gc.collect()
                
                # Log de progreso
                if num_batches > 1:
                    logger.info(f"Processed batch {batch_idx+1}/{num_batches}, rows so far: {len(all_rows)}")
            
            # 4. Crear DataFrame una sola vez al final (más eficiente)
            result_df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame([])
            
            # 5. Ejecutar vertex_issues de manera más eficiente
            vertex_issues = self._check_vertex_consistency(df)
            
            # Preparar resultado
            result = {
                'data': result_df,
                'ftp_upload': {'data': [], 'filename': ''},
                'summary': {
                    'total_missing_orgs': len(result_df[result_df['Status'] == 'Missing in Org']) if not result_df.empty else 0,
                    'total_vertex_issues': len(vertex_issues.get('issues', [])),
                    'processing_time': time.time() - start_time
                }
            }
            
            logger.info(f"Organization mismatch audit completed in {time.time() - start_time:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"Error in org mismatch audit: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            raise

    def _check_missing_orgs(self, df: pd.DataFrame, org_destination: List[str]) -> Dict:
        """
        Optimized implementation for checking missing organizations.
        Compatible with Polars 1.26+
        
        Args:
            df: DataFrame with audit data
            org_destination: List of target organizations
            
        Returns:
            Dict with missing organizations analysis
        """
        try:

            
            start_time = time.time()
            logger.info("=== CHECKING MISSING ORGS (OPTIMIZED) ===")
            
            # 1. Input Normalization - Single normalized copy of org_destination
            normalized_org_dest = sorted([str(org).strip().zfill(2) for org in org_destination])
            org_dest_set = set(normalized_org_dest)  # For O(1) lookup
            
            # 2. Convert input to Polars and keep conversion as lazy as possible
            if isinstance(df, pd.DataFrame):
                # Convert once with schema inference to avoid redundant analysis
                df_pl = pl.from_pandas(df, include_index=False)
            else:
                df_pl = df
                
            # 3. Use simplified non-lazy approach first to avoid version compatibility issues
            # For Polars 1.26, we'll use direct expressions rather than method chaining
            
            # Get available columns
            available_columns = set(df_pl.columns)
            
            # Normalize Organization column using Polars 1.26 compatible syntax
            df_pl = df_pl.with_columns([
                # Convert Organization to string and normalize
                pl.col("Organization")
                .cast(pl.Utf8)
                .fill_null("")
                .alias("org_raw")
            ])
            
            # Manually apply strip and zfill since str namespace has changed
            df_pl = df_pl.with_columns([
                pl.col("org_raw").str.strip_chars().alias("org_stripped")
            ])
            
            # In Polars 1.26, create a UDF for zfill since it might not be available directly
            df_pl = df_pl.with_columns([
                  pl.col("org_stripped").str.zfill(2).alias("org_normalized")
                ])
            
            # Keep Part Number for grouping
            df_pl = df_pl.with_columns([
                pl.col("Part Number").alias("part_key")
            ])
            
            # 4. Select and prepare columns directly
            select_columns = ["part_key", "org_normalized"]
            
            # Add metadata columns if they exist
            if "Vertex" in available_columns:
                select_columns.append("Vertex")
            elif "VERTEX PRODUCT CLASS" in available_columns:
                select_columns.append("VERTEX PRODUCT CLASS")
                
            if "Description" in available_columns:
                select_columns.append("Description")
            elif "DESCRIPTION" in available_columns:
                select_columns.append("DESCRIPTION")
                
            if "Status" in available_columns:
                select_columns.append("Status")
            elif "ITEM STATUS" in available_columns:
                select_columns.append("ITEM STATUS")
            
            # Select only needed columns to reduce memory usage
            df_pl = df_pl.select(select_columns)
            
            # 5. Group and aggregate data
            logger.debug("Calculating part information")
            
            # Process part metadata by grouping
            agg_expressions = [
                pl.col("org_normalized").unique().alias("current_orgs")
            ]
            
            # Add metadata columns to aggregation if they exist
            if "Vertex" in df_pl.columns:
                agg_expressions.append(pl.col("Vertex").first().alias("vertex_class"))
            elif "VERTEX PRODUCT CLASS" in df_pl.columns:
                agg_expressions.append(pl.col("VERTEX PRODUCT CLASS").first().alias("vertex_class"))
                
            if "Description" in df_pl.columns:
                agg_expressions.append(pl.col("Description").first().alias("description"))
            elif "DESCRIPTION" in df_pl.columns:
                agg_expressions.append(pl.col("DESCRIPTION").first().alias("description"))
                
            if "Status" in df_pl.columns:
                agg_expressions.append(pl.col("Status").first().alias("status"))
            elif "ITEM STATUS" in df_pl.columns:
                agg_expressions.append(pl.col("ITEM STATUS").first().alias("status"))
            
            # Perform group by and aggregation
            part_info = df_pl.group_by("part_key").agg(agg_expressions)
            
            # 6. Convert to Python structures for final processing
            logger.debug("Processing missing organizations")
            
            # Convert to dictionaries
            part_records = part_info.to_dicts()
            missing_items = []
            
            # Process each part
            for record in part_records:
                part_key = record["part_key"]
                
                # Default empty values for safety
                current_orgs = set(record.get("current_orgs", []))
                vertex_class = record.get("vertex_class", "")
                description = record.get("description", "")
                status = record.get("status", "Active")
                
                # Calculate missing orgs using set operations
                missing_orgs = sorted(list(set(normalized_org_dest) - current_orgs))
                current_orgs_filtered = sorted(list(current_orgs & org_dest_set))
                
                # Only include if relevant
                if not (missing_orgs or current_orgs_filtered):
                    continue
                    
                # Create org status mapping
                org_status = {org: status for org in current_orgs}
                    
                # Create result record
                missing_items.append({
                    'part_number': part_key,
                    'missing_orgs': missing_orgs,
                    'current_orgs': current_orgs_filtered,
                    'org_status': org_status,
                    'vertex_class': vertex_class,
                    'description': description
                })
            
            # 7. Create final result
            elapsed_time = time.time() - start_time
            
            result = {
                'missing_items': missing_items,
                'total_missing': len(missing_items),
                'processing_time': elapsed_time
            }
            
            logger.info(f"_check_missing_orgs completed in {elapsed_time:.2f} seconds")
            logger.info(f"Total parts with missing orgs: {len(missing_items)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in optimized _check_missing_orgs: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            
            # Fallback to simplified pandas implementation
            return self._check_missing_orgs_fallback(df, org_destination)
        
    def _check_missing_orgs_fallback(self, df: pd.DataFrame, org_destination: List[str]) -> Dict:
        """Reliable fallback implementation for missing orgs detection"""
        start_time = time.time()
        logger.warning("Using fallback implementation for missing orgs check")
        
        # Normalize org_destination once
        normalized_org_dest = [str(org).strip().zfill(2) for org in org_destination]
        missing_items = []
        
        # Process each part group
        for part_number, part_group in df.groupby('Part Number'):
            # Extract and normalize orgs in a single pass
            orgs = set()
            org_status = {}
            
            for _, row in part_group.iterrows():
                org = str(row['Organization']).strip().zfill(2)
                orgs.add(org)
                org_status[org] = row.get('Status', 'Active')
            
            # Calculate missing orgs efficiently
            missing_orgs = [org for org in normalized_org_dest if org not in orgs]
            current_orgs = [org for org in normalized_org_dest if org in orgs]
            
            # Only include if there are orgs to report
            if missing_orgs or current_orgs:
                # Extract metadata once
                vertex_class = ""
                description = ""
                
                if 'Vertex' in part_group.columns and not part_group['Vertex'].empty:
                    vertex_class = part_group['Vertex'].iloc[0]
                elif 'VERTEX PRODUCT CLASS' in part_group.columns and not part_group['VERTEX PRODUCT CLASS'].empty:
                    vertex_class = part_group['VERTEX PRODUCT CLASS'].iloc[0]
                    
                if 'Description' in part_group.columns and not part_group['Description'].empty:
                    description = part_group['Description'].iloc[0]
                elif 'DESCRIPTION' in part_group.columns and not part_group['DESCRIPTION'].empty:
                    description = part_group['DESCRIPTION'].iloc[0]
                
                missing_items.append({
                    'part_number': part_number,
                    'missing_orgs': missing_orgs,
                    'current_orgs': current_orgs,
                    'org_status': org_status,
                    'vertex_class': vertex_class,
                    'description': description
                })
        
        elapsed_time = time.time() - start_time
        logger.info(f"Fallback missing orgs check completed in {elapsed_time:.2f} seconds")
        
        return {
            'missing_items': missing_items,
            'total_missing': len(missing_items),
            'processing_time': elapsed_time
        }
    

    def _process_other_attributes_audit(
            self,
            df: pd.DataFrame,
            program_reqs: Dict
        ) -> Dict:
            """Process Other Attributes Audit (Customer ID, Cross Reference)"""
            try:
                results = []
                contract = program_reqs['contract']

                # Customer ID Checks (quitar awaits)
                if contract in ['Charter', 'Cox Comm', 'CoxRev', 'FCBK', 'ORCL']:
                    customer_results = self._check_customer_ids(
                        df, program_reqs['org_destination']
                    )
                    results.extend(customer_results['issues'])

                # Cross Reference Checks (quitar await)
                if contract == 'ORCL':
                    cross_ref_results = self._check_cross_references(df)
                    results.extend(cross_ref_results['issues'])

                # Convert results to DataFrame
                results_df = pd.DataFrame(results) if results else pd.DataFrame()
                
                return {
                    'data': results_df,
                    'summary': {
                        'total_customer_id_issues': len(customer_results['issues']) if 'customer_results' in locals() else 0,
                        'total_cross_ref_issues': len(cross_ref_results['issues']) if 'cross_ref_results' in locals() else 0,
                    }
                }

            except Exception as e:
                logger.error(f"Error in other attributes audit: {str(e)}")
                raise

    def _check_customer_ids(self, df: pd.DataFrame, org_destination: List[str]) -> Dict:
        try:
            issues = []
            
            for part_number in df['Part Number'].unique():
                part_data = df[df['Part Number'] == part_number]
                
                for org in org_destination:
                    org_data = part_data[part_data['Organization'] == org]  # Cambiado de 'Organization Code'
                    if not org_data.empty:
                        customer_id = org_data['Customer ID'].iloc[0]
                        
                        if pd.isna(customer_id) or not self._validate_customer_id_format(customer_id):
                            issues.append({
                                'Part Number': part_number,
                                'Organization': org,
                                'Issue Type': 'Customer ID',
                                'Current Value': customer_id if not pd.isna(customer_id) else 'Missing',
                                'Status': 'Invalid Customer ID',
                                'Action Required': 'Update Customer ID'
                            })
            
            return {
                'issues': issues,
                'total_issues': len(issues)
            }
            
        except Exception as e:
            logger.error(f"Error checking customer IDs: {str(e)}")
            raise

    def _validate_customer_id_format(self, customer_id: str) -> bool:
        """Validate Customer ID format according to program requirements"""
        if pd.isna(customer_id):
            return False
            
        customer_id = str(customer_id).strip().upper()
        
        # Implementar reglas específicas de validación
        # Por ejemplo: debe tener un prefijo específico, longitud mínima, etc.
        valid_prefixes = ['CHTR-', 'COX-', 'FB-', 'ORC-']
        min_length = 8
        
        return any(customer_id.startswith(prefix) for prefix in valid_prefixes) and len(customer_id) >= min_length

    def _check_cross_references(self, df: pd.DataFrame) -> Dict:
        """Check Cross References for Oracle items"""
        try:
            issues = []
            
            oracle_mask = df['Vendor Name'].str.contains('ORACLE', case=False, na=False)
            oracle_parts = df[oracle_mask]
            
            for _, row in oracle_parts.iterrows():
                # Verificar Oracle Marketing Part
                if pd.isna(row.get('Oracle MKTG Part')):
                    issues.append({
                        'Part Number': row['Part Number'],
                        'Organization': row['Organization'],
                        'Issue Type': 'Cross Reference',
                        'Missing Field': 'Oracle MKTG Part',
                        'Status': 'Missing Cross Reference',
                        'Action Required': 'Add Oracle Marketing Part'
                    })
                
                # Verificar Oracle Vendor Number
                if pd.isna(row.get('Oracle Vendor Number')):
                    issues.append({
                        'Part Number': row['Part Number'],
                        'Organization': row['Organization'],
                        'Issue Type': 'Cross Reference',
                        'Missing Field': 'Oracle Vendor Number',
                        'Status': 'Missing Cross Reference',
                        'Action Required': 'Add Oracle Vendor Number'
                    })
            
            return {
                'issues': issues,
                'total_issues': len(issues)
            }
            
        except Exception as e:
            logger.error(f"Error checking cross references: {str(e)}")
            raise
        
        
    def _combine_audit_results(
        self,
        serial_results: pd.DataFrame,
        org_results: pd.DataFrame,
        other_results: pd.DataFrame
    ) -> pd.DataFrame:
        try:
            # Primero mostramos las columnas disponibles para diagnóstico
            print("Available columns in DataFrames:")
            if not serial_results.empty:
                print(f"Serial results columns: {serial_results.columns.tolist()}")
            if not org_results.empty:
                print(f"Org results columns: {org_results.columns.tolist()}")
            if not other_results.empty:
                print(f"Other results columns: {other_results.columns.tolist()}")

            # Si no hay datos que combinar, retornamos un DataFrame vacío
            if serial_results.empty and org_results.empty and other_results.empty:
                return pd.DataFrame()

            # Definimos las columnas que siempre deben estar presentes
            common_columns = [
                'Part Number',
                'Organization',
                'Status',
                'Action Required'
            ]

            # Definimos todas las columnas que queremos de los resultados seriales
            desired_serial_columns = common_columns + [
                'Serial Control',
                'Base Org Serial Control',
                'On Hand Quantity',
                'Is Hardware',
                'has_inventory',
                'Value',
                'Subinventory Code',
                'Warehouse Code',
                'Aging_0_30',
                'Aging_31_60',
                'Aging_61_90',
            ]

            df_list = []

            if not serial_results.empty:
                # Verificamos qué columnas están realmente disponibles
                available_columns = [col for col in desired_serial_columns 
                                if col in serial_results.columns]
                
                # Creamos una copia con solo las columnas disponibles
                temp_serial = serial_results[available_columns].copy()
                
                # Logging para debug
                print("\nProcesando resultados seriales:")
                print(f"Columnas solicitadas: {desired_serial_columns}")
                print(f"Columnas disponibles: {available_columns}")
                
                df_list.append(temp_serial)

            # Procesamos los resultados de organización
            if not org_results.empty:
                org_columns = common_columns + [
                    'Vertex',
                    'Description'
                ]
                temp_org = org_results[org_columns].copy()
                df_list.append(temp_org)

            # Procesamos otros resultados
            if not other_results.empty:
                other_columns = common_columns + [
                    'Issue Type',
                    'Current Value'
                ]
                temp_other = other_results[other_columns].copy()
                df_list.append(temp_other)

            if not df_list:
                return pd.DataFrame()

            # Combinamos todos los resultados
            combined_df = pd.concat(df_list, ignore_index=True)
            
            # Verificamos las columnas críticas al final
            print("\nColumnas en el resultado final:", combined_df.columns.tolist())
            
            # Aseguramos que las columnas críticas existan
            critical_columns = {
                'has_inventory': False,  # valor por defecto para columna booleana
                'On Hand Quantity': 0.0,  # valor por defecto para cantidad
                'Value': 0.0  # valor por defecto para valor
            }
            
            for col, default_value in critical_columns.items():
                if col not in combined_df.columns:
                    print(f"Agregando columna faltante: {col}")
                    combined_df[col] = default_value

            return combined_df

        except Exception as e:
            logger.error(f"Error combining audit results: {str(e)}")
            logger.error(f"Error details: {e.__class__.__name__}")
            raise
    
    
    def generate_audit_summary(self, df: pd.DataFrame) -> Dict:
        """
        Generate summary of audit findings.
        
        Args:
            df: Combined audit results DataFrame
            
        Returns:
            Dict containing audit summary statistics
        """
        try:
            # Usar nombre mapeado 'Part Number'
            summary = {
                'total_items_checked': len(df['Part Number'].unique()),
                'total_issues': len(df),
                'issues_by_type': {},
                'issues_by_org': {},
                'severity_breakdown': {
                    'critical': 0,
                    'major': 0,
                    'minor': 0
                }
            }
            # Count issues by type
            status_counts = df['Status'].value_counts()
            summary['issues_by_type'] = status_counts.to_dict()

            # Count issues by organization
            org_counts = df['Organization'].value_counts()
            summary['issues_by_org'] = org_counts.to_dict()

            # Categorize issues by severity
            # Serial Control mismatches are critical
            summary['severity_breakdown']['critical'] = len(
                df[df['Status'] == 'Mismatch']
            )
            # Missing orgs are major
            summary['severity_breakdown']['major'] = len(
                df[df['Status'] == 'Missing in Org']
            )
            # Other issues are minor
            summary['severity_breakdown']['minor'] = len(df[
                ~df['Status'].isin(['Mismatch', 'Missing in Org'])
            ])

            # Add timestamp
            summary['timestamp'] = datetime.now().isoformat()

            return summary

        except Exception as e:
            logger.error(f"Error generating audit summary: {str(e)}")
            raise

    def _convert_to_audit_items(
            self, 
            df: pd.DataFrame, 
            org_destination: List[str], 
            audit_df: pd.DataFrame,
            inventory_df: Optional[pd.DataFrame] = None,
            existing_inventory_check: Optional[Dict] = None
        ) -> List[AuditItem]:
            """
            Convierte los resultados de auditoría a AuditItems preservando todas las organizaciones y estados
            """
            print("\n=== INICIO DE CONVERSIÓN A AUDIT ITEMS ===")
            print(f"Organizaciones destino: {org_destination}")
            print("\n=== COLUMNAS DISPONIBLES ===")
            print(df.columns.tolist())
                
            # Validar org_destination
            if not org_destination:
                logger.error("org_destination está vacío o es None")
                raise ValueError("org_destination no puede estar vacío")
            
            # Buscar la columna de inventario
            inventory_column = next(
                (col for col in df.columns if 'inventory' in col.lower()),
                None
            )
            print(f"Columna de inventario encontrada: {inventory_column}")
            
            # Inicializar inventory_data
            inventory_data = {}
            inventory_check_results = None  
            original_summary = None  
            
            if existing_inventory_check:  # Usar resultados existentes si están disponibles
                inventory_check_results = existing_inventory_check
                original_summary = inventory_check_results.get('summary', {})
            elif inventory_df is not None:
                inventory_check_results = self._check_inventory_for_mismatches(
                    mismatched_parts=df[df['Status'] == 'Mismatch']['Part Number'].unique().tolist(),
                    df=df,
                    org_destination=org_destination,
                    inventory_df=inventory_df
                )

            if inventory_check_results:  # ✅ Verificación crítica
                original_summary = inventory_check_results.get('summary', {})
                print(f"Original Inventory Summary: {original_summary}")

                for key, result in inventory_check_results.items(): 
                    if key != 'summary':
                        part_org = key.rsplit('_', 1)  
                        if len(part_org) != 2:
                            logger.error(f"Formato inválido para key: {key}")
                            continue
                        part_number, organization = part_org
                        inventory_data[key] ={
                            'quantity': result['quantity'],
                            'has_inventory': result['has_inventory'],
                            'value': result.get('value', 0.0),
                            'subinventory': result.get('subinventory'),  
                            'warehouse_code': result.get('warehouse_code'),  
                            'aging_info': result.get('aging_info', {  
                                'days_0_30': 0.0,
                                'days_31_60': 0.0,
                                'days_61_90': 0.0
                            })
                        }
                        
            # Análisis de organizaciones
            missing_orgs_result = self._check_missing_orgs(audit_df, org_destination)
            print(f"\nItems con análisis de organizaciones: {len(missing_orgs_result['missing_items'])}")
            
            # Creación del mapa de estados
            org_status_map = {}
            for item in missing_orgs_result['missing_items']:
                part_number = item['part_number']
                org_status_map[part_number] = {
                    'missing_orgs': item['missing_orgs'],
                    'current_orgs': item['current_orgs'],
                    'org_status': item['org_status'],
                    'base_info': next((row for _, row in audit_df[audit_df['Part Number'] == part_number].iterrows()), {})
                }
            
            print(f"\nNúmeros de parte en el mapa de estados: {len(org_status_map)}")
            if org_status_map:
                ejemplo_parte = next(iter(org_status_map))
                print(f"\nEjemplo para {ejemplo_parte}:")
                print(f"Missing orgs: {org_status_map[ejemplo_parte]['missing_orgs']}")
                print(f"Current orgs: {org_status_map[ejemplo_parte]['current_orgs']}")
                print(f"Org status: {org_status_map[ejemplo_parte]['org_status']}")

            audit_items = []
            print(f"\n=== PROCESANDO {len(df['Part Number'].unique())} NÚMEROS DE PARTE ===")
            
            # Procesamiento por Part Number
            for part_number, part_group in df.groupby('Part Number'):
                print(f"\nProcesando: {part_number}")
                
                base_info = org_status_map.get(part_number, {})
                if not base_info:
                    print(f"Parte no encontrada en mapa, creando nueva entrada...")
                    base_part_data = audit_df[audit_df['Part Number'] == part_number]
                    if not base_part_data.empty:
                        # Obtener organizaciones y normalizar usando Series.str
                        orgs_series = base_part_data['Organization'].astype(str).str.strip()
                        current_orgs = sorted(set(orgs_series.str.zfill(2)))
                        missing_orgs = sorted(set(org_destination) - set(current_orgs))
                        base_info = {
                            'missing_orgs': missing_orgs,
                            'current_orgs': current_orgs,
                            'org_status': {org: 'Active' for org in current_orgs},
                            'base_info': base_part_data.iloc[0].to_dict()
                        }
                        print(f"Nueva entrada creada - Current orgs: {current_orgs}, Missing orgs: {missing_orgs}")

                processed_orgs = set()
                print(f"Organizaciones a procesar: {[row['Organization'] for _, row in part_group.iterrows()]}")
                
                # Procesamiento por organización
                for _, row in part_group.iterrows():
                    organization = str(row['Organization']).strip().zfill(2)
                    if organization in processed_orgs:
                        print(f"Organización {organization} ya procesada, saltando...")
                        continue
                    processed_orgs.add(organization)
                    
                    item_status = row.get('Item Status', '') if 'Item Status' in row else ''

                    
                    if organization in base_info.get('missing_orgs', []):
                        status = 'Missing in Org'
                        action = f"Create in Org {organization}"
                    else:
                        status = base_info.get('org_status', {}).get(organization, 'Active')
                        action = 'None' if status == 'Active' else f'Review status in Org {organization}'
                    
                    print(f"Creando AuditItem para org {organization} - Status: {status}, Action: {action}")
                    print("\n=== DEBUG INVENTORY DATA ===")
                    print(f"Total de registros procesados: {len(df)}")
                    print("Columnas disponibles:", df.columns.tolist())
                        
                    # Antes de crear el InventoryInfo
                    inventory_key = f"{part_number}_{organization}"
                    inv_data = inventory_data.get(inventory_key, {})
                    has_inventory = inv_data.get('has_inventory', False)

                    inventory_info = InventoryInfo(
                        quantity=float(inv_data.get('quantity', 0) or 0),
                        value=float(inv_data.get('value', 0) or 0),
                        subinventory_code=str(inv_data.get('subinventory', '')),
                        warehouse_code=str(inv_data.get('warehouse_code', '')),
                        aging_info=InventoryAgingInfo()
                    )

                    # Actualizar aging usando el mapeo
                    aging_info = inv_data.get('aging_info', {})
                    if aging_info:
                        inventory_info.aging_info.update_from_aging_values({
                            'Aging 0-30 Quantity': aging_info.get('days_0_30'),
                            'Aging 31-60 Quantity': aging_info.get('days_31_60'),
                            'Aging 61-90 Quantity': aging_info.get('days_61_90')
                        })

                    print(f"Debug - Creando InventoryInfo para {part_number} en org {organization}:")
                    print(f"  Has Inventory flag: {has_inventory}")
                    print(f"  On Hand Quantity: {inventory_info.quantity}")
                    print(f"  Value: {inventory_info.value}")
                    print(f"  InventoryInfo creado:")
                    print(f"    quantity: {inventory_info.quantity}")
                    print(f"    has_stock: {inventory_info.has_stock}")
                    print(f"    aging_info: {inventory_info.aging_info.__dict__}")

                    # Crear SerialControlInfo
                    serial_control_info = SerialControlInfo(
                        current_value=str(row.get('Serial Control', 'Not found')),
                        is_active=bool(row.get('Serial Control', False))
                    )
                                    
                    item = AuditItem(
                        part_number=part_number,
                        organization=organization,
                        status=status,
                        item_status= row.get('Item Status',''),
                        action_required=action,
                        current_orgs=base_info.get('current_orgs', []),
                        missing_orgs=base_info.get('missing_orgs', []),
                        serial_control=serial_control_info,
                        inventory_info=inventory_info
                    )                 
                    audit_items.append(item)
                
                # Procesamiento de organizaciones faltantes
                missing_to_process = [org for org in base_info.get('missing_orgs', []) if org not in processed_orgs]
                if missing_to_process:
                    print(f"Procesando {len(missing_to_process)} organizaciones faltantes: {missing_to_process}")
                    
                for missing_org in missing_to_process:
                    print(f"Creando AuditItem para org faltante {missing_org}")
                    
                    item_status = ''
                    if 'base_info' in base_info and isinstance(base_info['base_info'], dict):
                        item_status = base_info['base_info'].get('Item Status', '')
                    
                    serial_control_info = SerialControlInfo(
                        current_value="Not found",
                        is_active=False
                    )
                    
                    # 2. Creamos el InventoryInfo para org faltante
                    inventory_info = InventoryInfo(
                        quantity=0.0)
                    
                    
                    item = AuditItem(
                        part_number=part_number,
                        organization=missing_org,
                        status='Missing in Org',
                        item_status= item_status,
                        action_required=f"Create in Org {missing_org}",
                        current_orgs=base_info.get('current_orgs', []),
                        missing_orgs=base_info.get('missing_orgs', []),
                        serial_control=serial_control_info,
                        inventory_info=inventory_info
                    )
                    audit_items.append(item)
                    
            # Justo antes del bloque final de validación
            print("\n=== DEBUG ANTES DE VALIDACIÓN FINAL ===")
            print(f"original_summary exists: {original_summary is not None}")
            if original_summary:
                print(f"total_inventory_records en original_summary: {original_summary.get('total_inventory_records')}")
            print(f"Número de audit_items creados: {len(audit_items)}")
            print(f"Items con inventory_info: {len([item for item in audit_items if item.inventory_info])}")

            final_stats = {
                'total_parts': len(set(item.part_number for item in audit_items)),
                'parts_with_inventory': len([item for item in audit_items if item.inventory_info.has_stock]),
                'total_inventory_records': sum(1 for item in audit_items if item.inventory_info.quantity > 0),
            }

            print("\n=== VALIDACIÓN DE CONSISTENCIA ===")
            if original_summary:
                print(f"Original inventory records: {original_summary.get('total_inventory_records', 'No Data')}")
            else:
                print("Original inventory records: No Data")
            print(f"Final inventory records: {final_stats['total_inventory_records']}")

            if original_summary and final_stats['total_inventory_records'] != original_summary.get('total_inventory_records', 0):
                logger.warning("¡Discrepancia en registros de inventario!")
                logger.warning(f"Original: {original_summary}")
                logger.warning(f"Final: {final_stats}")
            
            return audit_items

    def _read_audit_file(self, file_path: str) -> pd.DataFrame:
        """Read and validate audit file."""
        try:
            logger.info(f"Reading audit file: {file_path}")
            
            # Leer el archivo usando el método combinado, pasando el mapeo
            df = self.repository.validate_and_read_file(
                file_path, 
                is_inventory=False,
                column_mapping=self._config.column_mapping
            )
            
            # Aplicar el mapeo completo después de leer, incluso si viene del caché
            rename_dict = {original: mapped for original, mapped in self._config.column_mapping.items() if original in df.columns}
            if rename_dict:
                df = df.rename(columns=rename_dict)
                logger.info(f"Columnas renombradas después de lectura: {rename_dict}")
            
            # Verificación de columnas críticas
            critical_columns = ['Part Number', 'Organization', 'Serial Control']
            missing_critical = [col for col in critical_columns if col not in df.columns]
            if missing_critical:
                logger.error(f"Columnas críticas faltan después de renombramiento: {missing_critical}")
                raise ValueError(f"Columnas críticas faltan después de renombramiento: {missing_critical}")
            
            logger.debug(f"Columnas finales en DataFrame: {df.columns.tolist()}")
            return df
                
        except Exception as e:
            logger.error(f"Error reading audit file: {str(e)}")
            raise ValueError(f"Error reading audit file: {str(e)}")

    def _read_inventory_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Lee y procesa el archivo de inventario WMS.
        
        Este método maneja específicamente el formato WMS donde:
        - Item Number es el campo clave para coincidencia
        - Las columnas de aging proporcionan información histórica
        - Todas las columnas están en mayúsculas
        """
        if not file_path:
            return None
            
        try:
            logger.info(f"Leyendo archivo de inventario: {file_path}")
            
            # Leer archivo
            is_inventory_format = self.repository._is_inventory_file(Path(file_path))
            df = self.repository.read_excel_file(
                Path(file_path),
                is_inventory=is_inventory_format
            )
            
            # Diagnóstico: Mostrar columnas disponibles
            print("\n=== COLUMNAS EN ARCHIVO DE INVENTARIO ===")
            print("Columnas originales:")
            for col in df.columns:
                print(f"  - {col}")
            
            # Mapeo correcto de columnas usando Item Number
            required_columns = {
                'ITEM NUMBER': 'Part Number',        # Campo clave para coincidencia
                'ORGANIZATION CODE': 'Organization',
                'SUBINVENTORY CODE': 'Subinventory',
                'AGING 0-30 QUANTITY': 'Aging_0_30',  # Agregamos columnas de aging
                'AGING 31-60 QUANTITY': 'Aging_31_60',
                'AGING 61-90 QUANTITY': 'Aging_61_90',
                'QUANTITY': 'Total_Quantity',        # Cantidad total
                'TOTAL VALUE': 'Value'
            }
            
            # Verificar columnas antes del mapeo
            print("\nVerificando columnas requeridas...")
            missing_columns = set(required_columns.keys()) - set(df.columns)
            if missing_columns:
                print("Columnas faltantes:", missing_columns)
                raise ValueError(f"Faltan columnas requeridas: {missing_columns}")
            
            # Renombrar columnas
            df = df.rename(columns=required_columns)
            
            # Convertir columnas numéricas
            numeric_columns = ['Total_Quantity', 'Value', 'Aging_0_30', 'Aging_31_60', 'Aging_61_90']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Diagnóstico final
            print("\nColumnas después del mapeo:")
            for col in df.columns:
                print(f"  - {col}")
            print(f"\nTotal de filas procesadas: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error leyendo archivo de inventario: {str(e)}")
            raise ValueError(f"Error leyendo archivo de inventario: {str(e)}")
        
    def _determine_action_required(self, status: str, missing_orgs: List[str], current_orgs: List[str]) -> str:
        """
        Determina la acción requerida basada en el estado y las organizaciones
        
        Args:
            status: Estado actual del item
            missing_orgs: Lista de organizaciones faltantes
            current_orgs: Lista de organizaciones actuales
            
        Returns:
            str: Descripción de la acción requerida
        """
        if status == 'Missing in Org':
            return f"Create in Org(s): {', '.join(missing_orgs)}"
        elif status == 'Mismatch':
            return f"Review organization status in: {', '.join(current_orgs)}"
        return 'None'
    
    def _generate_detailed_statistics(self, audit_items: List[AuditItem]) -> Dict:
        """
        Genera estadísticas detalladas de los items de auditoría
        """
        stats = {
            'basic': {
                'total_items': len(audit_items),
                'unique_parts': len(set(item.part_number for item in audit_items)),
                'unique_orgs': len(set(item.organization for item in audit_items))
            },
            'serial_control': {
                'total_active': len([item for item in audit_items if item.serial_control.is_active]),
                'by_value': {}
            },
            'inventory': {
                # Corregido: inventory_info en lugar de inventory
                'total_with_stock': len([item for item in audit_items if item.inventory_info.quantity > 0]),
                'total_quantity': sum(item.inventory_info.quantity for item in audit_items),
                'avg_quantity': sum(item.inventory_info.quantity for item in audit_items) / len(audit_items) if audit_items else 0
            },
            'organizations': {
                'missing_count': len([item for item in audit_items if item.status == 'Missing in Org']),
                'active_count': len([item for item in audit_items if item.status == 'Active'])
            }
        }
        
        # Calcular distribución de valores de Serial Control
        for item in audit_items:
            value = item.serial_control.current_value
            stats['serial_control']['by_value'][value] = stats['serial_control']['by_value'].get(value, 0) + 1
        
        return stats

    def _print_detailed_statistics(self, stats: Dict) -> None:
        """
        Imprime las estadísticas detalladas en un formato legible
        
        Args:
            stats: Diccionario con las estadísticas
        """
        print("\n=== RESUMEN DE CONVERSIÓN ===")
        print(f"Total de AuditItems creados: {stats['basic']['total_items']}")
        print(f"Números de parte únicos procesados: {stats['basic']['unique_parts']}")
        print(f"Organizaciones únicas procesadas: {stats['basic']['unique_orgs']}")
        
        print("\n=== ESTADÍSTICAS DE SERIAL CONTROL ===")
        print(f"Total con Serial Control activo: {stats['serial_control']['total_active']}")
        print("\nDistribución de valores de Serial Control:")
        for value, count in stats['serial_control']['by_value'].items():
            print(f"- {value}: {count}")
        
        print("\n=== ESTADÍSTICAS DE INVENTARIO ===")
        print(f"Total con stock: {stats['inventory']['total_with_stock']}")
        print(f"Cantidad total en inventario: {stats['inventory']['total_quantity']:,.2f}")
        print(f"Cantidad promedio por item: {stats['inventory']['avg_quantity']:,.2f}")
        
        print("\n=== ESTADÍSTICAS DE ORGANIZACIONES ===")
        print(f"Total organizaciones faltantes: {stats['organizations']['missing_count']}")
        print(f"Total organizaciones activas: {stats['organizations']['active_count']}")