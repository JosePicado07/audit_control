from logging import Logger
import traceback
from typing import Dict, List
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment 
from domain.entities.audit_entity import AuditResult
from infrastructure.logging.logger import get_logger
from infrastructure.persistence.excel_repository import ExcelRepository
import json

# Configurar logger
logger = get_logger(__name__)

class ReportGenerator:
    # Constantes de formato y estilos
    COLORS = {
        'HEADER': '1F4E78',      # Azul oscuro para encabezados
        'SUCCESS': '4CAF50',     # Verde para estados positivos
        'ERROR': 'FF6B6B',       # Rojo para estados negativos
        'WARNING': 'FFD700',     # Amarillo para estados de advertencia
        'INFO': '2196F3',        # Azul para dynamic entry
        'NEUTRAL': 'FFFFFF',     # Blanco para valores neutros
        # Colores pasteles para mejor legibilidad
        'LIGHT_RED': 'FFCDD2',   # Rojo suave
        'LIGHT_GREEN': 'E8F5E9', # Verde suave
        'LIGHT_YELLOW': 'FFF2CC',# Amarillo suave
        'LIGHT_BLUE': 'E3F2FD'   # Azul suave
    }

    FORMATS = {
        'HEADER': Font(bold=True),
        'SUMMARY': Font(bold=True, size=12),
        'BORDER': Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
    }

    # Nombres de columnas y configuración de reportes
    SERIAL_VALIDATION_COLUMNS = [
        'Part Number', 'Manufacturer', 'Description', 'Vertex',
        'Serial Control match?'
    ]

    ORG_VALIDATION_COLUMNS = [
        'Part Number', 'Status', 'Organization code mismatch',
        'Action Required'
    ]

    # Nombres de sheets y configuración
    SHEET_NAMES = {
        'SERIAL_VALIDATION': "Serial Control Validation",
        'ORG_VALIDATION': "Audit Results",
        'SUMMARY': "Summary"
    }

    # Valores de estado
    STATUS_VALUES = {
        'MISSING': 'Missing Org',
        'CORRECT': 'Correct Org',
        'MISMATCH': 'Mismatch',
        'MATCH': 'Match'
    }
    
    def __init__(self, output_dir: str = "reports"):
        """Inicializa el generador de reportes."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
    def generate_report(self, audit_result: AuditResult, validation_results: Dict) -> Dict:
        try:
            # Verificar y completar base_org y org_destination si faltan
            if not validation_results.get('program_requirements', {}).get('base_org'):
                logger.error("Missing base_org in program requirements")
                validation_results['program_requirements']['base_org'] = (
                    audit_result.serial_control_results.get('program_requirements', {}).get('base_org')
                )
            
            if not validation_results.get('program_requirements', {}).get('org_destination'):
                logger.error("Missing org_destination in program requirements")
                validation_results['program_requirements']['org_destination'] = (
                    audit_result.serial_control_results.get('program_requirements', {}).get('org_destination', [])
                )
            
            # Añadir flag de validación de inventario
            inventory_validation_enabled = validation_results.get('use_inventory', True)
            validation_results['inventory_validation_enabled'] = inventory_validation_enabled
            
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            
            # Generar reporte externo pasando el flag de inventario
            external_report_path = self._generate_external_report(
                serial_results=audit_result.serial_control_results,
                program_requirements=validation_results['program_requirements'],
                timestamp=timestamp,
                inventory_validation_enabled=inventory_validation_enabled
            )
            logger.debug(f"External report generated at: {external_report_path}")
            
            # Generar reporte interno
            internal_report_path = self._generate_internal_report(
                audit_result=audit_result,
                validation_results=validation_results,
                timestamp=timestamp
            )
            logger.debug(f"Internal report generated at: {internal_report_path}")
            
            # Modificar el summary para incluir estado de inventario
            summary = audit_result.summary.copy()
            summary['inventory_validation'] = 'Enabled' if inventory_validation_enabled else 'Disabled'
            
            return {
                "external_report_path": str(external_report_path),
                "internal_report_path": str(internal_report_path),
                "summary": summary
            }
            
        except Exception as e:
            logger.error(f"Error generating reports: {str(e)}")
            traceback.print_exc()
            raise

    def _generate_external_report(
        self,
        serial_results: Dict,
        program_requirements: Dict,
        timestamp: str,
        inventory_validation_enabled: bool = True
    ) -> Path:
        try:
            # DEBUG: Imprimir contenido de serial_results
            logger.debug("Contenido de serial_results:")
            logger.debug(f"Claves disponibles: {serial_results.keys()}")
            logger.debug(f"Datos: {serial_results.get('data', 'Sin datos')}")
            logger.debug(f"Partes con discrepancias: {serial_results.get('mismatched_parts', 'Sin discrepancias')}")

            excel_path = self.output_dir / f"serial_control_validation_{timestamp}.xlsx"
            
            # MODIFICADO: Siempre leer organizaciones físicas del archivo de configuración
            logger.info("Obteniendo organizaciones físicas desde ALL_WWT_Dropship_and_Inventory_Organizations.xlsx...")
            config_file = Path("config") / "ALL_WWT_Dropship_and_Inventory_Organizations.xlsx"
            physical_orgs = []
            
            if config_file.exists():
                try:
                    logger.info(f"Leyendo organizaciones físicas de: {config_file}")
                    df_config = pd.read_excel(config_file)
                    
                    # Imprimir las columnas disponibles para diagnóstico
                    logger.info(f"Columnas en archivo de configuración: {df_config.columns.tolist()}")
                    
                    # Normalizar nombres de columnas
                    df_config.columns = [col.upper().strip() for col in df_config.columns]
                    
                    # Buscar columnas específicas
                    org_col = None
                    dropship_col = None
                    wms_col = None  # NUEVO: Variable para la columna WMS
                    
                    # Intentar encontrar las columnas relevantes
                    for col in df_config.columns:
                        if 'ORGAN' in col and 'CODE' in col:
                            org_col = col
                            logger.info(f"Columna de organización encontrada: {org_col}")
                        elif 'DROP' in col:
                            dropship_col = col
                            logger.info(f"Columna de dropship encontrada: {dropship_col}")
                        elif 'WMS' in col and 'FLAG' in col:  # NUEVO: Buscar columna WMS
                            wms_col = col
                            logger.info(f"Columna de WMS encontrada: {wms_col}")
                    
                    # MODIFICADO: Verificar que existan las tres columnas necesarias
                    if org_col and dropship_col and wms_col:
                        # Filtrar por organizaciones físicas que cumplan ambas condiciones
                        for _, row in df_config.iterrows():
                            if pd.isna(row[org_col]):
                                continue
                                
                            org_code = str(row[org_col]).strip()
                            # Convertir a número si es posible y luego a string con zfill
                            try:
                                org_code = str(int(float(org_code)))
                                # Usar str.zfill en lugar de zfill directamente
                                org_code = org_code.zfill(2)
                            except:
                                # Usar str.zfill en lugar de zfill directamente
                                org_code = org_code.zfill(2)
                            
                            # MODIFICADO: Verificar dos condiciones
                            # 1. DROPSHIP_ENABLED = 'N'
                            dropship_val = row.get(dropship_col, '')
                            is_not_dropship = isinstance(dropship_val, str) and dropship_val.strip().upper() == 'N'
                            
                            # 2. WMS_ENABLED_FLAG = 'Y'
                            wms_val = row.get(wms_col, '')
                            is_wms_enabled = isinstance(wms_val, str) and wms_val.strip().upper() == 'Y'
                            
                            # SOLO agregar si ambas condiciones se cumplen
                            if is_not_dropship and is_wms_enabled:
                                physical_orgs.append(org_code)
                                    
                        # Eliminar duplicados y ordenar
                        physical_orgs = sorted(list(set(physical_orgs)))
                        logger.info(f"Se encontraron {len(physical_orgs)} organizaciones físicas (DROPSHIP='N' y WMS='Y'): {physical_orgs}")
                    else:
                        # MEJORADO: Mensaje más específico sobre columnas faltantes
                        missing_cols = []
                        if not org_col:
                            missing_cols.append("ORGANIZATION_CODE")
                        if not dropship_col:
                            missing_cols.append("DROPSHIP_ENABLED")
                        if not wms_col:
                            missing_cols.append("WMS_ENABLED_FLAG")
                        
                        logger.warning(f"No se encontraron columnas necesarias en el archivo de configuración: {', '.join(missing_cols)}")
                            
                except Exception as e:
                    logger.error(f"Error procesando archivo de configuración: {str(e)}")
                    logger.error(traceback.format_exc())
            else:
                logger.error(f"Archivo de configuración no encontrado: {config_file}")
            
            # Si no se encontraron organizaciones físicas, usar valores de program_requirements como fallback
            if not physical_orgs:
                physical_orgs = program_requirements.get('physical_orgs', [])
                logger.warning(f"Usando physical_orgs de program_requirements como fallback: {physical_orgs}")
                
                # Si aún no hay organizaciones, usar org_destination como último recurso
                if not physical_orgs:
                    physical_orgs = program_requirements.get('org_destination', [])
                    logger.warning(f"Usando org_destination como último recurso: {physical_orgs}")
            
            # Normalizar las organizaciones para consistencia
            physical_orgs = [str(org).strip() for org in physical_orgs]
            # Convertir a números y aplicar zfill usando .str para Series
            physical_orgs = pd.Series(physical_orgs).str.zfill(2).tolist()
            
            # Filtrar organizaciones de prueba (que comienzan con Z)
            physical_orgs = [org for org in physical_orgs if not org.upper().startswith('Z')]
            logger.info(f"Después de filtrar orgs Z: {len(physical_orgs)} organizaciones físicas")
            
            # Generar datos de validación de serie con las organizaciones físicas
            serial_validation_df = self._generate_serial_validation_data(
                serial_results, 
                physical_orgs,  # Pasar physical_orgs como parámetro explícito
                program_requirements
            )
            
            # DEBUG: Verificar DataFrame de validación
            logger.debug("DataFrame de validación de serie:")
            logger.debug(f"Columnas: {serial_validation_df.columns.tolist()}")
            logger.debug(f"Primeros registros:\n{serial_validation_df.head()}")
            logger.debug(f"Total de registros: {len(serial_validation_df)}")

            # Generar resumen
            summary = {
                'total_parts_reviewed': len(serial_validation_df),
                'total_Serial Control mismatches': len(serial_validation_df[
                    serial_validation_df['Serial Control match?'] == 'Mismatch'
                ]),
                'total_with_inventory': len(serial_validation_df[
                    serial_validation_df['Inventory on Hand? Y/N'] == 'Y'
                ]),
                'parts_with_inventory_pct': f"{(len(serial_validation_df[serial_validation_df['Inventory on Hand? Y/N'] == 'Y']) / max(len(serial_validation_df), 1) * 100):.2f}%",
                'physical_orgs': ', '.join(physical_orgs),
                'total_physical_orgs': len(physical_orgs),
                'timestamp': datetime.now().isoformat()
            }

            print("\n=== EXTERNAL REPORT SUMMARY ===")
            for key, value in summary.items():
                print(f"{key}: {value}")
            
            # DEBUG: Verificar resumen
            logger.debug("Resumen generado:")
            logger.debug(str(summary))
            
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # Escribir resultados principales
                serial_validation_df.to_excel(writer, sheet_name="Serial Control Validation", index=False)
                self._format_worksheet_with_conditional(
                    writer.sheets["Serial Control Validation"],
                    serial_validation_df,
                    'Serial Control match?',
                    'Mismatch'
                )
                
                # Escribir resumen
                pd.DataFrame([summary]).to_excel(writer, sheet_name="Summary", index=False)
                self._format_worksheet(writer.sheets["Summary"])
            
            return excel_path
        
        except Exception as e:
            logger.error(f"Error generando reporte externo: {str(e)}")
            logger.error(f"Traza de error: {traceback.format_exc()}")
            raise

    def _generate_internal_report(self, audit_result: AuditResult, validation_results: Dict, timestamp: str, inventory_validation_enabled: bool = True) -> Path:
        try:
            excel_path = self.output_dir / f"organization_validation_report_{timestamp}.xlsx"
            org_destination = validation_results.get('program_requirements', {}).get('org_destination', [])

            # Convert audit results to base dataframe
            results_df = self._convert_to_dataframe(audit_result)
            print(f"\n[Internal Report] DataFrame antes de validación:\n{results_df.head()}")
            
            # Generate org validation dataframe
            org_validation_df = self._generate_org_validation_data(results_df, org_destination)
            print(f"\n[Internal Report] DataFrame después de validación:\n{org_validation_df.head()}")

            # Generate summary information
            summary = {
                'total_parts': len(org_validation_df['Part Number'].unique()),
                'missing_orgs_issues': len(org_validation_df[org_validation_df['Item Status'] == 'Missing in Org']),
                'issues_by_type': org_validation_df['Item Status'].value_counts().to_dict(),
                'issues_by_org': {
                    # Actualizado para usar las columnas de Serial Control
                    org: len(org_validation_df[org_validation_df[f'org {org} Serial Control'] == 'Not found in org'])
                    for org in org_destination
                },
                'severity_breakdown': {
                    'critical': len(org_validation_df[org_validation_df['Item Status'] == 'Missing in Org']),
                    'major': len(org_validation_df[org_validation_df['Organization Status'] == 'Mismatch']),
                    'minor': 0
                },
                'timestamp': datetime.now().isoformat()
            }
            
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                org_validation_df.to_excel(writer, sheet_name="Audit Results", index=False)
                self._format_worksheet_with_conditional(
                    writer.sheets["Audit Results"],
                    org_validation_df,
                    'Status',
                    'Missing in Org'
                )
                pd.DataFrame([summary]).to_excel(writer, sheet_name="Summary", index=False)
                self._format_worksheet(writer.sheets["Summary"])
            
            return excel_path, summary
            
        except Exception as e:
            logger.error(f"Error generating internal report: {str(e)}")
            raise
            
    def _format_worksheet_with_conditional(
        self,
        worksheet,
        df: pd.DataFrame,
        conditional_column: str = None,
        error_value: str = None
    ) -> None:
        """Aplica formato condicional mejorado."""
        try:
            # Actualizar colores con los nuevos tipos de notas
            self.COLORS.update({
                'SERIAL_NOTES': 'E3F2FD',    # Azul muy claro para notas de Serial Control
                'NPI_NOTES': 'F3E5F5',       # Morado muy claro para notas de NPI
                'PROCUREMENT_NOTES': 'E8F5E9' # Verde muy claro para notas de Procurement
            })

            # Configurar anchos de columna
            for idx, col in enumerate(worksheet.columns, 1):
                max_length = max(len(str(cell.value or '')) for cell in col)
                adjusted_width = min(max_length + 2, 50)  # Limitar el ancho máximo
                worksheet.column_dimensions[get_column_letter(idx)].width = adjusted_width

            # Formato de encabezados mejorado
            header_font = Font(bold=True, color='FFFFFF')
            header_fill = PatternFill(
                start_color=self.COLORS['HEADER'],
                end_color=self.COLORS['HEADER'],
                fill_type='solid'
            )
            header_alignment = Alignment(horizontal='center', vertical='center')

            for cell in worksheet[1]:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = header_alignment

            if df is not None:
                for col_idx, col_name in enumerate(df.columns, 1):
                    # Serial Control match?
                    if col_name == 'Serial Control match?' or col_name == 'Organization Status':
                        for row_idx, value in enumerate(df[col_name], 2):
                            cell = worksheet.cell(row=row_idx, column=col_idx)
                            value = str(value).strip().upper()

                            if value == 'MATCH':
                                color = self.COLORS['SUCCESS']
                                font_color = 'FFFFFF'
                            else:
                                color = self.COLORS['ERROR']
                                font_color = 'FFFFFF'

                            cell.fill = PatternFill(start_color=color, fill_type='solid')
                            cell.font = Font(color=font_color)

                    # XX Serial Control
                    elif 'Serial Control' in col_name:
                        for row_idx, value in enumerate(df[col_name], 2):
                            cell = worksheet.cell(row=row_idx, column=col_idx)
                            value = str(value).strip().upper()

                            color = self.COLORS['NEUTRAL']
                            font_color = '000000'

                            if 'NO SERIAL NUMBER CONTROL' in value:
                                color = self.COLORS['LIGHT_YELLOW']
                            elif 'DYNAMIC ENTRY' in value:
                                color = self.COLORS['LIGHT_BLUE']
                            elif 'NOT FOUND' in value:
                                color = self.COLORS['LIGHT_RED']

                            cell.fill = PatternFill(start_color=color, fill_type='solid')
                            cell.font = Font(color=font_color)

                    # Estados de organización
                    elif col_name.startswith('org ') and 'Serial Control' not in col_name:
                        for row_idx, value in enumerate(df[col_name], 2):
                            cell = worksheet.cell(row=row_idx, column=col_idx)
                            value = str(value).strip().upper()

                            if 'PRESENT IN ORG' in value:
                                color = self.COLORS['LIGHT_GREEN']
                            elif 'NOT FOUND IN ORG' in value:
                                color = self.COLORS['LIGHT_RED']
                            elif 'PHASE OUT' in value:
                                color = self.COLORS['LIGHT_YELLOW']
                            else:
                                color = self.COLORS['NEUTRAL']

                            cell.fill = PatternFill(start_color=color, fill_type='solid')

                    # Status (New, In Progress, Closed)
                    elif col_name == 'Status':
                        for row_idx, value in enumerate(df[col_name], 2):
                            cell = worksheet.cell(row=row_idx, column=col_idx)
                            value = str(value).strip().upper()

                            if value == 'NEW':
                                color = self.COLORS['LIGHT_YELLOW']
                            elif value == 'IN PROGRESS':
                                color = self.COLORS['LIGHT_BLUE']
                            elif value == 'CLOSED':
                                color = self.COLORS['LIGHT_GREEN']
                            else:
                                color = self.COLORS['NEUTRAL']

                            cell.fill = PatternFill(start_color=color, fill_type='solid')

                    # Inventory on Hand
                    elif col_name == 'Inventory on Hand? Y/N':
                        for row_idx, value in enumerate(df[col_name], 2):
                            cell = worksheet.cell(row=row_idx, column=col_idx)
                            value = str(value).strip().upper()

                            color = self.COLORS['LIGHT_GREEN'] if value == 'Y' else self.COLORS['LIGHT_YELLOW']
                            cell.fill = PatternFill(start_color=color, fill_type='solid')

                    # Notas de Serial Control
                    elif 'Serial Control Owner Notes' in col_name:
                        for row_idx in range(2, worksheet.max_row + 1):
                            cell = worksheet.cell(row=row_idx, column=col_idx)
                            cell.fill = PatternFill(
                                start_color=self.COLORS['SERIAL_NOTES'],
                                fill_type='solid'
                            )

                    # Notas de NPI
                    elif 'NPI Action' in col_name:
                        for row_idx in range(2, worksheet.max_row + 1):
                            cell = worksheet.cell(row=row_idx, column=col_idx)
                            cell.fill = PatternFill(
                                start_color=self.COLORS['NPI_NOTES'],
                                fill_type='solid'
                            )

                    # Notas de Procurement
                    elif 'Procurement/Order Management Team Action Notes' in col_name:
                        for row_idx in range(2, worksheet.max_row + 1):
                            cell = worksheet.cell(row=row_idx, column=col_idx)
                            cell.fill = PatternFill(
                                start_color=self.COLORS['PROCUREMENT_NOTES'],
                                fill_type='solid'
                            )

            # Bordes mejorados
            thin_border = Border(
                left=Side(style='thin', color='E0E0E0'),
                right=Side(style='thin', color='E0E0E0'),
                top=Side(style='thin', color='E0E0E0'),
                bottom=Side(style='thin', color='E0E0E0')
            )

            # Aplicar bordes y alineación
            for row in worksheet.iter_rows():
                for cell in row:
                    cell.border = thin_border
                    if cell.row > 1:  # No aplicar a encabezados
                        cell.alignment = Alignment(vertical='center')

            # Congelar primera fila y ajustar zoom
            worksheet.freeze_panes = 'A2'
            worksheet.sheet_view.zoomScale = 100

        except Exception as e:
            logger.error(f"Error en format_worksheet_with_conditional: {str(e)}")
            raise
    
    
    def _format_validation_dataframe(self, df: pd.DataFrame, physical_orgs: List[str], dynamic_columns: List[str]) -> pd.DataFrame:
        """
        Asegura que el DataFrame tenga todas las columnas necesarias en el orden correcto.
        Utiliza physical_orgs para las columnas de organizaciones físicas.
        """
        # Definir orden de columnas base
        columns = [
            'Part Number', 
            'Manufacturer', 
            'Description', 
            'Vertex', 
            'Serial Control match?'
        ]
        
        # Agregar solo las columnas de Serial Control para cada organización física
        for org in physical_orgs:
            columns.append(f'org {org} Serial Control')  # Agregar 'org' antes de la organización
        
        # Agregar columnas restantes en el orden específico
        remaining_columns = [
            'Inventory on Hand? Y/N',
            'Inventory Details',
            'NPI Recommendations',
            'Serial control Owner Action Notes(ISR) *Required',
            'Required Serial control Owner Notes (ISR) *Optional',
            'Optional NPI Action/Data update',
            'Procurement/Order Management Team Action Notes *Required',
            'Procurement/Order Management Team Action Notes *Optional',
            'NPI resolution notes',
            'Status',
            'Action Required'
        ]
        columns.extend(remaining_columns)

        # Asegurar que existan todas las columnas
        for col in columns:
            if col not in df.columns:
                df[col] = ''
        
        return df[columns]
        
    def _convert_to_dataframe(self, audit_result: AuditResult) -> pd.DataFrame:
        """
        Convierte un AuditResult en un DataFrame de pandas para procesamiento de reportes.
        
        Args:
            audit_result: Instancia de AuditResult con los resultados de la auditoría
            
        Returns:
            DataFrame con los datos procesados y normalizados
        """
        try:
            # Lista para almacenar los datos procesados
            processed_data = []
            
            # Extraer datos de serial_control_results de manera segura
            serial_control_data = {}
            if audit_result.serial_control_results:
                try:
                    # Asegurar que tenemos datos y que son del tipo correcto
                    if isinstance(audit_result.serial_control_results, dict):
                        raw_data = audit_result.serial_control_results.get('data', [])
                        if isinstance(raw_data, (list, pd.DataFrame)):
                            for result in raw_data:
                                if isinstance(result, dict):
                                    key = result.get('part_number')
                                    if key:
                                        serial_control_data[key] = {
                                            'manufacturer': result.get('manufacturer', ''),
                                            'description': result.get('description', ''),
                                            'vertex': result.get('vertex', '')
                                        }
                except Exception as e:
                    logger.warning(f"Error procesando serial_control_results: {str(e)}")
                    logger.warning("Continuando con el procesamiento...")
            
            # Procesar cada AuditItem
            for item in audit_result.items:
                # Datos base del item
                base_data = {
                    'Part Number': item.part_number,
                    'Organization': item.organization,
                    'Status': item.status,
                    'Action Required': item.action_required,
                    'Current Orgs': ','.join(item.current_orgs) if item.current_orgs else '',
                    'Missing Orgs': ','.join(item.missing_orgs) if item.missing_orgs else ''
                }
                
                # Añadir datos de program_requirements si están disponibles
                if hasattr(audit_result, 'program_requirements') and audit_result.program_requirements:
                    base_data.update({
                        'Base Org': audit_result.program_requirements.base_org,
                        'Org Destination': ','.join(audit_result.program_requirements.org_destination)
                    })
                
                # Agregar información de Serial Control
                if hasattr(item, 'serial_control') and item.serial_control:
                    base_data.update({
                        'Serial Control': item.serial_control.current_value,
                        'Serial Control Active': 'Yes' if item.serial_control.is_active else 'No'
                    })
                
                # Agregar información de inventario
                if hasattr(item, 'inventory_info') and item.inventory_info:
                    base_data.update({
                        'On Hand Quantity': item.inventory_info.quantity,
                        'Has Stock': 'Yes' if item.inventory_info.has_stock else 'No',
                        'Value': item.inventory_info.value,
                        'Subinventory': item.inventory_info.subinventory_code,
                        'Warehouse': item.inventory_info.warehouse_code
                    })
                    
                    # Agregar información de aging
                    if item.inventory_info.aging_info:
                        base_data.update({
                            'Aging 0-30': item.inventory_info.aging_info.days_0_30,
                            'Aging 31-60': item.inventory_info.aging_info.days_31_60,
                            'Aging 61-90': item.inventory_info.aging_info.days_61_90
                        })
                
                # Agregar datos adicionales de serial control si existen
                additional_data = serial_control_data.get(item.part_number, {})
                base_data.update({
                    'Manufacturer': additional_data.get('manufacturer', ''),
                    'Description': additional_data.get('description', ''),
                    'Vertex': additional_data.get('vertex', '')
                })
                
                processed_data.append(base_data)
            
            # Crear DataFrame
            df = pd.DataFrame(processed_data)
            
            # Asegurar tipos de datos correctos
            numeric_columns = ['On Hand Quantity', 'Value', 'Aging 0-30', 'Aging 31-60', 'Aging 61-90']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Ordenar columnas para mejor legibilidad
            column_order = [
                'Part Number', 'Organization', 'Status', 'Action Required',
                'Current Orgs', 'Missing Orgs', 'Base Org', 'Org Destination',
                'Serial Control', 'Serial Control Active',
                'On Hand Quantity', 'Has Stock', 'Value', 'Subinventory', 'Warehouse',
                'Aging 0-30', 'Aging 31-60', 'Aging 61-90',
                'Manufacturer', 'Description', 'Vertex'
            ]
            
            # Filtrar columnas que existen en el DataFrame
            existing_columns = [col for col in column_order if col in df.columns]
            df = df[existing_columns]
            
            logger.info(f"Successfully converted {len(processed_data)} items to DataFrame")
            return df
            
        except Exception as e:
            logger.error(f"Error converting audit result to DataFrame: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            raise ValueError(f"Failed to convert audit result to DataFrame: {str(e)}")
    
    
    def _generate_serial_validation_data(self, results: Dict, physical_orgs: List[str], program_requirements: Dict) -> pd.DataFrame:
        """
        Genera el reporte de validación de Serial Control con lógica mejorada.
        Ahora utiliza physical_orgs en lugar de org_destination para filtrar las organizaciones físicas
        """
        try:
            print("\n=== INICIO SERIAL VALIDATION DATA GENERATION ===")


            print("\nDEBUG - Input Data Analysis:")
            print(f"Results keys: {results.keys()}")
            print(f"Data shape: {pd.DataFrame(results['data']).shape}")
            print(f"Unique parts in data: {len(pd.DataFrame(results['data'])['Part Number'].unique())}")
            print(f"Mismatched parts: {len(results.get('mismatched_parts', []))}")
            
            inventory_map = results.get('inventory_map', {})
            print("\nDEBUG - Inventory Map Analysis:")
            print(f"Total inventory records: {len(inventory_map)}")
            print("Sample inventory records (first 3):")
            for key in list(inventory_map.keys())[:3]:
                print(f"Key: {key}")
                print(f"Data: {inventory_map[key]}")
                
            mismatched_parts = set(str(part).strip() for part in results.get('mismatched_parts', []))
            print("\nDEBUG - Mismatch Analysis:")
            print(f"Total mismatched parts: {len(mismatched_parts)}")
            print("Sample mismatched parts (first 3):")
            print(list(mismatched_parts)[:3])

            # Análisis de Program Requirements
            print("\nDEBUG - Program Requirements:")
            print(f"Base org: {program_requirements.get('base_org')}")
            print(f"Original org_destination: {program_requirements.get('org_destination')}")    
            print(f"Physical organizations: {physical_orgs}")
                
            # 1. Obtener TODAS las organizaciones del programa
            df_data = pd.DataFrame(results['data']) if isinstance(results['data'], list) else results['data']

            # Asegurar que physical_orgs está normalizado
            physical_orgs = [str(org).strip().zfill(2) for org in physical_orgs]
            
            # Log para verificar las organizaciones físicas
            logger.info(f"Usando organizaciones físicas para la validación: {physical_orgs}")
        
            # Normalizar organizaciones en el DataFrame
            df_data['Organization'] = df_data['Organization'].astype(str).str.strip()
            df_data['Organization'] = df_data['Organization'].str.zfill(2)
            logger.debug(f"Organizaciones únicas después de normalización: {df_data['Organization'].unique()}")
             
            # 2. Procesar TODOS los datos, no solo los mismatches
            all_parts = df_data['Part Number'].unique()
            # Normalizar consistentemente las partes con mismatch (usar UPPER para comparaciones)
            mismatched_parts = set(str(part).strip().upper() for part in results.get('mismatched_parts', []))
            
            print(f"\nDEBUG - Processing Stats:")
            print(f"Total unique parts: {len(all_parts)}")
            print(f"Total mismatches: {len(mismatched_parts)}")
            print(f"Organizations to process: {physical_orgs}")
                
            # 3. Procesamiento de datos principal
            validation_data = []
            inventory_map = results.get('inventory_map', {})
            # Ya hemos normalizado mismatched_parts arriba, no sobrescribir aquí
            # (mantenemos la versión normalizada)
            dynamic_columns = results.get('dynamic_columns', [])

            print(f"Processing {len(df_data)} records with {len(physical_orgs)} organizations")
            
            # Tracking para validación
            processed_parts = set()
            
            for part_number, part_group in df_data.groupby('Part Number'):
                # Normalización de part_number para comparación consistente
                part_norm = str(part_number).strip().upper()
                
                # También normalizar la lista de mismatched_parts para comparación exacta
                # Evaluar si este part_number está en la lista normalizada de mismatches
                is_mismatch = part_norm in set(str(p).strip().upper() for p in mismatched_parts)
                
                # Guardar de manera normalizada para validaciones posteriores
                processed_parts.add(part_norm)
                
                # Resto del código existente para procesamiento por parte
                has_inventory = False
                inventory_details = []
                serial_values = []

                
                # Crear registro base
                row = {
                    'Part Number': part_number,
                    'Manufacturer': part_group['Manufacturer'].iloc[0],
                    'Description': part_group['Description'].iloc[0],
                    'Vertex': part_group['Vertex'].iloc[0],
                    'Serial Control match?': 'Match'  # Asumir match por defecto
                }

                # Inicializar columnas de Serial Control para TODAS las organizaciones físicas
                for org in physical_orgs:
                    row[f'org {org} Serial Control'] = 'Not found in org'

                # Variables para inventario
                has_inventory = False
                inventory_details = []
                serial_values_normalized = []

                # Procesar cada organización física
                for org in physical_orgs:
                    org_data = part_group[part_group['Organization'].astype(str).str.strip() == org]
                    inventory_key = f"{part_number}_{org}"
                    inv_data = inventory_map.get(inventory_key, {}) 

                    if not org_data.empty:
                        serial_value = org_data['Serial Control'].iloc[0]
                        
                        # Normalización explícita del valor de Serial Control
                        normalized_serial_value = (
                            'Dynamic entry at inventory receipt' if serial_value == 'YES' 
                            else 'No serial number control' if serial_value == 'NO' 
                            else serial_value
                        )
                        
                        # Actualizar la columna de Serial Control para esta org
                        row[f'org {org} Serial Control'] = normalized_serial_value
                        
                        # Trackear valores normalizados de serie
                        serial_values_normalized.append(normalized_serial_value)
                        
                        # Procesamiento de inventario
                        quantity = float(inv_data.get('quantity', 0))
                        
                        # Antes de procesar el inventario, verificar existencia de campos
                        if 'quantity' not in inv_data:
                            logger.warning(f"No hay cantidad para {inventory_key}")
                            quantity = 0
                        else:
                            quantity = inv_data['quantity']
                        
                        if quantity > 0:
                            has_inventory = True
                            inventory_details.append(f"{org}: {quantity} units")
                    else:
                        # Mantener la lógica de no encontrado para inventario
                        row[f'Inventory Status {org}'] = 'Not found in org'

                # Determinar mismatch con mayor consistencia
                unique_serial_values = set(val for val in serial_values_normalized if val != 'Not found in org')
                
                # Determinar mismatch de dos maneras:
                # 1. Por detección dinámica (hay diferentes valores de serial control)
                has_value_mismatch = len(unique_serial_values) > 1
                # 2. Por clasificación previa (está en la lista de mismatches original)
                was_flagged_as_mismatch = part_norm in mismatched_parts
                
                # Considerar un mismatch si cualquiera de las dos condiciones se cumple
                row['Serial Control match?'] = 'Mismatch' if (has_value_mismatch or was_flagged_as_mismatch) else 'Match'
                
                # Para debug, registrar la causa del mismatch
                if row['Serial Control match?'] == 'Mismatch':
                    reason = "valores diferentes" if has_value_mismatch else "clasificación previa"
                    logger.debug(f"Parte {part_norm} marcada como Mismatch por {reason}")

                # Actualizar estado de inventario
                row['Inventory on Hand? Y/N'] = 'Y' if has_inventory else 'N'
                row['Inventory Details'] = ' | '.join(inventory_details) if inventory_details else 'No inventory found'

                # Determinar si se requiere acción basada en el resultado real
                row['Action Required'] = 'Review Serial Control' if row['Serial Control match?'] == 'Mismatch' else ''

                # Mantener campos adicionales existentes
                row.update({
                    'NPI Recommendations': '',
                    'Serial control Owner Notes (ISR) *Required': '',
                    'Serial Control Owner Notes (ISR) *Optional': '',
                    'NPI Action/Data update': '',
                    'Procurement/Order Management Team Action Notes *Required': '',
                    'Procurement/Order Management Team Action Notes *Optional': '',
                    'NPI resolution notes': '',
                    'Status': 'New',
                })

                validation_data.append(row)

            # 4. Validación final mejorada
            validation_df = pd.DataFrame(validation_data)

            # Validar que no perdimos partes en el proceso
            total_parts_processed = len(validation_df['Part Number'].unique())
            total_parts_original = len(df_data['Part Number'].unique())

            if total_parts_processed != total_parts_original:
                missing_parts = set(df_data['Part Number'].unique()) - set(validation_df['Part Number'].unique())
                logger.error(f"Pérdida de datos detectada:")
                logger.error(f"Total original: {total_parts_original}")
                logger.error(f"Total procesado: {total_parts_processed}")
                logger.error(f"Partes faltantes: {missing_parts}")
                raise ValueError("Pérdida de datos detectada en el proceso de validación")

            # Validar consistencia de mismatches con lógica más tolerante
            original_parts = set(str(part).strip().upper() for part in results.get('mismatched_parts', []))
            processed_mismatches = set(str(part).strip().upper() for part in validation_df[validation_df['Serial Control match?'] == 'Mismatch']['Part Number'])
            
            # Verificar y mostrar detalles sobre las partes en debug
            logger.debug(f"Total partes originales con mismatch: {len(original_parts)}")
            logger.debug(f"Total partes procesadas con mismatch: {len(processed_mismatches)}")
            
            # Normalizar las partes para mejorar coincidencia
            original_norm = set(re.sub(r'[.\s-]', '', p.upper()) for p in original_parts)
            processed_norm = set(re.sub(r'[.\s-]', '', p.upper()) for p in processed_mismatches)

            # Verificar niveles de tolerancia para discrepancias
            if len(mismatched_parts) == 0 and len(processed_mismatches) > 0:
                logger.warning(f"Se encontraron mismatches en partes sin mismatches originales: {processed_mismatches}")
                logger.warning("Continuando procesamiento a pesar de la discrepancia (tolerancia activa)")
            elif not processed_norm.issubset(original_norm) or not original_norm.issubset(processed_norm):
                # Usar versiones normalizadas para la comparación
                missing_norm = processed_norm - original_norm
                extra_norm = original_norm - processed_norm
                
                # Mapear IDs normalizados a los originales para reporting
                missing = {part for part in processed_mismatches 
                           if re.sub(r'[.\s-]', '', part.upper()) in missing_norm}
                extra = {part for part in original_parts 
                         if re.sub(r'[.\s-]', '', part.upper()) in extra_norm}
                
                # Calcular porcentajes para determinar gravedad
                total_original = len(original_parts)
                total_processed = len(processed_mismatches)
                missing_pct = (len(missing) / max(total_original, 1)) * 100
                extra_pct = (len(extra) / max(total_processed, 1)) * 100
                
                # Verificar si las discrepancias superan un umbral tolerable (20%)
                if missing_pct > 20 or extra_pct > 20:
                    logger.error(f"Discrepancia crítica en partes procesadas:")
                    logger.error(f"Extras en reporte: {extra} ({extra_pct:.1f}%)")
                    logger.error(f"Faltantes en reporte: {missing} ({missing_pct:.1f}%)")
                    raise ValueError("Inconsistencia crítica en la validación de datos: discrepancias superiores al 20%")
                else:
                    # Registrar advertencia pero continuar el proceso
                    logger.warning(f"Discrepancia en partes procesadas (dentro de tolerancia):")
                    logger.warning(f"Extras en reporte: {extra} ({extra_pct:.1f}%)")
                    logger.warning(f"Faltantes en reporte: {missing} ({missing_pct:.1f}%)")
                    
                    # Información adicional para diagnóstico
                    if missing:
                        logger.debug("Ejemplos de formatos de partes faltantes para diagnóstico:")
                        for i, part in enumerate(list(missing)[:3]):
                            norm_part = re.sub(r'[.\s-]', '', part.upper())
                            logger.debug(f"Parte {i+1}: Original '{part}' → Normalizada '{norm_part}'")
                    
                    logger.warning("Continuando procesamiento a pesar de la discrepancia")

            return self._format_validation_dataframe(validation_df, physical_orgs, dynamic_columns)

        except Exception as e:
            logger.error(f"Error en generación de datos de validación: {str(e)}")
            raise
    
    def _format_worksheet(self, worksheet) -> None:
        """Format worksheet with basic styling."""
        self._format_worksheet_with_conditional(worksheet, None)

        
    def _generate_org_validation_data(self, df: pd.DataFrame, org_destination: List[str]) -> pd.DataFrame:
        """
        Genera datos de validación por organización a partir de un DataFrame de auditoría.
        
        Args:
            df: DataFrame con los datos de auditoría
            org_destination: Lista de organizaciones destino
            
        Returns:
            DataFrame con datos de validación por organización
        """
        self._analyze_step(df, "BEFORE ORG VALIDATION")

        validation_data = []
        
        # Normalizar códigos de organización en org_destination
        normalized_org_dest = [str(org).strip().zfill(2) for org in org_destination]
        logger.info(f"Procesando {len(normalized_org_dest)} organizaciones: {normalized_org_dest}")
        
        # Helper function para limpiar valores nulos o 'nan'
        def is_empty_or_nan(value):
            """Determina si un valor es nulo, vacío o representa 'nan'."""
            if value is None:
                return True
            if pd.isna(value):
                return True
            if isinstance(value, str):
                cleaned = value.lower().strip()
                if cleaned in ('nan', 'none', 'null', '') or cleaned.startswith('nan'):
                    return True
            return False
            
        for part_number, part_group in df.groupby('Part Number'):
            # Extraer orgs originales y estado
            orig_status = part_group['Status'].iloc[0]
            current_orgs_str = part_group['Current Orgs'].iloc[0]
            current_orgs = set(org.strip() for org in current_orgs_str.split(',') if org.strip())
            
            row = {'Part Number': part_number}
            
            # Normalizar columna Organization para comparaciones consistentes
            part_group_normalized = part_group.copy()
            if 'Organization' in part_group.columns:
                part_group_normalized['Organization'] = part_group['Organization'].astype(str).str.strip().str.zfill(2)
                
            # Reemplazar cualquier valor NaN en las columnas 'Organization' y 'Serial Control'
            if 'Organization' in part_group.columns:
                part_group['Organization'] = part_group['Organization'].fillna('')
            if 'Serial Control' in part_group.columns:
                part_group['Serial Control'] = part_group['Serial Control'].fillna('')   
            
            # Inicializar columnas Serial Control para todas las organizaciones con el valor predeterminado
            for org in normalized_org_dest:
                row[f'org {org} Serial Control'] = 'Not found in org'
             
            # Procesar organizaciones - solo para Serial Control
            missing_orgs = []
            for org in normalized_org_dest:
                # Buscar la organización normalizada
                org_data = part_group_normalized[part_group_normalized['Organization'] == org]
                
                # Verificar si la organización está en current_orgs
                is_present = any(org == o.strip().zfill(2) for o in current_orgs if o.strip())
                
                # Establecer valor de Serial Control explícitamente
                if not org_data.empty and 'Serial Control' in org_data.columns:
                    # Obtener el primer valor o usar un valor por defecto
                    serial_value = org_data['Serial Control'].iloc[0]
                    
                    # Manejar valores nulos con verificación robusta
                    if is_empty_or_nan(serial_value):
                        row[f'org {org} Serial Control'] = 'Not found in org'
                    else:
                        # Normalizar valores conocidos
                        serial_str = str(serial_value).strip().upper()
                        if serial_str in ('YES', 'Y'):
                            row[f'org {org} Serial Control'] = 'Dynamic entry at inventory receipt'
                        elif serial_str in ('NO', 'N'):
                            row[f'org {org} Serial Control'] = 'No serial number control'
                        else:
                            # Verificar una vez más que no sea un valor nulo camuflado
                            if is_empty_or_nan(serial_str):
                                row[f'org {org} Serial Control'] = 'Not found in org'
                            else:
                                row[f'org {org} Serial Control'] = str(serial_value)
                
                # Asegurar que no haya valores nulos
                if is_empty_or_nan(row.get(f'org {org} Serial Control')):
                    row[f'org {org} Serial Control'] = 'Not found in org'
                
                # Agregar a missing_orgs si no está presente
                if not is_present:
                    missing_orgs.append(org)
            
            # Determinar Serial Control match/mismatch
            serial_values = []
            for org in normalized_org_dest:
                value = row.get(f'org {org} Serial Control')
                
                # Última comprobación antes de usar el valor
                if is_empty_or_nan(value):
                    row[f'org {org} Serial Control'] = 'Not found in org'
                    value = 'Not found in org'
                    
                if value != 'Not found in org':
                    serial_values.append(value)
            
            has_mismatch = len(set(serial_values)) > 1 if serial_values else False
                        
            row.update({
                'Item Status': orig_status,
                'Current Orgs': current_orgs_str,
                'Organization code mismatch': ','.join(missing_orgs) if missing_orgs else 'None',
                'Action Required': f"Create in Org {','.join(missing_orgs)}" if missing_orgs else 'None',
                'Audit Status': '',
                'Organization Status': 'Mismatch' if missing_orgs else 'Match'
            })
            
            # Verificación final de todas las columnas de Serial Control en esta fila
            for key in list(row.keys()):
                if 'Serial Control' in key and is_empty_or_nan(row[key]):
                    row[key] = 'Not found in org'
            
            validation_data.append(row)
        
        # Crear DataFrame
        result_df = pd.DataFrame(validation_data)
        logger.info(f"DataFrame creado con {len(result_df)} filas")
        
        # Asegurar que no haya NaN en ninguna columna, con énfasis en las de Serial Control
        serial_control_cols = [col for col in result_df.columns if 'Serial Control' in col]
        logger.info(f"Limpiando valores nulos en {len(serial_control_cols)} columnas de Serial Control")
        
        # 1. Reemplazar NaN con "Not found in org" en columnas de Serial Control
        for col in serial_control_cols:
            # Usando múltiples enfoques para garantizar que se capturen todos los casos posibles
            result_df[col] = result_df[col].fillna('Not found in org')
            result_df[col] = result_df[col].replace({None: 'Not found in org'})
            result_df[col] = result_df[col].replace({'nan': 'Not found in org', 'NaN': 'Not found in org'})
            result_df[col] = result_df[col].replace({'None': 'Not found in org', 'none': 'Not found in org'})
            result_df[col] = result_df[col].replace({'': 'Not found in org'})
            
            # Convertir a string para manejar cualquier valor no string
            result_df[col] = result_df[col].astype(str)
            
            # Usar función lambda para capturar casos adicionales
            result_df[col] = result_df[col].apply(
                lambda x: 'Not found in org' if x.lower() in ('nan', 'none', 'null', '') else x
            )
        
        # 2. Reemplazar NaN con cadena vacía en otras columnas
        for col in result_df.columns:
            if col not in serial_control_cols:
                result_df[col] = result_df[col].fillna('')
        
        # Simplificar ordenamiento de columnas
        base_cols = ['Part Number']
        serial_control_cols = [col for col in result_df.columns if 'Serial Control' in col and col != 'Organization Status']
        remaining_cols = [
            'Current Orgs',
            'Organization code mismatch',
            'Action Required',
            'Item Status',
            'Audit Status',
            'Organization Status'
        ]
        
        # Asegurar que todas las columnas existan
        for col in base_cols + remaining_cols:
            if col not in result_df.columns:
                result_df[col] = ''
        
        # Solo incluir columnas que existen en el DataFrame
        existing_cols = [col for col in base_cols + serial_control_cols + remaining_cols if col in result_df.columns]
        result_df = result_df[existing_cols]
        
        # Verificación FINAL EXHAUSTIVA para columnas de Serial Control
        for col in serial_control_cols:
            # Convertir a string nuevamente para asegurar uniformidad de tipos
            result_df[col] = result_df[col].astype(str)
            
            # Capturar variantes como 'nan ' (con espacios), 'NaN', etc.
            mask = result_df[col].str.lower().str.strip().isin(['nan', 'none', 'null', ''])
            if mask.any():
                logger.warning(f"¡Último reemplazo! Encontradas {mask.sum()} ocurrencias de nulos en {col}")
                result_df.loc[mask, col] = 'Not found in org'
        
        # Verificar que no quede ninguna columna con valores NaN antes de analizar
        self._analyze_step(result_df, "AFTER ORG VALIDATION")
        return result_df
            
    
    def _write_main_results(
        self, 
        df: pd.DataFrame, 
        writer: pd.ExcelWriter, 
        program_requirements: Dict
    ) -> pd.DataFrame:
        try:
            # DEBUG: Imprimir información inicial del DataFrame
            print("DataFrame inicial:")
            print.debug(f"Columnas: {df.columns.tolist()}")
            print(f"Primeros registros:\n{df.head()}")
            print(f"Total de registros: {len(df)}")
            print(f"Números de parte únicos: {df['Part Number'].nunique()}")

            # Verificar requisitos del programa
            all_orgs = sorted(program_requirements.get('org_destination', []))
            print(f"Organizaciones objetivo: {all_orgs}")

            # Verificar que todos los campos esperados estén presentes
            expected_columns = [
                'Part Number', 'Manufacturer', 'Description', 'Vertex', 
                'Serial Control', 'Status', 'Organization'
            ]
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Columnas faltantes: {missing_columns}")
                raise ValueError(f"Faltan columnas obligatorias: {missing_columns}")

            main_data = []

            for part_number in df['Part Number'].unique():
                # DEBUG: Rastrear cada número de parte
                print(f"Procesando número de parte: {part_number}")
                
                part_data = df[df['Part Number'] == part_number]
                
                # Validación adicional de datos
                if len(part_data) == 0:
                    logger.warning(f"No hay datos para el número de parte {part_number}")
                    continue

                base_info = part_data.iloc[0]
                
                # DEBUG: Imprimir información base de cada parte
                print(f"Información base de {part_number}:")
                print(str(base_info))

                row = {
                    'Part Number': part_number,
                    'Manufacturer': base_info['Manufacturer'],
                    'Description': base_info['Description'],
                    'Vertex': base_info['Vertex'],
                    'Serial Control match?': base_info.get('Serial Control', 'N/A')
                }
                
                # Procesamiento de organizaciones
                for org in all_orgs:
                    org_data = part_data[part_data['Organization'] == org]
                    
                    # DEBUG: Verificar datos por organización
                    print(f"Datos para org {org} de {part_number}:")
                    print(str(org_data))

                    if not org_data.empty:
                        # Lógica para determinar valor de serie
                        serial_value = ('Dynamic entry at inventory receipt' 
                                    if org_data['Serial Control'].iloc[0] == 'YES'
                                    else 'No serial number control')
                        row[f'org {org}'] = serial_value
                        row[f'Inventory on Hand {org}'] = 'Y' if org_data['On Hand Quantity'].iloc[0] > 0 else 'N'
                    else:
                        row[f'org {org}'] = 'Not found in org'
                        row[f'Inventory on Hand {org}'] = 'N'
                
                # Campos adicionales
                row.update({
                    'NPI Recommendations': '',
                    'Serial control Owner Notes': '',
                    'NPI Action/Data update': '',
                    'Status': base_info.get('Status', 'Unknown'),
                    'Item Org destination mismatch?': 'Yes' if part_data['Status'].eq('Missing Org').any() else 'No',
                    'Organization code mismatch': '',
                    'Action Required': ''
                })
                
                main_data.append(row)
            
            # Creación de DataFrame final
            main_df = pd.DataFrame(main_data)
            
            # DEBUG: Verificar DataFrame final
            print("DataFrame final antes de escribir:")
            print(f"Columnas: {main_df.columns.tolist()}")
            print(f"Primeros registros:\n{main_df.head()}")
            print(f"Total de registros: {len(main_df)}")

            # Definir orden de columnas
            columns = [
                'Part Number', 'Manufacturer', 'Description', 'Vertex', 
                'Serial Control match?'
            ]
            
            for org in all_orgs:
                columns.extend([
                    f'org {org}',
                    f'Inventory on Hand {org}'
                ])
            
            columns.extend([
                'NPI Recommendations',
                'Serial control Owner Notes',
                'NPI Action/Data update',
                'Status',
                'Item Org destination mismatch?',
                'Organization code mismatch',
                'Action Required'
            ])
            
            # Asegurar columnas
            for col in columns:
                if col not in main_df.columns:
                    main_df[col] = ''

            main_df = main_df[columns]
            
            main_df.to_excel(writer, sheet_name="Audit Results", index=False)
            
            self._format_worksheet_with_conditional(
                writer.sheets['Audit Results'],
                main_df,
                'Status',
                'Missing Org'
            )
            
            return writer.sheets['Audit Results']
            
        except Exception as e:
            logger.error(f"Error escribiendo resultados principales: {str(e)}")
            logger.error(f"Traza de error: {traceback.format_exc()}")
            raise
    
    def _write_serial_validation_report(self, df: pd.DataFrame, writer: pd.ExcelWriter):
        """
        Write and format Serial Control Validation report with conditional formatting.
        """
        try:
            # Write DataFrame
            df.to_excel(writer, sheet_name="Serial Control Validation", index=False)
            worksheet = writer.sheets["Serial Control Validation"]
            
            # Format headers
            for idx, col in enumerate(df.columns, 1):
                cell = worksheet.cell(row=1, column=idx)
                cell.font = Font(bold=True)
                cell.fill = PatternFill(
                    start_color=self.COLORS['HEADER'],
                    end_color=self.COLORS['HEADER'],
                    fill_type='solid'
                )
                
                # Adjust column width
                max_length = max(
                    len(str(col)),
                    df[col].astype(str).apply(len).max()
                )
                worksheet.column_dimensions[get_column_letter(idx)].width = max_length + 2

            # Apply conditional formatting
            for col_idx, col_name in enumerate(df.columns, 1):
                if col_name == 'Serial Control match?' or col_name == 'Status':
                    for row_idx, value in enumerate(df[col_name], 2):
                        cell = worksheet.cell(row=row_idx, column=col_idx)
                        if value in ['Mismatch', 'Missing Org']:
                            cell.fill = PatternFill(
                                start_color=self.COLORS['ERROR'],
                                end_color=self.COLORS['ERROR'],
                                fill_type='solid'
                            )
                        else:
                            cell.fill = PatternFill(
                                start_color=self.COLORS['SUCCESS'],
                                end_color=self.COLORS['SUCCESS'],
                                fill_type='solid'
                            )
                elif col_name.startswith('org '):
                    for row_idx, value in enumerate(df[col_name], 2):
                        cell = worksheet.cell(row=row_idx, column=col_idx)
                        if value == 'Not found in org':
                            cell.fill = PatternFill(
                                start_color=self.COLORS['ERROR'],
                                end_color=self.COLORS['ERROR'],
                                fill_type='solid'
                            )
                        else:
                            cell.fill = PatternFill(
                                start_color=self.COLORS['SUCCESS'],
                                end_color=self.COLORS['SUCCESS'],
                                fill_type='solid'
                            )

            # Add borders
            thin_border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
            
            for row in worksheet.iter_rows(min_row=1, max_row=len(df) + 1):
                for cell in row:
                    cell.border = thin_border

            # Freeze top row
            worksheet.freeze_panes = 'A2'
            
            return worksheet

        except Exception as e:
            logger.error(f"Error writing serial validation report: {str(e)}")
            raise
        
    def _analyze_step(self, df: pd.DataFrame, step_name: str) -> None:
        print(f"\n=== {step_name} ===")
        print("Columns:", df.columns.tolist())
        print(f"\nFirst 3 rows of {len(df)} total:")
        print(df.head(3).to_string())
        if 'Organization' in df.columns:
            print("\nUnique Organizations:")
            print(df['Organization'].unique())
        if 'Status' in df.columns:
            print("\nStatus Distribution:")
            print(df['Status'].value_counts())
            
            
    def _format_inventory_status(self, inv_data: Dict) -> str:
        if not inv_data:
            return 'Not found in org'
        
        quantity = inv_data.get('quantity', 0)
        return 'Y' if quantity > 0 else 'N'  # Simplificado y solo cantidad
    
    # En la clase ReportGenerator, agregar:
    def _get_physical_orgs_from_config(self, config_file_path: Path) -> List[str]:
        """
        Obtiene las organizaciones físicas (no dropship) del archivo de configuración.
        
        Args:
            config_file_path: Ruta al archivo ALL_WWT_Dropship_and_Inventory_Organizations.xlsx
            
        Returns:
            Lista de códigos de organizaciones físicas
        """
        physical_orgs = []
        
        if not config_file_path.exists():
            logger.error(f"Archivo de configuración no encontrado: {config_file_path}")
            return physical_orgs
            
        try:
            logger.info(f"Leyendo organizaciones físicas de: {config_file_path}")
            df_config = pd.read_excel(config_file_path)
            
            # Imprimir las columnas disponibles para diagnóstico
            logger.info(f"Columnas en archivo de configuración: {df_config.columns.tolist()}")
            
            # Normalizar nombres de columnas
            df_config.columns = [col.upper().strip() for col in df_config.columns]
            
            # Buscar columnas específicas
            org_col = None
            dropship_col = None
            
            # Intentar encontrar las columnas relevantes
            for col in df_config.columns:
                if 'ORGAN' in col and 'CODE' in col:
                    org_col = col
                    logger.info(f"Columna de organización encontrada: {org_col}")
                elif 'DROP' in col:
                    dropship_col = col
                    logger.info(f"Columna de dropship encontrada: {dropship_col}")
            
            if org_col and dropship_col:
                # Valores que indican dropship
                dropship_values = ['YES', 'Y', 'TRUE', 'T', '1', 1, True]
                
                # Filtrar por organizaciones NO dropship (físicas)
                for _, row in df_config.iterrows():
                    if pd.isna(row[org_col]):
                        continue
                        
                    org_code = str(row[org_col]).strip()
                    # Convertir a número si es posible y luego a string con zfill
                    try:
                        org_code = str(int(float(org_code)))
                        # Usar str.zfill en lugar de zfill directamente
                        org_code = org_code.zfill(2)
                    except:
                        # Usar str.zfill en lugar de zfill directamente
                        org_code = org_code.zfill(2)
                        
                    # Normalizar valor booleano para dropship
                    dropship_val = row.get(dropship_col)
                    is_dropship = False
                    
                    if pd.isna(dropship_val):
                        is_dropship = False
                    elif isinstance(dropship_val, bool):
                        is_dropship = dropship_val
                    elif isinstance(dropship_val, (int, float)):
                        is_dropship = bool(int(dropship_val))
                    elif isinstance(dropship_val, str):
                        is_dropship = dropship_val.strip().upper() in dropship_values
                    
                    # SOLO agregar si NO es dropship y NO comienza con Z (organización de prueba)
                    if not is_dropship and not org_code.upper().startswith('Z'):
                        physical_orgs.append(org_code)
                        
                # Eliminar duplicados y ordenar
                physical_orgs = sorted(list(set(physical_orgs)))
                logger.info(f"Se encontraron {len(physical_orgs)} organizaciones físicas (NO dropship): {physical_orgs}")
            else:
                logger.warning("No se encontraron las columnas necesarias en el archivo de configuración")
                
        except Exception as e:
            logger.error(f"Error procesando archivo de configuración: {str(e)}")
            logger.error(traceback.format_exc())
            
        return physical_orgs

    def _normalize_serial_value(self, value: str) -> str:
        value = str(value).strip().upper()
        if value in {"YES", "Y", "TRUE", "1", "DYNAMIC ENTRY AT INVENTORY RECEIPT"}:
            return "Dynamic entry at inventory receipt"
        elif value in {"NO", "N", "FALSE", "0", "NONE", "NO SERIAL NUMBER CONTROL"}:
            return "No serial number control"
        return value  # Mantener valor original si no coincide
    
    
    def _validate_serial_consistency(self, raw_data: Dict, report_df: pd.DataFrame):
        """Verifica coherencia entre datos originales y reporte."""
        audit_mismatches = len(raw_data.get('mismatched_parts', []))
        report_mismatches = len(report_df[report_df['Serial Control match?'] == 'Mismatch'])
        
        if audit_mismatches != report_mismatches:
            logger.error(f"¡Discrepancia en Mismatches! Audit: {audit_mismatches} vs Reporte: {report_mismatches}")
            raise ValueError("Inconsistencia crítica en resultados de Serial Control")
        
    def _validate_data_consistency(self, original_results: Dict, generated_df: pd.DataFrame) -> bool:
        """
        Validates data consistency with improved diagnostics.
        """
        try:
            # Debug: Mostrar información del DataFrame
            logger.debug(f"Columnas en DataFrame: {generated_df.columns.tolist()}")
            logger.debug(f"Total de filas: {len(generated_df)}")
            logger.debug(f"Distribución de 'Serial Control match?':\n{generated_df['Serial Control match?'].value_counts()}")
            
            # Normalizar Part Numbers
            original_mismatches = set(
                str(part).strip().upper() 
                for part in original_results.get('mismatched_parts', [])
            )
            
            mismatch_mask = generated_df['Serial Control match?'] == 'Mismatch'
            logger.debug(f"Filas con Mismatch: {mismatch_mask.sum()}")
            
            generated_mismatches = set(
                generated_df.loc[mismatch_mask, 'Part Number']
                .astype(str)
                .str.strip()
                .str.upper()
            )
            
            # Comparaciones detalladas
            logger.debug(f"Original mismatches ({len(original_mismatches)}): {original_mismatches}")
            logger.debug(f"Generated mismatches ({len(generated_mismatches)}): {generated_mismatches}")
            
            # Análisis de diferencias
            missing = original_mismatches - generated_mismatches
            extra = generated_mismatches - original_mismatches
            
            if missing or extra:
                logger.error("Inconsistencia detectada:")
                if missing:
                    logger.error(f"Faltantes en reporte: {missing}")
                if extra:
                    logger.error(f"Extras en reporte: {extra}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error en validación de consistencia: {str(e)}")
            logger.error(traceback.format_exc())
            return False