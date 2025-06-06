from typing import Dict, List, Set, Optional, Tuple
from pathlib import Path
import pandas as pd
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class OrganizationConfig:
    """Estructura para almacenar la configuración de organizaciones."""
    org_code: str
    is_physical: bool
    is_dropship: bool
    wms_enabled: bool
    attribute4: Optional[str] = None

class OrganizationStructureHandler:
    """
    Gestor de estructuras organizacionales para determinar
    tipos de organizaciones y su categorización.
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """Inicializa el gestor con la ruta de configuración."""
        self.config_path = config_path or Path("config")
        self.org_config: Dict[str, OrganizationConfig] = {}
        self._load_configuration()
    
    def _load_configuration(self) -> None:
        """
        Carga la configuración de organizaciones desde el archivo excel.
        El archivo debe contener las columnas:
        - ORGANIZATION_CODE
        - ATTRIBUTE4
        - DROPSHIP_ENABLED
        - WMS_ENABLED_FLAG
        """
        try:
            config_file = self.config_path / "ALL_WWT_Dropship_and_Inventory_Organizations.xlsx"
            
            if not config_file.exists():
                logger.warning(f"Archivo de configuración no encontrado: {config_file}")
                return
                
            logger.info(f"Cargando configuración de organizaciones desde: {config_file}")
            
            df = pd.read_excel(config_file)
            
            # Normalizar nombres de columnas
            df.columns = [col.upper().strip() for col in df.columns]
            
            # Verificar columnas requeridas
            required_columns = [
                'ORGANIZATION_CODE', 
                'DROPSHIP_ENABLED', 
                'WMS_ENABLED_FLAG'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Columnas faltantes en archivo de configuración: {missing_columns}")
                return
            
            # Normalizar valores booleanos
            for col in ['DROPSHIP_ENABLED', 'WMS_ENABLED_FLAG']:
                if col in df.columns:
                    df[col] = df[col].apply(self._normalize_boolean)
            
            # Procesar cada organización
            for _, row in df.iterrows():
                org_code = str(row['ORGANIZATION_CODE']).strip().zfill(2)
                
                self.org_config[org_code] = OrganizationConfig(
                    org_code=org_code,
                    is_physical=not self._normalize_boolean(row.get('DROPSHIP_ENABLED', False)),
                    is_dropship=self._normalize_boolean(row.get('DROPSHIP_ENABLED', False)),
                    wms_enabled=self._normalize_boolean(row.get('WMS_ENABLED_FLAG', False)),
                    attribute4=row.get('ATTRIBUTE4', None)
                )
            
            logger.info(f"Configuración cargada: {len(self.org_config)} organizaciones")
            
        except Exception as e:
            logger.error(f"Error cargando configuración de organizaciones: {str(e)}")
            logger.exception(e)
    
    def _normalize_boolean(self, value) -> bool:
        """
        Normaliza valores booleanos desde diferentes formatos posibles.
        """
        if pd.isna(value):
            return False
            
        if isinstance(value, bool):
            return value
            
        if isinstance(value, (int, float)):
            return bool(value)
            
        if isinstance(value, str):
            return value.strip().upper() in ('YES', 'Y', 'TRUE', 'T', '1')
            
        return False
    
    def get_physical_orgs(self) -> List[str]:
        """
        Devuelve la lista de organizaciones físicas (no dropship).
        """
        return [
            org_code for org_code, config in self.org_config.items()
            if config.is_physical
        ]
    
    def get_dropship_orgs(self) -> List[str]:
        """
        Devuelve la lista de organizaciones dropship.
        """
        return [
            org_code for org_code, config in self.org_config.items()
            if config.is_dropship
        ]
    
    def get_wms_enabled_orgs(self) -> List[str]:
        """
        Devuelve la lista de organizaciones con WMS habilitado.
        """
        return [
            org_code for org_code, config in self.org_config.items()
            if config.wms_enabled
        ]
    
    def is_physical_org(self, org_code: str) -> bool:
        """
        Verifica si una organización es física (no dropship).
        """
        org_code = str(org_code).strip().zfill(2)
        return self.org_config.get(org_code, OrganizationConfig(
            org_code=org_code,
            is_physical=True,
            is_dropship=False,
            wms_enabled=False
        )).is_physical
    
    def is_dropship_org(self, org_code: str) -> bool:
        """
        Verifica si una organización es dropship.
        """
        org_code = str(org_code).strip().zfill(2)
        return self.org_config.get(org_code, OrganizationConfig(
            org_code=org_code,
            is_physical=False,
            is_dropship=True,
            wms_enabled=False
        )).is_dropship
    
    def filter_orgs_by_type(self, org_list: List[str], org_type: str = 'ALL') -> List[str]:
        """
        Filtra una lista de organizaciones por tipo.
        
        Args:
            org_list: Lista de códigos de organización
            org_type: Tipo de organización a filtrar ('PHYSICAL', 'DROPSHIP', 'WMS', 'ALL')
            
        Returns:
            Lista filtrada de códigos de organización
        """
        org_list = [str(org).strip().zfill(2) for org in org_list]
        
        if org_type == 'ALL':
            return org_list
            
        if org_type == 'PHYSICAL':
            return [org for org in org_list if self.is_physical_org(org)]
            
        if org_type == 'DROPSHIP':
            return [org for org in org_list if self.is_dropship_org(org)]
            
        if org_type == 'WMS':
            return [org for org in org_list if org in self.get_wms_enabled_orgs()]
            
        return org_list
    
    
    def enrich_program_requirements(self, program_reqs: Dict) -> Dict:
        """
        Enriquece los requisitos del programa con información organizacional.
        
        Para el reporte interno:
        - Solo usar org_destination original
        
        Para el reporte externo:
        - Obtener TODAS las organizaciones físicas del archivo de configuración
        
        Args:
            program_reqs: Requisitos de programa existentes
            
        Returns:
            Requisitos de programa enriquecidos
        """
        if not program_reqs:
            return program_reqs
        
        # Obtener todas las organizaciones del destino original
        org_destination = program_reqs.get('org_destination', [])
        
        if not org_destination:
            return program_reqs
        
        # Copia para no modificar el diccionario original
        enriched_reqs = program_reqs.copy()
        
        # Para reporte interno: mantener org_destination original
        # Para reporte externo: obtener TODAS las organizaciones físicas
        enriched_reqs['physical_orgs'] = self.get_physical_orgs()
        
        return enriched_reqs