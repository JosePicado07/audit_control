# domain/entities/inventory_entity.py
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

from domain.criteria.inventory_criteria import InventoryMatchCriteria

@dataclass
class InventoryAgingInfo:
    """Información detallada del aging del inventario con validación mejorada"""
    days_0_30: float = 0.0
    days_31_60: float = 0.0
    days_61_90: float = 0.0
    days_91_plus: float = 0.0
    
    @property
    def total_aging(self) -> float:
        """Calcula el total de aging"""
        return self.days_0_30 + self.days_31_60 + self.days_61_90 + self.days_91_plus
    
    def update_from_aging_values(self, aging_values: dict) -> None:
        """
        Actualiza la información de aging desde valores raw con mejor manejo de NaN
        """
        # Mapeo de períodos conocidos
        aging_map = {
            'Aging 0-30 Quantity': ('days_0_30', 0.0),
            'Aging 31-60 Quantity': ('days_31_60', 0.0),
            'Aging 61-90 Quantity': ('days_61_90', 0.0)
        }
        
        # Procesar períodos conocidos
        for key, (attr, default) in aging_map.items():
            value = aging_values.get(key, default)
            setattr(self, attr, float(value if value and not pd.isna(value) else default))
        
        # Procesar días 91+ sumando todos los períodos mayores
        plus_91_value = sum(
            float(aging_values.get(f'Aging {period} Quantity', 0) or 0)
            for period in ['91-120', '121-150', '151-180', '181-365', 'over 365']
        )
        self.days_91_plus = plus_91_value if not pd.isna(plus_91_value) else 0.0
        
    def update_from_raw_data(self, raw_aging: Dict[str, Any], criteria: Optional[InventoryMatchCriteria] = None) -> None:
        """
        Actualiza aging desde datos raw con mejor validación
        
        Args:
            raw_aging: Diccionario con datos de aging
            criteria: Criterios de matching opcionales
        """
        # Mapeo de columnas a atributos
        aging_map = {
            'Aging_0_30': 'days_0_30',
            'Aging_31_60': 'days_31_60',
            'Aging_61_90': 'days_61_90'
        }
        
        # Procesar cada período con validación mejorada
        for col_name, attr_name in aging_map.items():
            value = raw_aging.get(col_name, 0)
            # Manejar NaN y conversión a float de forma segura
            normalized_value = float(value if value and not pd.isna(value) else 0)
            setattr(self, attr_name, normalized_value)
        
        # Calcular días_91_plus si hay datos adicionales
        plus_91_cols = [k for k in raw_aging.keys() if k.startswith('Aging_') and k not in aging_map]
        plus_91_value = sum(
            float(raw_aging.get(col, 0) or 0)
            for col in plus_91_cols
        )
        self.days_91_plus = plus_91_value if not pd.isna(plus_91_value) else 0.0
                
@dataclass
class InventoryMatch:
    """Resultado de coincidencia de inventario mejorado"""
    part_number: str
    organization: str
    has_inventory: bool
    quantity: float = 0.0
    error_message: Optional[str] = None
    aging_info: InventoryAgingInfo = field(default_factory=InventoryAgingInfo)
    subinventory: Optional[str] = None
    value: float = 0.0
    warehouse_code: Optional[str] = None 
    serial_numbers: List[str] = field(default_factory=list)
    match_criteria: InventoryMatchCriteria = field(
        default_factory=InventoryMatchCriteria
    )

    def update_from_raw_data(self, raw_data: Dict[str, Any]) -> None:
        """
        Actualiza la entidad con datos raw aplicando reglas de dominio
        
        Args:
            raw_data: Diccionario con datos crudos del inventario
        """
        # Actualizar cantidad y valor con mejor manejo de nulos
        raw_qty = raw_data.get('TOTAL_QUANTITY') or raw_data.get('Quantity')
        raw_value = raw_data.get('VALUE') or raw_data.get('Total Value')
        raw_warehouse = raw_data.get('ORG_WAREHOUSE_CODE')
        
        self.quantity = 0.0 if pd.isna(raw_qty) else float(raw_qty or 0)
        self.value = 0.0 if pd.isna(raw_value) else float(raw_value or 0)
        self.warehouse_code = str(raw_warehouse).strip() if raw_warehouse and not pd.isna(raw_warehouse) else None
        
        # Actualizar subinventory con validación mejorada
        if self.match_criteria.check_subinventory:
            raw_subinv = raw_data.get('SUBINVENTORY') or raw_data.get('Subinventory Code', '')
            if raw_subinv and not pd.isna(raw_subinv) and self.match_criteria.is_valid_subinventory(raw_subinv):
                self.subinventory = raw_subinv.strip().upper()
            else:
                self.subinventory = None
                self.quantity = 0.0  # Invalidar cantidad si subinventory no es válido
        
        # Actualizar estado de inventario
        self.has_inventory = self.quantity > 0 and (
            not self.match_criteria.check_subinventory or 
            self.subinventory in self.match_criteria.valid_subinventories
        )
        
        # Actualizar aging info si hay datos disponibles
        if any(key.startswith('AGING') or key.startswith('Aging') for key in raw_data):
            self.aging_info.update_from_raw_data(raw_data, self.match_criteria)

    @classmethod
    def create_from_raw(cls, 
                       part_number: str,
                       organization: str,
                       raw_data: Dict[str, Any],
                       criteria: Optional[InventoryMatchCriteria] = None) -> 'InventoryMatch':
        """
        Factory method para crear una instancia desde datos raw
        
        Args:
            part_number: Número de parte
            organization: Código de organización
            raw_data: Diccionario con datos crudos
            criteria: Criterios de matching opcionales
            
        Returns:
            Nueva instancia de InventoryMatch
        """
        match = cls(
            part_number=part_number,
            organization=organization,
            has_inventory=False,
            match_criteria=criteria or InventoryMatchCriteria()
        )
        match.update_from_raw_data(raw_data)
        return match