"""Entidades del dominio"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd

from domain.entities.inventory_entity import InventoryAgingInfo

@dataclass
class Organization:
    code: str
    name: str
    is_physical: bool
    is_dropship: bool

@dataclass
class ProgramRequirement:
    contract: str
    base_org: str
    org_destination: List[str]
    physical_orgs: List[str] = field(default_factory=list)  
    dropship_orgs: List[str] = field(default_factory=list)
    requires_serial_control: bool = False
    international: bool = False

@dataclass
class SerialControlInfo:
    """
    Representa la información detallada del control de serie de una parte.
    Esto nos permite tener una vista más clara del estado del serial control
    y mantener su historial.
    """
    current_value: str  # El valor actual ("Dynamic entry", "No serial control", "Not found")
    is_active: bool    # Si el control de serie está activo
    last_updated: datetime = field(default_factory=datetime.now)
    
    @property
    def is_valid(self) -> bool:
        """Verifica si el valor actual es válido según las reglas de negocio"""
        valid_values = {
            "Dynamic entry at inventory receipt",
            "Dynamic entry at sales order receipt",
            "No serial number control",
            "Not found"
        }
        return self.current_value in valid_values
        
@dataclass
class InventoryInfo:
    """Información detallada del inventario"""
    quantity: float = 0.0
    value: float = 0.0
    subinventory_code: str = ''
    has_stock: bool = False
    warehouse_code: str = ''
    aging_info: InventoryAgingInfo = field(default_factory=InventoryAgingInfo)
    
    def __post_init__(self):
        """Validación y normalización de datos"""
        # Asegurar que quantity sea un número válido
        if pd.isna(self.quantity):
            self.quantity = 0.0
        try:
            self.quantity = float(self.quantity)
        except (ValueError, TypeError):
            self.quantity = 0.0
            
        # Actualizar has_stock basado en quantity
        self.has_stock = self.quantity > 0.0

@dataclass
class AuditItem:
    part_number: str
    organization: str
    status: str
    item_status : str = ''
    action_required: Optional[str] = None
    current_orgs : List[str] = field(default_factory=list)
    missing_orgs: List[str] = field(default_factory=list)
    serial_control: SerialControlInfo = field(default_factory=lambda: SerialControlInfo("Not found", False))
    inventory_info: InventoryInfo = field(default_factory=InventoryInfo)
    
    # Metadatos adicionales que pueden ser útiles
    manufacturer: str = ''
    description: str = ''
    vertex_class: str = ''
    
    @property
    def on_hand_qty(self) -> float:
        """Mantiene compatibilidad con el código existente"""
        return self.inventory_info.quantity
    
    @property
    def has_serial_control(self) -> bool:
        """Mantiene compatibilidad con el código existente"""
        return self.serial_control.is_active


@dataclass
class AuditResult:
    audit_id: str
    contract: str
    timestamp: datetime
    items: List[AuditItem]
    summary: Dict
    manufacturer: str = ''  
    description: str = ''   
    vertex_class: str = '' 
    serial_control_results: Optional[Dict] = None
    org_mismatch_results: Optional[Dict] = None
    report_path: Optional[str] = None