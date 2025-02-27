# domain/entities/inventory_criteria.py
from dataclasses import dataclass, field
from typing import List, Set

@dataclass
class InventoryMatchCriteria:
    """Define los criterios para matching de inventario como una entidad del dominio"""
    match_by_item_number: bool = True
    match_by_material_designator: bool = False
    check_subinventory: bool = True
    valid_subinventories: Set[str] = field(
        default_factory=lambda: {'STOCK', 'LAB'}
    )
    aging_periods: List[str] = field(
        default_factory=lambda: ['0-30', '31-60', '61-90']
    )
    required_fields: List[str] = field(
        default_factory=lambda: [
            'Item Number', 
            'Organization Code',
            'Subinventory Code',
            'Quantity',
            'Total Value'
        ]
    )
    
    
    def is_valid_subinventory(self, subinv: str) -> bool:
        """Valida si un subinventory cumple con los criterios establecidos"""
        if not self.check_subinventory:
            return True
        return subinv.strip().upper() in self.valid_subinventories

    def get_aging_total(self, aging_values: dict) -> float:
        """Calcula el total de aging según los períodos configurados"""
        return sum(
            float(aging_values.get(f'Aging {period} Quantity', 0) or 0)
            for period in self.aging_periods
        )