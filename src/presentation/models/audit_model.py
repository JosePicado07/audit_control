"""Modelo de datos para la vista de auditoría"""
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class AuditViewModel:
    contract: str
    file_path: Optional[str]
    status: str
    results: Optional[Dict] = None
    
    @property
    def is_ready(self) -> bool:
        """Verifica si está listo para iniciar la auditoría"""
        return bool(self.contract and self.file_path)
    
    def update_status(self, status: str):
        """Actualiza el estado"""
        self.status = status
    
    def set_results(self, results: Dict):
        """Establece los resultados"""
        self.results = results