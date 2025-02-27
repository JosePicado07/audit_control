from abc import ABC, abstractmethod
from typing import Dict, Optional

class IAuditRepository(ABC):
    """Interfaz para el repositorio de auditorías"""
    
    @abstractmethod
    async def save_audit_results(self, audit_id: str, results: Dict) -> bool:
        """Guarda los resultados de una auditoría"""
        pass
    
    @abstractmethod
    async def get_audit_results(self, audit_id: str) -> Optional[Dict]:
        """Recupera los resultados de una auditoría"""
        pass