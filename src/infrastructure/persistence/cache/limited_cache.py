# infrastructure/persistence/cache/limited_cache.py
from typing import Dict, List, Optional, TypeVar, Generic, Any

T = TypeVar('T')

class LimitedCache(Generic[T]):
    """
    Caché con límite de tamaño y política LRU (Least Recently Used).
    Implementado con genericidad para mejorar tipado.
    """
    
    def __init__(self, max_items: int = 10):
        """
        Inicializa el caché con límite de tamaño.
        
        Args:
            max_items: Número máximo de elementos en caché
        """
        self.max_items = max_items
        self.cache: Dict[str, T] = {}
        self.access_order: List[str] = []
    
    def get(self, key: str) -> Optional[T]:
        """
        Obtiene item del caché actualizando orden LRU.
        
        Args:
            key: Clave a buscar
            
        Returns:
            Valor asociado o None si no existe
        """
        if key in self.cache:
            # Actualizar orden de acceso
            self.access_order.remove(key)
            self.access_order.append(key)
            return self.cache[key]
        return None
    
    def set(self, key: str, value: T) -> None:
        """
        Establece item en caché con evicción si es necesario.
        
        Args:
            key: Clave para almacenar
            value: Valor a almacenar
        """
        # Eliminar item si ya existe
        if key in self.cache:
            self.access_order.remove(key)
        
        # Evictar item más antiguo si alcanzamos límite
        if len(self.cache) >= self.max_items:
            oldest_key = self.access_order.pop(0)
            del self.cache[oldest_key]
        
        # Agregar nuevo item
        self.cache[key] = value
        self.access_order.append(key)
    
    def clear(self) -> None:
        """Limpia el caché completo."""
        self.cache.clear()
        self.access_order.clear()
        
    def __len__(self) -> int:
        """Retorna el número de elementos en el caché."""
        return len(self.cache)