from typing import List, Optional, Set

class InventoryColumns:
    """
    Constantes para nombres de columnas del inventario WMS para asegurar consistencia.
    Esta clase maneja tanto los nombres primarios como sus alternativas para una 
    coincidencia flexible de columnas.
    """
    
    # Nombres de columnas primarias para identificación
    ITEM_NUMBER = 'ITEM NUMBER'            # Campo clave para coincidencia
    ORGANIZATION_CODE = 'ORGANIZATION CODE'
    WAREHOUSE_CODE = 'ORG WAREHOUSE CODE'
    SUBINVENTORY_CODE = 'SUBINVENTORY CODE'
    
    # Columnas de aging para análisis de antigüedad del inventario
    AGING_0_30 = 'AGING 0-30 QUANTITY'
    AGING_31_60 = 'AGING 31-60 QUANTITY'
    AGING_61_90 = 'AGING 61-90 QUANTITY'
    AGING_91_120 = 'AGING 91-120 QUANTITY'
    AGING_121_150 = 'AGING 121-150 QUANTITY'
    AGING_151_180 = 'AGING 151-180 QUANTITY'
    AGING_181_365 = 'AGING 181-365 QUANTITY'
    AGING_OVER_365 = 'AGING OVER 365 QUANTITY'
    
    # Otras columnas importantes
    SERIAL_NUMBER = 'SERIAL NUMBER'
    QUANTITY = 'QUANTITY'
    TOTAL_VALUE = 'TOTAL VALUE'
    ITEM_DESCRIPTION = 'ITEM DESCRIPTION'
    MATERIAL_DESIGNATOR = 'MATERIAL DESIGNATOR'  # Ya no es el campo principal
    
    # Alternativas para el número de parte (ahora usando ITEM NUMBER)
    ITEM_NUMBER_ALTERNATIVES: Set[str] = {
        ITEM_NUMBER,
        'Item Number',
        'ITEM_NUMBER',
        'Part Number',
        'PART NUMBER',
        'PART_NUMBER',
        'Item_Number'
    }
    
    # Alternativas para código de organización
    ORGANIZATION_CODE_ALTERNATIVES: Set[str] = {
        ORGANIZATION_CODE,
        'Organization Code',
        'Org Code',
        'ORGANIZATION_CODE',
        'ORG_CODE',
        'Organization',
        'ORGANIZATION'
    }
    
    # Alternativas para cantidad
    QUANTITY_ALTERNATIVES: Set[str] = {
        QUANTITY,
        'Quantity',
        'Total Quantity',
        'TOTAL_QUANTITY'
    }
    
    # Columnas de aging agrupadas para facilitar el procesamiento
    AGING_COLUMNS = [
        AGING_0_30,
        AGING_31_60,
        AGING_61_90,
        AGING_91_120,
        AGING_121_150,
        AGING_151_180,
        AGING_181_365,
        AGING_OVER_365
    ]
    
    @classmethod
    def get_required_columns(cls) -> dict:
        """
        Obtiene las columnas requeridas con sus tipos para validación.
        Incluye todas las columnas necesarias del archivo WMS.
        """
        # Columnas base con sus tipos
        base_columns = {
            cls.ITEM_NUMBER: str,          # Cambiado a ITEM NUMBER como principal
            cls.ORGANIZATION_CODE: str,
            cls.WAREHOUSE_CODE: str,
            cls.SUBINVENTORY_CODE: str,
            cls.SERIAL_NUMBER: str,
            cls.QUANTITY: float,
            cls.TOTAL_VALUE: float,
            cls.ITEM_DESCRIPTION: str,
            cls.MATERIAL_DESIGNATOR: str   # Mantenido pero no es el principal
        }
        
        # Agregar columnas de aging
        aging_columns = {col: float for col in cls.AGING_COLUMNS}
        
        # Combinar diccionarios
        return {**base_columns, **aging_columns}
    
    @classmethod
    def find_best_column_match(cls, df_columns: List[str], alternatives: Set[str]) -> Optional[str]:
        """
        Encuentra la mejor coincidencia de columna desde un conjunto de alternativas.
        Incluye logging para facilitar la depuración.
        
        Args:
            df_columns: Columnas disponibles en el DataFrame
            alternatives: Conjunto de nombres de columna aceptables
        
        Returns:
            Nombre de columna coincidente o None
        """
        # Agregar logging para depuración
        print(f"\nBuscando coincidencia entre columnas:")
        print(f"Columnas disponibles: {df_columns}")
        print(f"Alternativas posibles: {alternatives}")
        
        matches = set(df_columns) & alternatives
        
        if matches:
            match = next(iter(matches))
            print(f"Coincidencia encontrada: {match}")
            return match
            
        print("No se encontraron coincidencias")
        return None