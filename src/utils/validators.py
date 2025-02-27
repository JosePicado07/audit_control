"""Validadores comunes para la aplicación"""
from pathlib import Path
from typing import List, Optional
import pandas as pd
from .constant import EXCEL_EXTENSIONS

class FileValidator:
    @staticmethod
    def validate_excel_file(file_path: str) -> bool:
        """Valida que el archivo sea un Excel válido"""
        path = Path(file_path)
        return path.exists() and path.suffix.lower() in EXCEL_EXTENSIONS

    @staticmethod
    def validate_required_columns(df: pd.DataFrame, required_columns: List[str]) -> bool:
        """Valida que el DataFrame tenga las columnas requeridas"""
        return all(column in df.columns for column in required_columns)

class ContractValidator:
    @staticmethod
    def validate_contract_format(contract: str) -> bool:
        """Valida el formato del código de contrato"""
        if not contract:
            return False
        # Implementar reglas específicas de validación de contratos
        return True

    @staticmethod
    def validate_contract_exists(contract: str, valid_contracts: List[str]) -> bool:
        """Valida que el contrato exista en la lista de contratos válidos"""
        return contract in valid_contracts