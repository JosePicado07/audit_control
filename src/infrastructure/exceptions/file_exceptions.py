# Crear en infrastructure/exceptions/file_exceptions.py
class FileOperationError(Exception):
    """Excepción lanzada cuando falla una operación de archivo."""
    def __init__(self, message: str, file_path: str = None, operation: str = None):
        self.file_path = file_path
        self.operation = operation
        self.message = f"Error en operación de archivo"
        if operation:
            self.message += f" durante {operation}"
        self.message += f": {message}"
        if file_path:
            self.message += f" (Archivo: {file_path})"
        super().__init__(self.message)