"""Constantes utilizadas en toda la aplicación"""

# Rutas de directorios
REPORTS_DIR = "reports"
LOGS_DIR = "logs"
TEMP_DIR = "temp"

# Configuración de Excel
EXCEL_EXTENSIONS = ['.xlsx', '.xls']
DEFAULT_SHEET_NAME = 'Sheet1'

# Mensajes de Error
ERROR_MESSAGES = {
    'FILE_NOT_FOUND': 'El archivo no existe: {}',
    'INVALID_CONTRACT': 'Contrato no válido: {}',
    'MISSING_REQUIREMENTS': 'No se encontraron requisitos para el contrato: {}',
    'INVALID_FILE_FORMAT': 'Formato de archivo no válido. Debe ser: {}',
}

# Estados de Auditoría
AUDIT_STATUS = {
    'PENDING': 'PENDING',
    'IN_PROGRESS': 'IN_PROGRESS',
    'COMPLETED': 'COMPLETED',
    'FAILED': 'FAILED'
}

"""Constantes para el sistema de logging"""

# Rutas de directorios
LOGS_DIR = "logs"

# Formato de logging
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Niveles de logging
LOG_LEVELS = {
    'DEBUG': 10,
    'INFO': 20,
    'WARNING': 30,
    'ERROR': 40,
    'CRITICAL': 50
}