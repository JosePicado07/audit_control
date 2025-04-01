from pathlib import Path
import time
from datetime import datetime
from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel
from PyQt6.QtGui import QColor

class AuditFileInfo:
    """Clase para almacenar y calcular información sobre archivos"""
    
    def __init__(self, filepath: str = None):
        self.filepath = filepath
        self.filesize = 0  # Tamaño en bytes
        self.rows = 0      # Número estimado de filas
        self.columns = 0   # Número estimado de columnas
        self.last_modified = None  # Última modificación
        self.estimated_time = 0  # Tiempo estimado en segundos
        
        if filepath and Path(filepath).exists():
            self.update_info()
    
    def update_info(self):
        """Actualiza la información del archivo"""
        if not self.filepath or not Path(self.filepath).exists():
            return
            
        # Información básica del archivo
        file_path = Path(self.filepath)
        self.filesize = file_path.stat().st_size
        self.last_modified = datetime.fromtimestamp(file_path.stat().st_mtime)
        
        # Estimar número de filas basado en el tamaño y tipo
        if file_path.suffix.lower() == '.xlsx':
            # Para archivos Excel, estimamos las filas basadas en el tamaño
            # 1MB ~ 10,000 filas como regla general (depende del contenido)
            self.rows = int((self.filesize / (1024 * 1024)) * 10000)
            # Si el archivo es pequeño, asegurar un mínimo
            self.rows = max(self.rows, 100)
            
            # Estimar columnas
            self.columns = 20  # Valor típico para archivos de auditoría
            
        else:
            # Para otros tipos de archivo, usar una estimación genérica
            self.rows = int(self.filesize / 100)  # Aproximación muy básica
            self.columns = 10
        
        # Estimar tiempo de procesamiento
        # Basado en benchmarks: ~10,000 filas/segundo en máquina promedio
        processing_speed = 10000  # filas por segundo
        
        # Cálculo base de tiempo de procesamiento
        base_time = self.rows / processing_speed
        
        # Factores de ajuste
        size_factor = 1.0 + (self.filesize / (1024 * 1024 * 100))  # Ajuste por tamaño (MB)
        column_factor = 1.0 + (self.columns / 50)  # Ajuste por columnas
        
        # Tiempo estimado final (con un mínimo razonable)
        self.estimated_time = max(5, base_time * size_factor * column_factor)
    
    def get_size_str(self) -> str:
        """Devuelve tamaño del archivo en formato legible"""
        if self.filesize < 1024:
            return f"{self.filesize} bytes"
        elif self.filesize < 1024 * 1024:
            return f"{self.filesize/1024:.1f} KB"
        elif self.filesize < 1024 * 1024 * 1024:
            return f"{self.filesize/(1024*1024):.1f} MB"
        else:
            return f"{self.filesize/(1024*1024*1024):.2f} GB"
    
    def get_rows_str(self) -> str:
        """Devuelve número de filas en formato legible"""
        if self.rows < 1000:
            return f"{self.rows} rows"
        elif self.rows < 1000000:
            return f"{self.rows/1000:.1f}K rows"
        else:
            return f"{self.rows/1000000:.1f}M rows"
    
    def get_time_estimate_str(self) -> str:
        """Devuelve tiempo estimado en formato legible"""
        if self.estimated_time < 60:
            return f"~{int(self.estimated_time)} seconds"
        elif self.estimated_time < 3600:
            return f"~{int(self.estimated_time/60)} minutes"
        else:
            hours = int(self.estimated_time / 3600)
            minutes = int((self.estimated_time % 3600) / 60)
            return f"~{hours} hour{'s' if hours > 1 else ''} {minutes} min"
    
    def get_summary(self) -> str:
        """Devuelve resumen completo de información"""
        if not self.filepath:
            return "No file selected"
            
        return (
            f"Size: {self.get_size_str()} | "
            f"Rows: {self.get_rows_str()} | "
            f"Est. Time: {self.get_time_estimate_str()}"
        )


class FileInfoWidget(QWidget):
    """Widget para mostrar información de archivos"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.file_info = None
        self.fg_color = QColor("#E2E8F0")  # Texto claro
        self.secondary_color = QColor("#94A3B8")  # Gris claro para info secundaria
        
        # Configurar widget
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(2)
        
        # Etiqueta con información
        self.info_label = QLabel("No file selected")
        self.info_label.setStyleSheet(f"color: {self.secondary_color.name()}; font-size: 12px;")
        
        self.layout.addWidget(self.info_label)
        
    def update_info(self, filepath: str):
        """Actualiza la información mostrada con un nuevo archivo"""
        if not filepath:
            self.info_label.setText("No file selected")
            return
            
        self.file_info = AuditFileInfo(filepath)
        self.info_label.setText(self.file_info.get_summary())