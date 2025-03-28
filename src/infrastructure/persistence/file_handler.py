import shutil
import os
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
from infrastructure.exceptions.file_exceptions import FileOperationError
from infrastructure.logging.logger import logger

class FileHandler:
    def __init__(self, base_dir: str = "temp", max_age_hours: int = 24):
        """
        Inicializa el manejador de archivos.
        
        Args:
            base_dir: Directorio base para archivos temporales
            max_age_hours: Edad máxima en horas para archivos antes de limpieza automática
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True, parents=True)
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.max_age_hours = max_age_hours
        
        # Limpiar archivos viejos automáticamente al iniciar
        self._clean_old_files()
            
    def save_temp_file(self, content: bytes, filename: str, max_size_mb: int = 100) -> str:
        """
        Guarda un archivo temporal con validación de tamaño.
        
        Args:
            content: Contenido del archivo en bytes
            filename: Nombre del archivo
            max_size_mb: Tamaño máximo en MB permitido
            
        Returns:
            Ruta al archivo guardado
            
        Raises:
            FileOperationError: Si falla la operación de archivo
        """
        try:
            # Validar tamaño
            content_size_mb = len(content) / (1024 * 1024)
            if content_size_mb > max_size_mb:
                raise FileOperationError(
                    f"Archivo demasiado grande: {content_size_mb:.2f}MB (máximo: {max_size_mb}MB)",
                    filename,
                    "save"
                )
            
            # Sanitizar nombre de archivo
            safe_filename = self._sanitize_filename(filename)
            temp_path = self.base_dir / safe_filename
            
            # Escribir archivo
            try:
                with open(temp_path, 'wb') as f:
                    f.write(content)
                
                logger.debug(f"Archivo temporal guardado: {temp_path}")
                return str(temp_path)
            except Exception as e:
                raise FileOperationError(f"Error en operación de escritura: {str(e)}", str(temp_path), "write")
                
        except FileOperationError:
            raise
        except Exception as e:
            error_message = f"Error guardando archivo temporal: {str(e)}"
            logger.error(error_message)
            raise FileOperationError(error_message, filename, "save") from e
        
    def _sanitize_filename(self, filename: str) -> str:
        """
        Sanitiza el nombre de archivo para seguridad.
        
        Args:
            filename: Nombre original
            
        Returns:
            Nombre sanitizado
        """
        # Eliminar caracteres problemáticos
        invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
        safe_name = filename
        for char in invalid_chars:
            safe_name = safe_name.replace(char, '_')
        
        return safe_name
            
    def cleanup_temp_file(self, file_path: str) -> None:
        """
        Limpia un archivo temporal.
        
        Args:
            file_path: Ruta al archivo a limpiar
        """
        try:
            path = Path(file_path)  # Normalizar a Path
            if path.exists():
                try:
                    path.unlink()  # Usar métodos de Path
                    logger.debug(f"Archivo temporal eliminado: {file_path}")
                except Exception as e:
                    logger.warning(f"No se pudo eliminar archivo temporal {file_path}: {str(e)}")
        except Exception as e:
            logger.error(f"Error limpiando archivo temporal: {str(e)}")
            # No lanzamos excepción para permitir continuidad en caso de errores de limpieza
            
    def _clean_old_files(self) -> None:
        """
        Limpia archivos temporales antiguos basado en max_age_hours.
        """
        try:
            import time
            from datetime import datetime, timedelta
            
            current_time = datetime.now()
            cutoff_time = current_time - timedelta(hours=self.max_age_hours)
            cutoff_timestamp = cutoff_time.timestamp()
            
            count = 0
            for temp_file in self.base_dir.glob('*'):
                if temp_file.is_file():
                    file_mtime = temp_file.stat().st_mtime
                    if file_mtime < cutoff_timestamp:
                        try:
                            temp_file.unlink()
                            count += 1
                        except Exception as e:
                            logger.warning(f"Error al eliminar archivo antiguo {temp_file}: {str(e)}")
            
            if count > 0:
                logger.info(f"Limpieza automática: {count} archivos temporales antiguos eliminados")
        
        except Exception as e:
            logger.error(f"Error en limpieza automática: {str(e)}")
          
    def move_file(self, src: str, dest: str) -> str:
        """
        Mueve un archivo a una nueva ubicación.
        
        Args:
            src: Ruta origen
            dest: Ruta destino
            
        Returns:
            Ruta al archivo movido
            
        Raises:
            FileOperationError: Si falla la operación de movimiento
        """
        try:
            src_path = Path(src)
            dest_path = Path(dest)
            
            if not src_path.exists():
                raise FileOperationError(f"Archivo de origen no existe", src, "move")
            
            try:
                # Crear directorio de destino si no existe
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Mover archivo
                shutil.move(str(src_path), str(dest_path))
                logger.debug(f"Archivo movido: {src} -> {dest}")
                
                return str(dest_path)
            except Exception as e:
                raise FileOperationError(f"Error moviendo archivo: {str(e)}", f"{src} -> {dest}", "move")
        except FileOperationError:
            raise
        except Exception as e:
            error_message = f"Error moviendo archivo: {str(e)}"
            logger.error(error_message)
            raise FileOperationError(error_message, f"{src} -> {dest}", "move") from e
        
    def cleanup(self) -> None:
        """
        Limpia los recursos utilizados de manera segura.
        """
        try:
            if hasattr(self, 'executor'):
                logger.debug("Apagando executor...")
                self.executor.shutdown(wait=False)  # No esperar para evitar bloqueo
                logger.debug("Executor apagado")
        except Exception as e:
            logger.error(f"Error durante limpieza: {str(e)}")

    def __del__(self):
        """Garantiza limpieza de recursos al destruir el objeto."""
        try:
            self.cleanup()
        except Exception as e:
            logger.error(f"Error durante limpieza en __del__: {str(e)}")
            
    def list_temp_files(self, pattern: str = "*") -> list:
        """
        Lista archivos temporales que coinciden con un patrón.
        
        Args:
            pattern: Patrón glob para filtrar archivos
            
        Returns:
            Lista de rutas a archivos
        """
        try:
            return [str(f) for f in self.base_dir.glob(pattern) if f.is_file()]
        except Exception as e:
            logger.error(f"Error listando archivos temporales: {str(e)}")
            return []