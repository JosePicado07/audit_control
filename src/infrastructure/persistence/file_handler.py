import shutil
import os
from pathlib import Path
from typing import Optional
import asyncio
from concurrent.futures import ThreadPoolExecutor
from infrastructure.logging.logger import logger

class FileHandler:
    def __init__(self, base_dir: str = "temp"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        self.executor = ThreadPoolExecutor()
        self.loop = asyncio.get_event_loop()
        
    async def save_temp_file(self, content: bytes, filename: str) -> str:
        """Guarda un archivo temporal de forma asíncrona"""
        try:
            return await self.loop.run_in_executor(
                self.executor,
                self._save_temp_file_sync,
                content,
                filename
            )
        except Exception as e:
            logger.error(f"Error saving temp file: {str(e)}")
            raise
            
    def _save_temp_file_sync(self, content: bytes, filename: str) -> str:
        """Implementación síncrona de guardado de archivo"""
        temp_path = self.base_dir / filename
        with open(temp_path, 'wb') as f:
            f.write(content)
        return str(temp_path)
            
    async def cleanup_temp_file(self, file_path: str) -> None:
        """Limpia un archivo temporal de forma asíncrona"""
        try:
            await self.loop.run_in_executor(
                self.executor,
                self._cleanup_temp_file_sync,
                file_path
            )
        except Exception as e:
            logger.error(f"Error cleaning up temp file: {str(e)}")
            
    def _cleanup_temp_file_sync(self, file_path: str) -> None:
        """Implementación síncrona de limpieza de archivo"""
        if os.path.exists(file_path):
            os.unlink(file_path)
            
    async def move_file(self, src: str, dest: str) -> Optional[str]:
        """Mueve un archivo a una nueva ubicación de forma asíncrona"""
        try:
            return await self.loop.run_in_executor(
                self.executor,
                self._move_file_sync,
                src,
                dest
            )
        except Exception as e:
            logger.error(f"Error moving file: {str(e)}")
            return None
            
    def _move_file_sync(self, src: str, dest: str) -> str:
        """Implementación síncrona de movimiento de archivo"""
        dest_path = Path(dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(src, dest)
        return dest
        
    def __del__(self):
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)