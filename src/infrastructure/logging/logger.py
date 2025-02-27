"""Configuración centralizada de logging"""
import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from utils.constant import LOGS_DIR, LOG_FORMAT, LOG_DATE_FORMAT

class LoggerConfig:
    """Configuración del sistema de logging"""
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not LoggerConfig._initialized:
            self.log_dir = Path(LOGS_DIR)
            self.log_dir.mkdir(parents=True, exist_ok=True)
            self._loggers = {}
            LoggerConfig._initialized = True

    def get_logger(self, name: str) -> logging.Logger:
        """
        Obtiene o crea un logger con la configuración especificada
        
        Args:
            name: Nombre del logger
            
        Returns:
            Logger configurado
        """
        if name not in self._loggers:
            self._loggers[name] = self._setup_logger(name)
        return self._loggers[name]

    def _setup_logger(self, name: str) -> logging.Logger:
        """
        Configura un nuevo logger
        
        Args:
            name: Nombre del logger
            
        Returns:
            Logger configurado
        """
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            # File Handler
            log_file = self.log_dir / f'audit_process_{datetime.now().strftime("%Y%m%d")}.log'
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setFormatter(
                logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
            )
            logger.addHandler(file_handler)

            # Console Handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(
                logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
            )
            logger.addHandler(console_handler)

        return logger

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Función de conveniencia para obtener un logger configurado
    
    Args:
        name: Nombre opcional del logger. Si no se proporciona, 
              se usa el nombre del módulo llamante
              
    Returns:
        Logger configurado
    """
    if name is None:
        import inspect
        frame = inspect.currentframe()
        if frame:
            caller = frame.f_back
            if caller:
                name = caller.f_globals.get('__name__', 'default')
    
    return LoggerConfig().get_logger(name or 'default')