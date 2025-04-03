import sys
from pathlib import Path
import logging
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
from PyQt6.QtWidgets import QApplication

# Configure PYTHONPATH
project_root = Path(__file__).parent.absolute()
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))
sys.path.insert(0, str(project_root))

# Configure basic logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('audit_debug.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

from presentation.views.audit_view import AuditView
from presentation.controllers.audit_controller import AuditController
from application.services.audit_service import AuditService
from infrastructure.persistence.excel_repository import ExcelRepository
from application.use_cases.audit_processor import AuditProcessor
from application.use_cases.report_generator import ReportGenerator
from infrastructure.logging.logger import get_logger

class AuditProcessApp:
    def __init__(self):
        self.logger = get_logger(__name__)
        self.executor = ThreadPoolExecutor()
        self._init_paths()

    def _init_paths(self) -> None:
        """Initialize and validate required paths"""
        self.base_path = project_root
        self.config_path = self.base_path / "config"
        self.logs_path = self.base_path / "logs"
        self.reports_path = self.base_path / "reports"
        
        # Create necessary directories
        for path in [self.config_path, self.logs_path, self.reports_path]:
            path.mkdir(exist_ok=True)

    def initialize(self) -> Optional[AuditView]:
        """Initialize application components"""
        try:
            self.logger.info("Starting Audit Process Tool")
            
            # Initialize repository
            excel_repository = ExcelRepository(
                base_path=self.base_path,
                config_path=self.config_path
            )
            
            # Initialize processors and generators
            audit_processor = AuditProcessor(
                repository=excel_repository,
                executor_workers=4
            )
            report_generator = ReportGenerator(output_dir=str(self.reports_path))
            
            # Initialize service
            audit_service = AuditService(
                audit_processor=audit_processor,
                report_generator=report_generator,
                excel_repository=excel_repository,
                config_path=self.config_path
            )
            
            # Initialize controller and view
            audit_controller = AuditController(audit_service)
            app = AuditView(controller=audit_controller)
            
            self.logger.info("Application initialized successfully")
            return app
                    
        except Exception as e:
            self.logger.error(f"Critical error during initialization: {str(e)}")
            self.cleanup()
            return None
        
        finally:
            self.logger.info("Initialization process completed")
 
    def cleanup(self) -> None:
        """Clean up application resources"""
        try:
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=True)
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
        finally:
            self.logger.info("Application cleanup completed")

def main():
    """Main entry point"""
    # Crear QApplication ANTES de inicializar cualquier componente
    app = QApplication(sys.argv)
    
    audit_process_app = AuditProcessApp()
    try:
        view = audit_process_app.initialize()
        if view:
            view.show()  # Cambiar run() por show()
            sys.exit(app.exec())  # Manejar el event loop aquí
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        input("Press Enter to exit...")
    finally:
        audit_process_app.cleanup()
        logger.info("Application terminated")

if __name__ == "__main__":
    main()