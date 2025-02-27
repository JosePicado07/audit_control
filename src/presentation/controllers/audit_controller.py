# Standard library imports
from datetime import datetime
from tkinter import messagebox
from pathlib import Path
from typing import Dict, Optional
import traceback
from concurrent.futures import ThreadPoolExecutor
import time
import openpyxl
import pandas as pd

# Local application imports
from infrastructure.persistence.excel_repository import ExcelRepository
from application.services.audit_service import AuditService
from infrastructure.logging.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

class AuditController:
    """
    Controller for managing the audit process.
    Handles user interactions and coordinates between view and service layers.
    """
    
    def __init__(self, audit_service: AuditService):
        """Initialize controller with audit service."""
        logger.debug("Initializing AuditController")
        self.audit_service = audit_service
        self.current_file_path: Optional[str] = None
        self.current_contract: Optional[str] = None
        # Executor for handling long-running operations
        self.executor = ThreadPoolExecutor(max_workers=4)

    def execute_audit(self, contract: str, audit_file_path: str, inventory_file: Optional[str] = None) -> Dict:
        """
        Execute the audit process with proper file validation.
        
        Args:
            contract: Contract identifier
            audit_file_path: Path to the audit file
            inventory_file: Optional path to WMS inventory file
            
        Returns:
            Dict containing audit results and status
        """
        try:
            logger.info(f"Starting audit execution for contract: {contract}")
            
            # Validate audit file
            if not Path(audit_file_path).exists():
                raise ValueError(f"Audit file {audit_file_path} does not exist")
            
            # Validate contract
            if not contract:
                raise ValueError("Contract is required")
            
            # Execute audit using service
            result = self.audit_service.execute_audit(
                contract=contract,
                file_path=audit_file_path,
                inventory_file=inventory_file
            )
            
            logger.debug(f"Audit execution completed with result: {result}")
            return result

        except Exception as e:
            logger.error(f"Error executing audit: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            messagebox.showerror(
                "Error",
                f"Error processing audit: {str(e)}"
            )
            raise

    def process_audit(
        self, 
        contract: str, 
        file_path: str,
        inventory_file: Optional[str] = None,
        use_inventory : bool = True,
        progress_callback=None,
        progress_steps=None
    ) -> Dict:
        """Process a new audit request with progress updates."""
        logger.debug("==== START PROCESS AUDIT ====")
        logger.debug(f"Contract: {contract}")
        logger.debug(f"Main file path: {file_path}")
        logger.debug(f"Inventory file: {inventory_file}")
        
        try:
            # Validate main audit file
            if progress_callback:
                progress_callback(0.05, "Validating main audit file...")
            logger.debug("Validating main audit file...")
            self.audit_service.excel_repository.validate_input_file(file_path)
            
            # Process inventory file if provided
            if inventory_file:
                if progress_callback:
                    progress_callback(0.10, "Processing inventory file...")
                logger.debug("Processing inventory file...")
                self._process_inventory_file(inventory_file)
                
            if use_inventory and inventory_file:
                if progress_callback:
                    progress_callback(0.10, "Processing Inventory file...")
                logger.debug("Processing inventory file")
                self._process_inventory_file(inventory_file)
            
            # Execute audit with progress updates
            audit_steps = [
                (0.15, "Starting audit process..."),
                (0.20, "Reading and validating data..."),
                (0.30, "Processing Serial Control validation..."),
                (0.40, "Checking organization structures..."),
                (0.50, "Analyzing inventory data..."),
                (0.60, "Validating program requirements..."),
                (0.70, "Generating serial control report..."),
                (0.80, "Creating organization validation report..."),
                (0.90, "Applying formatting to reports..."),
                (0.95, "Saving final reports...")
            ]

            for progress, message in audit_steps:
                if progress_callback:
                    progress_callback(progress, message)
            
            # Execute audit
            result = self.execute_audit(contract, file_path, inventory_file if use_inventory else None)
            
            if result["status"] == "success":
                if progress_callback:
                    progress_callback(1.0, "Audit completed successfully!")
                
                reports_info = {
                    "external": Path(result["data"]["external_report_path"]).name,
                    "internal": Path(result["data"]["internal_report_path"]).name,
                    "summary": {
                        "serial_control": result["data"]["summary"]["serial_control"],
                        "organization": result["data"]["summary"]["organization"]
                    }
                }
                return {
                    "status": "success",
                    "data": reports_info
                }
                
            logger.debug("==== END PROCESS AUDIT ====")
            return result

        except Exception as e:
            if progress_callback:
                progress_callback(1.0, "Error during audit process")
            logger.error(f"Error in process_audit: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return {
                "status": "error",
                "message": str(e)
            }
        
    def validate_inputs(self, contract: str, file_path: str) -> bool:
        """
        Validate audit inputs.
        Shows error messages to user if validation fails.
        
        Args:
            contract: Contract identifier to validate
            file_path: File path to validate
            
        Returns:
            bool indicating if inputs are valid
        """
        if not contract:
            messagebox.showerror("Error", "Contract is required")
            return False
            
        if not file_path or not Path(file_path).exists():
            messagebox.showerror("Error", "Invalid file")
            return False
            
        return True

    def _process_inventory_file(self, inventory_file: str) -> Optional[str]:
        """Process inventory file using ExcelRepository"""
        try:
            logger.debug("Validating inventory file structure...")
            self.audit_service.excel_repository.validate_inventory_file(inventory_file)
            
            # Read the validated file with normalized columns
            logger.debug("Reading validated inventory file...")
            inventory_df = self.audit_service.excel_repository.read_excel_file(
                inventory_file,
                is_inventory=True
            )
            
            if inventory_df is None or inventory_df.empty:
                logger.warning("No data found in inventory file")
                return None
                
            # Save processed inventory
            processed_path = self._save_processed_inventory(inventory_df)
            logger.info(f"Inventory file processed and saved: {processed_path}")
            
            return processed_path
            
        except Exception as e:
            logger.error(f"Error processing inventory file: {str(e)}")
            raise
        
    def _save_processed_inventory(self, inventory_df: pd.DataFrame) -> str:
        """Save processed inventory"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("reports/inventory")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"inventory_{timestamp}.xlsx"
        
        self.audit_service.excel_repository.save_excel_file(inventory_df, output_path)
        return str(output_path)

    def cleanup(self):
        """Clean up resources used by the controller."""
        logger.debug("Cleaning up controller resources")
        try:
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=True)
        except Exception as e:
            logger.error(f"Error during controller cleanup: {str(e)}")