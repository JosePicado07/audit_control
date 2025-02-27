# Standard library imports
import threading
import time
import traceback
from pathlib import Path
from typing import Optional
import os
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

# Third-party imports
import customtkinter as ctk
from PIL import Image
from tkinter import messagebox, filedialog

# Local application imports
from application.use_cases.audit_processor import AuditProcessor
from application.use_cases.report_generator import ReportGenerator
from infrastructure.logging.logger import get_logger
from infrastructure.persistence.excel_repository import ExcelRepository
from presentation.controllers.audit_controller import AuditController
from application.services.audit_service import AuditService

# Initialize logger
logger = get_logger(__name__)

class AuditView:
    def __init__(self, controller: Optional[AuditController] = None):
        """Initialize the audit view"""
        logger.debug("Initializing AuditView")
        self.root= ctk.CTk()
        self.use_inventory_var = ctk.BooleanVar(value=True)
        self.controller = controller or self._create_controller()
        self.executor = ThreadPoolExecutor(max_workers=4)
        self._setup_ui()

    def _create_controller(self) -> AuditController:
        """Create controller with dependencies"""
        logger.debug("Creating controller with dependencies...")
        
        # Create repository
        excel_repo = ExcelRepository()
        logger.debug("Created ExcelRepository")
        
        # Create processor
        audit_processor = AuditProcessor(
            repository=excel_repo,
            executor_workers=4
        )
        logger.debug("Created AuditProcessor")
        
        # Create report generator
        report_generator = ReportGenerator()
        logger.debug("Created ReportGenerator")
        
        # Create service
        audit_service = AuditService(
            audit_processor=audit_processor,
            report_generator=report_generator,
            excel_repository=excel_repo
        )
        logger.debug("Created AuditService")
        
        # Create and return controller
        return AuditController(audit_service)

    def _setup_ui(self) -> None:
        """Configure the user interface"""
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        
        self.root.title("Audit Process Tool")
        self.root.geometry("900x600")
        self.root.minsize(800,600)
        
        # Load icons
        try:
            self.folder_icon = ctk.CTkImage(
                light_image=Image.open("src/assets/folder_light.png"),
                dark_image=Image.open("src/assets/folder_dark.png"),
                size=(20, 20)
            )
        except FileNotFoundError:
            logger.warning("Icon files not found")
            self.folder_icon = None
        
        self._create_main_frame()
        self._create_inputs()
        self._create_buttons()
        self._create_progress_bar()
        self._create_status_bar()

    def _create_main_frame(self) -> None:
        """Create the main application frame"""
        self.main_frame = ctk.CTkFrame(self.root)
        self.main_frame.pack(padx=20, pady=20, fill="both", expand=True)


    def _toggle_inventory(self) -> None:
        """Handle inventory switch toggle"""
        use_inventory = self.use_inventory_var.get()
        if use_inventory:
            self.inventory_frame.pack(padx=5, pady=5, fill="x")
        else:
            self.inventory_frame.pack_forget()
            self.inventory_path_entry.delete(0, "end")

    def _create_buttons(self) -> None:
        """Create control buttons with better spacing and alignment"""
        button_frame = ctk.CTkFrame(self.main_frame)
        button_frame.pack(pady=20, fill="x", padx=20)

        self.start_button = ctk.CTkButton(
            button_frame,
            text="Start Audit",
            command=self.start_audit,
            height=40,
            width=200  # Ancho fijo para mejor apariencia
        )
        self.start_button.pack(side="left", padx=10)

        self.clean_reports_button = ctk.CTkButton(
            button_frame,
            text="Reset Fields",
            command=self._clean_reports,
            height=40,
            width=200,
            fg_color="#FF8C00",  # Color naranja más atractivo
            hover_color="#FF6B00"  # Color hover más oscuro
        )
        self.clean_reports_button.pack(side="right", padx=10)
        
        
    def _clean_reports(self) -> None:
        """Clean and reset input fields to start a new analysis"""
        try:
            # Reset UI elements
            self.contract_entry.delete(0, "end")
            self.file_path_entry.delete(0, "end")
            self.inventory_path_entry.delete(0, "end")
            self.use_inventory_var.set(True)  # Reset inventory validation to enabled
            self.inventory_path_entry.configure(state="normal")
            self.progress_bar.set(0)
            self.status_label.configure(text="Ready")
            
            messagebox.showinfo("Reset Complete", "All fields have been cleared. Ready for new analysis.")
                
        except Exception as e:
            logger.error(f"Error resetting fields: {str(e)}")
            messagebox.showerror("Reset Error", f"An error occurred while resetting the fields: {str(e)}")

    def _create_progress_bar(self) -> None:
        """Create progress bar"""
        self.progress_bar = ctk.CTkProgressBar(self.main_frame)
        self.progress_bar.pack(pady=20, fill="x", padx=10)
        self.progress_bar.set(0)

    def _create_status_bar(self) -> None:
        """Create status bar"""
        self.status_label = ctk.CTkLabel(
            self.root,
            text="Ready",
            anchor="w"
        )
        self.status_label.pack(side="bottom", fill="x", padx=10, pady=5)
        
    def _browse_file(self) -> None:
        """Open file selection dialog for audit file"""
        filename = filedialog.askopenfilename(
            title="Select Audit File",
            filetypes=(
                ("Excel files", "*.xlsx"),
                ("CSV files", "*.csv"),
                ("All files", "*.*")
            )
        )
        if filename:
            self.file_path_entry.delete(0, "end")
            self.file_path_entry.insert(0, filename)
        
    def _create_inputs(self) -> None:
        """Create input fields"""
        input_frame = ctk.CTkFrame(self.main_frame)
        input_frame.pack(padx=20, pady=10, fill="x")

        # Contract input
        contract_label = ctk.CTkLabel(input_frame, text="Contract:")
        contract_label.pack(padx=5, pady=(10,5), anchor="w")
        
        self.contract_entry = ctk.CTkEntry(
            input_frame, 
            placeholder_text="Enter contract number",
            height=32
        )
        self.contract_entry.pack(padx=5, pady=(0,10), fill="x")

        # Audit file frame
        audit_label = ctk.CTkLabel(input_frame, text="Audit File:")
        audit_label.pack(padx=5, pady=(5,5), anchor="w")
        
        file_frame = ctk.CTkFrame(input_frame)
        file_frame.pack(padx=5, pady=(0,10), fill="x")
        
        self.file_path_entry = ctk.CTkEntry(
            file_frame,
            placeholder_text="Select audit file",
            height=32
        )
        self.file_path_entry.pack(side="left", padx=5, fill="x", expand=True)
        
        browse_button = ctk.CTkButton(
            file_frame,
            text="Browse",
            command=self._browse_file,
            width=100,
            height=32,
            image=self.folder_icon if self.folder_icon else None,
            compound="left"
        )
        browse_button.pack(side="right", padx=5)

        # Inventory validation switch - Este es el switch que queremos ver
        self.use_inventory_check = ctk.CTkCheckBox(
            input_frame,
            text="Use Inventory Validation",
            variable=self.use_inventory_var,
            command=self._toggle_inventory,
            height=24,
            checkbox_height=20,
            checkbox_width=20,
            corner_radius=4,
            border_width=2,
            fg_color="blue",
            hover_color="#0066cc",
            text_color="white"
        )
        self.use_inventory_check.pack(padx=15, pady=(5,10), anchor="w")

        # Inventory file frame
        self.inventory_frame = ctk.CTkFrame(input_frame)
        self.inventory_frame.pack(padx=5, pady=(0,10), fill="x")
        
        inventory_label = ctk.CTkLabel(self.inventory_frame, text="Inventory File:")
        inventory_label.pack(side="left", padx=5)
        
        self.inventory_path_entry = ctk.CTkEntry(
            self.inventory_frame,
            placeholder_text="Select inventory file",
            height=32
        )
        self.inventory_path_entry.pack(side="left", padx=5, fill="x", expand=True)
        
        inventory_browse_button = ctk.CTkButton(
            self.inventory_frame,
            text="Browse",
            command=self._browse_inventory_file,
            width=100,
            height=32,
            image=self.folder_icon if self.folder_icon else None,
            compound="left"
        )
        inventory_browse_button.pack(side="right", padx=5)

    def _browse_inventory_file(self) -> None:
        """Open file selection dialog for inventory file"""
        filename = filedialog.askopenfilename(
            title="Select Inventory File",
            filetypes=(
                ("Excel files", "*.xlsx"),
                ("CSV files", "*.csv"),
                ("All files", "*.*")
            )
        )
        if filename:
            self.inventory_path_entry.delete(0, "end")
            self.inventory_path_entry.insert(0, filename)

    def start_audit(self) -> None:
        """Start the audit process with detailed progress updates"""
        logger.debug("-------------------- START AUDIT PRESSED --------------------")
        
        try:
            contract = self.contract_entry.get().strip()
            file_path = self.file_path_entry.get().strip()
            inventory_path = None
            
            if self.use_inventory_var.get():
                inventory_path = self.inventory_path_entry.get().strip()
            
            if not self._validate_inputs(contract, file_path, inventory_path):
                return

            # Update UI before processing
            self._update_ui_state(True)
            self._update_progress(0.05, "Initializing audit process...")
            
            def run_audit():
                try:
                    steps = {
                        0.05: "Initializing audit process...",
                        0.10: "Opening and validating input file structure...",
                        0.15: "Reading input file data (this may take several minutes)...",
                        0.20: "Processing row 1 to 50,000...",
                        0.25: "Processing row 50,001 to 100,000...",
                        0.30: "Processing row 100,001 to 150,000...",
                        0.35: "Processing row 150,001 to 200,000...",
                        0.40: "Processing row 200,001 to 250,000...",
                        0.45: "Normalizing data and validating columns...",
                        0.50: "Loading program requirements from PDM...",
                        # Serial Control Audit
                        0.55: "Starting Serial Control audit...",
                        0.58: "Comparing serial control across organizations...",
                        0.60: "Checking inventory systems for mismatches...",
                        0.62: "Validating non-hardware parts...",
                        # Organization Audit
                        0.65: "Starting Organization Structure audit...",
                        0.68: "Analyzing organization hierarchies...",
                        0.70: "Checking for missing organizations...",
                        0.72: "Validating organization relationships...",
                        # Customer ID Audit
                        0.75: "Starting Customer ID audit...",
                        0.78: "Validating Customer ID formats...",
                        0.80: "Checking Customer ID consistency...",
                        # Cross Reference Audit
                        0.82: "Starting Cross Reference audit...",
                        0.85: "Validating vendor references...",
                        0.87: "Checking marketing part numbers...",
                        # Final Steps
                        0.90: "Combining all audit results...",
                        0.92: "Generating comprehensive audit summary...",
                        0.94: "Creating final report...",
                        0.96: "Applying formatting and styles...",
                        0.98: "Saving final report...",
                    }

                    def progress_callback(progress: float, message: str):
                        self.root.after(0, self._update_progress, progress, message)
                        self.root.after(800)

                    # Execute audit with progress updates
                    result = self.controller.process_audit(
                        contract, 
                        file_path,
                        inventory_file = inventory_path,
                        use_inventory= self.use_inventory_var.get(),
                        progress_callback=progress_callback,
                        progress_steps=steps
                    )

                    # Handle result in main thread
                    self.root.after(0, self._handle_audit_result, result)

                except Exception as e:
                    logger.error(f"Error in audit: {str(e)}")
                    logger.error(f"Stack trace: {traceback.format_exc()}")
                    self.root.after(0, self._handle_audit_error, e)
                finally:
                    self.root.after(0, self._update_ui_state, False)

            # Start processing thread
            self.executor.submit(run_audit)

        except Exception as e:
            logger.error(f"Error starting audit: {str(e)}")
            self._handle_audit_error(e)
                
            
    def _update_progress(self, progress: float, status_text: str = None) -> None:
        """
        Update progress bar and status text.
        
        Args:
            progress: Float between 0 and 1 indicating progress
            status_text: Optional status message to display
        """
        self.progress_bar.set(progress)
        if status_text:
            self.status_label.configure(text=status_text)

    
    def _update_ui_state(self, is_processing: bool) -> None:
        """Update UI elements based on processing state"""
        if is_processing:
            self.start_button.configure(state="disabled")
            self.contract_entry.configure(state="disabled")
            self.file_path_entry.configure(state="disabled")
            self.progress_bar.set(0)
            self.status_label.configure(text="Processing audit request...")
        else:
            self.start_button.configure(state="normal")
            self.contract_entry.configure(state="normal")
            self.file_path_entry.configure(state="normal")

    def _validate_inputs(self, contract: str, file_path: str, inventory_path: str) -> bool:
        """Validate user inputs"""
        try:
            # Validar contrato
            if not contract:
                messagebox.showwarning("Warning", "Please enter a contract number")
                return False
            
            # Validar archivo de auditoría
            if not file_path or not Path(file_path).exists():
                messagebox.showwarning("Warning", "Please select a valid audit file")
                return False
                
            # Validar archivo principal como archivo de auditoría
            self.controller.audit_service.excel_repository.validate_input_file(file_path)
            
            # Validar archivo de inventario solo si el switch está activado
            if self.use_inventory_var.get():
                if not inventory_path:
                    messagebox.showwarning("Warning", "Please select an inventory file or disable inventory validation")
                    return False
                if not Path(inventory_path).exists():
                    messagebox.showwarning("Warning", "Please select a valid inventory file")
                    return False
                # Usar la validación específica para archivos de inventario
                self.controller.audit_service.excel_repository.validate_inventory_file(inventory_path)
            
            return True
        
        except Exception as e:
            messagebox.showwarning("Warning", f"File validation failed: {str(e)}")
            return False

    def _handle_audit_result(self, result: dict) -> None:
        """Handle audit results"""
        try:
            if result["status"] == "success":
                data = result.get("data", {})
                
                # Use .get() with default values to prevent KeyError
                external_report_name = Path(data.get('external_report_path', '')).name if data.get('external_report_path') else "N/A"
                internal_report_name = Path(data.get('internal_report_path', '')).name if data.get('internal_report_path') else "N/A"
                
                final_message = (
                    "Audit completed successfully.\n\n"
                    "Reports generated:\n"
                    f"1. Serial Control Report: {external_report_name}\n"
                    f"2. Organization Validation Report: {internal_report_name}\n\n"
                    "Please check the reports for detailed findings."
                )
                self._update_status("Audit completed successfully")
                messagebox.showinfo("Success", final_message)
                
                try:
                    # Open REPORTS directory
                    report_dir = Path('reports')
                    if report_dir.exists():
                        os.startfile(str(report_dir))
                    else:
                        logger.warning("REPORTS directory not found")
                except Exception as e:
                    logger.error(f"Error opening REPORTS directory: {str(e)}")
            else:
                self._update_status("Audit process failed")
                error_message = result.get("message", "Unknown error occurred")
                logger.error(f"Audit failed: {error_message}")
                messagebox.showerror("Error", error_message)
        
        except Exception as e:
            logger.error(f"Unexpected error in handling audit result: {str(e)}")
            traceback.print_exc()
            messagebox.showerror("Unexpected Error", f"An unexpected error occurred: {str(e)}")
            
    def _handle_audit_error(self, error: Exception) -> None:
        """Handle errors that occur during the audit process"""
        logger.error(f"Error during audit process: {str(error)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        messagebox.showerror("Audit Error", f"An error occurred during the audit process: {str(error)}")
        self._update_ui_state(False)

    def _update_status(self, text: str) -> None:
        """Update status text safely"""
        self.status_label.configure(text=text)

    def run(self) -> None:
        """Start the application"""
        self.root.protocol("WM_DELETE_WINDOW", self.cleanup)
        self.root.mainloop()

    def cleanup(self) -> None:
        """Clean up resources before closing"""
        try:
            logger.debug("Cleaning up resources...")
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=False)
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
        finally:
            self.root.destroy()