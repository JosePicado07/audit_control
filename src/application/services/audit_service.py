import traceback
from typing import Dict, List, Optional, Union
from pathlib import Path
import openpyxl
import pandas as pd
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor

from domain.entities.audit_entity import AuditResult, AuditItem, ProgramRequirement
from application.use_cases.audit_processor import AuditProcessor
from application.use_cases.report_generator import ReportGenerator
from infrastructure.persistence.excel_repository import ExcelRepository
from infrastructure.logging.logger import get_logger

logger = get_logger(__name__)

class AuditService:
    """
    Service for managing audit processes.
    Coordinates between repository, processor and report generator.
    """
    
    def __init__(
        self,
        audit_processor: AuditProcessor,
        report_generator: ReportGenerator,
        excel_repository: ExcelRepository,
        config_path: Optional[Union[str, Path]] = None
    ) -> None:
        """Initialize service with required components."""
        self.audit_processor = audit_processor
        self.report_generator = report_generator
        self.excel_repository = excel_repository
        self.config_path = Path(config_path) if config_path else Path.cwd() / "config"

    def execute_audit(self, contract: str, file_path: str, inventory_file: Optional[str] = None) -> Dict:
        try:
            logger.debug("==== START AUDIT SERVICE EXECUTION ====")
            logger.info(f"Starting audit for contract: {contract}")
            logger.debug(f"File path: {file_path}")
            logger.debug(f"Inventory file: {inventory_file}")
            
            use_inventory = inventory_file is not None
            
            # Check if main file is inventory format
            is_inventory = False
            if inventory_file:
                logger.debug("Checking if file is inventory format...")
                is_inventory = self.excel_repository._is_inventory_file(Path(file_path))
            
            # Validate files based on their type
            if is_inventory:
                logger.debug("Validating main file as inventory file...")
                self.excel_repository.validate_inventory_file(file_path)
            else:
                logger.debug("Validating main file as audit file...")
                self.excel_repository.validate_input_file(file_path)
            
            # Validate inventory file if provided
            if inventory_file:
                logger.debug("Validating inventory file...")
                self.excel_repository.validate_inventory_file(inventory_file)
                
            # Get program requirements
            logger.debug("Getting program requirements...")
            program_requirements_dict = self.excel_repository.get_program_requirements(contract)
            
            # Infer org_destination if not present
            if not program_requirements_dict.get('org_destination'):
                df = pd.read_excel(file_path)
                inferred_orgs = sorted(list(set(str(org).strip().zfill(2)
                                                for org in df['Organization'].unique())))
                program_requirements_dict['org_destination'] = inferred_orgs
                logger.info(f"Inferred org_destination: {inferred_orgs}")
            
            # Set base_org if not present
            if not program_requirements_dict.get('base_org'):
                program_requirements_dict['base_org'] = program_requirements_dict['org_destination'][0]
                logger.warning(f"Base org not found - using first org in org_destination: {program_requirements_dict['base_org']}")
                
            
            
             # Convert dict to ProgramRequirement object
            program_requirements = ProgramRequirement(
                contract=contract,
                base_org=program_requirements_dict.get('base_org', ''),
                org_destination=program_requirements_dict["org_destination"],
                physical_orgs=program_requirements_dict.get('physical_orgs', []),
                dropship_orgs=program_requirements_dict.get('dropship_orgs', []),
                requires_serial_control=program_requirements_dict.get('requires_serial_control', False),
                international=program_requirements_dict.get('international', False)
            )
            
            logger.debug("Processing audit...")
            audit_result = self.audit_processor.process_audit(
                file_path, 
                contract,
                inventory_file=inventory_file
            )

            # Validar audit_result
            if audit_result is None:
                raise ValueError("Audit processing failed - no results returned")
            if not hasattr(audit_result, 'items') or not audit_result.items:
                raise ValueError("Audit processing failed - invalid or empty results")

            # Validate program-specific requirements
            validation_results = self._validate_program_specific_requirements(
                audit_result.items,
                program_requirements
            )
            
            # Crear combined_results
            combined_results = {
                "program_requirements": {
                    "contract": contract,
                    "base_org": program_requirements.base_org,
                    "org_destination": program_requirements.org_destination,
                    "physical_orgs": program_requirements.physical_orgs,
                    "dropship_orgs": program_requirements.dropship_orgs,
                },
                "use_inventory": use_inventory
            }
            
            # Combinar validation_results sin sobrescribir program_requirements
            combined_results.update({
                k: v for k, v in validation_results.items() 
                if k != "program_requirements"
            })

            # Debug logs para verificar combined_results
            logger.debug(f"Combined results before generate_report: {combined_results}")

            # Generate reports
            report_results = self.report_generator.generate_report(
                audit_result, 
                combined_results
            )
            
            logger.info(f"Audit completed successfully for contract: {contract}")
            logger.debug("==== END AUDIT SERVICE EXECUTION ====")
            
            return {
                "status": "success",
                "data": {
                    "timestamp": datetime.now().isoformat(),
                    "contract": contract,
                    "external_report_path": report_results["external_report_path"],
                    "internal_report_path": report_results["internal_report_path"],
                    "program_requirements": combined_results["program_requirements"],
                    "summary": {
                        "serial_control": audit_result.serial_control_results["summary"],
                        "organization": audit_result.org_mismatch_results["summary"],
                        "inventory_validation": "Enabled" if use_inventory else "Disabled"
                    }
                }
            }
                    
        except Exception as e:
            logger.error(f"Error in audit service: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return {
                "status": "error",
                "message": str(e)
            }
            
    def _validate_program_specific_requirements(
        self,
        items: List[AuditItem],
        program_requirements: ProgramRequirement
    ) -> Dict:
        """
        Validate program-specific requirements.
        
        Args:
            items: List of audit items to validate
            program_requirements: Program requirements to validate against
            
        Returns:
            Dict containing validation results
        """
        validations = {
            "serial_control_consistency": True,
            "org_coverage": True,
            "issues_found": []
        }
        
        # Check serial control consistency if required
        if program_requirements.requires_serial_control:
            serial_mismatches = [
                item for item in items 
                if item.status == 'Mismatch'
            ]
            if serial_mismatches:
                validations["serial_control_consistency"] = False
                validations["issues_found"].append({
                    "type": "serial_control",
                    "description": "Serial Control inconsistencies found",
                    "affected_items": [item.part_number for item in serial_mismatches]
                })

        # Check organization coverage if org destination is not empty
        if program_requirements.org_destination:
            required_orgs = set(program_requirements.org_destination)
            existing_orgs = set(item.organization for item in items)
            missing_orgs = required_orgs - existing_orgs
            
            if missing_orgs:
                validations["org_coverage"] = False
                validations["issues_found"].append({
                    "type": "missing_orgs",
                    "description": "Missing required organizations",
                    "missing_orgs": list(missing_orgs)
                })
        
        return validations

    def cleanup(self):
        """Clean up service resources."""
        try:
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=True)
            self.excel_repository.cleanup()
        except Exception as e:
            logger.error(f"Error during service cleanup: {str(e)}")

    def __del__(self):
        """Ensure cleanup on destruction."""
        try:
            self.cleanup()
        except Exception as e:
            logger.error(f"Error during service destruction: {str(e)}")