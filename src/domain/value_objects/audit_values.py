from enum import Enum, auto
from dataclasses import dataclass
from typing import List, Optional, Dict
from datetime import datetime

class AuditStatus(Enum):
    """Estados posibles de una auditoría"""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class AuditType(Enum):
    """Tipos de auditoría disponibles"""
    SERIAL_CONTROL = "serial_control"
    ORG_MISMATCH = "org_mismatch"
    OTHER_ATTRIBUTES = "other_attributes"

class SerialControlStatus(Enum):
    """Estados de control de serie"""
    MATCH = "match"
    MISMATCH = "mismatch"
    MISSING = "missing"
    NOT_REQUIRED = "not_required"

class OrgStatus(Enum):
    """Estados de organizaciones"""
    PRESENT = "present"
    MISSING = "missing"
    INVALID = "invalid"

@dataclass
class SerialControlConfig:
    """Configuración para auditoría de control de serie"""
    base_org: str
    physical_orgs: List[str]
    requires_serial: bool
    check_inventory: bool = True
    validate_non_hardware: bool = True

@dataclass
class OrgMismatchConfig:
    """Configuración para auditoría de organizaciones"""
    required_orgs: List[str]
    prepare_ftp: bool = True
    validate_vertex: bool = True

@dataclass
class AttributeConfig:
    """Configuración para auditoría de atributos"""
    contract: str
    check_customer_id: bool = False
    check_cross_reference: bool = False
    special_customer_types: List[str] = None

@dataclass
class AuditValidationResult:
    """Resultado de validación de auditoría"""
    status: AuditStatus
    type: AuditType
    message: str
    timestamp: datetime
    details: Optional[Dict] = None
    has_errors: bool = False

@dataclass
class SerialControlResult:
    """Resultado de control de serie"""
    part_number: str
    organization: str
    status: SerialControlStatus
    base_serial: bool
    current_serial: bool
    inventory_qty: int = 0
    is_hardware: bool = True

@dataclass
class OrgMismatchResult:
    """Resultado de discrepancia de organización"""
    part_number: str
    missing_orgs: List[str]
    existing_orgs: List[str]
    vertex_class: str
    description: str
    requires_ftp: bool = False
    vertex_needs_update: bool = False

@dataclass
class AttributeResult:
    """Resultado de validación de atributos"""
    part_number: str
    organization: str
    customer_id: Optional[str]
    cross_reference: Optional[str]
    is_valid: bool
    error_message: Optional[str] = None

@dataclass
class AuditMetadata:
    """Metadatos de auditoría"""
    audit_id: str
    contract: str
    timestamp: datetime
    user: Optional[str] = None
    source_file: Optional[str] = None
    config: Optional[Dict] = None

@dataclass
class AuditSummary:
    """Resumen de auditoría"""
    total_parts: int
    total_issues: int
    serial_control_issues: int
    org_mismatch_issues: int
    attribute_issues: int
    parts_with_inventory: int
    timestamp: datetime
    validation_messages: List[str]