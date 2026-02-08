"""AI Lawyer schemas for API requests and responses"""

from pydantic import BaseModel, Field
from typing import List, Literal, Optional, Dict, Any
from datetime import datetime, date
from enum import Enum


# ==================== ENUMS ====================

class LawyerLanguage(str, Enum):
    """Supported languages for AI Lawyer"""
    RUSSIAN = "ru"
    KAZAKH = "kk"


class DocumentType(str, Enum):
    """Types of legal documents that can be generated"""
    # Contracts
    SUPPLY_CONTRACT = "supply_contract"
    SALE_CONTRACT = "sale_contract"
    SERVICE_CONTRACT = "service_contract"
    RENT_CONTRACT = "rent_contract"
    EMPLOYMENT_CONTRACT = "employment_contract"
    # Claims
    CLAIM_TO_SUPPLIER = "claim_to_supplier"
    CLAIM_TO_BUYER = "claim_to_buyer"
    COMPLAINT_TO_AUTHORITY = "complaint_to_authority"
    # Applications
    IP_REGISTRATION = "ip_registration"
    TOO_REGISTRATION = "too_registration"
    LICENSE_APPLICATION = "license_application"
    TAX_APPLICATION = "tax_application"
    # Acts
    ACCEPTANCE_ACT = "acceptance_act"
    WORK_COMPLETION_ACT = "work_completion_act"
    RECONCILIATION_ACT = "reconciliation_act"


class TaxType(str, Enum):
    """Types of tax calculations"""
    SIMPLIFIED_IP = "simplified_ip"  # ИП на упрощёнке
    STANDARD_IP = "standard_ip"      # ИП на общем режиме
    TOO_KPN = "too_kpn"              # ТОО - КПН
    VAT = "vat"                      # НДС
    SOCIAL = "social"                # Социальные отчисления


class RiskLevel(str, Enum):
    """Risk levels for contract analysis"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


# ==================== CHAT SCHEMAS ====================

class LawyerChatRequest(BaseModel):
    """Request for AI Lawyer chat"""
    message: str = Field(..., min_length=1, max_length=4000)
    language: LawyerLanguage = Field(default=LawyerLanguage.RUSSIAN)
    include_history: bool = Field(default=True)
    use_rag: bool = Field(default=True, description="Use RAG for legal context")


class LawyerChatResponse(BaseModel):
    """Response from AI Lawyer chat"""
    message: str
    language: LawyerLanguage
    sources: List[Dict[str, Any]] = Field(default_factory=list, description="Legal sources cited")
    created_at: datetime


class SetLanguageRequest(BaseModel):
    """Request to set preferred language"""
    language: LawyerLanguage


# ==================== DOCUMENT GENERATION SCHEMAS ====================

class SupplyContractData(BaseModel):
    """Data for supply contract"""
    seller_type: str = Field(..., description="ИП or ТОО")
    seller_name: str
    seller_representative: Optional[str] = None
    seller_basis: str = Field(default="Устава", description="На основании чего действует")
    seller_bin: Optional[str] = None
    seller_address: Optional[str] = None
    buyer_type: str
    buyer_name: str
    buyer_representative: Optional[str] = None
    buyer_basis: str = Field(default="Устава")
    buyer_bin: Optional[str] = None
    buyer_address: Optional[str] = None
    goods_description: str
    total_amount: int = Field(..., description="Amount in tenge")
    delivery_date: date
    payment_terms: Optional[str] = None
    city: str = Field(default="Алматы")


class ClaimData(BaseModel):
    """Data for claim/complaint"""
    claimant_name: str
    claimant_address: str
    claimant_contacts: str
    respondent_name: str
    respondent_address: Optional[str] = None
    contract_number: Optional[str] = None
    contract_date: Optional[date] = None
    claim_amount: Optional[int] = None
    claim_description: str
    requirements: str


class EmploymentContractData(BaseModel):
    """Data for employment contract"""
    employer_name: str
    employer_bin: str
    employer_address: str
    employer_representative: str
    employee_name: str
    employee_iin: str
    employee_address: str
    position: str
    salary: int = Field(..., description="Monthly salary in tenge")
    work_start_date: date
    probation_months: int = Field(default=0, le=3)
    work_schedule: str = Field(default="5/2, с 9:00 до 18:00")
    vacation_days: int = Field(default=24)


class GenerateDocumentRequest(BaseModel):
    """Request to generate a legal document"""
    document_type: DocumentType
    language: LawyerLanguage = Field(default=LawyerLanguage.RUSSIAN)
    data: Dict[str, Any] = Field(..., description="Document-specific data")


class GenerateDocumentResponse(BaseModel):
    """Response with generated document"""
    id: str
    document_type: DocumentType
    title: str
    content: str
    pdf_url: Optional[str] = None
    docx_url: Optional[str] = None
    created_at: datetime


class DocumentHistoryItem(BaseModel):
    """Item in document history"""
    id: str
    document_type: DocumentType
    title: str
    language: LawyerLanguage
    created_at: datetime


# ==================== CONTRACT ANALYSIS SCHEMAS ====================

class ContractRisk(BaseModel):
    """A risk identified in contract analysis"""
    level: RiskLevel
    title: str
    description: str
    clause: Optional[str] = None
    recommendation: str


class AnalyzeContractResponse(BaseModel):
    """Response from contract analysis"""
    summary: str
    key_conditions: List[str]
    risks: List[ContractRisk]
    recommendations: List[str]
    overall_risk_level: RiskLevel
    language: LawyerLanguage


# ==================== CALCULATOR SCHEMAS ====================

class CalculatePenaltyRequest(BaseModel):
    """Request to calculate penalty/interest"""
    principal_amount: int = Field(..., gt=0, description="Principal amount in tenge")
    start_date: date
    end_date: date
    rate_type: Literal["refinancing", "custom"] = Field(default="refinancing")
    custom_rate: Optional[float] = Field(None, gt=0, le=100, description="Custom rate in %")


class CalculatePenaltyResponse(BaseModel):
    """Response with penalty calculation"""
    principal_amount: int
    days: int
    rate: float
    penalty_amount: int
    total_amount: int
    calculation_details: str


class CalculateTaxRequest(BaseModel):
    """Request to calculate taxes"""
    tax_type: TaxType
    revenue: int = Field(..., ge=0, description="Revenue in tenge")
    expenses: int = Field(default=0, ge=0, description="Expenses in tenge")
    period: str = Field(..., description="Tax period (year or quarter)")
    employee_salary: Optional[int] = Field(None, description="For social contributions")


class TaxCalculationItem(BaseModel):
    """Single tax calculation item"""
    name: str
    rate: float
    base: int
    amount: int
    description: Optional[str] = None


class CalculateTaxResponse(BaseModel):
    """Response with tax calculations"""
    tax_type: TaxType
    period: str
    revenue: int
    expenses: int
    taxable_income: int
    taxes: List[TaxCalculationItem]
    total_tax: int
    net_income: int


class CalculateFeeRequest(BaseModel):
    """Request to calculate state fees"""
    fee_type: Literal[
        "ip_registration",
        "too_registration", 
        "court_fee_property",
        "court_fee_non_property",
        "license_fee"
    ]
    claim_amount: Optional[int] = Field(None, description="For court fee calculation")


class CalculateFeeResponse(BaseModel):
    """Response with fee calculation"""
    fee_type: str
    fee_amount: int
    calculation_details: str
    legal_basis: str


# ==================== FEEDBACK SCHEMAS ====================

class ChatFeedbackRequest(BaseModel):
    """Request to submit feedback on chat response"""
    message_id: str
    rating: Literal[-1, 1] = Field(..., description="-1 for negative, 1 for positive")
    comment: Optional[str] = Field(None, max_length=1000)
