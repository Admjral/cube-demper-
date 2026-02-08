"""AI Lawyer router - legal consultations, document generation, calculators"""

from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from typing import Annotated, List, Optional
import asyncpg
import logging
from datetime import datetime
from uuid import UUID
import io
import json

from ..schemas.lawyer import (
    LawyerChatRequest, LawyerChatResponse,
    SetLanguageRequest, LawyerLanguage,
    GenerateDocumentRequest, GenerateDocumentResponse, DocumentHistoryItem,
    AnalyzeContractResponse, ContractRisk, RiskLevel,
    CalculatePenaltyRequest, CalculatePenaltyResponse,
    CalculateTaxRequest, CalculateTaxResponse, TaxCalculationItem,
    CalculateFeeRequest, CalculateFeeResponse,
    ChatFeedbackRequest
)
from ..core.database import get_db_pool
from ..dependencies import get_current_user
from ..services.ai_lawyer_service import get_ai_lawyer

router = APIRouter()
logger = logging.getLogger(__name__)


# ==================== LANGUAGE ====================

@router.post("/set-language", status_code=status.HTTP_200_OK)
async def set_language(
    request: SetLanguageRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Set preferred language for AI Lawyer"""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET lawyer_language = $1 WHERE id = $2",
            request.language.value, current_user['id']
        )
    
    return {"language": request.language.value, "message": "Language updated"}


@router.get("/language")
async def get_language(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Get current language preference"""
    async with pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT lawyer_language FROM users WHERE id = $1",
            current_user['id']
        )
    
    return {"language": result or "ru"}


# ==================== CHAT ====================

@router.post("/chat", response_model=LawyerChatResponse)
async def chat_with_lawyer(
    request: LawyerChatRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """
    Chat with AI Lawyer.
    
    Uses RAG to retrieve relevant legal articles from Kazakhstan law database.
    Supports Russian and Kazakh languages.
    """
    lawyer = get_ai_lawyer()
    
    try:
        # Save user message to history
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO ai_chat_history (user_id, assistant_type, role, content)
                VALUES ($1, 'lawyer', 'user', $2)
            """, current_user['id'], request.message)
        
        # Get response from AI Lawyer
        response_text, sources = await lawyer.chat(
            message=request.message,
            pool=pool,
            user_id=current_user['id'],
            language=request.language,
            include_history=request.include_history,
            use_rag=request.use_rag
        )
        
        # Save assistant response to history
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO ai_chat_history (user_id, assistant_type, role, content)
                VALUES ($1, 'lawyer', 'assistant', $2)
            """, current_user['id'], response_text)
        
        return LawyerChatResponse(
            message=response_text,
            language=request.language,
            sources=sources,
            created_at=datetime.now()
        )
        
    except Exception as e:
        logger.error(f"Lawyer chat error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="AI service temporarily unavailable"
        )


@router.get("/chat/history")
async def get_chat_history(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    limit: int = 50
):
    """Get chat history with AI Lawyer"""
    async with pool.acquire() as conn:
        messages = await conn.fetch("""
            SELECT id, role, content, created_at
            FROM ai_chat_history
            WHERE user_id = $1 AND assistant_type = 'lawyer'
            ORDER BY created_at DESC
            LIMIT $2
        """, current_user['id'], limit)
    
    return {
        "messages": [
            {
                "id": str(m['id']),
                "role": m['role'],
                "content": m['content'],
                "created_at": m['created_at'].isoformat()
            }
            for m in reversed(messages)
        ]
    }


@router.delete("/chat/history", status_code=status.HTTP_204_NO_CONTENT)
async def clear_chat_history(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Clear chat history with AI Lawyer"""
    async with pool.acquire() as conn:
        await conn.execute("""
            DELETE FROM ai_chat_history
            WHERE user_id = $1 AND assistant_type = 'lawyer'
        """, current_user['id'])


@router.post("/chat/feedback", status_code=status.HTTP_201_CREATED)
async def submit_feedback(
    request: ChatFeedbackRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Submit feedback on AI Lawyer response"""
    try:
        message_id = UUID(request.message_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid message_id format"
        )
    
    async with pool.acquire() as conn:
        # Verify message belongs to user
        message = await conn.fetchrow("""
            SELECT id FROM ai_chat_history
            WHERE id = $1 AND user_id = $2
        """, message_id, current_user['id'])
        
        if not message:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Message not found"
            )
        
        await conn.execute("""
            INSERT INTO lawyer_chat_feedback (user_id, chat_message_id, rating, comment)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (chat_message_id) DO UPDATE
            SET rating = $3, comment = $4
        """, current_user['id'], message_id, request.rating, request.comment)
    
    return {"message": "Feedback submitted"}


# ==================== DOCUMENT GENERATION ====================

@router.post("/generate-document", response_model=GenerateDocumentResponse)
async def generate_document(
    request: GenerateDocumentRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """
    Generate a legal document from template.
    
    Supported document types:
    - supply_contract: Договор поставки
    - sale_contract: Договор купли-продажи
    - service_contract: Договор оказания услуг
    - employment_contract: Трудовой договор
    - claim_to_supplier: Претензия поставщику
    - claim_to_buyer: Претензия покупателю
    """
    lawyer = get_ai_lawyer()
    
    try:
        title, content = lawyer.generate_document(
            document_type=request.document_type,
            data=request.data,
            language=request.language
        )
        
        # Save to database
        async with pool.acquire() as conn:
            doc_id = await conn.fetchval("""
                INSERT INTO lawyer_documents (user_id, document_type, title, language, input_data, content)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
            """, current_user['id'], request.document_type.value, title,
                request.language.value, json.dumps(request.data, ensure_ascii=False, default=str), content)
        
        return GenerateDocumentResponse(
            id=str(doc_id),
            document_type=request.document_type,
            title=title,
            content=content,
            pdf_url=None,  # TODO: Generate PDF
            docx_url=None,  # TODO: Generate DOCX
            created_at=datetime.now()
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/documents", response_model=List[DocumentHistoryItem])
async def get_document_history(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    limit: int = 20
):
    """Get history of generated documents"""
    async with pool.acquire() as conn:
        docs = await conn.fetch("""
            SELECT id, document_type, title, language, created_at
            FROM lawyer_documents
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2
        """, current_user['id'], limit)
    
    from ..schemas.lawyer import DocumentType
    
    return [
        DocumentHistoryItem(
            id=str(d['id']),
            document_type=DocumentType(d['document_type']),
            title=d['title'],
            language=LawyerLanguage(d['language']),
            created_at=d['created_at']
        )
        for d in docs
    ]


@router.get("/documents/{document_id}")
async def get_document(
    document_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
):
    """Get a specific generated document"""
    try:
        doc_uuid = UUID(document_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid document_id format"
        )
    
    async with pool.acquire() as conn:
        doc = await conn.fetchrow("""
            SELECT id, document_type, title, language, input_data, content, 
                   pdf_url, docx_url, created_at
            FROM lawyer_documents
            WHERE id = $1 AND user_id = $2
        """, doc_uuid, current_user['id'])
    
    if not doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found"
        )
    
    return {
        "id": str(doc['id']),
        "document_type": doc['document_type'],
        "title": doc['title'],
        "language": doc['language'],
        "input_data": doc['input_data'],
        "content": doc['content'],
        "pdf_url": doc['pdf_url'],
        "docx_url": doc['docx_url'],
        "created_at": doc['created_at'].isoformat()
    }


# ==================== CONTRACT ANALYSIS ====================

@router.post("/analyze-contract", response_model=AnalyzeContractResponse)
async def analyze_contract(
    current_user: Annotated[dict, Depends(get_current_user)],
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)],
    file: UploadFile = File(...),
    language: LawyerLanguage = LawyerLanguage.RUSSIAN
):
    """
    Analyze a contract for risks and key conditions.
    
    Supports: PDF, DOCX, TXT files up to 10MB.
    """
    # Validate file size (10MB max)
    MAX_SIZE = 10 * 1024 * 1024
    content = await file.read()
    if len(content) > MAX_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="File too large. Maximum size is 10MB."
        )
    
    # Extract text based on file type
    filename = file.filename.lower() if file.filename else ""
    
    if filename.endswith('.txt'):
        contract_text = content.decode('utf-8', errors='ignore')
    elif filename.endswith('.pdf'):
        # TODO: Add PDF extraction with pypdf or pdfplumber
        contract_text = content.decode('utf-8', errors='ignore')
    elif filename.endswith('.docx'):
        # TODO: Add DOCX extraction with python-docx
        contract_text = content.decode('utf-8', errors='ignore')
    else:
        # Try to decode as text
        contract_text = content.decode('utf-8', errors='ignore')
    
    if len(contract_text.strip()) < 100:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not extract text from file or file is too short"
        )
    
    lawyer = get_ai_lawyer()
    
    try:
        analysis = await lawyer.analyze_contract(contract_text, language)
        
        return AnalyzeContractResponse(
            summary=analysis.get('summary', ''),
            key_conditions=analysis.get('key_conditions', []),
            risks=[
                ContractRisk(
                    level=RiskLevel(r['level']),
                    title=r['title'],
                    description=r['description'],
                    clause=r.get('clause'),
                    recommendation=r['recommendation']
                )
                for r in analysis.get('risks', [])
            ],
            recommendations=analysis.get('recommendations', []),
            overall_risk_level=RiskLevel(analysis.get('overall_risk_level', 'medium')),
            language=language
        )
        
    except Exception as e:
        logger.error(f"Contract analysis error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Contract analysis service temporarily unavailable"
        )


# ==================== CALCULATORS ====================

@router.post("/calculate-penalty", response_model=CalculatePenaltyResponse)
async def calculate_penalty(
    request: CalculatePenaltyRequest,
    current_user: Annotated[dict, Depends(get_current_user)]
):
    """
    Calculate penalty/interest amount.
    
    Uses NBK refinancing rate (15.75%) by default.
    Formula: Penalty = Principal × Days × (Rate / 365)
    """
    lawyer = get_ai_lawyer()
    
    try:
        result = lawyer.calculate_penalty(
            principal_amount=request.principal_amount,
            start_date=request.start_date,
            end_date=request.end_date,
            rate_type=request.rate_type,
            custom_rate=request.custom_rate
        )
        
        return CalculatePenaltyResponse(**result)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/calculate-tax", response_model=CalculateTaxResponse)
async def calculate_tax(
    request: CalculateTaxRequest,
    current_user: Annotated[dict, Depends(get_current_user)]
):
    """
    Calculate taxes for Kazakhstan.
    
    Supported tax types:
    - simplified_ip: ИП на упрощённой декларации (3% ИПН + соц. платежи)
    - standard_ip: ИП на общеустановленном режиме (10% ИПН)
    - too_kpn: ТОО - корпоративный подоходный налог (20%)
    - vat: НДС (12%)
    """
    lawyer = get_ai_lawyer()
    
    result = lawyer.calculate_tax(
        tax_type=request.tax_type,
        revenue=request.revenue,
        expenses=request.expenses,
        period=request.period,
        employee_salary=request.employee_salary
    )
    
    return CalculateTaxResponse(
        tax_type=request.tax_type,
        period=result['period'],
        revenue=result['revenue'],
        expenses=result['expenses'],
        taxable_income=result['taxable_income'],
        taxes=[TaxCalculationItem(**t) for t in result['taxes']],
        total_tax=result['total_tax'],
        net_income=result['net_income']
    )


@router.post("/calculate-fee", response_model=CalculateFeeResponse)
async def calculate_fee(
    request: CalculateFeeRequest,
    current_user: Annotated[dict, Depends(get_current_user)]
):
    """
    Calculate state fees.
    
    Supported fee types:
    - ip_registration: ИП регистрация (бесплатно через eGov)
    - too_registration: ТОО регистрация (1 МРП)
    - court_fee_property: Госпошлина по имущественному иску (1%)
    - court_fee_non_property: Госпошлина по неимущественному иску (0.5 МРП)
    - license_fee: Лицензионный сбор (зависит от вида)
    """
    lawyer = get_ai_lawyer()
    
    result = lawyer.calculate_fee(
        fee_type=request.fee_type,
        claim_amount=request.claim_amount
    )
    
    return CalculateFeeResponse(**result)


# ==================== FREQUENT QUESTIONS ====================

@router.get("/faq")
async def get_faq(
    language: LawyerLanguage = LawyerLanguage.RUSSIAN
):
    """Get frequently asked questions"""
    
    faq_ru = [
        {
            "question": "Какие налоги платит ИП на упрощёнке?",
            "category": "taxes"
        },
        {
            "question": "Как оформить возврат товара покупателю?",
            "category": "consumer"
        },
        {
            "question": "Какие документы нужны для регистрации ТОО?",
            "category": "business"
        },
        {
            "question": "Как правильно уволить сотрудника?",
            "category": "labor"
        },
        {
            "question": "Что делать если поставщик не выполнил договор?",
            "category": "contracts"
        },
        {
            "question": "Какие права имеет покупатель по гарантии?",
            "category": "consumer"
        },
        {
            "question": "Как составить претензию поставщику?",
            "category": "contracts"
        },
        {
            "question": "Нужна ли касса для ИП?",
            "category": "business"
        }
    ]
    
    faq_kk = [
        {
            "question": "Жеңілдетілген жүйедегі ЖК қандай салықтар төлейді?",
            "category": "taxes"
        },
        {
            "question": "Тауарды қайтаруды қалай рәсімдеуге болады?",
            "category": "consumer"
        },
        {
            "question": "ЖШС тіркеу үшін қандай құжаттар қажет?",
            "category": "business"
        }
    ]
    
    return {
        "questions": faq_ru if language == LawyerLanguage.RUSSIAN else faq_kk,
        "language": language.value
    }
