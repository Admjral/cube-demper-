"""
AI Lawyer Service - inteligent legal assistant for Kazakhstan law

Features:
1. Legal consultations with RAG (Retrieval-Augmented Generation)
2. Document generation (contracts, claims, applications)
3. Contract analysis with risk assessment
4. Calculators (penalties, taxes, fees)

Uses Google Gemini for LLM and embeddings.
"""
import logging
import asyncio
import asyncpg
import json
from typing import Optional, Dict, Any, List, Tuple
from uuid import UUID
from datetime import datetime, date
from dataclasses import dataclass

import google.generativeai as genai

from ..config import settings
from ..core.circuit_breaker import get_circuit_breaker, CircuitBreakerConfig, CircuitOpenError
from ..schemas.lawyer import (
    LawyerLanguage, DocumentType, TaxType, RiskLevel,
    ContractRisk, TaxCalculationItem
)

GEMINI_TIMEOUT = 30  # seconds

logger = logging.getLogger(__name__)


# ==================== SYSTEM PROMPTS ====================

LAWYER_SYSTEM_PROMPT_RU = """Ты - профессиональный юрист-консультант, специализирующийся на законодательстве Республики Казахстан. Ты консультируешь продавцов маркетплейса Kaspi.kz.

ОБЛАСТИ ЭКСПЕРТИЗЫ:
- Предпринимательское право (ИП, ТОО, лицензирование)
- Налоговое право (ИПН, КПН, НДС, упрощённая декларация)
- Трудовое право (найм, увольнение, отпуска, ТД)
- Защита прав потребителей (возвраты, гарантии, претензии)
- Договорное право (поставка, купля-продажа, услуги, аренда)
- Правила маркетплейса Kaspi

ФОРМАТ ОТВЕТОВ:
1. Отвечай формальным юридическим языком, но понятно
2. Цитируй конкретные статьи законов где применимо
3. Структурируй ответ с заголовками и списками
4. В конце давай практические рекомендации
5. Если есть ссылка на закон - указывай её (adilet.zan.kz)

ОГРАНИЧЕНИЯ:
- Отвечай только на вопросы по законодательству РК
- Если вопрос выходит за рамки твоей компетенции - честно скажи об этом
- Не давай советов по обходу законов
- По уголовным делам рекомендуй обратиться к адвокату

КОНТЕКСТ ИЗ БАЗЫ ЗАКОНОВ:
{context}"""

LAWYER_SYSTEM_PROMPT_KK = """Сен - Қазақстан Республикасының заңнамасына маманданған кәсіби заңгер-кеңесші. Сен Kaspi.kz маркетплейсінің сатушыларына кеңес бересің.

САРАПТАМА САЛАЛАРЫ:
- Кәсіпкерлік құқық (ЖК, ЖШС, лицензиялау)
- Салық құқығы (ЖТС, КТС, ҚҚС)
- Еңбек құқығы (жалдау, жұмыстан шығару, демалыстар)
- Тұтынушылар құқықтарын қорғау
- Шарттық құқық

ЖАУАП ФОРМАТЫ:
1. Ресми заң тілінде жауап бер
2. Заңдардың нақты баптарын келтір
3. Құрылымды түрде жауап бер
4. Соңында практикалық ұсыныстар бер

{context}"""

CONTRACT_ANALYSIS_PROMPT = """Проанализируй следующий договор на казахстанском праве.

ЗАДАЧИ:
1. Выдели ключевые условия договора
2. Найди потенциальные риски и "подводные камни"
3. Оцени каждый риск по уровню: критический, высокий, средний, низкий
4. Дай рекомендации по улучшению договора

КАТЕГОРИИ РИСКОВ:
- Критический: прямое нарушение закона РК
- Высокий: значительные финансовые риски
- Средний: потенциальные проблемы
- Низкий: рекомендации по улучшению

Ответь в формате JSON:
{
    "summary": "краткое описание договора",
    "key_conditions": ["условие 1", "условие 2"],
    "risks": [
        {
            "level": "critical|high|medium|low",
            "title": "название риска",
            "description": "описание проблемы",
            "clause": "цитата из договора (если есть)",
            "recommendation": "рекомендация"
        }
    ],
    "recommendations": ["общая рекомендация 1", "общая рекомендация 2"],
    "overall_risk_level": "critical|high|medium|low"
}

ТЕКСТ ДОГОВОРА:
{contract_text}"""


# ==================== DOCUMENT TEMPLATES ====================

DOCUMENT_TEMPLATES = {
    DocumentType.SUPPLY_CONTRACT: """# ДОГОВОР ПОСТАВКИ № {number}

г. {city}                                                              «{date}»

{seller_type} «{seller_name}»{seller_bin_text}, в лице {seller_representative}, действующего на основании {seller_basis}, именуемый в дальнейшем «Поставщик», с одной стороны, и

{buyer_type} «{buyer_name}»{buyer_bin_text}, в лице {buyer_representative}, действующего на основании {buyer_basis}, именуемый в дальнейшем «Покупатель», с другой стороны,

совместно именуемые «Стороны», заключили настоящий Договор о нижеследующем:

## 1. ПРЕДМЕТ ДОГОВОРА

1.1. Поставщик обязуется передать в собственность Покупателю, а Покупатель обязуется принять и оплатить следующий товар:
{goods_description}

1.2. Общая сумма Договора составляет: {total_amount} ({total_amount_words}) тенге.

## 2. СРОКИ И ПОРЯДОК ПОСТАВКИ

2.1. Поставщик обязуется поставить товар в срок до {delivery_date}.
2.2. Поставка осуществляется по адресу: {buyer_address}.
2.3. Датой поставки считается дата подписания Покупателем товарной накладной.

## 3. ПОРЯДОК РАСЧЁТОВ

3.1. {payment_terms}
3.2. Оплата производится путём перечисления денежных средств на расчётный счёт Поставщика.

## 4. КАЧЕСТВО И КОМПЛЕКТНОСТЬ

4.1. Качество поставляемого товара должно соответствовать стандартам РК.
4.2. Поставщик гарантирует качество товара в течение гарантийного срока.

## 5. ОТВЕТСТВЕННОСТЬ СТОРОН

5.1. За нарушение сроков поставки Поставщик уплачивает пеню в размере 0,1% от стоимости недопоставленного товара за каждый день просрочки.
5.2. За нарушение сроков оплаты Покупатель уплачивает пеню в размере 0,1% от суммы задолженности за каждый день просрочки.

## 6. ФОРС-МАЖОР

6.1. Стороны освобождаются от ответственности за неисполнение обязательств, если оно явилось следствием обстоятельств непреодолимой силы.

## 7. РАЗРЕШЕНИЕ СПОРОВ

7.1. Все споры разрешаются путём переговоров.
7.2. При недостижении согласия споры рассматриваются в суде по месту нахождения ответчика в соответствии с законодательством РК.

## 8. СРОК ДЕЙСТВИЯ

8.1. Настоящий Договор вступает в силу с момента подписания и действует до полного исполнения Сторонами своих обязательств.

## 9. РЕКВИЗИТЫ И ПОДПИСИ СТОРОН

**ПОСТАВЩИК:**
{seller_type} «{seller_name}»
{seller_address}
{seller_bin_line}

___________________ / {seller_representative} /


**ПОКУПАТЕЛЬ:**
{buyer_type} «{buyer_name}»
{buyer_address}
{buyer_bin_line}

___________________ / {buyer_representative} /
""",

    DocumentType.EMPLOYMENT_CONTRACT: """# ТРУДОВОЙ ДОГОВОР № {number}

г. {city}                                                              «{date}»

{employer_name}, БИН {employer_bin}, в лице {employer_representative}, действующего на основании Устава, именуемый в дальнейшем «Работодатель», с одной стороны, и

гражданин(ка) {employee_name}, ИИН {employee_iin}, именуемый(ая) в дальнейшем «Работник», с другой стороны,

заключили настоящий Трудовой договор о нижеследующем:

## 1. ПРЕДМЕТ ДОГОВОРА

1.1. Работодатель принимает Работника на должность: {position}.
1.2. Место работы: {employer_address}.
1.3. Дата начала работы: {work_start_date}.
{probation_text}

## 2. РЕЖИМ РАБОЧЕГО ВРЕМЕНИ

2.1. Работнику устанавливается режим рабочего времени: {work_schedule}.
2.2. Продолжительность рабочей недели: 40 часов.

## 3. ОПЛАТА ТРУДА

3.1. Работнику устанавливается должностной оклад в размере {salary} ({salary_words}) тенге в месяц.
3.2. Заработная плата выплачивается не реже одного раза в месяц не позднее 10 числа месяца, следующего за расчётным.

## 4. ОТПУСК

4.1. Работнику предоставляется ежегодный оплачиваемый трудовой отпуск продолжительностью {vacation_days} календарных дней.

## 5. ОБЯЗАННОСТИ РАБОТНИКА

5.1. Добросовестно исполнять трудовые обязанности.
5.2. Соблюдать трудовую дисциплину.
5.3. Соблюдать требования по охране труда.
5.4. Бережно относиться к имуществу Работодателя.

## 6. ОБЯЗАННОСТИ РАБОТОДАТЕЛЯ

6.1. Обеспечить условия труда, предусмотренные трудовым законодательством РК.
6.2. Своевременно выплачивать заработную плату.
6.3. Осуществлять обязательное социальное страхование Работника.

## 7. ПРЕКРАЩЕНИЕ ДОГОВОРА

7.1. Настоящий Договор может быть прекращён по основаниям, предусмотренным Трудовым кодексом РК.

## 8. РЕКВИЗИТЫ И ПОДПИСИ СТОРОН

**РАБОТОДАТЕЛЬ:**
{employer_name}
БИН: {employer_bin}
Адрес: {employer_address}

___________________ / {employer_representative} /


**РАБОТНИК:**
{employee_name}
ИИН: {employee_iin}
Адрес: {employee_address}

___________________ / {employee_name} /
""",

    DocumentType.CLAIM_TO_SUPPLIER: """# ПРЕТЕНЗИЯ

{city}                                                                 {date}

**Кому:** {respondent_name}
{respondent_address}

**От кого:** {claimant_name}
{claimant_address}
Тел.: {claimant_contacts}

## ПРЕТЕНЗИЯ
о ненадлежащем исполнении договора поставки

{contract_info}

{claim_description}

На основании статей 272, 404, 428 Гражданского кодекса Республики Казахстан,

**ТРЕБУЮ:**

{requirements}

{claim_amount_text}

В случае отказа в удовлетворении претензии оставляю за собой право обратиться в суд с требованием о взыскании указанной суммы, а также пени, убытков и судебных расходов.

Претензия должна быть рассмотрена в течение 10 календарных дней с момента её получения.

Приложения:
1. Копия договора
2. Копии документов, подтверждающих нарушение

С уважением,

___________________ / {claimant_name} /

Дата: {date}
""",

    DocumentType.SALE_CONTRACT: """# ДОГОВОР КУПЛИ-ПРОДАЖИ № {number}

г. {city}                                                              «{date}»

{seller_type} «{seller_name}»{seller_bin_text}, в лице {seller_representative}, действующего на основании {seller_basis}, именуемый в дальнейшем «Продавец», с одной стороны, и

{buyer_type} «{buyer_name}»{buyer_bin_text}, в лице {buyer_representative}, действующего на основании {buyer_basis}, именуемый в дальнейшем «Покупатель», с другой стороны,

совместно именуемые «Стороны», заключили настоящий Договор о нижеследующем:

## 1. ПРЕДМЕТ ДОГОВОРА

1.1. Продавец обязуется передать в собственность Покупателю, а Покупатель обязуется принять и оплатить следующий товар:
{goods_description}

1.2. Общая стоимость товара составляет: {total_amount} ({total_amount_words}) тенге.

## 2. ПОРЯДОК И СРОКИ ПЕРЕДАЧИ ТОВАРА

2.1. Продавец обязуется передать товар Покупателю в срок до {delivery_date}.
2.2. Передача товара оформляется актом приёма-передачи, подписанным обеими Сторонами.
2.3. Право собственности на товар переходит к Покупателю с момента подписания акта приёма-передачи.

## 3. ПОРЯДОК РАСЧЁТОВ

3.1. {payment_terms}
3.2. Оплата производится путём перечисления денежных средств на расчётный счёт Продавца.

## 4. КАЧЕСТВО ТОВАРА

4.1. Качество товара должно соответствовать стандартам РК и условиям настоящего Договора.
4.2. Продавец гарантирует, что товар не обременён правами третьих лиц.

## 5. ОТВЕТСТВЕННОСТЬ СТОРОН

5.1. За нарушение сроков передачи товара Продавец уплачивает пеню в размере 0,1% от стоимости товара за каждый день просрочки.
5.2. За нарушение сроков оплаты Покупатель уплачивает пеню в размере 0,1% от суммы задолженности за каждый день просрочки.

## 6. ФОРС-МАЖОР

6.1. Стороны освобождаются от ответственности за неисполнение обязательств при обстоятельствах непреодолимой силы.

## 7. РАЗРЕШЕНИЕ СПОРОВ

7.1. Все споры разрешаются путём переговоров.
7.2. При недостижении согласия споры рассматриваются в суде по месту нахождения ответчика в соответствии с законодательством РК.

## 8. РЕКВИЗИТЫ И ПОДПИСИ СТОРОН

**ПРОДАВЕЦ:**
{seller_type} «{seller_name}»
{seller_address}
{seller_bin_line}

___________________ / {seller_representative} /


**ПОКУПАТЕЛЬ:**
{buyer_type} «{buyer_name}»
{buyer_address}
{buyer_bin_line}

___________________ / {buyer_representative} /
""",

    DocumentType.SERVICE_CONTRACT: """# ДОГОВОР ОКАЗАНИЯ УСЛУГ № {number}

г. {city}                                                              «{date}»

{seller_type} «{seller_name}»{seller_bin_text}, в лице {seller_representative}, действующего на основании {seller_basis}, именуемый в дальнейшем «Исполнитель», с одной стороны, и

{buyer_type} «{buyer_name}»{buyer_bin_text}, в лице {buyer_representative}, действующего на основании {buyer_basis}, именуемый в дальнейшем «Заказчик», с другой стороны,

совместно именуемые «Стороны», заключили настоящий Договор о нижеследующем:

## 1. ПРЕДМЕТ ДОГОВОРА

1.1. Исполнитель обязуется оказать Заказчику следующие услуги:
{goods_description}

1.2. Общая стоимость услуг составляет: {total_amount} ({total_amount_words}) тенге.

## 2. СРОКИ ОКАЗАНИЯ УСЛУГ

2.1. Исполнитель обязуется оказать услуги в срок до {delivery_date}.
2.2. Факт оказания услуг подтверждается подписанием акта выполненных работ.

## 3. ПОРЯДОК РАСЧЁТОВ

3.1. {payment_terms}
3.2. Оплата производится путём перечисления денежных средств на расчётный счёт Исполнителя.

## 4. ОБЯЗАННОСТИ СТОРОН

4.1. Исполнитель обязуется оказать услуги качественно и в установленные сроки.
4.2. Заказчик обязуется предоставить необходимую информацию и своевременно произвести оплату.

## 5. ОТВЕТСТВЕННОСТЬ СТОРОН

5.1. За нарушение сроков оказания услуг Исполнитель уплачивает пеню в размере 0,1% от стоимости услуг за каждый день просрочки.
5.2. За нарушение сроков оплаты Заказчик уплачивает пеню в размере 0,1% от суммы задолженности за каждый день просрочки.

## 6. ФОРС-МАЖОР

6.1. Стороны освобождаются от ответственности за неисполнение обязательств при обстоятельствах непреодолимой силы.

## 7. РАЗРЕШЕНИЕ СПОРОВ

7.1. Все споры разрешаются путём переговоров.
7.2. При недостижении согласия споры рассматриваются в суде по месту нахождения ответчика в соответствии с законодательством РК.

## 8. РЕКВИЗИТЫ И ПОДПИСИ СТОРОН

**ИСПОЛНИТЕЛЬ:**
{seller_type} «{seller_name}»
{seller_address}
{seller_bin_line}

___________________ / {seller_representative} /


**ЗАКАЗЧИК:**
{buyer_type} «{buyer_name}»
{buyer_address}
{buyer_bin_line}

___________________ / {buyer_representative} /
""",

    DocumentType.CLAIM_TO_BUYER: """# ПРЕТЕНЗИЯ

{city}                                                                 {date}

**Кому:** {respondent_name}
{respondent_address}

**От кого:** {claimant_name}
{claimant_address}
Тел.: {claimant_contacts}

## ПРЕТЕНЗИЯ
о ненадлежащем исполнении обязательств по оплате

{contract_info}

{claim_description}

На основании статей 272, 353, 364 Гражданского кодекса Республики Казахстан,

**ТРЕБУЮ:**

{requirements}

{claim_amount_text}

В случае отказа в удовлетворении претензии оставляю за собой право обратиться в суд с требованием о взыскании указанной суммы, а также пени, убытков и судебных расходов.

Претензия должна быть рассмотрена в течение 10 календарных дней с момента её получения.

Приложения:
1. Копия договора
2. Копии документов, подтверждающих задолженность

С уважением,

___________________ / {claimant_name} /

Дата: {date}
""",
}


# ==================== TAX RATES 2026 ====================

TAX_RATES = {
    "simplified_ip": {
        "ipn_rate": 0.03,  # 3% ИПН
        "social_tax_rate": 0.015,  # 1.5% социальный налог
        "opv_rate": 0.10,  # 10% ОПВ
        "so_rate": 0.035,  # 3.5% СО
        "osms_rate": 0.02,  # 2% ОСМС (за себя)
    },
    "standard_ip": {
        "ipn_rate": 0.10,  # 10% ИПН
        "opv_rate": 0.10,
        "so_rate": 0.035,
        "osms_rate": 0.02,
    },
    "too_kpn": {
        "kpn_rate": 0.20,  # 20% КПН
        "social_tax_rate": 0.095,  # 9.5% социальный налог с работников
    },
    "vat_rate": 0.12,  # 12% НДС
    "mzp_2026": 85000,  # МЗП на 2026 год
    "mrp_2026": 3932,  # МРП на 2026 год
    "refinancing_rate": 0.1575,  # Ставка рефинансирования НБ РК (15.75%)
}


# ==================== SERVICE CLASS ====================

class AILawyerService:
    """
    Сервис ИИ-Юриста.
    
    Использует Google Gemini для генерации ответов
    и pgvector для семантического поиска по базе законов.
    """
    
    def __init__(self):
        self._configured = False
        
    def _configure_gemini(self):
        """Configure Gemini API"""
        if not self._configured:
            if not settings.gemini_api_key:
                raise ValueError("Gemini API key not configured")
            genai.configure(api_key=settings.gemini_api_key)
            self._configured = True
    
    def _get_model(self, system_prompt: str = None):
        """Get Gemini model"""
        self._configure_gemini()
        return genai.GenerativeModel(
            model_name=settings.gemini_lawyer_model,
            system_instruction=system_prompt
        )
    
    async def _get_embedding(self, text: str) -> List[float]:
        """Get embedding for text using Gemini"""
        self._configure_gemini()
        result = genai.embed_content(
            model=f"models/{settings.gemini_embedding_model}",
            content=text,
            task_type="retrieval_query"
        )
        return result['embedding']
    
    async def _search_legal_context(
        self,
        query: str,
        pool: asyncpg.Pool,
        limit: int = 5,
        language: str = "ru"
    ) -> List[Dict[str, Any]]:
        """
        Search for relevant legal articles.

        Uses vector similarity if pgvector is available, otherwise falls back to text search.
        Returns list of relevant articles with their metadata.
        """
        try:
            async with pool.acquire() as conn:
                # Check if embedding column exists (pgvector available)
                has_embedding = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'legal_articles' AND column_name = 'embedding'
                    )
                """)

                if has_embedding:
                    # Use vector similarity search
                    embedding = await self._get_embedding(query)
                    results = await conn.fetch("""
                        SELECT
                            la.id,
                            la.article_number,
                            la.title,
                            la.content,
                            ld.title as document_title,
                            ld.code as document_code,
                            ld.source_url,
                            1 - (la.embedding <=> $1::vector) as similarity
                        FROM legal_articles la
                        JOIN legal_documents ld ON la.document_id = ld.id
                        WHERE ld.language = $3
                        ORDER BY la.embedding <=> $1::vector
                        LIMIT $2
                    """, embedding, limit, language)
                else:
                    # Fallback to full-text search
                    # Clean query for tsquery
                    search_terms = ' & '.join(query.split()[:5])  # Use first 5 words
                    results = await conn.fetch("""
                        SELECT
                            la.id,
                            la.article_number,
                            la.title,
                            la.content,
                            ld.title as document_title,
                            ld.code as document_code,
                            ld.source_url,
                            ts_rank(
                                to_tsvector('russian', la.content),
                                plainto_tsquery('russian', $1)
                            ) as similarity
                        FROM legal_articles la
                        JOIN legal_documents ld ON la.document_id = ld.id
                        WHERE ld.language = $3
                          AND to_tsvector('russian', la.content) @@ plainto_tsquery('russian', $1)
                        ORDER BY similarity DESC
                        LIMIT $2
                    """, query, limit, language)

                return [
                    {
                        "id": str(r['id']),
                        "article_number": r['article_number'],
                        "title": r['title'],
                        "content": r['content'],
                        "document_title": r['document_title'],
                        "document_code": r['document_code'],
                        "source_url": r['source_url'],
                        "similarity": float(r['similarity']) if r['similarity'] else 0.0
                    }
                    for r in results
                ]
        except Exception as e:
            logger.warning(f"Legal context search failed: {e}")
            return []
    
    def _build_context(self, articles: List[Dict[str, Any]]) -> str:
        """Build context string from retrieved articles"""
        if not articles:
            return "Релевантные статьи законов не найдены в базе. Отвечай на основе своих знаний о законодательстве РК."
        
        context_parts = []
        for article in articles:
            part = f"""
---
Закон: {article['document_title']} ({article['document_code']})
Статья {article['article_number']}: {article['title']}

{article['content'][:2000]}

Источник: {article['source_url'] or 'adilet.zan.kz'}
---
"""
            context_parts.append(part)
        
        return "\n".join(context_parts)
    
    async def chat(
        self,
        message: str,
        pool: asyncpg.Pool,
        user_id: UUID,
        language: LawyerLanguage = LawyerLanguage.RUSSIAN,
        include_history: bool = True,
        use_rag: bool = True
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Chat with AI Lawyer.
        
        Returns tuple of (response_text, sources_list).
        """
        # Get relevant legal context via RAG
        sources = []
        context = ""
        
        if use_rag:
            articles = await self._search_legal_context(
                message, pool, limit=5, language=language.value
            )
            sources = articles
            context = self._build_context(articles)
        else:
            context = "Отвечай на основе своих знаний о законодательстве РК."
        
        # Select system prompt based on language
        if language == LawyerLanguage.KAZAKH:
            system_prompt = LAWYER_SYSTEM_PROMPT_KK.format(context=context)
        else:
            system_prompt = LAWYER_SYSTEM_PROMPT_RU.format(context=context)
        
        # Get chat history if requested
        messages = []
        if include_history:
            async with pool.acquire() as conn:
                history = await conn.fetch("""
                    SELECT role, content FROM ai_chat_history
                    WHERE user_id = $1 AND assistant_type = 'lawyer'
                    ORDER BY created_at ASC
                    LIMIT 10
                """, user_id)
                messages = [
                    {"role": "user" if h['role'] == "user" else "model", "parts": [h['content']]}
                    for h in history
                ]
        
        # Generate response with timeout and circuit breaker
        breaker = get_gemini_circuit_breaker()
        try:
            async with breaker:
                model = self._get_model(system_prompt)
                chat = model.start_chat(history=messages)

                response = await asyncio.wait_for(
                    chat.send_message_async(
                        message,
                        generation_config=genai.GenerationConfig(
                            max_output_tokens=settings.gemini_max_tokens,
                            temperature=0.7,
                        )
                    ),
                    timeout=GEMINI_TIMEOUT,
                )

                return response.text, sources
        except CircuitOpenError:
            logger.warning("Gemini circuit breaker is open, rejecting request")
            raise Exception("AI service temporarily unavailable. Please try again later.")
        except asyncio.TimeoutError:
            logger.error(f"Gemini API timeout after {GEMINI_TIMEOUT}s")
            raise Exception("AI service response timed out. Please try again.")
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            raise
    
    async def analyze_contract(
        self,
        contract_text: str,
        language: LawyerLanguage = LawyerLanguage.RUSSIAN
    ) -> Dict[str, Any]:
        """
        Analyze a contract for risks and key conditions.
        
        Returns structured analysis.
        """
        prompt = CONTRACT_ANALYSIS_PROMPT.format(contract_text=contract_text[:15000])
        
        breaker = get_gemini_circuit_breaker()
        try:
            async with breaker:
                model = self._get_model()
                response = await asyncio.wait_for(
                    model.generate_content_async(
                        prompt,
                        generation_config=genai.GenerationConfig(
                            max_output_tokens=4000,
                            temperature=0.3,
                            response_mime_type="application/json"
                        )
                    ),
                    timeout=GEMINI_TIMEOUT,
                )

            # Parse JSON response
            result = json.loads(response.text)
            return result

        except CircuitOpenError:
            logger.warning("Gemini circuit breaker is open, rejecting contract analysis")
            raise Exception("AI service temporarily unavailable. Please try again later.")
        except asyncio.TimeoutError:
            logger.error(f"Gemini contract analysis timeout after {GEMINI_TIMEOUT}s")
            raise Exception("AI service response timed out. Please try again.")
        except json.JSONDecodeError:
            # If JSON parsing fails, return basic structure
            logger.error("Failed to parse contract analysis JSON")
            return {
                "summary": "Не удалось проанализировать договор",
                "key_conditions": [],
                "risks": [],
                "recommendations": ["Повторите попытку или загрузите договор в другом формате"],
                "overall_risk_level": "medium"
            }
        except Exception as e:
            logger.error(f"Contract analysis error: {e}")
            raise
    
    def generate_document(
        self,
        document_type: DocumentType,
        data: Dict[str, Any],
        language: LawyerLanguage = LawyerLanguage.RUSSIAN
    ) -> Tuple[str, str]:
        """
        Generate a legal document from template.
        
        Returns tuple of (title, content).
        """
        template = DOCUMENT_TEMPLATES.get(document_type)
        if not template:
            raise ValueError(f"Template not found for document type: {document_type}")
        
        # Prepare common fields
        now = datetime.now()
        data['date'] = now.strftime("%d.%m.%Y")
        data['number'] = now.strftime("%Y%m%d-%H%M")
        data.setdefault('city', 'Алматы')
        
        # Process specific fields based on document type
        if document_type in (DocumentType.SUPPLY_CONTRACT, DocumentType.SALE_CONTRACT, DocumentType.SERVICE_CONTRACT):
            data = self._prepare_supply_contract_data(data)
        elif document_type == DocumentType.EMPLOYMENT_CONTRACT:
            data = self._prepare_employment_contract_data(data)
        elif document_type in (DocumentType.CLAIM_TO_SUPPLIER, DocumentType.CLAIM_TO_BUYER):
            data = self._prepare_claim_data(data)

        # Fill template
        content = template.format(**data)

        # Generate title
        titles = {
            DocumentType.SUPPLY_CONTRACT: f"Договор поставки № {data['number']}",
            DocumentType.SALE_CONTRACT: f"Договор купли-продажи № {data['number']}",
            DocumentType.SERVICE_CONTRACT: f"Договор оказания услуг № {data['number']}",
            DocumentType.EMPLOYMENT_CONTRACT: f"Трудовой договор № {data['number']}",
            DocumentType.CLAIM_TO_SUPPLIER: f"Претензия поставщику от {data['date']}",
            DocumentType.CLAIM_TO_BUYER: f"Претензия покупателю от {data['date']}",
        }
        title = titles.get(document_type, f"Документ № {data['number']}")
        
        return title, content
    
    def _prepare_supply_contract_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for supply contract template"""
        data.setdefault('seller_representative', 'Директора')
        data.setdefault('buyer_representative', 'Директора')
        data.setdefault('seller_basis', 'Устава')
        data.setdefault('buyer_basis', 'Устава')
        data.setdefault('seller_address', '')
        data.setdefault('buyer_address', '')
        data.setdefault('payment_terms', 'Оплата производится в течение 5 банковских дней после поставки товара.')
        
        # Format BIN lines
        data['seller_bin_text'] = f", БИН {data['seller_bin']}" if data.get('seller_bin') else ""
        data['buyer_bin_text'] = f", БИН {data['buyer_bin']}" if data.get('buyer_bin') else ""
        data['seller_bin_line'] = f"БИН: {data['seller_bin']}" if data.get('seller_bin') else ""
        data['buyer_bin_line'] = f"БИН: {data['buyer_bin']}" if data.get('buyer_bin') else ""
        
        # Format amount in words
        data['total_amount_words'] = self._amount_to_words(data['total_amount'])
        
        # Format date
        if isinstance(data.get('delivery_date'), date):
            data['delivery_date'] = data['delivery_date'].strftime("%d.%m.%Y")
        
        return data
    
    def _prepare_employment_contract_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for employment contract template"""
        # Probation period text
        if data.get('probation_months', 0) > 0:
            data['probation_text'] = f"1.4. Работнику устанавливается испытательный срок: {data['probation_months']} месяца(ев)."
        else:
            data['probation_text'] = ""
        
        # Salary in words
        data['salary_words'] = self._amount_to_words(data['salary'])
        
        # Format date
        if isinstance(data.get('work_start_date'), date):
            data['work_start_date'] = data['work_start_date'].strftime("%d.%m.%Y")
        
        return data
    
    def _prepare_claim_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for claim template"""
        data.setdefault('city', 'Алматы')
        
        # Contract info
        if data.get('contract_number') and data.get('contract_date'):
            contract_date = data['contract_date']
            if isinstance(contract_date, date):
                contract_date = contract_date.strftime("%d.%m.%Y")
            data['contract_info'] = f"Между мной и {data['respondent_name']} был заключён договор № {data['contract_number']} от {contract_date}."
        else:
            data['contract_info'] = ""
        
        # Claim amount
        if data.get('claim_amount'):
            data['claim_amount_text'] = f"Общая сумма требований: {data['claim_amount']:,} ({self._amount_to_words(data['claim_amount'])}) тенге.".replace(',', ' ')
        else:
            data['claim_amount_text'] = ""
        
        return data
    
    def _amount_to_words(self, amount: int) -> str:
        """Convert amount to words in Russian"""
        # Simplified version - in production use a proper library
        if amount < 1000:
            return f"{amount} тенге"
        elif amount < 1000000:
            thousands = amount // 1000
            remainder = amount % 1000
            if remainder:
                return f"{thousands} тысяч {remainder} тенге"
            return f"{thousands} тысяч тенге"
        else:
            millions = amount // 1000000
            remainder = amount % 1000000
            if remainder:
                return f"{millions} миллион(ов) {remainder // 1000} тысяч тенге"
            return f"{millions} миллион(ов) тенге"
    
    # ==================== CALCULATORS ====================
    
    def calculate_penalty(
        self,
        principal_amount: int,
        start_date: date,
        end_date: date,
        rate_type: str = "refinancing",
        custom_rate: float = None
    ) -> Dict[str, Any]:
        """
        Calculate penalty/interest amount.
        
        Formula: Penalty = Principal × Days × (Rate / 365)
        """
        days = (end_date - start_date).days
        if days <= 0:
            raise ValueError("End date must be after start date")
        
        # Determine rate
        if rate_type == "custom" and custom_rate:
            rate = custom_rate / 100
        else:
            rate = TAX_RATES["refinancing_rate"]
        
        # Calculate penalty
        penalty = int(principal_amount * days * rate / 365)
        total = principal_amount + penalty
        
        rate_percent = rate * 100
        
        return {
            "principal_amount": principal_amount,
            "days": days,
            "rate": rate_percent,
            "penalty_amount": penalty,
            "total_amount": total,
            "calculation_details": f"""
Расчёт пени по ставке рефинансирования НБ РК ({rate_percent}% годовых):

Сумма долга: {principal_amount:,} тенге
Период просрочки: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')} ({days} дней)

Формула: Пеня = Сумма × Дни × (Ставка / 365)
Пеня = {principal_amount:,} × {days} × ({rate_percent}% / 365) = {penalty:,} тенге

Итого к оплате: {total:,} тенге
(сумма долга + пеня)

Основание: статья 353 Гражданского кодекса РК
""".replace(',', ' ')
        }
    
    def calculate_tax(
        self,
        tax_type: TaxType,
        revenue: int,
        expenses: int = 0,
        period: str = "2026",
        employee_salary: int = None
    ) -> Dict[str, Any]:
        """Calculate taxes based on type and income"""
        
        taxes = []
        total_tax = 0
        taxable_income = revenue - expenses
        
        if tax_type == TaxType.SIMPLIFIED_IP:
            # ИП на упрощённой декларации
            rates = TAX_RATES["simplified_ip"]
            
            # 3% ИПН от дохода
            ipn = int(revenue * rates["ipn_rate"])
            taxes.append(TaxCalculationItem(
                name="ИПН (индивидуальный подоходный налог)",
                rate=rates["ipn_rate"] * 100,
                base=revenue,
                amount=ipn,
                description="3% от дохода"
            ))
            
            # 1.5% социальный налог от дохода минус СО
            # Сначала считаем СО
            mzp = TAX_RATES["mzp_2026"]
            so = int(mzp * rates["so_rate"])  # СО от МЗП
            social_tax_base = revenue
            social_tax = int(social_tax_base * rates["social_tax_rate"]) - so
            if social_tax < 0:
                social_tax = 0
            
            taxes.append(TaxCalculationItem(
                name="Социальный налог",
                rate=rates["social_tax_rate"] * 100,
                base=social_tax_base,
                amount=social_tax,
                description="1.5% от дохода минус СО"
            ))
            
            # ОПВ 10% от заявленного дохода (не менее МЗП)
            opv_base = max(revenue // 12, mzp)  # Месячный доход не менее МЗП
            opv = int(opv_base * rates["opv_rate"])
            taxes.append(TaxCalculationItem(
                name="ОПВ (обязательные пенсионные взносы)",
                rate=rates["opv_rate"] * 100,
                base=opv_base,
                amount=opv,
                description="10% от заявленного дохода (помесячно)"
            ))
            
            # СО 3.5% от дохода 
            taxes.append(TaxCalculationItem(
                name="СО (социальные отчисления)",
                rate=rates["so_rate"] * 100,
                base=mzp,
                amount=so,
                description="3.5% от МЗП"
            ))
            
            # ОСМС 2%
            osms = int(mzp * rates["osms_rate"])
            taxes.append(TaxCalculationItem(
                name="ОСМС (медицинское страхование)",
                rate=rates["osms_rate"] * 100,
                base=mzp,
                amount=osms,
                description="2% от МЗП за себя"
            ))
            
            total_tax = ipn + social_tax + opv + so + osms
            
        elif tax_type == TaxType.TOO_KPN:
            # ТОО - КПН
            rates = TAX_RATES["too_kpn"]
            
            # 20% КПН от налогооблагаемого дохода
            kpn = int(taxable_income * rates["kpn_rate"])
            taxes.append(TaxCalculationItem(
                name="КПН (корпоративный подоходный налог)",
                rate=rates["kpn_rate"] * 100,
                base=taxable_income,
                amount=kpn,
                description="20% от налогооблагаемого дохода"
            ))
            
            total_tax = kpn
            
        elif tax_type == TaxType.VAT:
            # НДС 12%
            vat_rate = TAX_RATES["vat_rate"]
            vat = int(revenue * vat_rate / (1 + vat_rate))  # НДС в том числе
            
            taxes.append(TaxCalculationItem(
                name="НДС (налог на добавленную стоимость)",
                rate=vat_rate * 100,
                base=revenue,
                amount=vat,
                description="12% (в том числе от выручки с НДС)"
            ))
            
            total_tax = vat
        
        net_income = taxable_income - total_tax
        
        return {
            "tax_type": tax_type,
            "period": period,
            "revenue": revenue,
            "expenses": expenses,
            "taxable_income": taxable_income,
            "taxes": [t.model_dump() for t in taxes],
            "total_tax": total_tax,
            "net_income": net_income
        }
    
    def calculate_fee(
        self,
        fee_type: str,
        claim_amount: int = None
    ) -> Dict[str, Any]:
        """Calculate state fees"""
        mrp = TAX_RATES["mrp_2026"]
        
        fees = {
            "ip_registration": {
                "amount": 0,  # Бесплатно через eGov
                "details": "Регистрация ИП через портал eGov.kz - бесплатно",
                "basis": "Закон РК 'О государственной регистрации юридических лиц и учетной регистрации филиалов и представительств'"
            },
            "too_registration": {
                "amount": mrp,  # 1 МРП
                "details": f"Регистрация ТОО: 1 МРП = {mrp:,} тенге".replace(',', ' '),
                "basis": "Налоговый кодекс РК, статья 554"
            },
            "court_fee_property": {
                "amount": self._calculate_court_fee(claim_amount or 0, is_property=True),
                "details": self._court_fee_details(claim_amount or 0, is_property=True),
                "basis": "Налоговый кодекс РК, статья 535"
            },
            "court_fee_non_property": {
                "amount": mrp // 2,  # 0.5 МРП для физлиц
                "details": f"Госпошлина по неимущественному иску для физлиц: 0.5 МРП = {mrp // 2:,} тенге".replace(',', ' '),
                "basis": "Налоговый кодекс РК, статья 535"
            },
            "license_fee": {
                "amount": mrp * 10,  # Примерно 10 МРП
                "details": f"Лицензионный сбор (ориентировочно): 10 МРП = {mrp * 10:,} тенге. Точная сумма зависит от вида деятельности.".replace(',', ' '),
                "basis": "Налоговый кодекс РК, статья 554"
            }
        }
        
        fee_info = fees.get(fee_type, fees["ip_registration"])
        
        return {
            "fee_type": fee_type,
            "fee_amount": fee_info["amount"],
            "calculation_details": fee_info["details"],
            "legal_basis": fee_info["basis"]
        }
    
    def _calculate_court_fee(self, claim_amount: int, is_property: bool) -> int:
        """Calculate court fee for property claims"""
        mrp = TAX_RATES["mrp_2026"]
        
        if not is_property or claim_amount <= 0:
            return mrp // 2  # 0.5 МРП
        
        # Для имущественных исков - 1% от суммы иска, но не менее 1 МРП
        fee = int(claim_amount * 0.01)
        return max(fee, mrp)
    
    def _court_fee_details(self, claim_amount: int, is_property: bool) -> str:
        """Generate court fee calculation details"""
        mrp = TAX_RATES["mrp_2026"]
        
        if claim_amount <= 0:
            return f"Госпошлина: 0.5 МРП = {mrp // 2:,} тенге".replace(',', ' ')
        
        fee = self._calculate_court_fee(claim_amount, is_property)
        return f"""Госпошлина по имущественному иску:
Сумма иска: {claim_amount:,} тенге
1% от суммы = {int(claim_amount * 0.01):,} тенге
Минимум: 1 МРП = {mrp:,} тенге

Итого госпошлина: {fee:,} тенге""".replace(',', ' ')


def get_gemini_circuit_breaker():
    """Get circuit breaker for Gemini API calls."""
    return get_circuit_breaker("gemini_api", CircuitBreakerConfig(
        failure_threshold=3,
        success_threshold=1,
        timeout_seconds=60.0,
        half_open_max_calls=1,
    ))


# Singleton instance
_ai_lawyer: Optional[AILawyerService] = None


def get_ai_lawyer() -> AILawyerService:
    """Get singleton instance of AILawyerService"""
    global _ai_lawyer
    if _ai_lawyer is None:
        _ai_lawyer = AILawyerService()
    return _ai_lawyer
