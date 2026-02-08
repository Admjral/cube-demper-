# ИИ-Юрист для Demper

## Что реализовано (29.01.2026)

### Backend (new-backend/)

**Конфигурация:**
- `app/config.py` — добавлены настройки Gemini API:
  - `GEMINI_API_KEY` — ключ API
  - `GEMINI_MODEL` / `GEMINI_LAWYER_MODEL` = `gemini-2.5-flash`
  - `GEMINI_EMBEDDING_MODEL` = `text-embedding-004`

**Миграция базы данных:**
- `migrations/versions/20260129100000_add_ai_lawyer_tables.py`:
  - Расширение `pgvector` для векторного поиска
  - Таблица `legal_documents` — полные тексты законов
  - Таблица `legal_articles` — чанки с эмбеддингами (VECTOR 768)
  - Таблица `lawyer_documents` — сгенерированные документы
  - Таблица `lawyer_chat_feedback` — фидбек пользователей
  - Индекс IVFFlat для быстрого поиска по эмбеддингам

**Схемы (schemas/lawyer.py):**
- `LawyerLanguage` — русский/казахский
- `DocumentType` — 8 типов документов
- `TaxType` — 5 режимов налогообложения
- `RiskLevel` — уровни риска для анализа договоров
- Все Request/Response модели для API

**Сервис (services/ai_lawyer_service.py):**
- RAG поиск по базе знаний с эмбеддингами
- Системные промпты на русском и казахском
- Шаблоны документов (договор поставки, трудовой, претензия)
- Калькулятор пени по ставке рефинансирования НБ РК
- Калькулятор налогов (ИП упрощёнка, ТОО, НДС, соц.налог)
- Калькулятор госпошлин
- Анализ договоров с выявлением рисков

**API эндпоинты (routers/lawyer.py):**
- `POST /ai/lawyer/chat` — консультация с RAG
- `POST /ai/lawyer/set-language` — установка языка
- `GET /ai/lawyer/language` — получение языка
- `POST /ai/lawyer/generate-document` — генерация документа
- `GET /ai/lawyer/documents` — история документов
- `POST /ai/lawyer/analyze-contract` — анализ договора
- `POST /ai/lawyer/calculate-penalty` — расчёт пени
- `POST /ai/lawyer/calculate-tax` — расчёт налогов
- `POST /ai/lawyer/calculate-fee` — расчёт госпошлины
- `POST /ai/lawyer/chat/feedback` — фидбек по ответам
- `GET /ai/lawyer/faq` — частые вопросы

**Скрипт загрузки PDF (scripts/load_legal_docs.py):**
- Извлечение текста из PDF
- Разбивка на чанки по 500 слов
- Генерация эмбеддингов через Gemini
- Загрузка в базу данных

---

### Frontend (frontend/)

**Хуки API (hooks/api/use-lawyer.ts):**
- Все TypeScript типы
- React Query хуки для всех эндпоинтов

**Главная страница (app/(dashboard)/dashboard/ai-lawyer/page.tsx):**
- Карточки функций с иконками
- Переключатель языка RU/KZ
- FAQ секция
- Табы для навигации

**Компоненты (components/lawyer/):**
- `lawyer-chat.tsx` — чат с источниками и фидбеком
- `penalty-calculator.tsx` — калькулятор пени
- `tax-calculator.tsx` — калькулятор налогов
- `fee-calculator.tsx` — калькулятор госпошлин
- `document-generator.tsx` — генератор документов
- `contract-analyzer.tsx` — анализ договоров с drag-drop

**Зависимости (package.json):**
- Добавлен `react-dropzone` для загрузки файлов

---

## Как запустить

### 1. ENV переменные (new-backend/.env):
```env
GEMINI_API_KEY=AIzaSyAxEVz3TbqhvjpIXNgcaMBa8RJFWrShVO0
GEMINI_MODEL=gemini-2.5-flash
GEMINI_LAWYER_MODEL=gemini-2.5-flash
GEMINI_EMBEDDING_MODEL=text-embedding-004
```

### 2. Миграция базы:
```bash
cd new-backend
alembic upgrade head
```

### 3. Загрузка PDF в RAG:
```bash
pip install pypdf google-generativeai
# Положить PDF в new-backend/legal_docs/
python scripts/load_legal_docs.py ./legal_docs/
```

### 4. Установка frontend зависимостей:
```bash
cd frontend
npm install
```

### 5. Запуск:
```bash
# Backend
cd new-backend
uvicorn app.main:app --reload --port 8010

# Frontend
cd frontend
npm run dev
```

---

## Актуальные данные (2026)

- МЗП: 85 000 ₸
- МРП: 3 932 ₸
- Ставка рефинансирования НБ РК: 15.75%
- ИП упрощёнка: 3% от дохода
- Соц.налог ИП: 2 МРП
- ТОО КПН: 20%
