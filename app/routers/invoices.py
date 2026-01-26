"""
Роутер для работы с накладными (Invoices Router)

Эндпоинты для склейки PDF-накладных на листах A4.
"""

from fastapi import APIRouter, File, UploadFile, Query, HTTPException, status
from fastapi.responses import StreamingResponse
from typing import Annotated
import io
import logging

from ..services.invoice_merger import (
    process_zip_archive,
    LayoutType,
    InvoiceMergerError,
    InvalidPDFError,
    EmptyArchiveError,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post(
    "/process-invoices",
    summary="Склейка накладных на листе A4",
    description="""
    Принимает ZIP-архив с PDF-накладными и возвращает один PDF-файл,
    где накладные объединены на листах формата A4 в заданной сетке.
    
    **Типы сеток:**
    - `4_on_1` — сетка 2x2 (4 накладных на листе)
    - `6_on_1` — сетка 2x3 (6 накладных на листе)
    - `8_on_1` — сетка 2x4 (8 накладных на листе)
    - `9_on_1` — сетка 3x3 (9 накладных на листе)
    - `16_on_1` — сетка 4x4 (16 накладных на листе)
    
    **Особенности:**
    - Накладные масштабируются с сохранением пропорций
    - Поддерживаются накладные разной ориентации
    - Повреждённые PDF пропускаются с логированием
    - Если накладных не кратно сетке, последние ячейки остаются пустыми
    """,
    response_class=StreamingResponse,
    responses={
        200: {
            "content": {"application/pdf": {}},
            "description": "PDF-файл с объединёнными накладными"
        },
        400: {
            "description": "Ошибка в запросе (пустой архив, нет PDF, все PDF повреждены)"
        },
        422: {
            "description": "Некорректный формат файла или параметров"
        }
    }
)
async def process_invoices(
    file: Annotated[
        UploadFile,
        File(description="ZIP-архив с PDF-накладными")
    ],
    layout: Annotated[
        LayoutType,
        Query(description="Тип сетки для размещения накладных")
    ] = LayoutType.FOUR_ON_ONE
):
    """
    Обрабатывает ZIP-архив с накладными и возвращает объединённый PDF.
    
    Пример использования с curl:
    ```bash
    curl -X POST "http://localhost:8000/api/invoices/process-invoices?layout=4_on_1" \
         -F "file=@invoices.zip" \
         -o merged_invoices.pdf
    ```
    """
    # Проверяем, что загружен файл
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Файл не загружен"
        )
    
    # Проверяем расширение файла
    if not file.filename.lower().endswith('.zip'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Ожидается ZIP-архив (расширение .zip)"
        )
    
    # Проверяем content-type (опционально, не все клиенты его устанавливают)
    allowed_content_types = [
        "application/zip",
        "application/x-zip-compressed",
        "application/x-zip",
        "multipart/x-zip",
        "application/octet-stream",  # Некоторые клиенты отправляют так
    ]
    
    if file.content_type and file.content_type not in allowed_content_types:
        logger.warning(f"Неожиданный content-type: {file.content_type}")
    
    logger.info(f"Получен запрос на склейку накладных: {file.filename}, layout={layout.value}")
    
    try:
        # Читаем содержимое файла
        content = await file.read()
        zip_buffer = io.BytesIO(content)
        
        # Обрабатываем архив
        result_pdf = process_zip_archive(zip_buffer, layout)
        
        # Генерируем имя файла для скачивания
        output_filename = f"merged_invoices_{layout.value}.pdf"
        
        logger.info(f"Успешно создан PDF: {output_filename}, размер {len(result_pdf)} байт")
        
        # Возвращаем PDF как streaming response
        return StreamingResponse(
            io.BytesIO(result_pdf),
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{output_filename}"',
                "Content-Length": str(len(result_pdf)),
            }
        )
        
    except EmptyArchiveError as e:
        logger.warning(f"Пустой архив: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
        
    except InvalidPDFError as e:
        logger.warning(f"Ошибка PDF: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
        
    except InvoiceMergerError as e:
        logger.error(f"Ошибка обработки: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
        
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обработке накладных: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера при обработке накладных"
        )


@router.get(
    "/layout-types",
    summary="Получить доступные типы сеток",
    description="Возвращает список доступных типов сеток для склейки накладных"
)
async def get_layout_types():
    """
    Возвращает информацию о доступных типах сеток.
    """
    return {
        "layouts": [
            {
                "value": LayoutType.FOUR_ON_ONE.value,
                "label": "4 на 1",
                "description": "Сетка 2x2 (4 накладных на листе A4)",
                "grid": "2x2"
            },
            {
                "value": LayoutType.SIX_ON_ONE.value,
                "label": "6 на 1",
                "description": "Сетка 2x3 (6 накладных на листе A4)",
                "grid": "2x3"
            },
            {
                "value": LayoutType.EIGHT_ON_ONE.value,
                "label": "8 на 1",
                "description": "Сетка 2x4 (8 накладных на листе A4)",
                "grid": "2x4"
            },
            {
                "value": LayoutType.NINE_ON_ONE.value,
                "label": "9 на 1",
                "description": "Сетка 3x3 (9 накладных на листе A4)",
                "grid": "3x3"
            },
            {
                "value": LayoutType.SIXTEEN_ON_ONE.value,
                "label": "16 на 1",
                "description": "Сетка 4x4 (16 накладных на листе A4)",
                "grid": "4x4"
            }
        ]
    }
