"""
Сервис для склейки накладных (Invoice Merger Service)

Объединяет несколько PDF-накладных на одном листе A4 в сетке:
- 4 на 1 (2x2)
- 6 на 1 (2x3)
- 8 на 1 (2x4)
- 9 на 1 (3x3)  
- 16 на 1 (4x4)

Использует библиотеку pypdf для манипуляции PDF.
"""

import io
import zipfile
import logging
from typing import List, Tuple, Literal, BinaryIO
from dataclasses import dataclass
from enum import Enum

from pypdf import PdfReader, PdfWriter, PageObject, Transformation

logger = logging.getLogger(__name__)

# Размеры страницы A4 в points (72 points = 1 inch)
A4_WIDTH = 595.27
A4_HEIGHT = 841.89

# Отступы в points
CELL_PADDING = 10


class LayoutType(str, Enum):
    """Типы сеток для размещения накладных"""
    FOUR_ON_ONE = "4_on_1"      # 2x2
    SIX_ON_ONE = "6_on_1"       # 2x3
    EIGHT_ON_ONE = "8_on_1"     # 2x4
    NINE_ON_ONE = "9_on_1"      # 3x3
    SIXTEEN_ON_ONE = "16_on_1"  # 4x4


@dataclass
class GridConfig:
    """Конфигурация сетки для размещения накладных"""
    cols: int
    rows: int
    cell_width: float
    cell_height: float
    
    @classmethod
    def from_layout(cls, layout: LayoutType) -> "GridConfig":
        """Создаёт конфигурацию сетки на основе типа layout"""
        if layout == LayoutType.FOUR_ON_ONE:
            cols, rows = 2, 2
        elif layout == LayoutType.SIX_ON_ONE:
            cols, rows = 2, 3
        elif layout == LayoutType.EIGHT_ON_ONE:
            cols, rows = 2, 4
        elif layout == LayoutType.NINE_ON_ONE:
            cols, rows = 3, 3
        elif layout == LayoutType.SIXTEEN_ON_ONE:
            cols, rows = 4, 4
        else:
            raise ValueError(f"Неподдерживаемый тип layout: {layout}")
        
        # Вычисляем размеры ячейки с учётом отступов
        cell_width = (A4_WIDTH - CELL_PADDING * (cols + 1)) / cols
        cell_height = (A4_HEIGHT - CELL_PADDING * (rows + 1)) / rows
        
        return cls(cols=cols, rows=rows, cell_width=cell_width, cell_height=cell_height)


class InvoiceMergerError(Exception):
    """Базовое исключение для ошибок слияния накладных"""
    pass


class InvalidPDFError(InvoiceMergerError):
    """Ошибка при обработке повреждённого PDF"""
    pass


class EmptyArchiveError(InvoiceMergerError):
    """Ошибка при пустом или некорректном архиве"""
    pass


def _extract_pdfs_from_zip(zip_file: BinaryIO) -> List[Tuple[str, bytes]]:
    """
    Извлекает PDF-файлы из ZIP-архива.
    
    Args:
        zip_file: Файловый объект ZIP-архива
        
    Returns:
        Список кортежей (имя файла, содержимое байт)
        
    Raises:
        EmptyArchiveError: Если архив пуст или не содержит PDF
    """
    pdf_files = []
    
    try:
        with zipfile.ZipFile(zip_file, 'r') as zf:
            for file_info in zf.infolist():
                # Пропускаем директории и скрытые файлы macOS
                if file_info.is_dir() or file_info.filename.startswith('__MACOSX'):
                    continue
                    
                # Проверяем расширение .pdf (регистронезависимо)
                if file_info.filename.lower().endswith('.pdf'):
                    try:
                        content = zf.read(file_info.filename)
                        pdf_files.append((file_info.filename, content))
                        logger.debug(f"Извлечён PDF: {file_info.filename}")
                    except Exception as e:
                        logger.warning(f"Не удалось прочитать файл {file_info.filename}: {e}")
                        
    except zipfile.BadZipFile as e:
        raise EmptyArchiveError(f"Некорректный ZIP-архив: {e}")
    
    if not pdf_files:
        raise EmptyArchiveError("Архив не содержит PDF-файлов")
    
    # Сортируем по имени для предсказуемого порядка
    pdf_files.sort(key=lambda x: x[0])
    
    logger.info(f"Извлечено {len(pdf_files)} PDF-файлов из архива")
    return pdf_files


def _read_pdf_page(pdf_content: bytes, filename: str) -> PageObject:
    """
    Читает первую страницу PDF-файла.
    
    Args:
        pdf_content: Содержимое PDF в байтах
        filename: Имя файла для логирования
        
    Returns:
        Объект страницы PDF
        
    Raises:
        InvalidPDFError: Если PDF повреждён или не может быть прочитан
    """
    try:
        reader = PdfReader(io.BytesIO(pdf_content))
        
        if len(reader.pages) == 0:
            raise InvalidPDFError(f"PDF '{filename}' не содержит страниц")
            
        # Берём только первую страницу (накладная обычно одностраничная)
        page = reader.pages[0]
        return page
        
    except Exception as e:
        if isinstance(e, InvalidPDFError):
            raise
        raise InvalidPDFError(f"Не удалось прочитать PDF '{filename}': {e}")


def _calculate_scale_and_position(
    page: PageObject,
    cell_width: float,
    cell_height: float,
    cell_x: float,
    cell_y: float
) -> Transformation:
    """
    Вычисляет трансформацию для размещения страницы в ячейке.
    
    Страница масштабируется с сохранением пропорций, чтобы вписаться
    в ячейку с учётом отступов. Центрируется внутри ячейки.
    
    Args:
        page: Страница PDF для размещения
        cell_width: Ширина ячейки
        cell_height: Высота ячейки
        cell_x: X-координата левого нижнего угла ячейки
        cell_y: Y-координата левого нижнего угла ячейки
        
    Returns:
        Объект трансформации для применения к странице
    """
    # Получаем размеры исходной страницы
    media_box = page.mediabox
    page_width = float(media_box.width)
    page_height = float(media_box.height)
    
    # Если страница в альбомной ориентации и ячейка портретная,
    # или наоборот - поворачиваем страницу
    page_is_landscape = page_width > page_height
    cell_is_landscape = cell_width > cell_height
    
    # Для накладных обычно лучше не поворачивать, просто масштабируем
    # Но если разница значительная, учитываем это
    
    # Вычисляем масштаб с сохранением пропорций
    scale_x = cell_width / page_width
    scale_y = cell_height / page_height
    scale = min(scale_x, scale_y)  # Берём меньший, чтобы вписаться полностью
    
    # Вычисляем размеры после масштабирования
    scaled_width = page_width * scale
    scaled_height = page_height * scale
    
    # Центрируем в ячейке
    offset_x = cell_x + (cell_width - scaled_width) / 2
    offset_y = cell_y + (cell_height - scaled_height) / 2
    
    # Создаём трансформацию: сначала масштаб, потом смещение
    transformation = Transformation().scale(scale, scale).translate(offset_x, offset_y)
    
    return transformation


def merge_invoices(
    input_pdfs: List[Tuple[str, bytes]],
    layout_type: LayoutType
) -> bytes:
    """
    Объединяет несколько PDF-накладных на листах A4 в заданной сетке.
    
    Args:
        input_pdfs: Список кортежей (имя файла, содержимое PDF в байтах)
        layout_type: Тип сетки (4_on_1, 9_on_1, 16_on_1)
        
    Returns:
        Содержимое результирующего PDF в байтах
        
    Raises:
        InvoiceMergerError: При ошибках обработки
    """
    if not input_pdfs:
        raise EmptyArchiveError("Нет PDF-файлов для обработки")
    
    # Получаем конфигурацию сетки
    grid = GridConfig.from_layout(layout_type)
    cells_per_page = grid.cols * grid.rows
    
    logger.info(f"Объединение {len(input_pdfs)} накладных в сетку {grid.cols}x{grid.rows}")
    
    # Создаём writer для результирующего PDF
    writer = PdfWriter()
    
    # Читаем все страницы
    pages: List[Tuple[str, PageObject]] = []
    errors: List[str] = []
    
    for filename, content in input_pdfs:
        try:
            page = _read_pdf_page(content, filename)
            pages.append((filename, page))
        except InvalidPDFError as e:
            errors.append(str(e))
            logger.warning(f"Пропущен файл: {e}")
    
    if not pages:
        raise InvalidPDFError(
            f"Не удалось прочитать ни один PDF. Ошибки: {'; '.join(errors)}"
        )
    
    if errors:
        logger.warning(f"Пропущено {len(errors)} файлов из-за ошибок")
    
    # Группируем страницы по листам A4
    for page_idx in range(0, len(pages), cells_per_page):
        batch = pages[page_idx:page_idx + cells_per_page]
        
        # Создаём новую пустую страницу A4
        new_page = PageObject.create_blank_page(width=A4_WIDTH, height=A4_HEIGHT)
        
        # Размещаем накладные в ячейках сетки
        for i, (filename, invoice_page) in enumerate(batch):
            # Вычисляем позицию ячейки в сетке
            col = i % grid.cols
            row = grid.rows - 1 - (i // grid.cols)  # Снизу вверх в PDF координатах
            
            # Вычисляем координаты ячейки
            cell_x = CELL_PADDING + col * (grid.cell_width + CELL_PADDING)
            cell_y = CELL_PADDING + row * (grid.cell_height + CELL_PADDING)
            
            # Получаем трансформацию для размещения страницы в ячейке
            transformation = _calculate_scale_and_position(
                invoice_page,
                grid.cell_width,
                grid.cell_height,
                cell_x,
                cell_y
            )
            
            # Применяем трансформацию и добавляем на страницу
            new_page.merge_transformed_page(invoice_page, transformation)
            
            logger.debug(f"Добавлена накладная '{filename}' в ячейку ({col}, {row})")
        
        writer.add_page(new_page)
        logger.debug(f"Создан лист {page_idx // cells_per_page + 1} с {len(batch)} накладными")
    
    # Записываем результат в байты
    output = io.BytesIO()
    writer.write(output)
    output.seek(0)
    
    result = output.read()
    logger.info(f"Создан PDF размером {len(result)} байт, {len(writer.pages)} страниц")
    
    return result


def process_zip_archive(
    zip_file: BinaryIO,
    layout_type: LayoutType
) -> bytes:
    """
    Главная функция: обрабатывает ZIP-архив и возвращает объединённый PDF.
    
    Args:
        zip_file: Файловый объект ZIP-архива с накладными
        layout_type: Тип сетки для размещения
        
    Returns:
        Содержимое результирующего PDF в байтах
        
    Raises:
        InvoiceMergerError: При любых ошибках обработки
    """
    # Извлекаем PDF из архива
    pdf_files = _extract_pdfs_from_zip(zip_file)
    
    # Объединяем накладные
    result = merge_invoices(pdf_files, layout_type)
    
    return result
