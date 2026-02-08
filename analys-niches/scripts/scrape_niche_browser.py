"""
Скрейпер данных аналитики ниш через браузер

Использует Playwright для:
1. Авторизации
2. Навигации по категориям
3. Сбора данных о товарах
4. Сохранения в CSV
"""

import asyncio
import csv
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page, Browser

# Загрузка credentials
load_dotenv()

EMAIL = os.getenv("NICHE_SCRAPER_EMAIL")
PASSWORD = os.getenv("NICHE_SCRAPER_PASSWORD")
SITE_URL = os.getenv("NICHE_SCRAPER_SITE", "https://app.algatop.kz")
NICHE_URL = os.getenv("NICHE_SCRAPER_NICHE", "https://app.algatop.kz/niche")

# Путь для сохранения данных
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)


class NicheDataScraper:
    def __init__(self, headless: bool = False):
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.products = []
        self.categories = []

    async def start(self):
        """Запуск браузера"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=self.headless,
            slow_mo=100  # Замедление для стабильности
        )
        self.page = await self.browser.new_page()

        # Увеличенные таймауты
        self.page.set_default_timeout(60000)

        print("🚀 Браузер запущен")

    async def login(self):
        """Авторизация на сайте"""
        print(f"🔐 Авторизация как {EMAIL}...")

        # Переход на страницу входа
        await self.page.goto(SITE_URL)
        await self.page.wait_for_load_state("networkidle")

        # Ищем кнопку входа или форму
        # Адаптируй селекторы под реальную структуру сайта
        try:
            # Попробуем найти кнопку "Войти"
            login_btn = await self.page.query_selector('text=Войти')
            if login_btn:
                await login_btn.click()
                await self.page.wait_for_load_state("networkidle")
        except:
            pass

        # Заполняем форму
        # Эти селекторы нужно адаптировать под реальный сайт
        email_input = await self.page.query_selector('input[type="email"], input[name="email"], input[placeholder*="mail"]')
        if email_input:
            await email_input.fill(EMAIL)

        password_input = await self.page.query_selector('input[type="password"]')
        if password_input:
            await password_input.fill(PASSWORD)

        # Клик на кнопку входа
        submit_btn = await self.page.query_selector('button[type="submit"], button:has-text("Войти")')
        if submit_btn:
            await submit_btn.click()
            await self.page.wait_for_load_state("networkidle")

        # Ждём редиректа
        await asyncio.sleep(3)
        print(f"✅ Авторизация завершена. URL: {self.page.url}")

    async def go_to_niche_page(self):
        """Переход на страницу поиска ниш"""
        print(f"📊 Переход на {NICHE_URL}...")
        await self.page.goto(NICHE_URL)
        await self.page.wait_for_load_state("networkidle")
        await asyncio.sleep(2)
        print(f"✅ На странице ниш. URL: {self.page.url}")

    async def get_categories(self) -> list:
        """Получение списка категорий"""
        print("📂 Получение списка категорий...")

        # Ищем элементы категорий
        # Адаптируй селекторы под реальную структуру
        category_elements = await self.page.query_selector_all(
            '[class*="category"], [class*="niche"], li[data-category], .sidebar a, .menu a'
        )

        categories = []
        for el in category_elements:
            text = await el.text_content()
            href = await el.get_attribute("href")
            if text and text.strip():
                categories.append({
                    "name": text.strip(),
                    "url": href
                })

        print(f"✅ Найдено {len(categories)} категорий")
        self.categories = categories
        return categories

    async def scrape_products_from_table(self) -> list:
        """Скрейпинг товаров из таблицы на текущей странице"""
        products = []

        # Ждём загрузки таблицы
        await self.page.wait_for_selector('table, [class*="table"], [class*="grid"]', timeout=10000)

        # Ищем строки таблицы
        rows = await self.page.query_selector_all('table tbody tr, [class*="row"], [class*="item"]')

        for row in rows:
            try:
                # Извлекаем данные из ячеек
                # Адаптируй селекторы под реальную структуру таблицы Algatop
                cells = await row.query_selector_all('td, [class*="cell"], [class*="col"]')

                if len(cells) >= 4:
                    product = {
                        "name": await self._get_text(cells[0]) if len(cells) > 0 else "",
                        "sales": await self._get_number(cells[1]) if len(cells) > 1 else 0,
                        "rating": await self._get_float(cells[2]) if len(cells) > 2 else 0.0,
                        "reviews": await self._get_number(cells[3]) if len(cells) > 3 else 0,
                        "sellers": await self._get_number(cells[4]) if len(cells) > 4 else 0,
                        "revenue": await self._get_number(cells[5]) if len(cells) > 5 else 0,
                        "scraped_at": datetime.now().isoformat()
                    }

                    if product["name"]:  # Пропускаем пустые строки
                        products.append(product)

            except Exception as e:
                print(f"⚠️ Ошибка парсинга строки: {e}")
                continue

        return products

    async def _get_text(self, element) -> str:
        """Получение текста из элемента"""
        if element:
            text = await element.text_content()
            return text.strip() if text else ""
        return ""

    async def _get_number(self, element) -> int:
        """Получение числа из элемента"""
        text = await self._get_text(element)
        # Убираем пробелы и нечисловые символы
        numbers = re.sub(r'[^\d]', '', text)
        return int(numbers) if numbers else 0

    async def _get_float(self, element) -> float:
        """Получение float из элемента"""
        text = await self._get_text(element)
        # Заменяем запятую на точку
        text = text.replace(',', '.')
        numbers = re.findall(r'[\d.]+', text)
        return float(numbers[0]) if numbers else 0.0

    async def scroll_and_load_all(self, max_scrolls: int = 50):
        """Прокрутка страницы для загрузки всех данных"""
        print("📜 Прокрутка для загрузки всех данных...")

        last_height = 0
        scrolls = 0

        while scrolls < max_scrolls:
            # Прокрутка вниз
            await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)

            # Проверяем, изменилась ли высота
            new_height = await self.page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                break

            last_height = new_height
            scrolls += 1

            # Показываем прогресс
            if scrolls % 10 == 0:
                print(f"  ... прокрутка {scrolls}/{max_scrolls}")

        print(f"✅ Прокрутка завершена ({scrolls} скроллов)")

    async def scrape_category(self, category_url: str, category_name: str) -> list:
        """Скрейпинг одной категории"""
        print(f"\n📁 Скрейпинг категории: {category_name}")

        await self.page.goto(category_url)
        await self.page.wait_for_load_state("networkidle")
        await asyncio.sleep(2)

        # Загружаем все данные через скролл
        await self.scroll_and_load_all()

        # Парсим таблицу
        products = await self.scrape_products_from_table()

        # Добавляем категорию к каждому товару
        for product in products:
            product["category"] = category_name

        print(f"✅ Собрано {len(products)} товаров из категории {category_name}")
        return products

    async def save_to_csv(self, filename: str = None):
        """Сохранение данных в CSV"""
        if not filename:
            filename = f"algatop_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        filepath = DATA_DIR / filename

        if not self.products:
            print("⚠️ Нет данных для сохранения")
            return

        # Определяем заголовки из первого продукта
        headers = list(self.products[0].keys())

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(self.products)

        print(f"\n💾 Данные сохранены в {filepath}")
        print(f"   Всего товаров: {len(self.products)}")

    async def save_to_json(self, filename: str = None):
        """Сохранение данных в JSON"""
        if not filename:
            filename = f"algatop_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        filepath = DATA_DIR / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                "scraped_at": datetime.now().isoformat(),
                "total_products": len(self.products),
                "categories": self.categories,
                "products": self.products
            }, f, ensure_ascii=False, indent=2)

        print(f"💾 JSON сохранён в {filepath}")

    async def close(self):
        """Закрытие браузера"""
        if self.browser:
            await self.browser.close()
            print("🔒 Браузер закрыт")

    async def debug_page_structure(self):
        """Отладка: показать структуру страницы"""
        print("\n🔍 DEBUG: Структура страницы")
        print(f"URL: {self.page.url}")
        print(f"Title: {await self.page.title()}")

        # Скриншот
        screenshot_path = DATA_DIR / "debug_screenshot.png"
        await self.page.screenshot(path=screenshot_path, full_page=True)
        print(f"📸 Скриншот сохранён: {screenshot_path}")

        # HTML структура
        html_path = DATA_DIR / "debug_page.html"
        html = await self.page.content()
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"📄 HTML сохранён: {html_path}")


async def main():
    """Основной скрипт"""
    print("=" * 60)
    print("ALGATOP SCRAPER")
    print("=" * 60)

    if not EMAIL or not PASSWORD:
        print("❌ Ошибка: Не заданы ALGATOP_EMAIL и ALGATOP_PASSWORD в .env")
        return

    scraper = NicheDataScraper(headless=False)  # headless=False для отладки

    try:
        await scraper.start()
        await scraper.login()
        await scraper.go_to_niche_page()

        # Сначала посмотрим структуру страницы
        await scraper.debug_page_structure()

        # Попробуем спарсить текущую страницу
        products = await scraper.scrape_products_from_table()
        scraper.products.extend(products)

        # Сохраняем
        await scraper.save_to_csv()
        await scraper.save_to_json()

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

        # Сохраняем скриншот ошибки
        if scraper.page:
            await scraper.page.screenshot(path=DATA_DIR / "error_screenshot.png")

    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
