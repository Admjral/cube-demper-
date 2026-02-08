"""
Скрипт для анализа данных ниш и вычисления формулы расчёта продаж.

Использование:
    python analyze_niche_data.py ../data/niche_products.csv

Что делает:
1. Загружает CSV/Excel с данными аналитики ниш
2. Анализирует корреляции между полями
3. Пытается вычислить формулу: sales = f(reviews, rating, sellers, ...)
4. Выводит найденные коэффициенты
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path


def load_data(filepath: str) -> pd.DataFrame:
    """Загрузка данных из CSV или Excel"""
    path = Path(filepath)

    if path.suffix.lower() == '.csv':
        # Пробуем разные кодировки
        for encoding in ['utf-8', 'cp1251', 'latin1']:
            try:
                return pd.read_csv(filepath, encoding=encoding)
            except UnicodeDecodeError:
                continue
        raise ValueError("Не удалось прочитать CSV файл")

    elif path.suffix.lower() in ['.xlsx', '.xls']:
        return pd.read_excel(filepath)

    else:
        raise ValueError(f"Неподдерживаемый формат: {path.suffix}")


def analyze_columns(df: pd.DataFrame) -> dict:
    """Анализ колонок датасета"""
    print("\n" + "="*60)
    print("АНАЛИЗ КОЛОНОК")
    print("="*60)

    print(f"\nВсего колонок: {len(df.columns)}")
    print(f"Всего строк: {len(df)}")

    print("\nКолонки:")
    for i, col in enumerate(df.columns, 1):
        dtype = df[col].dtype
        non_null = df[col].notna().sum()
        print(f"  {i}. {col} ({dtype}) - {non_null} значений")

    return {
        'columns': list(df.columns),
        'shape': df.shape
    }


def find_sales_columns(df: pd.DataFrame) -> list:
    """Поиск колонок с продажами по месяцам"""
    sales_cols = []

    # Паттерны для поиска колонок продаж
    patterns = [
        'продаж', 'sales', 'янв', 'фев', 'мар', 'апр', 'май', 'июн',
        'июл', 'авг', 'сен', 'окт', 'ноя', 'дек', 'jan', 'feb', 'mar',
        'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
        '2024', '2025', '2026'
    ]

    for col in df.columns:
        col_lower = col.lower()
        if any(p in col_lower for p in patterns):
            if df[col].dtype in ['int64', 'float64'] or df[col].str.isnumeric().any() if df[col].dtype == 'object' else False:
                sales_cols.append(col)

    return sales_cols


def calculate_correlations(df: pd.DataFrame, target_col: str) -> pd.Series:
    """Расчёт корреляций с целевой переменной (продажи)"""
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    correlations = {}

    for col in numeric_cols:
        if col != target_col:
            corr = df[target_col].corr(df[col])
            if not np.isnan(corr):
                correlations[col] = corr

    return pd.Series(correlations).sort_values(ascending=False)


def reverse_engineer_formula(df: pd.DataFrame, sales_col: str, feature_cols: list) -> dict:
    """
    Попытка вычислить формулу расчёта продаж через линейную регрессию
    """
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import r2_score, mean_absolute_error

    # Подготовка данных
    X = df[feature_cols].fillna(0)
    y = df[sales_col].fillna(0)

    # Убираем строки где все нули
    mask = (X.sum(axis=1) > 0) & (y > 0)
    X = X[mask]
    y = y[mask]

    if len(X) < 10:
        return {'error': 'Недостаточно данных для анализа'}

    # Обучение модели
    model = LinearRegression()
    model.fit(X, y)

    # Оценка качества
    y_pred = model.predict(X)
    r2 = r2_score(y, y_pred)
    mae = mean_absolute_error(y, y_pred)

    # Коэффициенты
    coefficients = dict(zip(feature_cols, model.coef_))

    return {
        'intercept': model.intercept_,
        'coefficients': coefficients,
        'r2_score': r2,
        'mae': mae,
        'formula': build_formula_string(model.intercept_, coefficients)
    }


def build_formula_string(intercept: float, coefficients: dict) -> str:
    """Построение строки формулы"""
    parts = [f"{intercept:.2f}"]

    for col, coef in sorted(coefficients.items(), key=lambda x: abs(x[1]), reverse=True):
        if abs(coef) > 0.01:
            sign = "+" if coef > 0 else "-"
            parts.append(f"{sign} {abs(coef):.4f} * {col}")

    return "sales = " + " ".join(parts)


def analyze_category_coefficients(df: pd.DataFrame, category_col: str, sales_col: str, reviews_col: str) -> pd.DataFrame:
    """
    Вычисление коэффициентов по категориям
    coefficient = sales / delta_reviews
    """
    results = []

    for category in df[category_col].unique():
        cat_data = df[df[category_col] == category]

        if len(cat_data) < 5:
            continue

        # Среднее соотношение продаж к отзывам
        valid_data = cat_data[(cat_data[reviews_col] > 0) & (cat_data[sales_col] > 0)]

        if len(valid_data) > 0:
            ratio = (valid_data[sales_col] / valid_data[reviews_col]).median()
            results.append({
                'category': category,
                'coefficient': ratio,
                'sample_size': len(valid_data)
            })

    return pd.DataFrame(results).sort_values('coefficient', ascending=False)


def main():
    if len(sys.argv) < 2:
        print("Использование: python analyze_algatop.py <путь_к_файлу>")
        print("\nПример:")
        print("  python analyze_algatop.py ../data/algatop_export.csv")
        sys.exit(1)

    filepath = sys.argv[1]

    print(f"\nЗагрузка данных из: {filepath}")
    df = load_data(filepath)

    # Анализ структуры
    info = analyze_columns(df)

    # Показать первые строки
    print("\n" + "="*60)
    print("ПЕРВЫЕ 5 СТРОК")
    print("="*60)
    print(df.head().to_string())

    # Поиск колонок продаж
    sales_cols = find_sales_columns(df)
    if sales_cols:
        print("\n" + "="*60)
        print("НАЙДЕННЫЕ КОЛОНКИ ПРОДАЖ")
        print("="*60)
        for col in sales_cols:
            print(f"  - {col}")

    # Статистика числовых колонок
    print("\n" + "="*60)
    print("СТАТИСТИКА ЧИСЛОВЫХ КОЛОНОК")
    print("="*60)
    print(df.describe().to_string())

    print("\n" + "="*60)
    print("СЛЕДУЮЩИЕ ШАГИ")
    print("="*60)
    print("""
После анализа структуры данных:

1. Укажи какая колонка содержит ПРОДАЖИ (target)
2. Укажи какие колонки использовать как features:
   - отзывы
   - рейтинг
   - количество продавцов
   - цена
   - и т.д.

Запусти с дополнительными параметрами:
  python analyze_algatop.py data.csv --target "Продажи" --features "Отзывы,Рейтинг,Продавцы"
    """)


if __name__ == "__main__":
    main()
