"""
Анализ данных Algatop и обучение ML модели для предсказания продаж

Цель: Найти формулу sales = f(reviews, rating, merchants, price, ...)
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import warnings
warnings.filterwarnings('ignore')

DATA_DIR = Path(__file__).parent.parent / "data"


def load_data():
    """Загрузка данных из JSON"""
    # Найти последний файл с продуктами
    json_files = sorted(DATA_DIR.glob("algatop_products_*.json"))
    if not json_files:
        raise FileNotFoundError("Нет файлов с продуктами")

    latest_file = json_files[-1]
    print(f"📂 Загружаю: {latest_file.name}")

    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    products = data.get("products", [])
    print(f"✅ Загружено {len(products)} товаров")

    return pd.DataFrame(products)


def explore_data(df):
    """Исследование данных"""
    print("\n" + "="*60)
    print("ИССЛЕДОВАНИЕ ДАННЫХ")
    print("="*60)

    print(f"\n📊 Размер датасета: {df.shape[0]} строк, {df.shape[1]} колонок")

    print("\n📋 Колонки:")
    for col in df.columns:
        print(f"  - {col}: {df[col].dtype}")

    # Числовые колонки
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    print(f"\n📈 Числовые колонки: {numeric_cols}")

    # Базовая статистика
    print("\n📊 Статистика по ключевым полям:")
    key_cols = ['sale_qty', 'sale_amount', 'review_qty', 'product_rate',
                'merchant_count', 'sale_price', 'min_price', 'max_price']

    existing_cols = [c for c in key_cols if c in df.columns]
    if existing_cols:
        print(df[existing_cols].describe().round(2))

    return df


def prepare_features(df):
    """Подготовка признаков для ML"""
    print("\n" + "="*60)
    print("ПОДГОТОВКА ПРИЗНАКОВ")
    print("="*60)

    # Целевая переменная - продажи за месяц
    target = 'sale_qty'

    if target not in df.columns:
        print("❌ Нет колонки sale_qty!")
        return None, None, None

    # Признаки для обучения
    feature_candidates = [
        'review_qty',        # Количество отзывов
        'product_rate',      # Рейтинг товара
        'merchant_count',    # Количество продавцов
        'sale_price',        # Цена продажи
        'min_price',         # Мин цена
        'max_price',         # Макс цена
        'sale_amount',       # Выручка (может утечь в модель, но интересно)
    ]

    # Оставляем только существующие колонки
    features = [f for f in feature_candidates if f in df.columns]
    print(f"✅ Используемые признаки: {features}")

    # Удаляем строки с пропусками
    df_clean = df[[target] + features].dropna()
    print(f"📊 После очистки: {len(df_clean)} товаров")

    # Удаляем выбросы (товары с 0 продаж или 0 отзывов)
    df_clean = df_clean[df_clean[target] > 0]
    df_clean = df_clean[df_clean['review_qty'] > 0]
    print(f"📊 После фильтрации: {len(df_clean)} товаров")

    X = df_clean[features]
    y = df_clean[target]

    return X, y, features


def analyze_correlations(df):
    """Анализ корреляций"""
    print("\n" + "="*60)
    print("КОРРЕЛЯЦИОННЫЙ АНАЛИЗ")
    print("="*60)

    target = 'sale_qty'
    if target not in df.columns:
        return

    # Числовые колонки
    numeric_df = df.select_dtypes(include=[np.number])

    # Корреляции с целевой переменной
    correlations = numeric_df.corr()[target].sort_values(ascending=False)

    print(f"\n📈 Корреляции с {target}:")
    for col, corr in correlations.items():
        if col != target:
            print(f"  {col}: {corr:.4f}")

    return correlations


def calculate_simple_coefficient(df):
    """Расчёт простого коэффициента: sales = reviews × K"""
    print("\n" + "="*60)
    print("РАСЧЁТ ПРОСТОГО КОЭФФИЦИЕНТА")
    print("="*60)

    df_valid = df[(df['sale_qty'] > 0) & (df['review_qty'] > 0)].copy()

    # Коэффициент = продажи / отзывы
    df_valid['coefficient'] = df_valid['sale_qty'] / df_valid['review_qty']

    # Общая статистика
    print(f"\n📊 Коэффициент sale_qty/review_qty:")
    print(f"  Медиана: {df_valid['coefficient'].median():.2f}")
    print(f"  Среднее: {df_valid['coefficient'].mean():.2f}")
    print(f"  Мин: {df_valid['coefficient'].min():.4f}")
    print(f"  Макс: {df_valid['coefficient'].max():.2f}")
    print(f"  25%: {df_valid['coefficient'].quantile(0.25):.2f}")
    print(f"  75%: {df_valid['coefficient'].quantile(0.75):.2f}")

    # По категориям
    print(f"\n📁 Коэффициенты по категориям:")
    category_stats = df_valid.groupby('_category_name').agg({
        'coefficient': ['median', 'mean', 'count'],
        'sale_qty': 'sum',
        'review_qty': 'sum'
    }).round(2)

    # Плоские имена колонок
    category_stats.columns = ['coef_median', 'coef_mean', 'count', 'total_sales', 'total_reviews']
    category_stats['global_coef'] = (category_stats['total_sales'] / category_stats['total_reviews']).round(2)
    category_stats = category_stats.sort_values('global_coef', ascending=False)

    print(category_stats)

    return df_valid['coefficient'].median(), category_stats


def train_models(X, y, features):
    """Обучение ML моделей"""
    print("\n" + "="*60)
    print("ОБУЧЕНИЕ ML МОДЕЛЕЙ")
    print("="*60)

    # Разделение на train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"📊 Train: {len(X_train)}, Test: {len(X_test)}")

    # Масштабирование
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Модели для тестирования
    models = {
        'Linear Regression': LinearRegression(),
        'Ridge Regression': Ridge(alpha=1.0),
        'Lasso Regression': Lasso(alpha=1.0),
        'Random Forest': RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
        'Gradient Boosting': GradientBoostingRegressor(n_estimators=100, random_state=42),
    }

    results = []
    best_model = None
    best_score = -999

    for name, model in models.items():
        print(f"\n🔧 {name}...")

        # Используем масштабированные данные для линейных моделей
        if 'Linear' in name or 'Ridge' in name or 'Lasso' in name:
            model.fit(X_train_scaled, y_train)
            y_pred = model.predict(X_test_scaled)
        else:
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)

        r2 = r2_score(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))

        results.append({
            'model': name,
            'r2': r2,
            'mae': mae,
            'rmse': rmse
        })

        print(f"  R²: {r2:.4f}, MAE: {mae:.2f}, RMSE: {rmse:.2f}")

        if r2 > best_score:
            best_score = r2
            best_model = (name, model)

    # Важность признаков для лучшей модели
    print(f"\n" + "="*60)
    print(f"ЛУЧШАЯ МОДЕЛЬ: {best_model[0]} (R² = {best_score:.4f})")
    print("="*60)

    model = best_model[1]

    if hasattr(model, 'feature_importances_'):
        print(f"\n📊 Важность признаков:")
        importances = pd.DataFrame({
            'feature': features,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)

        for _, row in importances.iterrows():
            pct = row['importance'] * 100
            bar = '█' * int(pct / 2)
            print(f"  {row['feature']:20s} {pct:6.2f}% {bar}")

    elif hasattr(model, 'coef_'):
        print(f"\n📊 Коэффициенты линейной модели:")
        coefs = pd.DataFrame({
            'feature': features,
            'coefficient': model.coef_
        }).sort_values('coefficient', ascending=False, key=abs)

        for _, row in coefs.iterrows():
            print(f"  {row['feature']:20s} {row['coefficient']:12.4f}")

        print(f"\n  Intercept (смещение): {model.intercept_:.4f}")

    return best_model, results, scaler


def derive_formula(df, X, y, features, best_model, scaler):
    """Вывод итоговой формулы"""
    print("\n" + "="*60)
    print("ИТОГОВАЯ ФОРМУЛА ДЛЯ РАСЧЁТА ПРОДАЖ")
    print("="*60)

    # 1. Простая формула: продажи = отзывы × коэффициент
    df_valid = df[(df['sale_qty'] > 0) & (df['review_qty'] > 0)]
    simple_coef = (df_valid['sale_qty'].sum() / df_valid['review_qty'].sum())

    print(f"\n📌 ПРОСТАЯ ФОРМУЛА:")
    print(f"   sale_qty = review_qty × {simple_coef:.2f}")
    print(f"   (глобальный коэффициент для всех категорий)")

    # 2. Линейная регрессия без sale_amount
    features_no_amount = [f for f in features if f != 'sale_amount']
    if len(features_no_amount) > 0:
        X_clean = X[features_no_amount]

        lr = LinearRegression()
        lr.fit(X_clean, y)

        print(f"\n📌 ЛИНЕЙНАЯ ФОРМУЛА:")
        formula_parts = []
        for feat, coef in zip(features_no_amount, lr.coef_):
            if abs(coef) > 0.001:
                sign = '+' if coef > 0 else ''
                formula_parts.append(f"{sign}{coef:.4f} × {feat}")

        formula = f"   sale_qty = {lr.intercept_:.2f} " + " ".join(formula_parts)
        print(formula)

    # 3. Категорийные коэффициенты
    print(f"\n📌 КАТЕГОРИЙНЫЕ КОЭФФИЦИЕНТЫ (sale_qty/review_qty):")
    cat_coefs = df_valid.groupby('_category_name').apply(
        lambda x: x['sale_qty'].sum() / x['review_qty'].sum()
    ).sort_values(ascending=False)

    for cat, coef in cat_coefs.items():
        print(f"   {cat}: {coef:.2f}")

    # Сохраняем коэффициенты в JSON
    coefficients = {
        'global_coefficient': round(simple_coef, 2),
        'category_coefficients': {cat: round(coef, 2) for cat, coef in cat_coefs.items()},
        'formula': f'sale_qty = review_qty × category_coefficient',
        'default_coefficient': round(simple_coef, 2)
    }

    output_file = DATA_DIR / 'sales_coefficients.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(coefficients, f, ensure_ascii=False, indent=2)

    print(f"\n💾 Коэффициенты сохранены: {output_file}")

    return coefficients


def main():
    print("="*60)
    print("АНАЛИЗ ДАННЫХ ALGATOP И ОБУЧЕНИЕ ML МОДЕЛИ")
    print("="*60)

    # 1. Загрузка данных
    df = load_data()

    # 2. Исследование
    df = explore_data(df)

    # 3. Корреляции
    correlations = analyze_correlations(df)

    # 4. Простой коэффициент
    global_coef, category_coefs = calculate_simple_coefficient(df)

    # 5. Подготовка признаков
    X, y, features = prepare_features(df)

    if X is not None and len(X) > 100:
        # 6. Обучение моделей
        best_model, results, scaler = train_models(X, y, features)

        # 7. Вывод формулы
        coefficients = derive_formula(df, X, y, features, best_model, scaler)
    else:
        print("❌ Недостаточно данных для обучения модели")
        coefficients = None

    print("\n" + "="*60)
    print("✅ АНАЛИЗ ЗАВЕРШЁН")
    print("="*60)

    return coefficients


if __name__ == "__main__":
    coefficients = main()
