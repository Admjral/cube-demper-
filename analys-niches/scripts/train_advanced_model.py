"""
Продвинутое обучение ML модели на данных Algatop

Цель: найти точную формулу как у Algatop
Признаки: отзывы, цена, продавцы, рейтинг, категория
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error, mean_absolute_percentage_error
import warnings
warnings.filterwarnings('ignore')

DATA_DIR = Path(__file__).parent.parent / "data"


def load_algatop_data():
    """Загрузка всех данных Algatop"""
    json_files = sorted(DATA_DIR.glob("algatop_products_*.json"))
    if not json_files:
        raise FileNotFoundError("Нет файлов с данными Algatop")

    latest = json_files[-1]
    print(f"Загружаю: {latest.name}")

    with open(latest, 'r', encoding='utf-8') as f:
        data = json.load(f)

    products = data.get("products", [])
    print(f"Загружено: {len(products)} товаров")

    return pd.DataFrame(products)


def prepare_data(df):
    """Подготовка данных для обучения"""
    print("\n" + "="*60)
    print("ПОДГОТОВКА ДАННЫХ")
    print("="*60)

    # Целевая переменная
    target = 'sale_qty'

    # Признаки
    features = ['review_qty', 'sale_price', 'merchant_count', 'product_rate']

    # Фильтруем данные
    df_clean = df.copy()

    # Убираем нулевые и отрицательные значения
    df_clean = df_clean[df_clean[target] > 0]
    df_clean = df_clean[df_clean['review_qty'] > 0]
    df_clean = df_clean[df_clean['sale_price'] > 0]

    print(f"После фильтрации: {len(df_clean)} товаров")

    # Добавляем производные признаки
    df_clean['price_per_review'] = df_clean['sale_price'] / df_clean['review_qty']
    df_clean['log_reviews'] = np.log1p(df_clean['review_qty'])
    df_clean['log_price'] = np.log1p(df_clean['sale_price'])
    df_clean['log_sales'] = np.log1p(df_clean[target])

    # Кодируем категории
    le = LabelEncoder()
    df_clean['category_encoded'] = le.fit_transform(df_clean['_category_name'])

    # Расширенные признаки
    extended_features = features + ['log_reviews', 'log_price', 'category_encoded']

    print(f"Признаки: {extended_features}")
    print(f"\nСтатистика целевой переменной ({target}):")
    print(df_clean[target].describe())

    return df_clean, extended_features, target, le


def train_models(df, features, target):
    """Обучение разных моделей"""
    print("\n" + "="*60)
    print("ОБУЧЕНИЕ МОДЕЛЕЙ")
    print("="*60)

    X = df[features].fillna(0)
    y = df[target]

    # Также пробуем предсказывать log(sales)
    y_log = np.log1p(y)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    _, _, y_train_log, y_test_log = train_test_split(X, y_log, test_size=0.2, random_state=42)

    # Масштабирование
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    results = []

    # 1. Линейная регрессия на исходных данных
    print("\n1. Linear Regression...")
    lr = LinearRegression()
    lr.fit(X_train_scaled, y_train)
    y_pred = lr.predict(X_test_scaled)
    y_pred = np.maximum(y_pred, 0)  # не может быть отрицательных продаж

    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mape = mean_absolute_percentage_error(y_test, y_pred) * 100

    print(f"  R²: {r2:.4f}, MAE: {mae:.1f}, MAPE: {mape:.1f}%")
    results.append(('LinearRegression', r2, mae, mape, lr, scaler, False))

    # 2. Ridge Regression
    print("\n2. Ridge Regression...")
    ridge = Ridge(alpha=10)
    ridge.fit(X_train_scaled, y_train)
    y_pred = ridge.predict(X_test_scaled)
    y_pred = np.maximum(y_pred, 0)

    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mape = mean_absolute_percentage_error(y_test, y_pred) * 100

    print(f"  R²: {r2:.4f}, MAE: {mae:.1f}, MAPE: {mape:.1f}%")
    results.append(('Ridge', r2, mae, mape, ridge, scaler, False))

    # 3. Random Forest
    print("\n3. Random Forest...")
    rf = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)
    y_pred = rf.predict(X_test)

    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mape = mean_absolute_percentage_error(y_test, y_pred) * 100

    print(f"  R²: {r2:.4f}, MAE: {mae:.1f}, MAPE: {mape:.1f}%")
    results.append(('RandomForest', r2, mae, mape, rf, None, False))

    # 4. Gradient Boosting
    print("\n4. Gradient Boosting...")
    gb = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42)
    gb.fit(X_train, y_train)
    y_pred = gb.predict(X_test)

    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mape = mean_absolute_percentage_error(y_test, y_pred) * 100

    print(f"  R²: {r2:.4f}, MAE: {mae:.1f}, MAPE: {mape:.1f}%")
    results.append(('GradientBoosting', r2, mae, mape, gb, None, False))

    # 5. Log-трансформация (предсказываем log(sales))
    print("\n5. Linear Regression (log-transform)...")
    lr_log = LinearRegression()
    lr_log.fit(X_train_scaled, y_train_log)
    y_pred_log = lr_log.predict(X_test_scaled)
    y_pred = np.expm1(y_pred_log)  # обратное преобразование
    y_pred = np.maximum(y_pred, 0)

    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mape = mean_absolute_percentage_error(y_test, y_pred) * 100

    print(f"  R²: {r2:.4f}, MAE: {mae:.1f}, MAPE: {mape:.1f}%")
    results.append(('LinearRegression_Log', r2, mae, mape, lr_log, scaler, True))

    # Лучшая модель
    best = max(results, key=lambda x: x[1])
    print(f"\n{'='*60}")
    print(f"ЛУЧШАЯ МОДЕЛЬ: {best[0]}")
    print(f"  R²: {best[1]:.4f}")
    print(f"  MAE: {best[2]:.1f}")
    print(f"  MAPE: {best[3]:.1f}%")
    print(f"{'='*60}")

    return results, best, features


def analyze_coefficients(df, features, target):
    """Анализ коэффициентов по категориям"""
    print("\n" + "="*60)
    print("АНАЛИЗ КОЭФФИЦИЕНТОВ ПО КАТЕГОРИЯМ")
    print("="*60)

    # Для каждой категории обучаем отдельную модель
    category_models = {}

    for cat in df['_category_name'].unique():
        cat_df = df[df['_category_name'] == cat]

        if len(cat_df) < 50:
            continue

        X = cat_df[['review_qty', 'sale_price', 'merchant_count', 'product_rate']].fillna(0)
        y = cat_df[target]

        # Простая линейная модель: sales = a * reviews + b * price + ...
        lr = LinearRegression()
        lr.fit(X, y)

        # Также считаем простой коэффициент
        simple_coef = y.sum() / cat_df['review_qty'].sum()

        category_models[cat] = {
            'coef_reviews': lr.coef_[0],
            'coef_price': lr.coef_[1],
            'coef_merchants': lr.coef_[2],
            'coef_rating': lr.coef_[3],
            'intercept': lr.intercept_,
            'simple_coef': simple_coef,
            'samples': len(cat_df),
            'avg_sales': y.mean(),
            'avg_reviews': cat_df['review_qty'].mean(),
            'avg_price': cat_df['sale_price'].mean()
        }

    # Сортируем по simple_coef
    sorted_cats = sorted(category_models.items(), key=lambda x: x[1]['simple_coef'], reverse=True)

    print("\nКоэффициенты по категориям (sales/reviews):")
    print("-" * 80)

    for cat, data in sorted_cats:
        print(f"\n{cat}:")
        print(f"  Простой коэф (sales/reviews): {data['simple_coef']:.2f}")
        print(f"  Коэф. отзывов в модели: {data['coef_reviews']:.4f}")
        print(f"  Коэф. цены: {data['coef_price']:.6f}")
        print(f"  Intercept: {data['intercept']:.2f}")
        print(f"  Средние продажи: {data['avg_sales']:.1f}")
        print(f"  Средние отзывы: {data['avg_reviews']:.1f}")

    return category_models


def create_formula(df, category_models):
    """Создание финальной формулы"""
    print("\n" + "="*60)
    print("ФИНАЛЬНАЯ ФОРМУЛА ALGATOP")
    print("="*60)

    # Универсальная формула на основе анализа
    # sales = reviews * category_coef + price_adjustment + merchant_adjustment

    # Сохраняем модель
    model_config = {
        "version": "2.0",
        "formula": "sales = reviews * category_coef * (1 + price_factor + merchant_factor)",
        "category_coefficients": {},
        "price_factor": 0,
        "merchant_factor": 0,
        "default_coefficient": 1.0
    }

    for cat, data in category_models.items():
        model_config["category_coefficients"][cat] = {
            "base_coef": round(data['simple_coef'], 4),
            "review_weight": round(data['coef_reviews'], 4),
            "price_weight": round(data['coef_price'], 8),
            "intercept": round(data['intercept'], 2)
        }

    # Вычисляем средние факторы
    all_coefs = [d['simple_coef'] for d in category_models.values()]
    model_config["default_coefficient"] = round(np.median(all_coefs), 4)

    # Сохраняем
    output_file = DATA_DIR / "advanced_model.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(model_config, f, ensure_ascii=False, indent=2)

    print(f"\nМодель сохранена: {output_file}")

    # Простая формула
    print("\n" + "="*60)
    print("ПРОСТАЯ ФОРМУЛА ДЛЯ РАСЧЁТА:")
    print("="*60)
    print("""
    sales = reviews × category_coefficient

    Где category_coefficient:
    """)

    for cat, data in sorted(category_models.items(), key=lambda x: x[1]['simple_coef'], reverse=True):
        print(f"    {cat}: {data['simple_coef']:.2f}")

    return model_config


def validate_on_sample(df, model_config):
    """Валидация на примере"""
    print("\n" + "="*60)
    print("ВАЛИДАЦИЯ НА ПРИМЕРАХ")
    print("="*60)

    # Берём случайные товары
    samples = df.sample(10, random_state=42)

    for _, row in samples.iterrows():
        actual = row['sale_qty']
        reviews = row['review_qty']
        category = row['_category_name']
        price = row['sale_price']

        # Получаем коэффициент
        cat_config = model_config['category_coefficients'].get(category, {})
        coef = cat_config.get('base_coef', model_config['default_coefficient'])

        predicted = reviews * coef

        error_pct = abs(actual - predicted) / actual * 100 if actual > 0 else 0

        print(f"\n{row['product_name'][:40]}...")
        print(f"  Категория: {category}")
        print(f"  Отзывов: {reviews}, Цена: {price:,.0f}")
        print(f"  Реально: {actual}, Предсказано: {predicted:.0f}")
        print(f"  Ошибка: {error_pct:.1f}%")


def main():
    print("="*60)
    print("ОБУЧЕНИЕ ПРОДВИНУТОЙ ML МОДЕЛИ ALGATOP")
    print("="*60)

    # 1. Загрузка данных
    df = load_algatop_data()

    # 2. Подготовка
    df_clean, features, target, label_encoder = prepare_data(df)

    # 3. Обучение моделей
    results, best_model, feature_names = train_models(df_clean, features, target)

    # 4. Анализ по категориям
    category_models = analyze_coefficients(df_clean, features, target)

    # 5. Создание финальной формулы
    model_config = create_formula(df_clean, category_models)

    # 6. Валидация
    validate_on_sample(df_clean, model_config)

    print("\n" + "="*60)
    print("ГОТОВО!")
    print("="*60)

    return model_config


if __name__ == "__main__":
    config = main()
