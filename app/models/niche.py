from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
import uuid


@dataclass
class NicheCategory:
    """Категория ниши на Kaspi"""
    id: uuid.UUID
    name: str
    parent_id: Optional[uuid.UUID]  # Для иерархии категорий
    kaspi_category_id: Optional[str]  # ID категории на Kaspi
    coefficient: float  # Коэффициент sales/reviews для этой категории
    total_products: int
    total_sellers: int
    avg_price: int  # В тенге
    total_revenue: int  # Общая выручка категории за месяц
    status: str  # 'open', 'restricted', 'closed'
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create NicheCategory from database row"""
        return cls(
            id=row['id'],
            name=row['name'],
            parent_id=row.get('parent_id'),
            kaspi_category_id=row.get('kaspi_category_id'),
            coefficient=row.get('coefficient', 15.0),
            total_products=row.get('total_products', 0),
            total_sellers=row.get('total_sellers', 0),
            avg_price=row.get('avg_price', 0),
            total_revenue=row.get('total_revenue', 0),
            status=row.get('status', 'open'),
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'name': self.name,
            'parent_id': str(self.parent_id) if self.parent_id else None,
            'kaspi_category_id': self.kaspi_category_id,
            'coefficient': self.coefficient,
            'total_products': self.total_products,
            'total_sellers': self.total_sellers,
            'avg_price': self.avg_price,
            'total_revenue': self.total_revenue,
            'status': self.status,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }


@dataclass
class NicheProduct:
    """Товар для анализа ниш (данные с Kaspi)"""
    id: uuid.UUID
    category_id: uuid.UUID
    kaspi_product_id: str
    name: str
    brand: Optional[str]
    price: int  # В тенге
    reviews_count: int
    rating: float
    sellers_count: int
    estimated_sales: int  # Расчётные продажи
    estimated_revenue: int  # Расчётная выручка
    image_url: Optional[str]
    kaspi_url: Optional[str]
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create NicheProduct from database row"""
        return cls(
            id=row['id'],
            category_id=row['category_id'],
            kaspi_product_id=row['kaspi_product_id'],
            name=row['name'],
            brand=row.get('brand'),
            price=row.get('price', 0),
            reviews_count=row.get('reviews_count', 0),
            rating=row.get('rating', 0.0),
            sellers_count=row.get('sellers_count', 0),
            estimated_sales=row.get('estimated_sales', 0),
            estimated_revenue=row.get('estimated_revenue', 0),
            image_url=row.get('image_url'),
            kaspi_url=row.get('kaspi_url'),
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'category_id': str(self.category_id),
            'kaspi_product_id': self.kaspi_product_id,
            'name': self.name,
            'brand': self.brand,
            'price': self.price,
            'reviews_count': self.reviews_count,
            'rating': self.rating,
            'sellers_count': self.sellers_count,
            'estimated_sales': self.estimated_sales,
            'estimated_revenue': self.estimated_revenue,
            'image_url': self.image_url,
            'kaspi_url': self.kaspi_url,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
        }


@dataclass
class NicheProductHistory:
    """История показателей товара по месяцам"""
    id: uuid.UUID
    product_id: uuid.UUID
    year: int
    month: int
    reviews_count: int
    estimated_sales: int
    estimated_revenue: int
    price: int
    sellers_count: int
    created_at: datetime

    @classmethod
    def from_row(cls, row):
        """Create NicheProductHistory from database row"""
        return cls(
            id=row['id'],
            product_id=row['product_id'],
            year=row['year'],
            month=row['month'],
            reviews_count=row.get('reviews_count', 0),
            estimated_sales=row.get('estimated_sales', 0),
            estimated_revenue=row.get('estimated_revenue', 0),
            price=row.get('price', 0),
            sellers_count=row.get('sellers_count', 0),
            created_at=row['created_at']
        )

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'product_id': str(self.product_id),
            'year': self.year,
            'month': self.month,
            'reviews_count': self.reviews_count,
            'estimated_sales': self.estimated_sales,
            'estimated_revenue': self.estimated_revenue,
            'price': self.price,
            'sellers_count': self.sellers_count,
            'created_at': self.created_at.isoformat(),
        }
