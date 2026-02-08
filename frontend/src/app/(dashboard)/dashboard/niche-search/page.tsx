"use client"

import { useState } from "react"
import { useStore } from "@/store/use-store"
import { useNicheSearch, useNicheCategories } from "@/hooks/api/use-niche-search"
import type { NicheSearchParams, NicheProduct } from "@/types/api"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  TrendingUp,
  Package,
  Users,
  Star,
  DollarSign,
  Filter,
  ChevronLeft,
  ChevronRight,
  ExternalLink,
  BarChart3,
  Sparkles,
  RefreshCw,
} from "lucide-react"

function formatPrice(price: number): string {
  return new Intl.NumberFormat('ru-KZ').format(price) + ' ₸'
}

function formatNumber(num: number): string {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M'
  }
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K'
  }
  return num.toString()
}

function getCompetitionBadge(merchantCount: number) {
  if (merchantCount <= 5) {
    return <Badge className="bg-green-500/20 text-green-500 hover:bg-green-500/30">Низкая</Badge>
  }
  if (merchantCount <= 15) {
    return <Badge className="bg-yellow-500/20 text-yellow-500 hover:bg-yellow-500/30">Средняя</Badge>
  }
  return <Badge className="bg-red-500/20 text-red-500 hover:bg-red-500/30">Высокая</Badge>
}

export default function NicheSearchPage() {
  const { locale } = useStore()
  const [searchParams, setSearchParams] = useState<NicheSearchParams>({
    page: 1,
    limit: 20,
    sort_by: 'revenue',
    sort_order: 'desc',
  })
  const [selectedProduct, setSelectedProduct] = useState<NicheProduct | null>(null)
  const [showFilters, setShowFilters] = useState(false)

  // Используем реальные API хуки
  const { data: searchData, isLoading: isLoadingProducts, error: productsError, refetch } = useNicheSearch(searchParams)
  const { data: categoriesData, isLoading: isLoadingCategories } = useNicheCategories()

  const isLoading = isLoadingProducts || isLoadingCategories
  const hasError = !!productsError
  const products = searchData?.products ?? []
  const categories = categoriesData ?? []
  const totalProducts = searchData?.total ?? 0

  // Handlers
  const handleCategoryChange = (categoryId: string) => {
    setSearchParams(prev => ({
      ...prev,
      category_id: categoryId === 'all' ? undefined : categoryId,
      page: 1,
    }))
  }

  const handleSortChange = (sortBy: string) => {
    setSearchParams(prev => ({
      ...prev,
      sort_by: sortBy as 'sales' | 'revenue' | 'reviews' | 'margin',
    }))
  }

  const handleCompetitionChange = (competition: string) => {
    setSearchParams(prev => ({
      ...prev,
      competition: competition === 'all' ? undefined : competition as 'low' | 'medium' | 'high',
      page: 1,
    }))
  }

  const handlePriceRangeChange = (field: 'min_price' | 'max_price', value: string) => {
    const numValue = value ? parseInt(value) : undefined
    setSearchParams(prev => ({
      ...prev,
      [field]: numValue,
      page: 1,
    }))
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <Sparkles className="h-6 w-6 text-primary" />
            {locale === 'ru' ? 'Поиск ниш' : 'Niche Search'}
          </h1>
          <p className="text-muted-foreground mt-1">
            {locale === 'ru'
              ? 'Найдите прибыльные товары на Kaspi.kz'
              : 'Find profitable products on Kaspi.kz'}
          </p>
        </div>

        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setShowFilters(!showFilters)}
            className="gap-2"
          >
            <Filter className="h-4 w-4" />
            {locale === 'ru' ? 'Фильтры' : 'Filters'}
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="gap-2"
            onClick={() => refetch()}
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            {locale === 'ru' ? 'Обновить' : 'Refresh'}
          </Button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Card className="glass-card">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-primary/10">
                <Package className="h-5 w-5 text-primary" />
              </div>
              <div>
                <p className="text-sm text-muted-foreground">
                  {locale === 'ru' ? 'Товаров' : 'Products'}
                </p>
                <p className="text-xl font-semibold">156,250</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="glass-card">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-green-500/10">
                <TrendingUp className="h-5 w-5 text-green-500" />
              </div>
              <div>
                <p className="text-sm text-muted-foreground">
                  {locale === 'ru' ? 'Средние продажи' : 'Avg Sales'}
                </p>
                <p className="text-xl font-semibold">127/мес</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="glass-card">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-blue-500/10">
                <DollarSign className="h-5 w-5 text-blue-500" />
              </div>
              <div>
                <p className="text-sm text-muted-foreground">
                  {locale === 'ru' ? 'Средняя выручка' : 'Avg Revenue'}
                </p>
                <p className="text-xl font-semibold">2.4M ₸</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="glass-card">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-yellow-500/10">
                <BarChart3 className="h-5 w-5 text-yellow-500" />
              </div>
              <div>
                <p className="text-sm text-muted-foreground">
                  {locale === 'ru' ? 'Категорий' : 'Categories'}
                </p>
                <p className="text-xl font-semibold">21</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Filters */}
      {showFilters && (
        <Card className="glass-card">
          <CardContent className="p-4">
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
              {/* Category */}
              <div className="space-y-2">
                <label className="text-sm font-medium">
                  {locale === 'ru' ? 'Категория' : 'Category'}
                </label>
                <Select
                  value={searchParams.category_id || 'all'}
                  onValueChange={handleCategoryChange}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={locale === 'ru' ? 'Все категории' : 'All categories'} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">
                      {locale === 'ru' ? 'Все категории' : 'All categories'}
                    </SelectItem>
                    {categories.map(cat => (
                      <SelectItem key={cat.id} value={cat.id}>
                        {cat.name} ({formatNumber(cat.products_count)})
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Competition */}
              <div className="space-y-2">
                <label className="text-sm font-medium">
                  {locale === 'ru' ? 'Конкуренция' : 'Competition'}
                </label>
                <Select
                  value={searchParams.competition || 'all'}
                  onValueChange={handleCompetitionChange}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={locale === 'ru' ? 'Любая' : 'Any'} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">{locale === 'ru' ? 'Любая' : 'Any'}</SelectItem>
                    <SelectItem value="low">{locale === 'ru' ? 'Низкая (до 5)' : 'Low (up to 5)'}</SelectItem>
                    <SelectItem value="medium">{locale === 'ru' ? 'Средняя (5-15)' : 'Medium (5-15)'}</SelectItem>
                    <SelectItem value="high">{locale === 'ru' ? 'Высокая (15+)' : 'High (15+)'}</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {/* Min Price */}
              <div className="space-y-2">
                <label className="text-sm font-medium">
                  {locale === 'ru' ? 'Цена от' : 'Price from'}
                </label>
                <Input
                  type="number"
                  placeholder="0 ₸"
                  value={searchParams.min_price || ''}
                  onChange={(e) => handlePriceRangeChange('min_price', e.target.value)}
                />
              </div>

              {/* Max Price */}
              <div className="space-y-2">
                <label className="text-sm font-medium">
                  {locale === 'ru' ? 'Цена до' : 'Price to'}
                </label>
                <Input
                  type="number"
                  placeholder="∞ ₸"
                  value={searchParams.max_price || ''}
                  onChange={(e) => handlePriceRangeChange('max_price', e.target.value)}
                />
              </div>

              {/* Sort */}
              <div className="space-y-2">
                <label className="text-sm font-medium">
                  {locale === 'ru' ? 'Сортировка' : 'Sort by'}
                </label>
                <Select
                  value={searchParams.sort_by || 'revenue'}
                  onValueChange={handleSortChange}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="revenue">{locale === 'ru' ? 'По выручке' : 'By revenue'}</SelectItem>
                    <SelectItem value="sales">{locale === 'ru' ? 'По продажам' : 'By sales'}</SelectItem>
                    <SelectItem value="reviews">{locale === 'ru' ? 'По отзывам' : 'By reviews'}</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Products Table */}
      <Card className="glass-card">
        <CardHeader className="pb-3">
          <CardTitle className="text-lg flex items-center justify-between">
            <span>{locale === 'ru' ? 'Товары' : 'Products'}</span>
            <span className="text-sm font-normal text-muted-foreground">
              {locale === 'ru' ? `Найдено: ${totalProducts}` : `Found: ${totalProducts}`}
            </span>
          </CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {isLoading ? (
            <div className="p-4 space-y-4">
              {[...Array(5)].map((_, i) => (
                <div key={i} className="flex items-center gap-4">
                  <Skeleton className="h-16 w-16 rounded-lg" />
                  <div className="flex-1 space-y-2">
                    <Skeleton className="h-4 w-3/4" />
                    <Skeleton className="h-3 w-1/2" />
                  </div>
                  <Skeleton className="h-8 w-24" />
                </div>
              ))}
            </div>
          ) : hasError ? (
            <div className="p-8 text-center">
              <p className="text-muted-foreground mb-4">
                {locale === 'ru' ? 'Ошибка загрузки данных' : 'Error loading data'}
              </p>
              <Button onClick={() => refetch()} variant="outline" size="sm">
                <RefreshCw className="h-4 w-4 mr-2" />
                {locale === 'ru' ? 'Повторить' : 'Retry'}
              </Button>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="w-[300px]">{locale === 'ru' ? 'Товар' : 'Product'}</TableHead>
                    <TableHead>{locale === 'ru' ? 'Цена' : 'Price'}</TableHead>
                    <TableHead>{locale === 'ru' ? 'Продажи/мес' : 'Sales/mo'}</TableHead>
                    <TableHead>{locale === 'ru' ? 'Выручка/мес' : 'Revenue/mo'}</TableHead>
                    <TableHead>{locale === 'ru' ? 'Отзывы' : 'Reviews'}</TableHead>
                    <TableHead>{locale === 'ru' ? 'Конкуренция' : 'Competition'}</TableHead>
                    <TableHead className="w-[100px]"></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {products.map((product) => (
                    <TableRow
                      key={product.id}
                      className="cursor-pointer hover:bg-muted/50"
                      onClick={() => setSelectedProduct(product)}
                    >
                      <TableCell>
                        <div className="flex items-center gap-3">
                          <div className="h-12 w-12 rounded-lg bg-muted flex items-center justify-center overflow-hidden">
                            {product.image_url ? (
                              <img
                                src={product.image_url}
                                alt={product.name}
                                className="h-full w-full object-cover"
                              />
                            ) : (
                              <Package className="h-6 w-6 text-muted-foreground" />
                            )}
                          </div>
                          <div className="min-w-0">
                            <p className="font-medium truncate max-w-[200px]">{product.name}</p>
                            <p className="text-sm text-muted-foreground">{product.category_name}</p>
                          </div>
                        </div>
                      </TableCell>
                      <TableCell className="font-medium">
                        {formatPrice(product.price)}
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          <TrendingUp className="h-4 w-4 text-green-500" />
                          <span className="font-medium">{product.estimated_sales}</span>
                        </div>
                      </TableCell>
                      <TableCell>
                        <span className="font-medium text-green-500">
                          {formatPrice(product.estimated_revenue)}
                        </span>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          <Star className="h-4 w-4 text-yellow-500 fill-yellow-500" />
                          <span>{product.rating}</span>
                          <span className="text-muted-foreground">({product.review_count})</span>
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          {getCompetitionBadge(product.merchant_count)}
                          <span className="text-sm text-muted-foreground">
                            {product.merchant_count}
                          </span>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={(e) => {
                            e.stopPropagation()
                            window.open(product.kaspi_url, '_blank')
                          }}
                        >
                          <ExternalLink className="h-4 w-4" />
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Pagination */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          {locale === 'ru'
            ? `Страница ${searchParams.page} из ${Math.ceil(totalProducts / (searchParams.limit || 20))}`
            : `Page ${searchParams.page} of ${Math.ceil(totalProducts / (searchParams.limit || 20))}`}
        </p>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={searchParams.page === 1}
            onClick={() => setSearchParams(prev => ({ ...prev, page: (prev.page || 1) - 1 }))}
          >
            <ChevronLeft className="h-4 w-4" />
          </Button>
          <Button
            variant="outline"
            size="sm"
            disabled={searchParams.page! >= Math.ceil(totalProducts / (searchParams.limit || 20))}
            onClick={() => setSearchParams(prev => ({ ...prev, page: (prev.page || 1) + 1 }))}
          >
            <ChevronRight className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Product Detail Dialog */}
      <Dialog open={!!selectedProduct} onOpenChange={() => setSelectedProduct(null)}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle className="flex items-start gap-4">
              {selectedProduct?.image_url && (
                <img
                  src={selectedProduct.image_url}
                  alt={selectedProduct.name}
                  className="h-20 w-20 rounded-lg object-cover"
                />
              )}
              <div className="min-w-0 flex-1">
                <p className="text-lg font-semibold">{selectedProduct?.name}</p>
                <p className="text-sm text-muted-foreground">{selectedProduct?.category_name}</p>
              </div>
            </DialogTitle>
          </DialogHeader>

          {selectedProduct && (
            <div className="space-y-6">
              {/* Stats Grid */}
              <div className="grid grid-cols-2 gap-4">
                <Card className="glass-card">
                  <CardContent className="p-4">
                    <div className="flex items-center gap-2 text-muted-foreground mb-1">
                      <DollarSign className="h-4 w-4" />
                      <span className="text-sm">{locale === 'ru' ? 'Цена' : 'Price'}</span>
                    </div>
                    <p className="text-xl font-semibold">{formatPrice(selectedProduct.price)}</p>
                  </CardContent>
                </Card>

                <Card className="glass-card">
                  <CardContent className="p-4">
                    <div className="flex items-center gap-2 text-muted-foreground mb-1">
                      <TrendingUp className="h-4 w-4" />
                      <span className="text-sm">{locale === 'ru' ? 'Продажи/мес' : 'Sales/mo'}</span>
                    </div>
                    <p className="text-xl font-semibold">{selectedProduct.estimated_sales}</p>
                  </CardContent>
                </Card>

                <Card className="glass-card">
                  <CardContent className="p-4">
                    <div className="flex items-center gap-2 text-muted-foreground mb-1">
                      <BarChart3 className="h-4 w-4" />
                      <span className="text-sm">{locale === 'ru' ? 'Выручка/мес' : 'Revenue/mo'}</span>
                    </div>
                    <p className="text-xl font-semibold text-green-500">
                      {formatPrice(selectedProduct.estimated_revenue)}
                    </p>
                  </CardContent>
                </Card>

                <Card className="glass-card">
                  <CardContent className="p-4">
                    <div className="flex items-center gap-2 text-muted-foreground mb-1">
                      <Users className="h-4 w-4" />
                      <span className="text-sm">{locale === 'ru' ? 'Продавцов' : 'Sellers'}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <p className="text-xl font-semibold">{selectedProduct.merchant_count}</p>
                      {getCompetitionBadge(selectedProduct.merchant_count)}
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Reviews */}
              <div className="flex items-center gap-4 p-4 rounded-lg bg-muted/50">
                <div className="flex items-center gap-1">
                  <Star className="h-5 w-5 text-yellow-500 fill-yellow-500" />
                  <span className="text-lg font-semibold">{selectedProduct.rating}</span>
                </div>
                <div className="text-muted-foreground">
                  {selectedProduct.review_count} {locale === 'ru' ? 'отзывов' : 'reviews'}
                </div>
              </div>

              {/* Actions */}
              <div className="flex gap-3">
                <Button
                  className="flex-1"
                  onClick={() => window.open(selectedProduct.kaspi_url, '_blank')}
                >
                  <ExternalLink className="h-4 w-4 mr-2" />
                  {locale === 'ru' ? 'Открыть на Kaspi' : 'Open on Kaspi'}
                </Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
