"use client"

import { useState } from "react"
import Image from "next/image"
import { useT } from "@/lib/i18n"
import { SubscriptionGate } from "@/components/shared/subscription-gate"
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

function getCompetitionBadge(merchantCount: number, t: (key: string) => string) {
  if (merchantCount <= 5) {
    return <Badge className="bg-green-500/20 text-green-500 hover:bg-green-500/30">{t("niche.competitionLow")}</Badge>
  }
  if (merchantCount <= 15) {
    return <Badge className="bg-yellow-500/20 text-yellow-500 hover:bg-yellow-500/30">{t("niche.competitionMedium")}</Badge>
  }
  return <Badge className="bg-red-500/20 text-red-500 hover:bg-red-500/30">{t("niche.competitionHigh")}</Badge>
}

export default function NicheSearchPage() {
  const t = useT()
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
    <SubscriptionGate>
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <Sparkles className="h-6 w-6 text-primary" />
            {t("niche.title")}
          </h1>
          <p className="text-muted-foreground mt-1">
            {t("niche.subtitle")}
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
            {t("common.filters")}
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="gap-2"
            onClick={() => refetch()}
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            {t("common.refresh")}
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
                  {t("niche.products")}
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
                  {t("niche.avgSales")}
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
                  {t("niche.avgRevenue")}
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
                  {t("niche.categories")}
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
                  {t("niche.category")}
                </label>
                <Select
                  value={searchParams.category_id || 'all'}
                  onValueChange={handleCategoryChange}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={t("niche.allCategories")} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">
                      {t("niche.allCategories")}
                    </SelectItem>
                    {categories.map(cat => (
                      <SelectItem key={cat.id} value={cat.id}>
                        {cat.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Competition */}
              <div className="space-y-2">
                <label className="text-sm font-medium">
                  {t("niche.competition")}
                </label>
                <Select
                  value={searchParams.competition || 'all'}
                  onValueChange={handleCompetitionChange}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={t("niche.any")} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">{t("niche.any")}</SelectItem>
                    <SelectItem value="low">{t("niche.low")}</SelectItem>
                    <SelectItem value="medium">{t("niche.medium")}</SelectItem>
                    <SelectItem value="high">{t("niche.high")}</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {/* Min Price */}
              <div className="space-y-2">
                <label className="text-sm font-medium">
                  {t("niche.priceFrom")}
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
                  {t("niche.priceTo")}
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
                  {t("niche.sortBy")}
                </label>
                <Select
                  value={searchParams.sort_by || 'revenue'}
                  onValueChange={handleSortChange}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="revenue">{t("niche.byRevenue")}</SelectItem>
                    <SelectItem value="sales">{t("niche.bySales")}</SelectItem>
                    <SelectItem value="reviews">{t("niche.byReviews")}</SelectItem>
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
            <span>{t("niche.productsTable")}</span>
            <span className="text-sm font-normal text-muted-foreground">
              {`${t("niche.found")} ${totalProducts}`}
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
                {t("niche.errorLoading")}
              </p>
              <Button onClick={() => refetch()} variant="outline" size="sm">
                <RefreshCw className="h-4 w-4 mr-2" />
                {t("niche.retry")}
              </Button>
            </div>
          ) : (
            <>
              {/* Mobile view - Cards */}
              <div className="lg:hidden space-y-3">
                {products.map((product) => (
                  <div
                    key={product.id}
                    className="p-4 rounded-lg border cursor-pointer hover:bg-muted/50 transition-colors"
                    onClick={() => setSelectedProduct(product)}
                  >
                    <div className="flex items-center gap-3">
                      <div className="h-12 w-12 rounded-lg bg-muted flex items-center justify-center overflow-hidden shrink-0 relative">
                        {product.image_url ? (
                          <Image
                            src={product.image_url}
                            alt={product.name}
                            fill
                            className="object-cover"
                            unoptimized
                          />
                        ) : (
                          <Package className="h-6 w-6 text-muted-foreground" />
                        )}
                      </div>
                      <div className="min-w-0 flex-1">
                        <p className="font-medium truncate">{product.name}</p>
                        <p className="text-sm text-muted-foreground">{product.category_name}</p>
                      </div>
                    </div>
                    <div className="mt-3 grid grid-cols-2 gap-3">
                      <div>
                        <p className="text-xs text-muted-foreground">{t("niche.price")}</p>
                        <p className="font-semibold">{formatPrice(product.price)}</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">{t("niche.salesMo")}</p>
                        <p className="font-semibold">{product.estimated_sales}</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">{t("niche.revenueMo")}</p>
                        <p className="font-semibold text-green-500">{formatPrice(product.estimated_revenue)}</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">{t("niche.reviews")}</p>
                        <div className="flex items-center gap-1">
                          <Star className="h-3 w-3 text-yellow-500 fill-yellow-500" />
                          <span className="font-medium">{product.rating}</span>
                          <span className="text-xs text-muted-foreground">({product.review_count})</span>
                        </div>
                      </div>
                    </div>
                    <div className="mt-3 flex items-center justify-between">
                      {getCompetitionBadge(product.merchant_count, t)}
                      <span className="text-sm text-muted-foreground">
                        {product.merchant_count} {t("niche.sellers")}
                      </span>
                    </div>
                  </div>
                ))}
              </div>

              {/* Desktop view - Table */}
              <div className="hidden lg:block overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow className="hover:bg-transparent">
                      <TableHead className="w-[300px]">{t("niche.product")}</TableHead>
                      <TableHead>{t("niche.price")}</TableHead>
                      <TableHead>{t("niche.salesMo")}</TableHead>
                      <TableHead>{t("niche.revenueMo")}</TableHead>
                      <TableHead>{t("niche.reviews")}</TableHead>
                      <TableHead>{t("niche.competition")}</TableHead>
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
                            <div className="h-12 w-12 rounded-lg bg-muted flex items-center justify-center overflow-hidden relative">
                              {product.image_url ? (
                                <Image
                                  src={product.image_url}
                                  alt={product.name}
                                  fill
                                  className="object-cover"
                                  unoptimized
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
                            {getCompetitionBadge(product.merchant_count, t)}
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
            </>
          )}
        </CardContent>
      </Card>

      {/* Pagination */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          {`${t("niche.page")} ${searchParams.page} / ${Math.ceil(totalProducts / (searchParams.limit || 20))}`}
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
                <div className="h-20 w-20 rounded-lg overflow-hidden relative shrink-0">
                  <Image
                    src={selectedProduct.image_url}
                    alt={selectedProduct.name}
                    fill
                    className="object-cover"
                    unoptimized
                  />
                </div>
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
                      <span className="text-sm">{t("niche.price")}</span>
                    </div>
                    <p className="text-xl font-semibold">{formatPrice(selectedProduct.price)}</p>
                  </CardContent>
                </Card>

                <Card className="glass-card">
                  <CardContent className="p-4">
                    <div className="flex items-center gap-2 text-muted-foreground mb-1">
                      <TrendingUp className="h-4 w-4" />
                      <span className="text-sm">{t("niche.salesMo")}</span>
                    </div>
                    <p className="text-xl font-semibold">{selectedProduct.estimated_sales}</p>
                  </CardContent>
                </Card>

                <Card className="glass-card">
                  <CardContent className="p-4">
                    <div className="flex items-center gap-2 text-muted-foreground mb-1">
                      <BarChart3 className="h-4 w-4" />
                      <span className="text-sm">{t("niche.revenueMo")}</span>
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
                      <span className="text-sm">{t("niche.sellers")}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <p className="text-xl font-semibold">{selectedProduct.merchant_count}</p>
                      {getCompetitionBadge(selectedProduct.merchant_count, t)}
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
                  {selectedProduct.review_count} {t("niche.reviews")}
                </div>
              </div>

              {/* Actions */}
              <div className="flex gap-3">
                <Button
                  className="flex-1"
                  onClick={() => window.open(selectedProduct.kaspi_url, '_blank')}
                >
                  <ExternalLink className="h-4 w-4 mr-2" />
                  {t("niche.openOnKaspi")}
                </Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
    </SubscriptionGate>
  )
}
