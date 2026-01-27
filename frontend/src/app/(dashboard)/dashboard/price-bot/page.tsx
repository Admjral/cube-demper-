"use client"

import { useState } from "react"
import { useStore } from "@/store/use-store"
import { useProducts, useDempingSettings, useUpdateProduct, useUpdateDempingSettings, useSyncProducts, useBulkUpdateProducts } from "@/hooks/api/use-products"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { Badge } from "@/components/ui/badge"
import { Switch } from "@/components/ui/switch"
import { Checkbox } from "@/components/ui/checkbox"
import { Skeleton } from "@/components/ui/skeleton"
import { Label } from "@/components/ui/label"
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
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog"
import {
  Bot,
  Search,
  Filter,
  Play,
  Pause,
  Settings2,
  TrendingUp,
  TrendingDown,
  MoreHorizontal,
  Store,
  Loader2,
  AlertCircle,
  DollarSign,
  RefreshCw,
  CheckSquare,
  Square,
} from "lucide-react"
import Link from "next/link"
import { toast } from "sonner"
import type { KaspiProduct } from "@/types/api"
import { ProductDempingDialog } from "@/components/shared/product-demping-dialog"
import { formatPrice } from "@/lib/utils"

function ProductsLoading() {
  return (
    <div className="space-y-4">
      {[1, 2, 3, 4].map((i) => (
        <Card key={i} className="glass-card">
          <CardContent className="p-4">
            <div className="flex items-start justify-between gap-4">
              <div className="flex-1 space-y-2">
                <Skeleton className="h-5 w-3/4" />
                <Skeleton className="h-4 w-1/4" />
              </div>
              <Skeleton className="h-6 w-12 rounded-full" />
            </div>
            <div className="mt-4 grid grid-cols-2 gap-4">
              <Skeleton className="h-12" />
              <Skeleton className="h-12" />
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}

function NoStoreSelected({ locale }: { locale: string }) {
  return (
    <Card className="glass-card">
      <CardContent className="p-8 text-center">
        <Store className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-lg font-semibold mb-2">
          {locale === "ru" ? "Выберите магазин" : "Select a store"}
        </h3>
        <p className="text-muted-foreground mb-4">
          {locale === "ru"
            ? "Для управления демпингом выберите магазин или добавьте новый"
            : "Select a store or add a new one to manage price bot"}
        </p>
        <Link href="/dashboard/integrations">
          <Button>
            {locale === "ru" ? "Добавить магазин" : "Add store"}
          </Button>
        </Link>
      </CardContent>
    </Card>
  )
}

function NoProducts({ locale }: { locale: string }) {
  return (
    <Card className="glass-card">
      <CardContent className="p-8 text-center">
        <Bot className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-lg font-semibold mb-2">
          {locale === "ru" ? "Товары не найдены" : "No products found"}
        </h3>
        <p className="text-muted-foreground mb-4">
          {locale === "ru"
            ? "Синхронизируйте магазин для загрузки товаров"
            : "Sync your store to load products"}
        </p>
        <Link href="/dashboard/integrations">
          <Button>
            {locale === "ru" ? "Синхронизировать" : "Sync store"}
          </Button>
        </Link>
      </CardContent>
    </Card>
  )
}

export default function PriceBotPage() {
  const { locale, selectedStore } = useStore()
  const [searchQuery, setSearchQuery] = useState("")
  const [statusFilter, setStatusFilter] = useState<"all" | "active" | "inactive">("all")
  const [showSettingsDialog, setShowSettingsDialog] = useState(false)
  const [editingProduct, setEditingProduct] = useState<KaspiProduct | null>(null)
  const [selectedProductId, setSelectedProductId] = useState<string | null>(null)
  const [excludedMerchantsInput, setExcludedMerchantsInput] = useState("")
  const [selectedProductIds, setSelectedProductIds] = useState<Set<string>>(new Set())

  const {
    data: products,
    isLoading: productsLoading,
    error: productsError,
  } = useProducts(selectedStore?.id, {
    bot_active: statusFilter === "all" ? undefined : statusFilter === "active",
    search: searchQuery || undefined,
  })

  const {
    data: dempingSettings,
    isLoading: settingsLoading,
  } = useDempingSettings(selectedStore?.id)

  const updateProduct = useUpdateProduct()
  const updateDempingSettings = useUpdateDempingSettings()
  const syncProducts = useSyncProducts()
  const bulkUpdate = useBulkUpdateProducts()

  const handleSyncProducts = async () => {
    if (!selectedStore?.id) return

    try {
      await syncProducts.mutateAsync(selectedStore.id)
      toast.success(
        locale === "ru"
          ? "Синхронизация запущена. Товары обновятся через несколько секунд."
          : "Sync started. Products will update in a few seconds."
      )
    } catch (error) {
      toast.error(locale === "ru" ? "Ошибка синхронизации" : "Sync failed")
    }
  }

  const toggleDemping = async (product: KaspiProduct) => {
    try {
      await updateProduct.mutateAsync({
        productId: product.id,
        data: { bot_active: !product.bot_active },
      })
      toast.success(
        locale === "ru"
          ? product.bot_active
            ? "Демпинг отключён"
            : "Демпинг включён"
          : product.bot_active
          ? "Demping disabled"
          : "Demping enabled"
      )
    } catch (error) {
      toast.error(locale === "ru" ? "Ошибка обновления" : "Update failed")
    }
  }

  const saveGlobalSettings = async () => {
    if (!selectedStore?.id || !dempingSettings) return

    // Parse excluded merchant IDs from input (comma or newline separated)
    const excludedIds = excludedMerchantsInput
      .split(/[,\n]/)
      .map(id => id.trim())
      .filter(id => id.length > 0)

    try {
      await updateDempingSettings.mutateAsync({
        storeId: selectedStore.id,
        data: {
          is_enabled: dempingSettings.is_enabled,
          price_step: dempingSettings.price_step,
          min_margin_percent: dempingSettings.min_margin_percent,
          check_interval_minutes: dempingSettings.check_interval_minutes,
          excluded_merchant_ids: excludedIds,
        },
      })
      toast.success(locale === "ru" ? "Настройки сохранены" : "Settings saved")
      setShowSettingsDialog(false)
    } catch (error) {
      toast.error(locale === "ru" ? "Ошибка сохранения" : "Save failed")
    }
  }

  // Initialize excluded merchants input when settings load
  const handleOpenSettingsDialog = () => {
    if (dempingSettings?.excluded_merchant_ids) {
      setExcludedMerchantsInput(dempingSettings.excluded_merchant_ids.join(", "))
    } else {
      setExcludedMerchantsInput("")
    }
    setShowSettingsDialog(true)
  }

  const activeBotsCount = products?.filter((p) => p.bot_active).length || 0
  const totalProducts = products?.length || 0

  // Selection handlers
  const isAllSelected = products && products.length > 0 && selectedProductIds.size === products.length
  const isSomeSelected = selectedProductIds.size > 0 && !isAllSelected

  const toggleSelectAll = () => {
    if (isAllSelected) {
      setSelectedProductIds(new Set())
    } else if (products) {
      setSelectedProductIds(new Set(products.map(p => p.id)))
    }
  }

  const toggleSelectProduct = (productId: string) => {
    const newSelected = new Set(selectedProductIds)
    if (newSelected.has(productId)) {
      newSelected.delete(productId)
    } else {
      newSelected.add(productId)
    }
    setSelectedProductIds(newSelected)
  }

  const handleBulkToggleDemping = async (enable: boolean) => {
    if (selectedProductIds.size === 0) return

    try {
      await bulkUpdate.mutateAsync({
        product_ids: Array.from(selectedProductIds),
        bot_active: enable,
      })
      toast.success(
        locale === "ru"
          ? `Демпинг ${enable ? "включён" : "отключён"} для ${selectedProductIds.size} товаров`
          : `Demping ${enable ? "enabled" : "disabled"} for ${selectedProductIds.size} products`
      )
      setSelectedProductIds(new Set())
    } catch (error) {
      toast.error(locale === "ru" ? "Ошибка обновления" : "Update failed")
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">
            {locale === "ru" ? "Демпинг цен" : "Price Bot"}
          </h1>
          <p className="text-muted-foreground">
            {locale === "ru"
              ? "Автоматическое управление ценами"
              : "Automatic price management"}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="secondary" className="text-sm">
            <Bot className="h-4 w-4 mr-1" />
            {activeBotsCount} {locale === "ru" ? "активных" : "active"}
          </Badge>
          <Button
            variant="outline"
            onClick={handleSyncProducts}
            disabled={!selectedStore || syncProducts.isPending}
          >
            {syncProducts.isPending ? (
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4 mr-2" />
            )}
            {locale === "ru" ? "Обновить товары" : "Sync products"}
          </Button>
          <Button onClick={handleOpenSettingsDialog} disabled={!selectedStore}>
            <Settings2 className="h-4 w-4 mr-2" />
            {locale === "ru" ? "Настройки" : "Settings"}
          </Button>
        </div>
      </div>

      {/* No store selected */}
      {!selectedStore && <NoStoreSelected locale={locale} />}

      {/* Content when store is selected */}
      {selectedStore && (
        <>
          {/* Stats cards */}
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <Card className="glass-card">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <p className="text-sm text-muted-foreground">
                    {locale === "ru" ? "Всего товаров" : "Total products"}
                  </p>
                  <Bot className="h-4 w-4 text-muted-foreground" />
                </div>
                <p className="text-2xl font-semibold mt-2">
                  {productsLoading ? <Skeleton className="h-8 w-12" /> : totalProducts}
                </p>
              </CardContent>
            </Card>
            <Card className="glass-card">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <p className="text-sm text-muted-foreground">
                    {locale === "ru" ? "Демпинг включён" : "Demping active"}
                  </p>
                  <Play className="h-4 w-4 text-green-500" />
                </div>
                <p className="text-2xl font-semibold mt-2">
                  {productsLoading ? <Skeleton className="h-8 w-12" /> : activeBotsCount}
                </p>
              </CardContent>
            </Card>
            <Card className="glass-card">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <p className="text-sm text-muted-foreground">
                    {locale === "ru" ? "Шаг цены" : "Price step"}
                  </p>
                  <DollarSign className="h-4 w-4 text-muted-foreground" />
                </div>
                <p className="text-2xl font-semibold mt-2">
                  {settingsLoading ? (
                    <Skeleton className="h-8 w-20" />
                  ) : (
                    formatPrice(dempingSettings?.price_step)
                  )}
                </p>
              </CardContent>
            </Card>
            <Card className="glass-card">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <p className="text-sm text-muted-foreground">
                    {locale === "ru" ? "Мин. маржа" : "Min margin"}
                  </p>
                  <TrendingDown className="h-4 w-4 text-muted-foreground" />
                </div>
                <p className="text-2xl font-semibold mt-2">
                  {settingsLoading ? (
                    <Skeleton className="h-8 w-16" />
                  ) : (
                    `${dempingSettings?.min_margin_percent || 0}%`
                  )}
                </p>
              </CardContent>
            </Card>
          </div>

          {/* Filters and search */}
          <Card className="glass-card">
            <CardContent className="p-4">
              <div className="flex flex-col sm:flex-row gap-4">
                <div className="relative flex-1">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                  <Input
                    placeholder={locale === "ru" ? "Поиск товаров..." : "Search products..."}
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="pl-10"
                  />
                </div>
                <Select
                  value={statusFilter}
                  onValueChange={(v) => setStatusFilter(v as "all" | "active" | "inactive")}
                >
                  <SelectTrigger className="w-full sm:w-[180px]">
                    <SelectValue placeholder={locale === "ru" ? "Статус" : "Status"} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">
                      {locale === "ru" ? "Все" : "All"}
                    </SelectItem>
                    <SelectItem value="active">
                      {locale === "ru" ? "Демпинг вкл." : "Demping on"}
                    </SelectItem>
                    <SelectItem value="inactive">
                      {locale === "ru" ? "Демпинг выкл." : "Demping off"}
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {/* Bulk actions */}
              {selectedProductIds.size > 0 && (
                <div className="flex items-center gap-2 mt-4 pt-4 border-t">
                  <span className="text-sm text-muted-foreground">
                    {locale === "ru"
                      ? `Выбрано: ${selectedProductIds.size}`
                      : `Selected: ${selectedProductIds.size}`}
                  </span>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleBulkToggleDemping(true)}
                    disabled={bulkUpdate.isPending}
                  >
                    {bulkUpdate.isPending ? (
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    ) : (
                      <Play className="h-4 w-4 mr-2" />
                    )}
                    {locale === "ru" ? "Включить демпинг" : "Enable demping"}
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleBulkToggleDemping(false)}
                    disabled={bulkUpdate.isPending}
                  >
                    {bulkUpdate.isPending ? (
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    ) : (
                      <Pause className="h-4 w-4 mr-2" />
                    )}
                    {locale === "ru" ? "Отключить демпинг" : "Disable demping"}
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setSelectedProductIds(new Set())}
                  >
                    {locale === "ru" ? "Сбросить" : "Clear"}
                  </Button>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Loading state */}
          {productsLoading && <ProductsLoading />}

          {/* Error state */}
          {productsError && (
            <Card className="glass-card border-destructive/50">
              <CardContent className="p-6 flex items-center gap-4">
                <AlertCircle className="h-8 w-8 text-destructive" />
                <div>
                  <h3 className="font-semibold">
                    {locale === "ru" ? "Ошибка загрузки" : "Loading error"}
                  </h3>
                  <p className="text-sm text-muted-foreground">
                    {locale === "ru"
                      ? "Не удалось загрузить товары"
                      : "Failed to load products"}
                  </p>
                </div>
              </CardContent>
            </Card>
          )}

          {/* No products */}
          {!productsLoading && !productsError && products?.length === 0 && (
            <NoProducts locale={locale} />
          )}

          {/* Products list - Cards on mobile, Table on desktop */}
          {!productsLoading && !productsError && products && products.length > 0 && (
            <div className="space-y-4">
              {/* Mobile view - Cards */}
              <div className="lg:hidden space-y-4">
                {products.map((product) => (
                  <Card
                    key={product.id}
                    className="glass-card cursor-pointer hover:border-primary transition-colors"
                    onClick={() => setSelectedProductId(product.id)}
                  >
                    <CardContent className="p-4">
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1 min-w-0">
                          <h3 className="font-medium truncate">{product.name}</h3>
                          <p className="text-sm text-muted-foreground">{product.kaspi_sku}</p>
                        </div>
                        <Switch
                          checked={product.bot_active}
                          onCheckedChange={() => toggleDemping(product)}
                          disabled={updateProduct.isPending}
                        />
                      </div>
                      <div className="mt-4 grid grid-cols-2 gap-4">
                        <div>
                          <p className="text-xs text-muted-foreground">
                            {locale === "ru" ? "Текущая цена" : "Current price"}
                          </p>
                          <p className="font-semibold">{formatPrice(product.price)}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">
                            {locale === "ru" ? "Мин. прибыль" : "Min profit"}
                          </p>
                          <p className="text-sm">{formatPrice(product.min_profit)}</p>
                        </div>
                      </div>
                      <div className="mt-4 flex items-center justify-between">
                        <Badge variant={product.bot_active ? "default" : "secondary"}>
                          {product.bot_active
                            ? locale === "ru"
                              ? "Демпинг вкл."
                              : "Demping on"
                            : locale === "ru"
                            ? "Демпинг выкл."
                            : "Demping off"}
                        </Badge>
                        <Badge variant="outline">
                          {product.kaspi_sku || "N/A"}
                        </Badge>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>

              {/* Desktop view - Table */}
              <Card className="glass-card hidden lg:block">
                <CardContent className="p-0">
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="border-b border-border">
                          <th className="w-12 p-4">
                            <Checkbox
                              checked={isAllSelected}
                              onCheckedChange={toggleSelectAll}
                              aria-label={locale === "ru" ? "Выбрать все" : "Select all"}
                            />
                          </th>
                          <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                            {locale === "ru" ? "Товар" : "Product"}
                          </th>
                          <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                            {locale === "ru" ? "Цена" : "Price"}
                          </th>
                          <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                            {locale === "ru" ? "Мин. прибыль" : "Min profit"}
                          </th>
                          <th className="text-center p-4 text-sm font-medium text-muted-foreground">
                            {locale === "ru" ? "Артикул" : "SKU"}
                          </th>
                          <th className="text-center p-4 text-sm font-medium text-muted-foreground">
                            {locale === "ru" ? "Демпинг" : "Demping"}
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {products.map((product) => (
                          <tr
                            key={product.id}
                            className={`border-b border-border last:border-0 hover:bg-muted/50 transition-colors cursor-pointer ${
                              selectedProductIds.has(product.id) ? "bg-muted/30" : ""
                            }`}
                          >
                            <td className="p-4" onClick={(e) => e.stopPropagation()}>
                              <Checkbox
                                checked={selectedProductIds.has(product.id)}
                                onCheckedChange={() => toggleSelectProduct(product.id)}
                                aria-label={locale === "ru" ? "Выбрать товар" : "Select product"}
                              />
                            </td>
                            <td className="p-4" onClick={() => setSelectedProductId(product.id)}>
                              <div>
                                <p className="font-medium">{product.name}</p>
                                <p className="text-sm text-muted-foreground">
                                  {product.kaspi_sku}
                                </p>
                              </div>
                            </td>
                            <td className="p-4" onClick={() => setSelectedProductId(product.id)}>
                              <p className="font-semibold">
                                {formatPrice(product.price)}
                              </p>
                            </td>
                            <td className="p-4" onClick={() => setSelectedProductId(product.id)}>
                              <p className="text-sm">
                                {formatPrice(product.min_profit)}
                              </p>
                            </td>
                            <td className="p-4 text-center" onClick={() => setSelectedProductId(product.id)}>
                              <Badge variant="secondary">{product.kaspi_sku || "N/A"}</Badge>
                            </td>
                            <td className="p-4 text-center" onClick={(e) => e.stopPropagation()}>
                              <Switch
                                checked={product.bot_active}
                                onCheckedChange={() => toggleDemping(product)}
                                disabled={updateProduct.isPending}
                              />
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            </div>
          )}
        </>
      )}

      {/* Settings Dialog */}
      <Dialog open={showSettingsDialog} onOpenChange={setShowSettingsDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {locale === "ru" ? "Настройки демпинга" : "Demping Settings"}
            </DialogTitle>
            <DialogDescription>
              {locale === "ru"
                ? "Настройте параметры автоматического демпинга цен"
                : "Configure automatic price demping parameters"}
            </DialogDescription>
          </DialogHeader>

          {dempingSettings && (
            <div className="space-y-4 py-4">
              <div className="flex items-center justify-between">
                <Label htmlFor="demping-enabled">
                  {locale === "ru" ? "Автодемпинг включён" : "Auto-demping enabled"}
                </Label>
                <Switch
                  id="demping-enabled"
                  checked={dempingSettings.is_enabled}
                  onCheckedChange={(checked) => {
                    // This would need a local state to work properly
                  }}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="price-step">
                  {locale === "ru" ? "Шаг снижения цены (₸)" : "Price step (₸)"}
                </Label>
                <Input
                  id="price-step"
                  type="number"
                  defaultValue={dempingSettings.price_step}
                  min={1}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="min-margin">
                  {locale === "ru" ? "Минимальная маржа (%)" : "Minimum margin (%)"}
                </Label>
                <Input
                  id="min-margin"
                  type="number"
                  defaultValue={dempingSettings.min_margin_percent}
                  min={0}
                  max={100}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="check-interval">
                  {locale === "ru"
                    ? "Интервал проверки (минут)"
                    : "Check interval (minutes)"}
                </Label>
                <Input
                  id="check-interval"
                  type="number"
                  defaultValue={dempingSettings.check_interval_minutes}
                  min={5}
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="work-start">
                    {locale === "ru" ? "Начало работы" : "Work start"}
                  </Label>
                  <Input
                    id="work-start"
                    type="time"
                    defaultValue={dempingSettings.work_hours_start}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="work-end">
                    {locale === "ru" ? "Конец работы" : "Work end"}
                  </Label>
                  <Input
                    id="work-end"
                    type="time"
                    defaultValue={dempingSettings.work_hours_end}
                  />
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="excluded-merchants">
                  {locale === "ru" 
                    ? "Исключённые магазины (не конкурировать с ними)" 
                    : "Excluded stores (don't compete with them)"}
                </Label>
                <Textarea
                  id="excluded-merchants"
                  placeholder={locale === "ru" 
                    ? "Введите ID магазинов через запятую или с новой строки" 
                    : "Enter store IDs separated by comma or newline"}
                  value={excludedMerchantsInput}
                  onChange={(e) => setExcludedMerchantsInput(e.target.value)}
                  rows={3}
                />
                <p className="text-xs text-muted-foreground">
                  {locale === "ru"
                    ? "Укажите merchant ID магазинов, с которыми не нужно конкурировать (например, ваши собственные магазины)"
                    : "Enter merchant IDs of stores you don't want to compete with (e.g., your own stores)"}
                </p>
              </div>
            </div>
          )}

          <DialogFooter>
            <Button variant="outline" onClick={() => setShowSettingsDialog(false)}>
              {locale === "ru" ? "Отмена" : "Cancel"}
            </Button>
            <Button
              onClick={saveGlobalSettings}
              disabled={updateDempingSettings.isPending}
            >
              {updateDempingSettings.isPending && (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              )}
              {locale === "ru" ? "Сохранить" : "Save"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Product Demping Details Dialog */}
      <ProductDempingDialog
        productId={selectedProductId}
        open={!!selectedProductId}
        onOpenChange={(open) => !open && setSelectedProductId(null)}
      />
    </div>
  )
}
