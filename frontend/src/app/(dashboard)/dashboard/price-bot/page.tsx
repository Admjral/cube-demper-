"use client"

import { useState } from "react"
import { useStore } from "@/store/use-store"
import { useT } from "@/lib/i18n"
import { SubscriptionGate } from "@/components/shared/subscription-gate"
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

function NoStoreSelected() {
  const t = useT()
  return (
    <Card className="glass-card">
      <CardContent className="p-8 text-center">
        <Store className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-lg font-semibold mb-2">
          {t("priceBot.selectStore")}
        </h3>
        <p className="text-muted-foreground mb-4">
          {t("priceBot.selectStoreDesc")}
        </p>
        <Link href="/dashboard/integrations">
          <Button>
            {t("priceBot.addStore")}
          </Button>
        </Link>
      </CardContent>
    </Card>
  )
}

function NoProducts() {
  const t = useT()
  return (
    <Card className="glass-card">
      <CardContent className="p-8 text-center">
        <Bot className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-lg font-semibold mb-2">
          {t("priceBot.noProducts")}
        </h3>
        <p className="text-muted-foreground mb-4">
          {t("priceBot.syncDesc")}
        </p>
        <Link href="/dashboard/integrations">
          <Button>
            {t("priceBot.syncStore")}
          </Button>
        </Link>
      </CardContent>
    </Card>
  )
}

export default function PriceBotPage() {
  const { selectedStore } = useStore()
  const t = useT()
  const [searchQuery, setSearchQuery] = useState("")
  const [statusFilter, setStatusFilter] = useState<"all" | "active" | "inactive">("all")
  const [showSettingsDialog, setShowSettingsDialog] = useState(false)
  const [editingProduct, setEditingProduct] = useState<KaspiProduct | null>(null)
  const [selectedProductId, setSelectedProductId] = useState<string | null>(null)
  const [excludedMerchantsInput, setExcludedMerchantsInput] = useState("")
  const [selectedProductIds, setSelectedProductIds] = useState<Set<string>>(new Set())

  // Form state for demping settings
  const [formIsEnabled, setFormIsEnabled] = useState(true)
  const [formPriceStep, setFormPriceStep] = useState(1)
  const [formMinMargin, setFormMinMargin] = useState(5)
  const [formCheckInterval, setFormCheckInterval] = useState(15)
  const [formWorkHoursStart, setFormWorkHoursStart] = useState("00:00")
  const [formWorkHoursEnd, setFormWorkHoursEnd] = useState("23:59")

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
        t("priceBot.syncProducts")
      )
    } catch (error) {
      toast.error(t("priceBot.loadingError"))
    }
  }

  const toggleDemping = async (product: KaspiProduct) => {
    try {
      await updateProduct.mutateAsync({
        productId: product.id,
        data: { bot_active: !product.bot_active },
      })
      toast.success(
        product.bot_active
          ? t("priceBot.disableDemping")
          : t("priceBot.enableDemping")
      )
    } catch (error) {
      toast.error(t("common.error"))
    }
  }

  const saveGlobalSettings = async () => {
    if (!selectedStore?.id) return

    // Parse excluded merchant IDs from input (comma or newline separated)
    const excludedIds = excludedMerchantsInput
      .split(/[,\n]/)
      .map(id => id.trim())
      .filter(id => id.length > 0)

    try {
      await updateDempingSettings.mutateAsync({
        storeId: selectedStore.id,
        data: {
          is_enabled: formIsEnabled,
          price_step: formPriceStep,
          min_margin_percent: formMinMargin,
          check_interval_minutes: formCheckInterval,
          work_hours_start: formWorkHoursStart,
          work_hours_end: formWorkHoursEnd,
          excluded_merchant_ids: excludedIds,
        },
      })
      toast.success(t("common.save"))
      setShowSettingsDialog(false)
    } catch (error) {
      toast.error(t("common.error"))
    }
  }

  // Initialize form state when settings dialog opens
  const handleOpenSettingsDialog = () => {
    if (dempingSettings) {
      setFormIsEnabled(dempingSettings.is_enabled ?? true)
      setFormPriceStep(dempingSettings.price_step ?? 1)
      setFormMinMargin(dempingSettings.min_margin_percent ?? 5)
      setFormCheckInterval(dempingSettings.check_interval_minutes ?? 15)
      setFormWorkHoursStart(dempingSettings.work_hours_start || "00:00")
      setFormWorkHoursEnd(dempingSettings.work_hours_end || "23:59")
      setExcludedMerchantsInput(dempingSettings.excluded_merchant_ids?.join(", ") || "")
    } else {
      setFormIsEnabled(true)
      setFormPriceStep(1)
      setFormMinMargin(5)
      setFormCheckInterval(15)
      setFormWorkHoursStart("00:00")
      setFormWorkHoursEnd("23:59")
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
        `${enable ? t("priceBot.enableDemping") : t("priceBot.disableDemping")} (${selectedProductIds.size})`
      )
      setSelectedProductIds(new Set())
    } catch (error) {
      toast.error(t("common.error"))
    }
  }

  return (
    <SubscriptionGate>
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">
            {t("priceBot.title")}
          </h1>
          <p className="text-muted-foreground">
            {t("priceBot.subtitle")}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="secondary" className="text-sm">
            <Bot className="h-4 w-4 mr-1" />
            {activeBotsCount} {t("priceBot.active")}
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
            {t("priceBot.syncProducts")}
          </Button>
          <Button onClick={handleOpenSettingsDialog} disabled={!selectedStore}>
            <Settings2 className="h-4 w-4 mr-2" />
            {t("nav.settings")}
          </Button>
        </div>
      </div>

      {/* No store selected */}
      {!selectedStore && <NoStoreSelected />}

      {/* Content when store is selected */}
      {selectedStore && (
        <>
          {/* Stats cards */}
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <Card className="glass-card">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <p className="text-sm text-muted-foreground">
                    {t("priceBot.totalProducts")}
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
                    {t("priceBot.dempingActive")}
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
                    {t("priceBot.priceStep")}
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
                    {t("priceBot.minMargin")}
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
                    placeholder={t("priceBot.searchProducts")}
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
                    <SelectValue placeholder={t("common.status")} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">
                      {t("common.all")}
                    </SelectItem>
                    <SelectItem value="active">
                      {t("priceBot.dempingOn")}
                    </SelectItem>
                    <SelectItem value="inactive">
                      {t("priceBot.dempingOff")}
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {/* Bulk actions */}
              {selectedProductIds.size > 0 && (
                <div className="flex items-center gap-2 mt-4 pt-4 border-t">
                  <span className="text-sm text-muted-foreground">
                    {t("priceBot.selected")} {selectedProductIds.size}
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
                    {t("priceBot.enableDemping")}
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
                    {t("priceBot.disableDemping")}
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setSelectedProductIds(new Set())}
                  >
                    {t("priceBot.clear")}
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
                    {t("priceBot.loadingError")}
                  </h3>
                  <p className="text-sm text-muted-foreground">
                    {t("priceBot.loadingErrorDesc")}
                  </p>
                </div>
              </CardContent>
            </Card>
          )}

          {/* No products */}
          {!productsLoading && !productsError && products?.length === 0 && (
            <NoProducts />
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
                        <div className="flex-1 min-w-0" >
                          <h3 className="font-medium truncate">{product.name}</h3>
                          <p className="text-sm text-muted-foreground">{product.kaspi_sku}</p>
                        </div>
                        <div className="p-2 -m-2" onClick={(e) => e.stopPropagation()}>
                          <Switch
                            checked={product.bot_active}
                            onCheckedChange={() => toggleDemping(product)}
                            disabled={updateProduct.isPending}
                          />
                        </div>
                      </div>
                      <div className="mt-4 grid grid-cols-2 gap-4">
                        <div>
                          <p className="text-xs text-muted-foreground">
                            {t("priceBot.currentPrice")}
                          </p>
                          <p className="font-semibold">{formatPrice(product.price)}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">
                            {t("priceBot.minProfit")}
                          </p>
                          <p className="text-sm">{formatPrice(product.min_profit)}</p>
                        </div>
                      </div>
                      <div className="mt-4 flex items-center justify-between">
                        <Badge variant={product.bot_active ? "default" : "secondary"}>
                          {product.bot_active
                            ? t("priceBot.dempingOn")
                            : t("priceBot.dempingOff")}
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
                              aria-label={t("priceBot.selectAll")}
                            />
                          </th>
                          <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                            {t("priceBot.product")}
                          </th>
                          <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                            {t("priceBot.price")}
                          </th>
                          <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                            {t("priceBot.minProfit")}
                          </th>
                          <th className="text-center p-4 text-sm font-medium text-muted-foreground">
                            {t("priceBot.sku")}
                          </th>
                          <th className="text-center p-4 text-sm font-medium text-muted-foreground">
                            {t("priceBot.demping")}
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
                                aria-label={t("priceBot.selectProduct")}
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
              {t("priceBot.settingsTitle")}
            </DialogTitle>
            <DialogDescription>
              {t("priceBot.settingsDesc")}
            </DialogDescription>
          </DialogHeader>

          {dempingSettings && (
            <div className="space-y-4 py-4">
              <div className="flex items-center justify-between">
                <Label htmlFor="demping-enabled">
                  {t("priceBot.autoEnabled")}
                </Label>
                <Switch
                  id="demping-enabled"
                  checked={formIsEnabled}
                  onCheckedChange={setFormIsEnabled}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="price-step">
                  {t("priceBot.stepLabel")}
                </Label>
                <Input
                  id="price-step"
                  type="number"
                  value={formPriceStep}
                  onChange={(e) => setFormPriceStep(parseInt(e.target.value) || 1)}
                  min={1}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="min-margin">
                  {t("priceBot.marginLabel")}
                </Label>
                <Input
                  id="min-margin"
                  type="number"
                  value={formMinMargin}
                  onChange={(e) => setFormMinMargin(parseInt(e.target.value) || 0)}
                  min={0}
                  max={100}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="check-interval">
                  {t("priceBot.intervalLabel")}
                </Label>
                <Input
                  id="check-interval"
                  type="number"
                  value={formCheckInterval}
                  onChange={(e) => setFormCheckInterval(parseInt(e.target.value) || 5)}
                  min={5}
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="work-start">
                    {t("priceBot.workStart")}
                  </Label>
                  <Input
                    id="work-start"
                    type="time"
                    value={formWorkHoursStart}
                    onChange={(e) => setFormWorkHoursStart(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="work-end">
                    {t("priceBot.workEnd")}
                  </Label>
                  <Input
                    id="work-end"
                    type="time"
                    value={formWorkHoursEnd}
                    onChange={(e) => setFormWorkHoursEnd(e.target.value)}
                  />
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="excluded-merchants">
                  {t("priceBot.excludedStores")}
                </Label>
                <Textarea
                  id="excluded-merchants"
                  placeholder={t("priceBot.excludedStoresPlaceholder")}
                  value={excludedMerchantsInput}
                  onChange={(e) => setExcludedMerchantsInput(e.target.value)}
                  rows={3}
                />
                <p className="text-xs text-muted-foreground">
                  {t("priceBot.excludedStoresHint")}
                </p>
              </div>
            </div>
          )}

          <DialogFooter>
            <Button variant="outline" onClick={() => setShowSettingsDialog(false)}>
              {t("common.cancel2")}
            </Button>
            <Button
              onClick={saveGlobalSettings}
              disabled={updateDempingSettings.isPending}
            >
              {updateDempingSettings.isPending && (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              )}
              {t("common.save")}
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
    </SubscriptionGate>
  )
}
