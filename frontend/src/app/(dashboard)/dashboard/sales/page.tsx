"use client"

import { useState } from "react"
import { useStore } from "@/store/use-store"
import { useSalesAnalytics, useTopProducts, useSyncOrders } from "@/hooks/api/use-analytics"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Skeleton } from "@/components/ui/skeleton"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  TrendingUp,
  TrendingDown,
  Calendar,
  Download,
  ShoppingCart,
  DollarSign,
  Package,
  BarChart3,
  Store,
  AlertCircle,
  RefreshCw,
  Loader2,
} from "lucide-react"
import Link from "next/link"
import { toast } from "sonner"

function formatPrice(price: number) {
  return new Intl.NumberFormat("ru-RU").format(price) + " ₸"
}

function formatNumber(value: number) {
  return new Intl.NumberFormat("ru-RU").format(value)
}

function StatsLoading() {
  return (
    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
      {[1, 2, 3, 4].map((i) => (
        <Card key={i} className="glass-card">
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <Skeleton className="h-9 w-9 rounded-xl" />
              <Skeleton className="h-4 w-16" />
            </div>
            <div className="mt-4 space-y-2">
              <Skeleton className="h-8 w-24" />
              <Skeleton className="h-4 w-20" />
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}

function ChartLoading() {
  return (
    <Card className="glass-card">
      <CardHeader>
        <Skeleton className="h-6 w-32" />
      </CardHeader>
      <CardContent>
        <Skeleton className="h-[300px] w-full rounded-xl" />
      </CardContent>
    </Card>
  )
}

function TopProductsLoading() {
  return (
    <div className="space-y-4">
      {[1, 2, 3, 4, 5].map((i) => (
        <div
          key={i}
          className="flex items-center justify-between p-4 rounded-xl bg-muted/30"
        >
          <div className="flex items-center gap-4">
            <Skeleton className="h-6 w-6 rounded" />
            <div className="space-y-2">
              <Skeleton className="h-5 w-40" />
              <Skeleton className="h-4 w-24" />
            </div>
          </div>
          <Skeleton className="h-6 w-20 rounded-full" />
        </div>
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
            ? "Для просмотра аналитики выберите магазин или добавьте новый"
            : "Select a store or add a new one to view analytics"}
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

function SimpleBarChart({ data, locale }: { data: { date: string; revenue: number; orders: number }[]; locale: string }) {
  if (!data || data.length === 0) {
    return (
      <div className="h-[300px] flex items-center justify-center bg-muted/30 rounded-xl">
        <p className="text-muted-foreground">
          {locale === "ru" ? "Нет данных" : "No data"}
        </p>
      </div>
    )
  }

  const maxRevenue = Math.max(...data.map((d) => d.revenue))

  return (
    <div className="h-[300px] flex items-end gap-1 p-4">
      {data.map((day, index) => {
        const height = maxRevenue > 0 ? (day.revenue / maxRevenue) * 100 : 0
        const date = new Date(day.date)
        const dayLabel = date.toLocaleDateString(locale === "ru" ? "ru-RU" : "en-US", {
          day: "numeric",
          month: "short",
        })

        return (
          <div
            key={index}
            className="flex-1 flex flex-col items-center gap-2 group"
          >
            <div className="relative w-full flex justify-center">
              <div
                className="w-full max-w-[40px] bg-primary/20 hover:bg-primary/40 rounded-t transition-all cursor-pointer"
                style={{ height: `${Math.max(height, 2)}%`, minHeight: "8px" }}
                title={`${dayLabel}: ${formatPrice(day.revenue)} (${day.orders} ${locale === "ru" ? "заказов" : "orders"})`}
              />
            </div>
            {data.length <= 14 && (
              <span className="text-[10px] text-muted-foreground rotate-45 origin-left">
                {dayLabel}
              </span>
            )}
          </div>
        )
      })}
    </div>
  )
}

export default function SalesPage() {
  const { locale, selectedStore } = useStore()
  const [period, setPeriod] = useState<"7d" | "30d" | "90d">("7d")

  const {
    data: analytics,
    isLoading: analyticsLoading,
    error: analyticsError,
  } = useSalesAnalytics(selectedStore?.id, period)

  const {
    data: topProducts,
    isLoading: topProductsLoading,
  } = useTopProducts(selectedStore?.id)

  const syncOrders = useSyncOrders()

  const handleSyncOrders = async () => {
    if (!selectedStore?.id) return

    try {
      await syncOrders.mutateAsync({
        storeId: selectedStore.id,
        daysBack: period === "7d" ? 7 : period === "30d" ? 30 : 90,
      })
      toast.success(
        locale === "ru"
          ? "Синхронизация заказов запущена. Данные обновятся через несколько секунд."
          : "Orders sync started. Data will update in a few seconds."
      )
    } catch (error) {
      toast.error(locale === "ru" ? "Ошибка синхронизации" : "Sync failed")
    }
  }

  const stats = analytics
    ? [
        {
          title: "Выручка",
          titleEn: "Revenue",
          value: formatPrice(analytics.total_revenue),
          change: "",
          trend: "up" as const,
          icon: DollarSign,
        },
        {
          title: "Заказы",
          titleEn: "Orders",
          value: formatNumber(analytics.total_orders),
          change: "",
          trend: "up" as const,
          icon: ShoppingCart,
        },
        {
          title: "Товаров продано",
          titleEn: "Items sold",
          value: formatNumber(analytics.total_items_sold),
          change: "",
          trend: "up" as const,
          icon: Package,
        },
        {
          title: "Средний чек",
          titleEn: "Avg. order",
          value: formatPrice(analytics.avg_order_value),
          change: "",
          trend: "up" as const,
          icon: BarChart3,
        },
      ]
    : []

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">
            {locale === "ru" ? "Продажи и аналитика" : "Sales & Analytics"}
          </h1>
          <p className="text-muted-foreground">
            {locale === "ru"
              ? "Детальная статистика продаж"
              : "Detailed sales statistics"}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Select
            value={period}
            onValueChange={(v) => setPeriod(v as "7d" | "30d" | "90d")}
          >
            <SelectTrigger className="w-[140px]">
              <Calendar className="h-4 w-4 mr-2" />
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="7d">
                {locale === "ru" ? "7 дней" : "7 days"}
              </SelectItem>
              <SelectItem value="30d">
                {locale === "ru" ? "30 дней" : "30 days"}
              </SelectItem>
              <SelectItem value="90d">
                {locale === "ru" ? "90 дней" : "90 days"}
              </SelectItem>
            </SelectContent>
          </Select>
          <Button
            variant="outline"
            onClick={handleSyncOrders}
            disabled={!selectedStore || syncOrders.isPending}
          >
            {syncOrders.isPending ? (
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4 mr-2" />
            )}
            {locale === "ru" ? "Обновить заказы" : "Sync orders"}
          </Button>
          <Button variant="outline" disabled={!selectedStore}>
            <Download className="h-4 w-4 mr-2" />
            {locale === "ru" ? "Экспорт" : "Export"}
          </Button>
        </div>
      </div>

      {/* No store selected */}
      {!selectedStore && <NoStoreSelected locale={locale} />}

      {/* Content when store is selected */}
      {selectedStore && (
        <>
          {/* Stats */}
          {analyticsLoading && <StatsLoading />}

          {analyticsError && (
            <Card className="glass-card border-destructive/50">
              <CardContent className="p-6 flex items-center gap-4">
                <AlertCircle className="h-8 w-8 text-destructive" />
                <div>
                  <h3 className="font-semibold">
                    {locale === "ru" ? "Ошибка загрузки" : "Loading error"}
                  </h3>
                  <p className="text-sm text-muted-foreground">
                    {locale === "ru"
                      ? "Не удалось загрузить аналитику"
                      : "Failed to load analytics"}
                  </p>
                </div>
              </CardContent>
            </Card>
          )}

          {!analyticsLoading && !analyticsError && analytics && (
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
              {stats.map((stat) => (
                <Card key={stat.title} className="glass-card glass-hover">
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div className="p-2 rounded-xl bg-muted">
                        <stat.icon className="h-5 w-5 text-muted-foreground" />
                      </div>
                      {stat.change && (
                        <div
                          className={`flex items-center gap-1 text-sm ${
                            stat.trend === "up" ? "text-green-600" : "text-red-500"
                          }`}
                        >
                          {stat.trend === "up" ? (
                            <TrendingUp className="h-4 w-4" />
                          ) : (
                            <TrendingDown className="h-4 w-4" />
                          )}
                          {stat.change}
                        </div>
                      )}
                    </div>
                    <div className="mt-4">
                      <p className="text-2xl font-semibold">{stat.value}</p>
                      <p className="text-sm text-muted-foreground">
                        {locale === "ru" ? stat.title : stat.titleEn}
                      </p>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}

          {/* Chart */}
          {analyticsLoading && <ChartLoading />}

          {!analyticsLoading && !analyticsError && analytics && (
            <Card className="glass-card">
              <CardHeader>
                <CardTitle>
                  {locale === "ru" ? "График продаж" : "Sales chart"}
                </CardTitle>
              </CardHeader>
              <CardContent>
                <SimpleBarChart data={analytics.daily_stats} locale={locale} />
              </CardContent>
            </Card>
          )}

          {/* Top products */}
          <Card className="glass-card">
            <CardHeader>
              <CardTitle>
                {locale === "ru" ? "Топ товаров" : "Top products"}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {topProductsLoading && <TopProductsLoading />}

              {!topProductsLoading && topProducts && topProducts.length > 0 && (
                <Tabs defaultValue="sales">
                  <TabsList className="mb-4">
                    <TabsTrigger value="sales">
                      {locale === "ru" ? "По продажам" : "By sales"}
                    </TabsTrigger>
                    <TabsTrigger value="revenue">
                      {locale === "ru" ? "По выручке" : "By revenue"}
                    </TabsTrigger>
                  </TabsList>
                  <TabsContent value="sales" className="space-y-4">
                    {[...topProducts]
                      .sort((a, b) => b.sales_count - a.sales_count)
                      .map((product, index) => (
                        <div
                          key={product.id}
                          className="flex items-center justify-between p-4 rounded-xl bg-muted/30"
                        >
                          <div className="flex items-center gap-4">
                            <span className="text-lg font-semibold text-muted-foreground w-6">
                              {index + 1}
                            </span>
                            <div>
                              <p className="font-medium">{product.name}</p>
                              <p className="text-sm text-muted-foreground">
                                {formatPrice(product.revenue)}
                              </p>
                            </div>
                          </div>
                          <Badge variant="secondary" className="text-base">
                            {product.sales_count} {locale === "ru" ? "шт" : "pcs"}
                          </Badge>
                        </div>
                      ))}
                  </TabsContent>
                  <TabsContent value="revenue" className="space-y-4">
                    {[...topProducts]
                      .sort((a, b) => b.revenue - a.revenue)
                      .map((product, index) => (
                        <div
                          key={product.id}
                          className="flex items-center justify-between p-4 rounded-xl bg-muted/30"
                        >
                          <div className="flex items-center gap-4">
                            <span className="text-lg font-semibold text-muted-foreground w-6">
                              {index + 1}
                            </span>
                            <div>
                              <p className="font-medium">{product.name}</p>
                              <p className="text-sm text-muted-foreground">
                                {product.sales_count} {locale === "ru" ? "шт" : "pcs"}
                              </p>
                            </div>
                          </div>
                          <Badge variant="secondary" className="text-base">
                            {formatPrice(product.revenue)}
                          </Badge>
                        </div>
                      ))}
                  </TabsContent>
                </Tabs>
              )}

              {!topProductsLoading && (!topProducts || topProducts.length === 0) && (
                <div className="py-8 text-center text-muted-foreground">
                  {locale === "ru" ? "Нет данных о продажах" : "No sales data"}
                </div>
              )}
            </CardContent>
          </Card>
        </>
      )}
    </div>
  )
}
