"use client"

import { useStore } from "@/store/use-store"
import { useStoreStats } from "@/hooks/api/use-dashboard"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import {
  TrendingUp,
  TrendingDown,
  Package,
  ShoppingCart,
  DollarSign,
  Bot,
  ArrowRight,
  AlertCircle,
  Store,
} from "lucide-react"
import Link from "next/link"

const quickActions = [
  {
    title: "Демпинг цен",
    titleEn: "Price Bot",
    description: "Управление ценовыми стратегиями",
    descriptionEn: "Manage price strategies",
    href: "/dashboard/price-bot",
    icon: Bot,
  },
  {
    title: "Аналитика продаж",
    titleEn: "Sales Analytics",
    description: "Детальная статистика",
    descriptionEn: "Detailed statistics",
    href: "/dashboard/sales",
    icon: TrendingUp,
  },
  {
    title: "WhatsApp",
    titleEn: "WhatsApp",
    description: "Автоматизация сообщений",
    descriptionEn: "Message automation",
    href: "/dashboard/whatsapp",
    icon: Package,
  },
]

function formatCurrency(value: number): string {
  return new Intl.NumberFormat('ru-KZ', {
    style: 'currency',
    currency: 'KZT',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value)
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat('ru-KZ').format(value)
}

function calculateChange(current: number, previous: number): { value: string; trend: 'up' | 'down' } {
  if (previous === 0) return { value: '+0%', trend: 'up' }
  const change = ((current - previous) / previous) * 100
  return {
    value: `${change >= 0 ? '+' : ''}${change.toFixed(1)}%`,
    trend: change >= 0 ? 'up' : 'down',
  }
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

function NoStoreSelected({ locale }: { locale: string }) {
  return (
    <Card className="glass-card">
      <CardContent className="p-8 text-center">
        <Store className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-lg font-semibold mb-2">
          {locale === 'ru' ? 'Выберите магазин' : 'Select a store'}
        </h3>
        <p className="text-muted-foreground mb-4">
          {locale === 'ru'
            ? 'Для просмотра статистики выберите магазин в меню или добавьте новый'
            : 'Select a store from the menu or add a new one to view statistics'}
        </p>
        <Link href="/dashboard/integrations">
          <Button>
            {locale === 'ru' ? 'Добавить магазин' : 'Add store'}
          </Button>
        </Link>
      </CardContent>
    </Card>
  )
}

export default function DashboardPage() {
  const { locale, selectedStore } = useStore()
  const { data: stats, isLoading, error } = useStoreStats(selectedStore?.id)

  // Calculate stats with trends (comparing today vs week average)
  const statsCards = stats ? [
    {
      title: "Продажи сегодня",
      titleEn: "Sales today",
      value: formatCurrency(stats.today_revenue),
      change: calculateChange(stats.today_revenue, stats.week_revenue / 7),
      icon: ShoppingCart,
    },
    {
      title: "Заказы сегодня",
      titleEn: "Orders today",
      value: formatNumber(stats.today_orders),
      change: calculateChange(stats.today_orders, stats.week_orders / 7),
      icon: Package,
    },
    {
      title: "Средний чек",
      titleEn: "Avg. order",
      value: formatCurrency(stats.avg_order_value),
      change: { value: '', trend: 'up' as const },
      icon: DollarSign,
    },
    {
      title: "Активный демпинг",
      titleEn: "Active demping",
      value: formatNumber(stats.demping_enabled_count),
      change: { value: `из ${stats.products_count}`, trend: 'up' as const },
      icon: Bot,
    },
  ] : []

  return (
    <div className="space-y-6">
      {/* Welcome */}
      <div>
        <h1 className="text-2xl font-semibold">
          {locale === 'ru' ? 'Добро пожаловать' : 'Welcome'}
        </h1>
        <p className="text-muted-foreground">
          {locale === 'ru'
            ? `Обзор ${selectedStore?.name || 'магазина'}`
            : `Overview of ${selectedStore?.name || 'store'}`}
        </p>
      </div>

      {/* No store selected */}
      {!selectedStore && <NoStoreSelected locale={locale} />}

      {/* Loading state */}
      {selectedStore && isLoading && <StatsLoading />}

      {/* Error state */}
      {selectedStore && error && (
        <Card className="glass-card border-destructive/50">
          <CardContent className="p-6 flex items-center gap-4">
            <AlertCircle className="h-8 w-8 text-destructive" />
            <div>
              <h3 className="font-semibold">
                {locale === 'ru' ? 'Ошибка загрузки' : 'Loading error'}
              </h3>
              <p className="text-sm text-muted-foreground">
                {locale === 'ru'
                  ? 'Не удалось загрузить статистику. Попробуйте синхронизировать магазин.'
                  : 'Failed to load statistics. Try syncing the store.'}
              </p>
            </div>
            <Link href="/dashboard/integrations" className="ml-auto">
              <Button variant="outline">
                {locale === 'ru' ? 'Синхронизировать' : 'Sync'}
              </Button>
            </Link>
          </CardContent>
        </Card>
      )}

      {/* Stats grid */}
      {selectedStore && stats && (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {statsCards.map((stat) => (
            <Card key={stat.title} className="glass-card glass-hover">
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div className="p-2 rounded-xl bg-muted">
                    <stat.icon className="h-5 w-5 text-muted-foreground" />
                  </div>
                  {stat.change.value && (
                    <div
                      className={`flex items-center gap-1 text-sm ${
                        stat.change.trend === 'up' ? 'text-green-600' : 'text-red-500'
                      }`}
                    >
                      {stat.change.trend === 'up' ? (
                        <TrendingUp className="h-4 w-4" />
                      ) : (
                        <TrendingDown className="h-4 w-4" />
                      )}
                      {stat.change.value}
                    </div>
                  )}
                </div>
                <div className="mt-4">
                  <p className="text-2xl font-semibold">{stat.value}</p>
                  <p className="text-sm text-muted-foreground">
                    {locale === 'ru' ? stat.title : stat.titleEn}
                  </p>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Monthly summary */}
      {selectedStore && stats && (
        <div className="grid gap-4 sm:grid-cols-3">
          <Card className="glass-card">
            <CardContent className="p-6">
              <p className="text-sm text-muted-foreground mb-1">
                {locale === 'ru' ? 'За неделю' : 'This week'}
              </p>
              <p className="text-xl font-semibold">{formatCurrency(stats.week_revenue)}</p>
              <p className="text-sm text-muted-foreground">
                {formatNumber(stats.week_orders)} {locale === 'ru' ? 'заказов' : 'orders'}
              </p>
            </CardContent>
          </Card>
          <Card className="glass-card">
            <CardContent className="p-6">
              <p className="text-sm text-muted-foreground mb-1">
                {locale === 'ru' ? 'За месяц' : 'This month'}
              </p>
              <p className="text-xl font-semibold">{formatCurrency(stats.month_revenue)}</p>
              <p className="text-sm text-muted-foreground">
                {formatNumber(stats.month_orders)} {locale === 'ru' ? 'заказов' : 'orders'}
              </p>
            </CardContent>
          </Card>
          <Card className="glass-card">
            <CardContent className="p-6">
              <p className="text-sm text-muted-foreground mb-1">
                {locale === 'ru' ? 'Товаров в магазине' : 'Products in store'}
              </p>
              <p className="text-xl font-semibold">{formatNumber(stats.products_count)}</p>
              <p className="text-sm text-muted-foreground">
                {formatNumber(stats.active_products_count)} {locale === 'ru' ? 'активных' : 'active'}
              </p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Quick actions */}
      <div>
        <h2 className="text-lg font-semibold mb-4">
          {locale === 'ru' ? 'Быстрые действия' : 'Quick actions'}
        </h2>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {quickActions.map((action) => (
            <Link key={action.href} href={action.href}>
              <Card className="glass-card glass-hover cursor-pointer h-full">
                <CardContent className="p-6">
                  <div className="flex items-start justify-between">
                    <div className="p-2 rounded-xl bg-muted">
                      <action.icon className="h-5 w-5 text-muted-foreground" />
                    </div>
                    <ArrowRight className="h-5 w-5 text-muted-foreground" />
                  </div>
                  <div className="mt-4">
                    <h3 className="font-semibold">
                      {locale === 'ru' ? action.title : action.titleEn}
                    </h3>
                    <p className="text-sm text-muted-foreground mt-1">
                      {locale === 'ru' ? action.description : action.descriptionEn}
                    </p>
                  </div>
                </CardContent>
              </Card>
            </Link>
          ))}
        </div>
      </div>

      {/* Last sync info */}
      {selectedStore && stats?.last_sync && (
        <p className="text-xs text-muted-foreground text-center">
          {locale === 'ru' ? 'Последняя синхронизация:' : 'Last sync:'}{' '}
          {new Date(stats.last_sync).toLocaleString(locale === 'ru' ? 'ru-RU' : 'en-US')}
        </p>
      )}
    </div>
  )
}
