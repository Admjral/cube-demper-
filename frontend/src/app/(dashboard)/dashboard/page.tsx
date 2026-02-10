"use client"

import { useStore } from "@/store/use-store"
import { useStoreStats } from "@/hooks/api/use-dashboard"
import { useFeatures, useActivateTrial } from "@/hooks/api/use-features"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Skeleton } from "@/components/ui/skeleton"
import { planConfig, getDaysRemaining, getDaysColor, getDaysText } from "@/lib/features"
import { cn } from "@/lib/utils"
import { useT } from "@/lib/i18n"
import { format } from "date-fns"
import { ru } from "date-fns/locale"
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
  Users,
  BarChart3,
  Zap,
  CreditCard,
  Sparkles,
  Clock,
  Loader2,
} from "lucide-react"
import Link from "next/link"

const quickActions = [
  {
    titleKey: "dashboard.priceBot",
    descKey: "dashboard.priceBotDesc",
    href: "/dashboard/price-bot",
    icon: Bot,
  },
  {
    titleKey: "dashboard.salesAnalytics",
    descKey: "dashboard.salesAnalyticsDesc",
    href: "/dashboard/sales",
    icon: TrendingUp,
  },
  {
    titleKey: "dashboard.whatsapp",
    descKey: "dashboard.whatsappDesc",
    href: "/dashboard/whatsapp",
    icon: Package,
  },
  {
    titleKey: "dashboard.referral",
    descKey: "dashboard.referralDesc",
    href: "/partner",
    icon: Users,
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

function NoStoreSelected() {
  const t = useT()
  return (
    <Card className="glass-card">
      <CardContent className="p-8 text-center">
        <Store className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-lg font-semibold mb-2">
          {t("dashboard.selectStore")}
        </h3>
        <p className="text-muted-foreground mb-4">
          {t("dashboard.selectStoreDesc")}
        </p>
        <Link href="/dashboard/integrations">
          <Button>
            {t("dashboard.addStore")}
          </Button>
        </Link>
      </CardContent>
    </Card>
  )
}

export default function DashboardPage() {
  const { selectedStore } = useStore()
  const { data: stats, isLoading, error } = useStoreStats(selectedStore?.id)
  const { data: features } = useFeatures()
  const activateTrial = useActivateTrial()
  const t = useT()

  // Calculate stats with trends (comparing today vs week average)
  const statsCards = stats ? [
    {
      title: t("dashboard.salesToday"),
      value: formatCurrency(stats.today_revenue),
      change: calculateChange(stats.today_revenue, stats.week_revenue / 7),
      icon: ShoppingCart,
    },
    {
      title: t("dashboard.ordersToday"),
      value: formatNumber(stats.today_orders),
      change: calculateChange(stats.today_orders, stats.week_orders / 7),
      icon: Package,
    },
    {
      title: t("dashboard.avgOrder"),
      value: formatCurrency(stats.avg_order_value),
      change: { value: '', trend: 'up' as const },
      icon: DollarSign,
    },
    {
      title: t("dashboard.activeDemping"),
      value: formatNumber(stats.demping_enabled_count),
      change: { value: `${t("dashboard.of")} ${stats.products_count}`, trend: 'up' as const },
      icon: Bot,
    },
  ] : []

  return (
    <div className="space-y-6">
      {/* Welcome */}
      <div>
        <h1 className="text-2xl font-semibold">
          {t("dashboard.welcome")}
        </h1>
        <p className="text-muted-foreground">
          {t("dashboard.overviewOf")} {selectedStore?.name || t("dashboard.store")}
        </p>
      </div>

      {/* No store selected */}
      {!selectedStore && <NoStoreSelected />}

      {/* Loading state */}
      {selectedStore && isLoading && <StatsLoading />}

      {/* Error state */}
      {selectedStore && error && (
        <Card className="glass-card border-destructive/50">
          <CardContent className="p-6 flex items-center gap-4">
            <AlertCircle className="h-8 w-8 text-destructive" />
            <div>
              <h3 className="font-semibold">
                {t("dashboard.loadingError")}
              </h3>
              <p className="text-sm text-muted-foreground">
                {t("dashboard.loadingErrorDesc")}
              </p>
            </div>
            <Link href="/dashboard/integrations" className="ml-auto">
              <Button variant="outline">
                {t("dashboard.sync")}
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
                    {stat.title}
                  </p>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Subscription card */}
      {features && (
        features.plan_code && features.plan_code !== 'free' ? (
          // Has a paid plan — show subscription info with days counter
          <Card className="glass-card border-primary/20 bg-primary/5">
            <CardContent className="p-6">
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-4">
                <div className="flex items-center gap-3">
                  {(() => {
                    const config = planConfig[features.plan_code || ''] || { icon: Zap, color: 'text-primary' }
                    const Icon = config.icon
                    return (
                      <div className="p-2 rounded-xl bg-muted">
                        <Icon className={cn("h-5 w-5", config.color)} />
                      </div>
                    )
                  })()}
                  <div>
                    <p className="font-semibold text-foreground">
                      {features.plan_name || 'Тариф'}
                    </p>
                    {features.subscription_ends_at && (() => {
                      const days = getDaysRemaining(features.subscription_ends_at)
                      return (
                        <div className="flex items-center gap-2">
                          <p className="text-xs text-muted-foreground">
                            до {format(new Date(features.subscription_ends_at), 'd MMM yyyy', { locale: ru })}
                          </p>
                          {days !== null && (
                            <span className={cn("text-xs font-medium flex items-center gap-0.5", getDaysColor(days))}>
                              <Clock className="h-3 w-3" />
                              {getDaysText(days)}
                            </span>
                          )}
                        </div>
                      )
                    })()}
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  {features.is_trial && (
                    <Badge variant="outline" className="text-xs">{t("dashboard.trialBadge")}</Badge>
                  )}
                  <Badge variant="default" className="text-xs">{t("dashboard.activeBadge")}</Badge>
                </div>
              </div>

              <div className="grid sm:grid-cols-2 gap-4">
                <div className="space-y-1.5">
                  <div className="flex justify-between text-sm">
                    <span className="text-muted-foreground flex items-center gap-1.5">
                      <BarChart3 className="h-3.5 w-3.5" />
                      {t("dashboard.analyticsLabel")}
                    </span>
                    <span className="font-medium">
                      {stats?.products_count ?? 0} / {features.analytics_limit === -1 ? '∞' : features.analytics_limit}
                    </span>
                  </div>
                  {features.analytics_limit !== -1 && (
                    <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                      <div
                        className="h-full bg-primary rounded-full transition-all"
                        style={{ width: `${Math.min(((stats?.products_count ?? 0) / features.analytics_limit) * 100, 100)}%` }}
                      />
                    </div>
                  )}
                </div>
                <div className="space-y-1.5">
                  <div className="flex justify-between text-sm">
                    <span className="text-muted-foreground flex items-center gap-1.5">
                      <TrendingUp className="h-3.5 w-3.5" />
                      {t("dashboard.dempingLabel")}
                    </span>
                    <span className="font-medium">
                      {stats?.demping_enabled_count ?? 0} / {features.demping_limit}
                    </span>
                  </div>
                  <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                    <div
                      className={cn(
                        "h-full rounded-full transition-all",
                        features.demping_limit > 0 && ((stats?.demping_enabled_count ?? 0) / features.demping_limit) >= 0.8
                          ? "bg-yellow-500"
                          : "bg-primary"
                      )}
                      style={{ width: `${features.demping_limit > 0 ? Math.min(((stats?.demping_enabled_count ?? 0) / features.demping_limit) * 100, 100) : 0}%` }}
                    />
                  </div>
                </div>
              </div>

              <div className="mt-3 text-right">
                <Link href="/dashboard/billing">
                  <Button variant="ghost" size="sm" className="text-xs">
                    {t("dashboard.details")} <ArrowRight className="h-3 w-3 ml-1" />
                  </Button>
                </Link>
              </div>
            </CardContent>
          </Card>
        ) : (
          // No real plan — show trial CTA
          <Card className="glass-card border-green-500/30 bg-green-500/5">
            <CardContent className="p-6">
              <div className="flex flex-col sm:flex-row items-center justify-between gap-4">
                <div className="flex items-center gap-3">
                  <div className="p-2.5 rounded-xl bg-green-500/20">
                    <Sparkles className="h-5 w-5 text-green-500" />
                  </div>
                  <div>
                    <p className="font-semibold text-foreground">{t("dashboard.tryFree3Days")}</p>
                    <p className="text-sm text-muted-foreground">
                      {t("dashboard.tryFree3DaysDesc")}
                    </p>
                  </div>
                </div>
                <Button
                  className="bg-green-600 hover:bg-green-700 text-white shrink-0"
                  onClick={() => activateTrial.mutate()}
                  disabled={activateTrial.isPending}
                >
                  {activateTrial.isPending ? (
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  ) : (
                    <Sparkles className="h-4 w-4 mr-2" />
                  )}
                  {t("dashboard.activate")}
                </Button>
              </div>
              {activateTrial.isError && (
                <p className="text-sm text-red-500 mt-2">
                  {(activateTrial.error as any)?.message || t("dashboard.trialError")}
                </p>
              )}
            </CardContent>
          </Card>
        )
      )}

      {/* Monthly summary */}
      {selectedStore && stats && (
        <div className="grid gap-4 sm:grid-cols-3">
          <Card className="glass-card">
            <CardContent className="p-6">
              <p className="text-sm text-muted-foreground mb-1">
                {t("dashboard.thisWeek")}
              </p>
              <p className="text-xl font-semibold">{formatCurrency(stats.week_revenue)}</p>
              <p className="text-sm text-muted-foreground">
                {formatNumber(stats.week_orders)} {t("dashboard.orders")}
              </p>
            </CardContent>
          </Card>
          <Card className="glass-card">
            <CardContent className="p-6">
              <p className="text-sm text-muted-foreground mb-1">
                {t("dashboard.thisMonth")}
              </p>
              <p className="text-xl font-semibold">{formatCurrency(stats.month_revenue)}</p>
              <p className="text-sm text-muted-foreground">
                {formatNumber(stats.month_orders)} {t("dashboard.orders")}
              </p>
            </CardContent>
          </Card>
          <Card className="glass-card">
            <CardContent className="p-6">
              <p className="text-sm text-muted-foreground mb-1">
                {t("dashboard.productsInStore")}
              </p>
              <p className="text-xl font-semibold">{formatNumber(stats.products_count)}</p>
              <p className="text-sm text-muted-foreground">
                {formatNumber(stats.active_products_count)} {t("dashboard.activeProducts")}
              </p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Quick actions */}
      <div>
        <h2 className="text-lg font-semibold mb-4">
          {t("dashboard.quickActions")}
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
                      {t(action.titleKey)}
                    </h3>
                    <p className="text-sm text-muted-foreground mt-1">
                      {t(action.descKey)}
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
          {t("dashboard.lastSync")}{' '}
          {new Date(stats.last_sync).toLocaleString('ru-RU')}
        </p>
      )}
    </div>
  )
}
