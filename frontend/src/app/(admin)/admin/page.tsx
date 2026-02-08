'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useAdminStats, useAdminUsers } from '@/hooks/api/use-admin'
import {
  Users,
  CreditCard,
  Package,
  TrendingUp,
  Loader2,
  UserPlus,
  Receipt,
  Zap,
  Ban,
  Wifi,
} from 'lucide-react'
import { format } from 'date-fns'
import { ru } from 'date-fns/locale'

export default function AdminDashboardPage() {
  const { data: stats, isLoading: statsLoading } = useAdminStats()
  const { data: usersData, isLoading: usersLoading } = useAdminUsers(1, 5)

  if (statsLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  const statCards = [
    {
      title: 'Всего пользователей',
      value: (stats?.total_users ?? 0).toLocaleString(),
      icon: Users,
      subtext: `${stats?.blocked_users ?? 0} заблокировано`,
      color: 'text-blue-500',
    },
    {
      title: 'Активных подписок',
      value: (stats?.active_subscriptions ?? 0).toLocaleString(),
      icon: CreditCard,
      subtext: `${stats?.online_users ?? 0} онлайн`,
      color: 'text-green-500',
    },
    {
      title: 'Всего товаров',
      value: (stats?.total_products ?? 0).toLocaleString(),
      icon: Package,
      subtext: `${stats?.active_demping_products ?? 0} с демпингом`,
      color: 'text-purple-500',
    },
    {
      title: 'Общая выручка',
      value: `${((stats?.total_revenue_tiyns ?? 0) / 100).toLocaleString()} ₸`,
      icon: TrendingUp,
      subtext: `${((stats?.monthly_revenue ?? 0) / 100).toLocaleString()} ₸ за месяц`,
      color: 'text-orange-500',
    },
  ]

  const secondaryStats = [
    {
      title: 'Онлайн сейчас',
      value: stats?.online_users ?? 0,
      icon: Wifi,
      color: 'text-green-500',
    },
    {
      title: 'Заблокировано',
      value: stats?.blocked_users ?? 0,
      icon: Ban,
      color: 'text-red-500',
    },
    {
      title: 'Новых подключений',
      value: stats?.new_connections ?? 0,
      icon: UserPlus,
      color: 'text-blue-500',
    },
    {
      title: 'Активный демпинг',
      value: stats?.active_demping_products ?? 0,
      icon: Zap,
      color: 'text-yellow-500',
    },
  ]

  // Get recent users from API
  const recentUsers = usersData?.users?.slice(0, 5) ?? []

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Dashboard</h1>
        <p className="text-muted-foreground">Обзор статистики платформы</p>
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {statCards.map((stat) => (
          <Card key={stat.title} className="glass-card">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                {stat.title}
              </CardTitle>
              <stat.icon className={`h-4 w-4 ${stat.color}`} />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stat.value}</div>
              <p className="text-xs text-muted-foreground mt-1">
                {stat.subtext}
              </p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Secondary stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {secondaryStats.map((stat) => (
          <Card key={stat.title} className="glass-card">
            <CardContent className="p-4 flex items-center gap-3">
              <stat.icon className={`h-8 w-8 ${stat.color}`} />
              <div>
                <p className="text-2xl font-bold">{stat.value}</p>
                <p className="text-xs text-muted-foreground">{stat.title}</p>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Quick actions */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <UserPlus className="h-5 w-5" />
              Новые пользователи
            </CardTitle>
          </CardHeader>
          <CardContent>
            {usersLoading ? (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
              </div>
            ) : recentUsers.length === 0 ? (
              <p className="text-muted-foreground text-sm text-center py-4">
                Нет новых пользователей
              </p>
            ) : (
              <div className="space-y-2">
                {recentUsers.map((user) => (
                  <div
                    key={user.id}
                    className="flex items-center justify-between py-2 border-b border-border last:border-0"
                  >
                    <div>
                      <p className="text-sm font-medium">{user.email}</p>
                      <p className="text-xs text-muted-foreground">
                        {user.created_at
                          ? format(new Date(user.created_at), 'd MMM yyyy, HH:mm', { locale: ru })
                          : '-'}
                      </p>
                    </div>
                    <div className="text-right">
                      <p className="text-xs text-muted-foreground">
                        {user.subscription_plan || 'Без подписки'}
                      </p>
                      <p className="text-xs text-muted-foreground">
                        {user.stores_count} магазин(ов)
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <Receipt className="h-5 w-5" />
              Воркеры демпинга
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <div className="flex items-center justify-between p-3 bg-muted/50 rounded-lg">
                <div>
                  <p className="text-sm font-medium">Статус воркеров</p>
                  <p className="text-xs text-muted-foreground">
                    {stats?.demper_workers_status?.note || 'Мониторинг не настроен'}
                  </p>
                </div>
                <div className="text-right">
                  <p className="text-lg font-bold">
                    {stats?.demper_workers_status?.running_workers ?? 0}/
                    {stats?.demper_workers_status?.expected_workers ?? 0}
                  </p>
                  <p className="text-xs text-muted-foreground">активных</p>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="p-3 bg-muted/50 rounded-lg">
                  <p className="text-xl font-bold text-green-600">
                    {(stats?.active_demping_products ?? 0).toLocaleString()}
                  </p>
                  <p className="text-xs text-muted-foreground">Товаров с демпингом</p>
                </div>
                <div className="p-3 bg-muted/50 rounded-lg">
                  <p className="text-xl font-bold text-blue-600">
                    {stats?.new_connections ?? 0}
                  </p>
                  <p className="text-xs text-muted-foreground">Новых подключений</p>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
