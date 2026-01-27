'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useAdminStats } from '@/hooks/api/use-admin'
import {
  Users,
  CreditCard,
  Store,
  TrendingUp,
  Loader2,
  UserPlus,
  Receipt,
} from 'lucide-react'

export default function AdminDashboardPage() {
  const { data: stats, isLoading } = useAdminStats()

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  // Mock data for demo
  const mockStats = stats || {
    total_users: 156,
    active_subscriptions: 89,
    total_stores: 234,
    total_revenue: 2450000,
    new_users_today: 12,
    new_subscriptions_today: 5,
  }

  const statCards = [
    {
      title: 'Всего пользователей',
      value: mockStats.total_users.toLocaleString(),
      icon: Users,
      change: `+${mockStats.new_users_today} сегодня`,
      changePositive: true,
    },
    {
      title: 'Активных подписок',
      value: mockStats.active_subscriptions.toLocaleString(),
      icon: CreditCard,
      change: `+${mockStats.new_subscriptions_today} сегодня`,
      changePositive: true,
    },
    {
      title: 'Всего магазинов',
      value: mockStats.total_stores.toLocaleString(),
      icon: Store,
      change: 'Подключено к Kaspi',
      changePositive: true,
    },
    {
      title: 'Общая выручка',
      value: `${(mockStats.total_revenue / 1000).toLocaleString()}K ₸`,
      icon: TrendingUp,
      change: 'За всё время',
      changePositive: true,
    },
  ]

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
              <stat.icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stat.value}</div>
              <p className="text-xs text-muted-foreground mt-1">
                {stat.change}
              </p>
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
            <p className="text-muted-foreground text-sm">
              {mockStats.new_users_today} новых регистраций за сегодня
            </p>
            <div className="mt-4 space-y-2">
              {[1, 2, 3].map((i) => (
                <div
                  key={i}
                  className="flex items-center justify-between py-2 border-b border-border last:border-0"
                >
                  <div>
                    <p className="text-sm font-medium">user{i}@example.com</p>
                    <p className="text-xs text-muted-foreground">
                      Зарегистрирован {i} час назад
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <Receipt className="h-5 w-5" />
              Последние платежи
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-muted-foreground text-sm">
              {mockStats.new_subscriptions_today} новых оплат за сегодня
            </p>
            <div className="mt-4 space-y-2">
              {[
                { email: 'seller1@mail.kz', amount: 25000, plan: 'Комбо 500' },
                { email: 'shop2@gmail.com', amount: 20000, plan: 'Бот 1000' },
                { email: 'store3@inbox.kz', amount: 5000, plan: 'Бот 500' },
              ].map((payment, i) => (
                <div
                  key={i}
                  className="flex items-center justify-between py-2 border-b border-border last:border-0"
                >
                  <div>
                    <p className="text-sm font-medium">{payment.email}</p>
                    <p className="text-xs text-muted-foreground">{payment.plan}</p>
                  </div>
                  <span className="text-sm font-medium text-success">
                    +{payment.amount.toLocaleString()}₸
                  </span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
