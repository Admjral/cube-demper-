"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Users, UserX, TrendingUp, Store, Wifi } from "lucide-react"
import { Skeleton } from "@/components/ui/skeleton"
import { formatPrice } from "@/lib/utils"
import type { AdminStats } from "@/types/admin"

interface StatsCardsProps {
  stats: AdminStats | undefined
  isLoading: boolean
}

export function StatsCards({ stats, isLoading }: StatsCardsProps) {
  if (isLoading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
        {[...Array(5)].map((_, i) => (
          <Card key={i} className="glass-card">
            <CardHeader>
              <Skeleton className="h-4 w-24" />
            </CardHeader>
            <CardContent>
              <Skeleton className="h-8 w-16" />
            </CardContent>
          </Card>
        ))}
      </div>
    )
  }

  if (!stats) return null

  const cards = [
    {
      title: "Активных пользователей",
      value: stats.total_users - stats.blocked_users,
      icon: Users,
      change: `${stats.blocked_users} заблокировано`,
    },
    {
      title: "Блокированных пользователей",
      value: stats.blocked_users,
      icon: UserX,
      change: `${((stats.blocked_users / stats.total_users) * 100).toFixed(1)}% от всех`,
    },
    {
      title: "Доход (текущий месяц)",
      value: formatPrice(stats.monthly_revenue),
      icon: TrendingUp,
      change: "За текущий месяц",
    },
    {
      title: "Новых подключений",
      value: stats.new_connections,
      icon: Store,
      change: "Магазинов за месяц",
    },
    {
      title: "Онлайн пользователей",
      value: stats.online_users,
      icon: Wifi,
      change: "Активных сейчас",
    },
  ]

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
      {cards.map((card) => (
        <Card key={card.title} className="glass-card">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              {card.title}
            </CardTitle>
            <card.icon className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{card.value}</div>
            <p className="text-xs text-muted-foreground mt-1">{card.change}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
