'use client'

import { useState } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { useAdminSubscriptions } from '@/hooks/api/use-admin'
import {
  Repeat,
  Search,
  Loader2,
  Calendar,
  MoreHorizontal,
} from 'lucide-react'

export default function AdminSubscriptionsPage() {
  const [search, setSearch] = useState('')
  const { data: subscriptions, isLoading } = useAdminSubscriptions()

  // Mock data for demo
  const mockSubscriptions = subscriptions || [
    { id: '1', user_id: '1', user_email: 'seller1@mail.kz', plan: 'Комбо 500', status: 'active', current_period_end: '2024-02-25', created_at: '2024-01-25' },
    { id: '2', user_id: '2', user_email: 'shop2@gmail.com', plan: 'Бот 1000', status: 'active', current_period_end: '2024-02-20', created_at: '2024-01-20' },
    { id: '3', user_id: '4', user_email: 'store3@inbox.kz', plan: 'Бот 500', status: 'canceled', current_period_end: '2024-01-30', created_at: '2024-01-10' },
    { id: '4', user_id: '6', user_email: 'trial@test.kz', plan: 'Комбо 1000', status: 'trialing', current_period_end: '2024-02-01', created_at: '2024-01-25' },
    { id: '5', user_id: '7', user_email: 'expired@mail.kz', plan: 'Бот 500', status: 'past_due', current_period_end: '2024-01-15', created_at: '2024-01-01' },
  ]

  const filteredSubscriptions = mockSubscriptions.filter((sub) =>
    sub.user_email.toLowerCase().includes(search.toLowerCase())
  )

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'active':
        return <Badge className="bg-success/10 text-success border-0">Активна</Badge>
      case 'canceled':
        return <Badge variant="secondary">Отменена</Badge>
      case 'trialing':
        return <Badge className="bg-warning/10 text-warning border-0">Пробный</Badge>
      case 'past_due':
        return <Badge variant="destructive">Просрочена</Badge>
      default:
        return <Badge variant="secondary">{status}</Badge>
    }
  }

  const activeCount = mockSubscriptions.filter((s) => s.status === 'active').length

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <Repeat className="h-6 w-6" />
            Подписки
          </h1>
          <p className="text-muted-foreground">Управление подписками пользователей</p>
        </div>
        <div className="text-right">
          <p className="text-sm text-muted-foreground">Активных подписок</p>
          <p className="text-2xl font-bold">{activeCount}</p>
        </div>
      </div>

      {/* Search */}
      <div className="relative max-w-md">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
        <Input
          placeholder="Поиск по email..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="pl-10"
        />
      </div>

      {/* Subscriptions table */}
      <Card className="glass-card">
        <CardContent className="p-0">
          {isLoading ? (
            <div className="flex items-center justify-center h-64">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Пользователь
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      План
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Статус
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Действует до
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Создана
                    </th>
                    <th className="text-right p-4 text-sm font-medium text-muted-foreground">
                      Действия
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredSubscriptions.map((sub) => (
                    <tr
                      key={sub.id}
                      className="border-b border-border last:border-0 hover:bg-muted/50"
                    >
                      <td className="p-4">
                        <span className="text-sm">{sub.user_email}</span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm font-medium">{sub.plan}</span>
                      </td>
                      <td className="p-4">{getStatusBadge(sub.status)}</td>
                      <td className="p-4">
                        <span className="text-sm text-muted-foreground flex items-center gap-1">
                          <Calendar className="h-3 w-3" />
                          {new Date(sub.current_period_end).toLocaleDateString('ru-RU')}
                        </span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm text-muted-foreground">
                          {new Date(sub.created_at).toLocaleDateString('ru-RU')}
                        </span>
                      </td>
                      <td className="p-4 text-right">
                        <DropdownMenu>
                          <DropdownMenuTrigger asChild>
                            <Button variant="ghost" size="icon">
                              <MoreHorizontal className="h-4 w-4" />
                            </Button>
                          </DropdownMenuTrigger>
                          <DropdownMenuContent align="end">
                            <DropdownMenuItem>Продлить</DropdownMenuItem>
                            <DropdownMenuItem>Изменить план</DropdownMenuItem>
                            <DropdownMenuItem>Активировать</DropdownMenuItem>
                            <DropdownMenuItem className="text-destructive">
                              Отменить
                            </DropdownMenuItem>
                          </DropdownMenuContent>
                        </DropdownMenu>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
