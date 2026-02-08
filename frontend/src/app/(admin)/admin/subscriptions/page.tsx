'use client'

import { useState } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { useAdminUsers } from '@/hooks/api/use-admin'
import {
  Repeat,
  Search,
  Loader2,
  Calendar,
  CheckCircle,
  XCircle,
  Clock,
  AlertTriangle,
} from 'lucide-react'
import { format } from 'date-fns'
import { ru } from 'date-fns/locale'

export default function AdminSubscriptionsPage() {
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(1)
  const { data, isLoading } = useAdminUsers(page, 100)

  // Filter users with subscriptions
  const usersWithSubs = (data?.users ?? []).filter(
    (u) => u.subscription_plan || u.subscription_status
  )

  const filteredSubscriptions = usersWithSubs.filter((user) =>
    user.email.toLowerCase().includes(search.toLowerCase())
  )

  const getStatusBadge = (status: string | null) => {
    switch (status) {
      case 'active':
        return (
          <Badge className="bg-green-500/10 text-green-500 border-0">
            <CheckCircle className="h-3 w-3 mr-1" />
            Активна
          </Badge>
        )
      case 'canceled':
        return (
          <Badge variant="secondary">
            <XCircle className="h-3 w-3 mr-1" />
            Отменена
          </Badge>
        )
      case 'trialing':
        return (
          <Badge className="bg-yellow-500/10 text-yellow-600 border-0">
            <Clock className="h-3 w-3 mr-1" />
            Пробный
          </Badge>
        )
      case 'past_due':
        return (
          <Badge variant="destructive">
            <AlertTriangle className="h-3 w-3 mr-1" />
            Просрочена
          </Badge>
        )
      default:
        return <Badge variant="secondary">{status || '—'}</Badge>
    }
  }

  const activeCount = usersWithSubs.filter((u) => u.subscription_status === 'active').length
  const total = data?.total ?? 0

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <Repeat className="h-6 w-6" />
            Подписки
          </h1>
          <p className="text-muted-foreground">
            Пользователей с подписками: {usersWithSubs.length}
          </p>
        </div>
        <div className="text-right">
          <p className="text-sm text-muted-foreground">Активных подписок</p>
          <p className="text-2xl font-bold text-green-600">{activeCount}</p>
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
          ) : filteredSubscriptions.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-64">
              <Repeat className="h-12 w-12 text-muted-foreground mb-4" />
              <p className="text-muted-foreground">Подписки не найдены</p>
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
                      Магазины / Товары
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Регистрация
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredSubscriptions.map((user) => (
                    <tr
                      key={user.id}
                      className="border-b border-border last:border-0 hover:bg-muted/50"
                    >
                      <td className="p-4">
                        <div>
                          <p className="text-sm font-medium">
                            {user.full_name || 'Без имени'}
                          </p>
                          <p className="text-xs text-muted-foreground">{user.email}</p>
                        </div>
                      </td>
                      <td className="p-4">
                        <span className="text-sm font-medium">
                          {user.subscription_plan || '—'}
                        </span>
                      </td>
                      <td className="p-4">{getStatusBadge(user.subscription_status)}</td>
                      <td className="p-4">
                        {user.subscription_end_date ? (
                          <span className="text-sm text-muted-foreground flex items-center gap-1">
                            <Calendar className="h-3 w-3" />
                            {format(new Date(user.subscription_end_date), 'd MMM yyyy', {
                              locale: ru,
                            })}
                          </span>
                        ) : (
                          <span className="text-sm text-muted-foreground">—</span>
                        )}
                      </td>
                      <td className="p-4">
                        <span className="text-sm text-muted-foreground">
                          {user.stores_count} / {user.products_count}
                        </span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm text-muted-foreground">
                          {user.created_at
                            ? format(new Date(user.created_at), 'd MMM yyyy', { locale: ru })
                            : '-'}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Pagination */}
      {total > 100 && (
        <div className="flex items-center justify-center gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={page === 1}
            onClick={() => setPage(page - 1)}
          >
            Назад
          </Button>
          <span className="text-sm text-muted-foreground">
            Страница {page} из {Math.ceil(total / 100)}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= Math.ceil(total / 100)}
            onClick={() => setPage(page + 1)}
          >
            Вперёд
          </Button>
        </div>
      )}
    </div>
  )
}
