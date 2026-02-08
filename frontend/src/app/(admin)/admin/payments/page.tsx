'use client'

import { useState } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { useAdminPayments } from '@/hooks/api/use-admin'
import {
  CreditCard,
  Search,
  Loader2,
  Calendar,
  CheckCircle,
  XCircle,
  Clock,
  RefreshCw,
} from 'lucide-react'
import { format } from 'date-fns'
import { ru } from 'date-fns/locale'

export default function AdminPaymentsPage() {
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(1)
  const { data, isLoading } = useAdminPayments(page, 50)

  const payments = data?.payments ?? []
  const total = data?.total ?? 0

  const filteredPayments = payments.filter((payment) =>
    payment.user_email.toLowerCase().includes(search.toLowerCase())
  )

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'completed':
        return (
          <Badge className="bg-green-500/10 text-green-500 border-0">
            <CheckCircle className="h-3 w-3 mr-1" />
            Оплачено
          </Badge>
        )
      case 'pending':
        return (
          <Badge variant="secondary">
            <Clock className="h-3 w-3 mr-1" />
            Ожидает
          </Badge>
        )
      case 'failed':
        return (
          <Badge variant="destructive">
            <XCircle className="h-3 w-3 mr-1" />
            Ошибка
          </Badge>
        )
      case 'refunded':
        return (
          <Badge variant="outline">
            <RefreshCw className="h-3 w-3 mr-1" />
            Возврат
          </Badge>
        )
      default:
        return <Badge variant="secondary">{status}</Badge>
    }
  }

  const totalRevenue = payments
    .filter((p) => p.status === 'completed')
    .reduce((sum, p) => sum + (p.amount ?? 0), 0)

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <CreditCard className="h-6 w-6" />
            Платежи
          </h1>
          <p className="text-muted-foreground">
            Всего: {total} платежей
          </p>
        </div>
        <div className="text-right">
          <p className="text-sm text-muted-foreground">Выручка на странице</p>
          <p className="text-2xl font-bold text-green-600">
            {(totalRevenue / 100).toLocaleString()} ₸
          </p>
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

      {/* Payments table */}
      <Card className="glass-card">
        <CardContent className="p-0">
          {isLoading ? (
            <div className="flex items-center justify-center h-64">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : filteredPayments.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-64">
              <CreditCard className="h-12 w-12 text-muted-foreground mb-4" />
              <p className="text-muted-foreground">Платежи не найдены</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      ID
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Пользователь
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      План
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Сумма
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Статус
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Дата
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredPayments.map((payment) => (
                    <tr
                      key={payment.id}
                      className="border-b border-border last:border-0 hover:bg-muted/50"
                    >
                      <td className="p-4">
                        <span className="text-sm font-mono text-muted-foreground">
                          #{payment.id.slice(0, 8)}
                        </span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm">{payment.user_email}</span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm font-medium">
                          {payment.plan || '—'}
                        </span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm font-semibold">
                          {((payment.amount ?? 0) / 100).toLocaleString()} ₸
                        </span>
                      </td>
                      <td className="p-4">{getStatusBadge(payment.status)}</td>
                      <td className="p-4">
                        <span className="text-sm text-muted-foreground flex items-center gap-1">
                          <Calendar className="h-3 w-3" />
                          {payment.created_at
                            ? format(new Date(payment.created_at), 'd MMM yyyy, HH:mm', {
                                locale: ru,
                              })
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
      {total > 50 && (
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
            Страница {page} из {Math.ceil(total / 50)}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= Math.ceil(total / 50)}
            onClick={() => setPage(page + 1)}
          >
            Вперёд
          </Button>
        </div>
      )}
    </div>
  )
}
