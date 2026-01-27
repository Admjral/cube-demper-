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

export default function AdminPaymentsPage() {
  const [search, setSearch] = useState('')
  const { data: payments, isLoading } = useAdminPayments()

  // Mock data for demo
  const mockPayments = payments || [
    { id: '1', user_id: '1', user_email: 'seller1@mail.kz', amount: 25000, status: 'completed', plan: 'Комбо 500', created_at: '2024-01-25T10:30:00' },
    { id: '2', user_id: '2', user_email: 'shop2@gmail.com', amount: 20000, status: 'completed', plan: 'Бот 1000', created_at: '2024-01-25T09:15:00' },
    { id: '3', user_id: '4', user_email: 'store3@inbox.kz', amount: 5000, status: 'completed', plan: 'Бот 500', created_at: '2024-01-24T18:45:00' },
    { id: '4', user_id: '5', user_email: 'newuser@test.kz', amount: 25000, status: 'pending', plan: 'Комбо 500', created_at: '2024-01-25T11:00:00' },
    { id: '5', user_id: '6', user_email: 'failed@mail.kz', amount: 30000, status: 'failed', plan: 'Комбо 1000', created_at: '2024-01-25T08:00:00' },
    { id: '6', user_id: '7', user_email: 'refund@test.kz', amount: 20000, status: 'refunded', plan: 'Бот 1000', created_at: '2024-01-23T14:30:00' },
  ]

  const filteredPayments = mockPayments.filter((payment) =>
    payment.user_email.toLowerCase().includes(search.toLowerCase())
  )

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'completed':
        return (
          <Badge className="bg-success/10 text-success border-0">
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

  const totalRevenue = mockPayments
    .filter((p) => p.status === 'completed')
    .reduce((sum, p) => sum + p.amount, 0)

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <CreditCard className="h-6 w-6" />
            Платежи
          </h1>
          <p className="text-muted-foreground">История всех транзакций</p>
        </div>
        <div className="text-right">
          <p className="text-sm text-muted-foreground">Общая выручка</p>
          <p className="text-2xl font-bold text-success">
            {totalRevenue.toLocaleString()}₸
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
                          #{payment.id}
                        </span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm">{payment.user_email}</span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm font-medium">{payment.plan}</span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm font-semibold">
                          {payment.amount.toLocaleString()}₸
                        </span>
                      </td>
                      <td className="p-4">{getStatusBadge(payment.status)}</td>
                      <td className="p-4">
                        <span className="text-sm text-muted-foreground flex items-center gap-1">
                          <Calendar className="h-3 w-3" />
                          {new Date(payment.created_at).toLocaleString('ru-RU', {
                            day: 'numeric',
                            month: 'short',
                            hour: '2-digit',
                            minute: '2-digit',
                          })}
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
    </div>
  )
}
