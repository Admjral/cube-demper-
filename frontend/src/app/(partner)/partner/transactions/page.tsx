'use client'

import { useEffect, useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { getReferralTransactions, ReferralTransactionsResponse } from '@/hooks/use-referral'
import { Loader2, Receipt, ArrowUpRight, ArrowDownLeft } from 'lucide-react'
import { format } from 'date-fns'
import { ru } from 'date-fns/locale'

export default function PartnerTransactionsPage() {
  const [data, setData] = useState<ReferralTransactionsResponse | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function fetchData() {
      try {
        const result = await getReferralTransactions(100)
        setData(result)
      } catch (error) {
        console.error('Failed to fetch transactions:', error)
      } finally {
        setLoading(false)
      }
    }
    fetchData()
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'completed':
        return <Badge variant="default">Выполнено</Badge>
      case 'pending':
        return <Badge variant="secondary">В обработке</Badge>
      case 'failed':
        return <Badge variant="destructive">Ошибка</Badge>
      default:
        return <Badge variant="outline">{status}</Badge>
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">История транзакций</h1>
        <p className="text-muted-foreground">
          Всего: {data?.total ?? 0} транзакций
        </p>
      </div>

      {data?.transactions.length === 0 ? (
        <Card className="glass-card">
          <CardContent className="flex flex-col items-center justify-center py-12">
            <Receipt className="h-12 w-12 text-muted-foreground mb-4" />
            <p className="text-muted-foreground">Пока нет транзакций</p>
          </CardContent>
        </Card>
      ) : (
        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <Receipt className="h-5 w-5" />
              Транзакции
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {data?.transactions.map((tx) => (
                <div
                  key={tx.id}
                  className="flex items-center justify-between p-4 bg-muted/30 rounded-lg"
                >
                  <div className="flex items-center gap-3">
                    <div
                      className={`p-2 rounded-full ${
                        tx.type === 'income'
                          ? 'bg-green-500/10 text-green-500'
                          : 'bg-orange-500/10 text-orange-500'
                      }`}
                    >
                      {tx.type === 'income' ? (
                        <ArrowDownLeft className="h-4 w-4" />
                      ) : (
                        <ArrowUpRight className="h-4 w-4" />
                      )}
                    </div>
                    <div>
                      <p className="font-medium">
                        {tx.type === 'income' ? 'Начисление' : 'Вывод средств'}
                      </p>
                      <p className="text-sm text-muted-foreground">{tx.description}</p>
                      <p className="text-xs text-muted-foreground mt-1">
                        {tx.created_at
                          ? format(new Date(tx.created_at), 'd MMM yyyy, HH:mm', { locale: ru })
                          : '-'}
                      </p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p
                      className={`font-semibold ${
                        tx.type === 'income' ? 'text-green-600' : 'text-orange-600'
                      }`}
                    >
                      {tx.type === 'income' ? '+' : '-'}
                      {(Math.abs(tx.amount) / 100).toLocaleString()} ₸
                    </p>
                    <div className="mt-1">{getStatusBadge(tx.status)}</div>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
