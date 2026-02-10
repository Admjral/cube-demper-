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

  const getStatusBadge = (status: string, small?: boolean) => {
    const cls = small ? 'text-xs' : ''
    switch (status) {
      case 'completed':
        return <Badge variant="default" className={cls}>Выполнено</Badge>
      case 'pending':
        return <Badge variant="secondary" className={cls}>В обработке</Badge>
      case 'failed':
        return <Badge variant="destructive" className={cls}>Ошибка</Badge>
      default:
        return <Badge variant="outline" className={cls}>{status}</Badge>
    }
  }

  return (
    <div className="space-y-4 sm:space-y-6">
      <div>
        <h1 className="text-xl sm:text-2xl font-semibold">История транзакций</h1>
        <p className="text-sm text-muted-foreground">
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
        <>
          {/* Mobile view - compact cards */}
          <div className="sm:hidden space-y-2">
            {data?.transactions.map((tx) => (
              <Card key={tx.id} className="glass-card">
                <CardContent className="p-3">
                  <div className="flex items-start gap-2.5">
                    <div
                      className={`p-1.5 rounded-full shrink-0 mt-0.5 ${
                        tx.type === 'income'
                          ? 'bg-green-500/10 text-green-500'
                          : 'bg-orange-500/10 text-orange-500'
                      }`}
                    >
                      {tx.type === 'income' ? (
                        <ArrowDownLeft className="h-3.5 w-3.5" />
                      ) : (
                        <ArrowUpRight className="h-3.5 w-3.5" />
                      )}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between gap-2">
                        <p className="text-sm font-medium truncate">
                          {tx.type === 'income' ? 'Начисление' : 'Вывод средств'}
                        </p>
                        <p
                          className={`text-sm font-semibold shrink-0 ${
                            tx.type === 'income' ? 'text-green-600' : 'text-orange-600'
                          }`}
                        >
                          {tx.type === 'income' ? '+' : '-'}
                          {(Math.abs(tx.amount) / 100).toLocaleString()} {'\u20B8'}
                        </p>
                      </div>
                      <div className="flex items-center justify-between gap-2 mt-1">
                        <p className="text-xs text-muted-foreground truncate">
                          {tx.description || (tx.type === 'income' ? 'Комиссия с реферала' : 'Запрос на вывод')}
                        </p>
                        {getStatusBadge(tx.status, true)}
                      </div>
                      <p className="text-xs text-muted-foreground mt-1">
                        {tx.created_at
                          ? format(new Date(tx.created_at), 'd MMM yyyy, HH:mm', { locale: ru })
                          : '-'}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>

          {/* Desktop view - list inside card */}
          <Card className="glass-card hidden sm:block">
            <CardHeader className="pb-3">
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
                        {(Math.abs(tx.amount) / 100).toLocaleString()} {'\u20B8'}
                      </p>
                      <div className="mt-1">{getStatusBadge(tx.status)}</div>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  )
}
