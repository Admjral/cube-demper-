'use client'

import { useState, useEffect } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { getReferralStats, requestPayout, ReferralStats } from '@/hooks/use-referral'
import { Loader2, CreditCard, CheckCircle } from 'lucide-react'

export default function PartnerPayoutPage() {
  const [stats, setStats] = useState<ReferralStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [submitting, setSubmitting] = useState(false)
  const [success, setSuccess] = useState(false)
  const [error, setError] = useState('')

  const [amount, setAmount] = useState('')
  const [requisites, setRequisites] = useState('')

  useEffect(() => {
    async function fetchData() {
      try {
        const result = await getReferralStats()
        setStats(result)
      } catch (error) {
        console.error('Failed to fetch stats:', error)
      } finally {
        setLoading(false)
      }
    }
    fetchData()
  }, [])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError('')
    setSubmitting(true)

    try {
      const amountInTiyns = Math.round(parseFloat(amount) * 100)

      await requestPayout(amountInTiyns, requisites)

      setSuccess(true)
      setAmount('')
      setRequisites('')

      // Refresh stats
      const newStats = await getReferralStats()
      setStats(newStats)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Ошибка при создании заявки')
    } finally {
      setSubmitting(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  const availableBalance = (stats?.available_balance ?? 0) / 100

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Вывод средств</h1>
        <p className="text-muted-foreground">
          Доступно к выводу: {availableBalance.toLocaleString()} ₸
        </p>
      </div>

      {success ? (
        <Card className="glass-card">
          <CardContent className="flex flex-col items-center justify-center py-12">
            <CheckCircle className="h-12 w-12 text-green-500 mb-4" />
            <p className="text-lg font-medium">Заявка отправлена!</p>
            <p className="text-muted-foreground mt-1">
              Выплата будет обработана в течение 24 часов
            </p>
            <Button
              variant="outline"
              className="mt-4"
              onClick={() => setSuccess(false)}
            >
              Создать новую заявку
            </Button>
          </CardContent>
        </Card>
      ) : (
        <Card className="glass-card max-w-md">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <CreditCard className="h-5 w-5" />
              Заявка на вывод
            </CardTitle>
            <CardDescription>
              Минимальная сумма вывода: 5 000 ₸
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form onSubmit={handleSubmit} className="space-y-4">
              {error && (
                <div className="p-3 rounded-lg bg-destructive/10 text-destructive text-sm">
                  {error}
                </div>
              )}

              <div className="space-y-2">
                <Label htmlFor="amount">Сумма (₸)</Label>
                <Input
                  id="amount"
                  type="number"
                  placeholder="5000"
                  min="5000"
                  max={availableBalance}
                  value={amount}
                  onChange={(e) => setAmount(e.target.value)}
                  required
                />
                <p className="text-xs text-muted-foreground">
                  Максимум: {availableBalance.toLocaleString()} ₸
                </p>
              </div>

              <div className="space-y-2">
                <Label htmlFor="requisites">Реквизиты для выплаты</Label>
                <Textarea
                  id="requisites"
                  placeholder="Kaspi Gold: +7 777 123 45 67 (Имя Фамилия)"
                  value={requisites}
                  onChange={(e) => setRequisites(e.target.value)}
                  required
                />
                <p className="text-xs text-muted-foreground">
                  Укажите номер карты Kaspi или другие реквизиты
                </p>
              </div>

              <Button
                type="submit"
                className="w-full"
                disabled={submitting || availableBalance < 5000}
              >
                {submitting && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                Отправить заявку
              </Button>

              {availableBalance < 5000 && (
                <p className="text-sm text-muted-foreground text-center">
                  Минимальная сумма для вывода — 5 000 ₸
                </p>
              )}
            </form>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
