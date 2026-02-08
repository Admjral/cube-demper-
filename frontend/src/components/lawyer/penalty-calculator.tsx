'use client'

import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { useCalculatePenalty, type PenaltyResult } from '@/hooks/api/use-lawyer'
import { Calculator, Loader2, Copy, Check } from 'lucide-react'

export function PenaltyCalculator() {
  const [amount, setAmount] = useState('')
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [result, setResult] = useState<PenaltyResult | null>(null)
  const [copied, setCopied] = useState(false)

  const { mutate: calculate, isPending } = useCalculatePenalty()

  const handleCalculate = () => {
    if (!amount || !startDate || !endDate) return
    
    calculate({
      principal_amount: parseInt(amount),
      start_date: startDate,
      end_date: endDate,
      rate_type: 'refinancing',
    }, {
      onSuccess: (data) => {
        setResult(data)
      }
    })
  }

  const handleCopy = () => {
    if (result) {
      navigator.clipboard.writeText(result.calculation_details)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }

  const formatAmount = (value: number) => {
    return new Intl.NumberFormat('ru-RU').format(value)
  }

  return (
    <div className="p-6 space-y-6 overflow-auto">
      <div className="max-w-2xl mx-auto">
        {/* Input Form */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Calculator className="h-5 w-5" />
              Калькулятор пени
            </CardTitle>
            <CardDescription>
              Расчёт по ставке рефинансирования НБ РК (15.75% годовых)
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="amount">Сумма долга (тенге)</Label>
              <Input
                id="amount"
                type="number"
                placeholder="1000000"
                value={amount}
                onChange={(e) => setAmount(e.target.value)}
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="startDate">Дата начала просрочки</Label>
                <Input
                  id="startDate"
                  type="date"
                  value={startDate}
                  onChange={(e) => setStartDate(e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="endDate">Дата окончания</Label>
                <Input
                  id="endDate"
                  type="date"
                  value={endDate}
                  onChange={(e) => setEndDate(e.target.value)}
                />
              </div>
            </div>

            <Button 
              onClick={handleCalculate} 
              disabled={!amount || !startDate || !endDate || isPending}
              className="w-full"
            >
              {isPending ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Расчёт...
                </>
              ) : (
                'Рассчитать пени'
              )}
            </Button>
          </CardContent>
        </Card>

        {/* Result */}
        {result && (
          <Card className="mt-6">
            <CardHeader>
              <CardTitle className="flex items-center justify-between">
                Результат расчёта
                <Button variant="ghost" size="sm" onClick={handleCopy}>
                  {copied ? (
                    <Check className="h-4 w-4 text-green-500" />
                  ) : (
                    <Copy className="h-4 w-4" />
                  )}
                </Button>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div className="p-4 bg-muted rounded-lg">
                    <p className="text-sm text-muted-foreground">Сумма долга</p>
                    <p className="text-xl font-semibold">{formatAmount(result.principal_amount)} ₸</p>
                  </div>
                  <div className="p-4 bg-muted rounded-lg">
                    <p className="text-sm text-muted-foreground">Дней просрочки</p>
                    <p className="text-xl font-semibold">{result.days}</p>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div className="p-4 bg-orange-500/10 rounded-lg">
                    <p className="text-sm text-muted-foreground">Сумма пени</p>
                    <p className="text-xl font-semibold text-orange-500">{formatAmount(result.penalty_amount)} ₸</p>
                  </div>
                  <div className="p-4 bg-primary/10 rounded-lg">
                    <p className="text-sm text-muted-foreground">Итого к оплате</p>
                    <p className="text-xl font-semibold text-primary">{formatAmount(result.total_amount)} ₸</p>
                  </div>
                </div>

                <div className="p-4 bg-muted rounded-lg">
                  <p className="text-sm text-muted-foreground mb-2">Детали расчёта</p>
                  <pre className="text-sm whitespace-pre-wrap font-mono">
                    {result.calculation_details}
                  </pre>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  )
}
