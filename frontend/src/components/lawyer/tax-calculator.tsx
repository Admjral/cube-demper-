'use client'

import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { useCalculateTax, type TaxType, type TaxResult } from '@/hooks/api/use-lawyer'
import { Receipt, Loader2, Info } from 'lucide-react'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"

const taxTypes: { value: TaxType; label: string; description: string }[] = [
  { 
    value: 'simplified_ip', 
    label: 'ИП на упрощёнке', 
    description: '3% ИПН + соц. отчисления' 
  },
  { 
    value: 'standard_ip', 
    label: 'ИП на общем режиме', 
    description: '10% ИПН от чистого дохода' 
  },
  { 
    value: 'too_kpn', 
    label: 'ТОО (КПН)', 
    description: '20% от налогооблагаемого дохода' 
  },
  { 
    value: 'vat', 
    label: 'НДС', 
    description: '12% (в т.ч. от выручки с НДС)' 
  },
]

export function TaxCalculator() {
  const [taxType, setTaxType] = useState<TaxType>('simplified_ip')
  const [revenue, setRevenue] = useState('')
  const [expenses, setExpenses] = useState('')
  const [result, setResult] = useState<TaxResult | null>(null)

  const { mutate: calculate, isPending } = useCalculateTax()

  const handleCalculate = () => {
    if (!revenue) return
    
    calculate({
      tax_type: taxType,
      revenue: parseInt(revenue),
      expenses: parseInt(expenses) || 0,
      period: '2026',
    }, {
      onSuccess: (data) => {
        setResult(data)
      }
    })
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
              <Receipt className="h-5 w-5" />
              Калькулятор налогов
            </CardTitle>
            <CardDescription>
              Расчёт налогов для ИП и ТОО по ставкам 2026 года
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label>Тип налогообложения</Label>
              <Select value={taxType} onValueChange={(v) => setTaxType(v as TaxType)}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {taxTypes.map((type) => (
                    <SelectItem key={type.value} value={type.value}>
                      <div className="flex flex-col">
                        <span>{type.label}</span>
                        <span className="text-xs text-muted-foreground">{type.description}</span>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="revenue" className="flex items-center gap-2">
                Доход (тенге)
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger>
                      <Info className="h-4 w-4 text-muted-foreground" />
                    </TooltipTrigger>
                    <TooltipContent>
                      <p>Общий доход за период (выручка)</p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              </Label>
              <Input
                id="revenue"
                type="number"
                placeholder="5000000"
                value={revenue}
                onChange={(e) => setRevenue(e.target.value)}
              />
            </div>

            {(taxType === 'standard_ip' || taxType === 'too_kpn') && (
              <div className="space-y-2">
                <Label htmlFor="expenses">Расходы (тенге)</Label>
                <Input
                  id="expenses"
                  type="number"
                  placeholder="2000000"
                  value={expenses}
                  onChange={(e) => setExpenses(e.target.value)}
                />
              </div>
            )}

            <Button 
              onClick={handleCalculate} 
              disabled={!revenue || isPending}
              className="w-full"
            >
              {isPending ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Расчёт...
                </>
              ) : (
                'Рассчитать налоги'
              )}
            </Button>
          </CardContent>
        </Card>

        {/* Result */}
        {result && (
          <Card className="mt-6">
            <CardHeader>
              <CardTitle>Результат расчёта</CardTitle>
              <CardDescription>
                {taxTypes.find(t => t.value === result.tax_type)?.label} за {result.period} год
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {/* Summary */}
                <div className="grid grid-cols-2 gap-4">
                  <div className="p-4 bg-muted rounded-lg">
                    <p className="text-sm text-muted-foreground">Доход</p>
                    <p className="text-xl font-semibold">{formatAmount(result.revenue)} ₸</p>
                  </div>
                  {result.expenses > 0 && (
                    <div className="p-4 bg-muted rounded-lg">
                      <p className="text-sm text-muted-foreground">Расходы</p>
                      <p className="text-xl font-semibold">-{formatAmount(result.expenses)} ₸</p>
                    </div>
                  )}
                </div>

                {/* Tax breakdown */}
                <div className="border rounded-lg overflow-hidden">
                  <table className="w-full">
                    <thead className="bg-muted">
                      <tr>
                        <th className="text-left p-3 text-sm font-medium">Налог/Отчисление</th>
                        <th className="text-right p-3 text-sm font-medium">Ставка</th>
                        <th className="text-right p-3 text-sm font-medium">Сумма</th>
                      </tr>
                    </thead>
                    <tbody>
                      {result.taxes.map((tax, idx) => (
                        <tr key={idx} className="border-t">
                          <td className="p-3">
                            <div>
                              <p className="font-medium text-sm">{tax.name}</p>
                              {tax.description && (
                                <p className="text-xs text-muted-foreground">{tax.description}</p>
                              )}
                            </div>
                          </td>
                          <td className="p-3 text-right text-sm">{tax.rate}%</td>
                          <td className="p-3 text-right font-medium">{formatAmount(tax.amount)} ₸</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                {/* Totals */}
                <div className="grid grid-cols-2 gap-4">
                  <div className="p-4 bg-red-500/10 rounded-lg">
                    <p className="text-sm text-muted-foreground">Всего налогов</p>
                    <p className="text-xl font-semibold text-red-500">{formatAmount(result.total_tax)} ₸</p>
                  </div>
                  <div className="p-4 bg-green-500/10 rounded-lg">
                    <p className="text-sm text-muted-foreground">Чистый доход</p>
                    <p className="text-xl font-semibold text-green-500">{formatAmount(result.net_income)} ₸</p>
                  </div>
                </div>

                <p className="text-xs text-muted-foreground text-center">
                  * Расчёт носит ознакомительный характер. Для точного расчёта обратитесь к бухгалтеру.
                </p>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  )
}
