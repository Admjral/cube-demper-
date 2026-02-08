'use client'

import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { useCalculateFee, type FeeResult } from '@/hooks/api/use-lawyer'
import { Building, Loader2 } from 'lucide-react'

type FeeType = 'ip_registration' | 'too_registration' | 'court_fee_property' | 'court_fee_non_property' | 'license_fee'

const feeTypes: { value: FeeType; label: string; description: string; needsAmount?: boolean }[] = [
  { 
    value: 'ip_registration', 
    label: 'Регистрация ИП', 
    description: 'Через eGov.kz' 
  },
  { 
    value: 'too_registration', 
    label: 'Регистрация ТОО', 
    description: '1 МРП' 
  },
  { 
    value: 'court_fee_property', 
    label: 'Госпошлина (имущественный иск)', 
    description: '1% от суммы иска',
    needsAmount: true
  },
  { 
    value: 'court_fee_non_property', 
    label: 'Госпошлина (неимущественный иск)', 
    description: '0.5 МРП для физлиц' 
  },
  { 
    value: 'license_fee', 
    label: 'Лицензионный сбор', 
    description: 'Ориентировочно' 
  },
]

export function FeeCalculator() {
  const [feeType, setFeeType] = useState<FeeType>('ip_registration')
  const [claimAmount, setClaimAmount] = useState('')
  const [result, setResult] = useState<FeeResult | null>(null)

  const { mutate: calculate, isPending } = useCalculateFee()
  
  const selectedFeeType = feeTypes.find(f => f.value === feeType)

  const handleCalculate = () => {
    calculate({
      fee_type: feeType,
      claim_amount: selectedFeeType?.needsAmount ? parseInt(claimAmount) : undefined,
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
              <Building className="h-5 w-5" />
              Калькулятор госпошлин
            </CardTitle>
            <CardDescription>
              МРП на 2026 год: 3 932 тенге
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label>Тип госпошлины</Label>
              <Select value={feeType} onValueChange={(v) => {
                setFeeType(v as FeeType)
                setResult(null)
              }}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {feeTypes.map((type) => (
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

            {selectedFeeType?.needsAmount && (
              <div className="space-y-2">
                <Label htmlFor="claimAmount">Сумма иска (тенге)</Label>
                <Input
                  id="claimAmount"
                  type="number"
                  placeholder="1000000"
                  value={claimAmount}
                  onChange={(e) => setClaimAmount(e.target.value)}
                />
              </div>
            )}

            <Button 
              onClick={handleCalculate} 
              disabled={isPending || (selectedFeeType?.needsAmount && !claimAmount)}
              className="w-full"
            >
              {isPending ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Расчёт...
                </>
              ) : (
                'Рассчитать'
              )}
            </Button>
          </CardContent>
        </Card>

        {/* Result */}
        {result && (
          <Card className="mt-6">
            <CardHeader>
              <CardTitle>Результат расчёта</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="p-6 bg-primary/10 rounded-lg text-center">
                  <p className="text-sm text-muted-foreground mb-1">Размер госпошлины</p>
                  <p className="text-3xl font-bold text-primary">
                    {result.fee_amount === 0 ? 'Бесплатно' : `${formatAmount(result.fee_amount)} ₸`}
                  </p>
                </div>

                <div className="p-4 bg-muted rounded-lg">
                  <p className="text-sm text-muted-foreground mb-2">Детали расчёта</p>
                  <p className="text-sm whitespace-pre-wrap">{result.calculation_details}</p>
                </div>

                <div className="p-4 border rounded-lg">
                  <p className="text-sm text-muted-foreground mb-2">Правовое основание</p>
                  <p className="text-sm">{result.legal_basis}</p>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  )
}
