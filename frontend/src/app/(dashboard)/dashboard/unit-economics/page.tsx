"use client"

import { useState } from "react"
import { useStore } from "@/store/use-store"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Separator } from "@/components/ui/separator"
import {
  Calculator,
  DollarSign,
  Percent,
  Truck,
  Package,
  TrendingUp,
  RefreshCw,
} from "lucide-react"

export default function UnitEconomicsPage() {
  const { locale } = useStore()
  const [values, setValues] = useState({
    sellingPrice: 100000,
    purchasePrice: 60000,
    kaspiCommission: 12,
    deliveryCost: 2000,
    packagingCost: 500,
    otherCosts: 1000,
  })

  const calculateMargin = () => {
    const commission = values.sellingPrice * (values.kaspiCommission / 100)
    const totalCosts =
      values.purchasePrice +
      commission +
      values.deliveryCost +
      values.packagingCost +
      values.otherCosts
    const profit = values.sellingPrice - totalCosts
    const margin = (profit / values.sellingPrice) * 100
    return { profit, margin, commission, totalCosts }
  }

  const { profit, margin, commission, totalCosts } = calculateMargin()

  const formatPrice = (price: number) => {
    return new Intl.NumberFormat('ru-RU').format(Math.round(price)) + ' ₸'
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold">
          {locale === 'ru' ? 'Юнит-экономика' : 'Unit Economics'}
        </h1>
        <p className="text-muted-foreground">
          {locale === 'ru'
            ? 'Расчёт маржинальности и прибыли'
            : 'Margin and profit calculation'}
        </p>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        {/* Calculator */}
        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Calculator className="h-5 w-5" />
              {locale === 'ru' ? 'Калькулятор' : 'Calculator'}
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            {/* Selling price */}
            <div className="space-y-2">
              <Label htmlFor="sellingPrice" className="flex items-center gap-2">
                <DollarSign className="h-4 w-4 text-muted-foreground" />
                {locale === 'ru' ? 'Цена продажи' : 'Selling price'}
              </Label>
              <Input
                id="sellingPrice"
                type="number"
                value={values.sellingPrice}
                onChange={(e) =>
                  setValues({ ...values, sellingPrice: Number(e.target.value) })
                }
                className="text-lg"
              />
            </div>

            {/* Purchase price */}
            <div className="space-y-2">
              <Label htmlFor="purchasePrice" className="flex items-center gap-2">
                <Package className="h-4 w-4 text-muted-foreground" />
                {locale === 'ru' ? 'Цена закупки' : 'Purchase price'}
              </Label>
              <Input
                id="purchasePrice"
                type="number"
                value={values.purchasePrice}
                onChange={(e) =>
                  setValues({ ...values, purchasePrice: Number(e.target.value) })
                }
              />
            </div>

            {/* Kaspi commission */}
            <div className="space-y-2">
              <Label htmlFor="kaspiCommission" className="flex items-center gap-2">
                <Percent className="h-4 w-4 text-muted-foreground" />
                {locale === 'ru' ? 'Комиссия Kaspi (%)' : 'Kaspi commission (%)'}
              </Label>
              <Input
                id="kaspiCommission"
                type="number"
                value={values.kaspiCommission}
                onChange={(e) =>
                  setValues({ ...values, kaspiCommission: Number(e.target.value) })
                }
              />
            </div>

            {/* Delivery cost */}
            <div className="space-y-2">
              <Label htmlFor="deliveryCost" className="flex items-center gap-2">
                <Truck className="h-4 w-4 text-muted-foreground" />
                {locale === 'ru' ? 'Стоимость доставки' : 'Delivery cost'}
              </Label>
              <Input
                id="deliveryCost"
                type="number"
                value={values.deliveryCost}
                onChange={(e) =>
                  setValues({ ...values, deliveryCost: Number(e.target.value) })
                }
              />
            </div>

            {/* Packaging */}
            <div className="space-y-2">
              <Label htmlFor="packagingCost" className="flex items-center gap-2">
                <Package className="h-4 w-4 text-muted-foreground" />
                {locale === 'ru' ? 'Упаковка' : 'Packaging'}
              </Label>
              <Input
                id="packagingCost"
                type="number"
                value={values.packagingCost}
                onChange={(e) =>
                  setValues({ ...values, packagingCost: Number(e.target.value) })
                }
              />
            </div>

            {/* Other costs */}
            <div className="space-y-2">
              <Label htmlFor="otherCosts" className="flex items-center gap-2">
                {locale === 'ru' ? 'Прочие расходы' : 'Other costs'}
              </Label>
              <Input
                id="otherCosts"
                type="number"
                value={values.otherCosts}
                onChange={(e) =>
                  setValues({ ...values, otherCosts: Number(e.target.value) })
                }
              />
            </div>

            <Button
              variant="outline"
              className="w-full"
              onClick={() =>
                setValues({
                  sellingPrice: 100000,
                  purchasePrice: 60000,
                  kaspiCommission: 12,
                  deliveryCost: 2000,
                  packagingCost: 500,
                  otherCosts: 1000,
                })
              }
            >
              <RefreshCw className="h-4 w-4 mr-2" />
              {locale === 'ru' ? 'Сбросить' : 'Reset'}
            </Button>
          </CardContent>
        </Card>

        {/* Results */}
        <div className="space-y-6">
          {/* Main result */}
          <Card
            className={`glass-card ${
              margin > 20
                ? 'border-green-500/30'
                : margin > 10
                ? 'border-yellow-500/30'
                : 'border-red-500/30'
            }`}
          >
            <CardContent className="p-6">
              <div className="text-center">
                <p className="text-sm text-muted-foreground mb-2">
                  {locale === 'ru' ? 'Чистая прибыль' : 'Net profit'}
                </p>
                <p
                  className={`text-4xl font-bold ${
                    profit > 0 ? 'text-green-600' : 'text-red-500'
                  }`}
                >
                  {formatPrice(profit)}
                </p>
                <p className="text-lg text-muted-foreground mt-2">
                  {locale === 'ru' ? 'Маржа' : 'Margin'}: {margin.toFixed(1)}%
                </p>
              </div>
            </CardContent>
          </Card>

          {/* Breakdown */}
          <Card className="glass-card">
            <CardHeader>
              <CardTitle>
                {locale === 'ru' ? 'Детализация' : 'Breakdown'}
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex justify-between items-center">
                <span className="text-muted-foreground">
                  {locale === 'ru' ? 'Цена продажи' : 'Selling price'}
                </span>
                <span className="font-semibold">{formatPrice(values.sellingPrice)}</span>
              </div>
              <Separator />
              <div className="flex justify-between items-center">
                <span className="text-muted-foreground">
                  {locale === 'ru' ? 'Цена закупки' : 'Purchase price'}
                </span>
                <span className="text-red-500">-{formatPrice(values.purchasePrice)}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-muted-foreground">
                  {locale === 'ru' ? 'Комиссия Kaspi' : 'Kaspi commission'}
                </span>
                <span className="text-red-500">-{formatPrice(commission)}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-muted-foreground">
                  {locale === 'ru' ? 'Доставка' : 'Delivery'}
                </span>
                <span className="text-red-500">-{formatPrice(values.deliveryCost)}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-muted-foreground">
                  {locale === 'ru' ? 'Упаковка' : 'Packaging'}
                </span>
                <span className="text-red-500">-{formatPrice(values.packagingCost)}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-muted-foreground">
                  {locale === 'ru' ? 'Прочее' : 'Other'}
                </span>
                <span className="text-red-500">-{formatPrice(values.otherCosts)}</span>
              </div>
              <Separator />
              <div className="flex justify-between items-center">
                <span className="font-semibold">
                  {locale === 'ru' ? 'Итого расходов' : 'Total costs'}
                </span>
                <span className="font-semibold text-red-500">
                  -{formatPrice(totalCosts)}
                </span>
              </div>
              <div className="flex justify-between items-center text-lg">
                <span className="font-bold">
                  {locale === 'ru' ? 'Прибыль' : 'Profit'}
                </span>
                <span
                  className={`font-bold ${
                    profit > 0 ? 'text-green-600' : 'text-red-500'
                  }`}
                >
                  {formatPrice(profit)}
                </span>
              </div>
            </CardContent>
          </Card>

          {/* Tips */}
          <Card className="glass-card">
            <CardContent className="p-4">
              <div className="flex items-start gap-3">
                <TrendingUp className="h-5 w-5 text-muted-foreground mt-0.5" />
                <div>
                  <p className="font-medium">
                    {locale === 'ru' ? 'Рекомендация' : 'Recommendation'}
                  </p>
                  <p className="text-sm text-muted-foreground">
                    {margin < 15
                      ? locale === 'ru'
                        ? 'Маржа ниже 15%. Рассмотрите возможность повышения цены или снижения расходов.'
                        : 'Margin below 15%. Consider raising price or reducing costs.'
                      : locale === 'ru'
                      ? 'Хорошая маржинальность! Товар прибыльный.'
                      : 'Good margin! Product is profitable.'}
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}
