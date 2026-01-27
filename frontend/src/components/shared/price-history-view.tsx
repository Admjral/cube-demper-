'use client'

import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Badge } from '@/components/ui/badge'
import { TrendingDown, TrendingUp, Minus } from 'lucide-react'
import type { PriceHistory } from '@/types/api'
import { formatPrice } from '@/lib/utils'

function formatDate(dateString: string) {
  const date = new Date(dateString)
  const now = new Date()
  const diffInHours = Math.floor((now.getTime() - date.getTime()) / (1000 * 60 * 60))

  if (diffInHours < 1) {
    const diffInMinutes = Math.floor((now.getTime() - date.getTime()) / (1000 * 60))
    return `${diffInMinutes} мин. назад`
  } else if (diffInHours < 24) {
    return `${diffInHours} ч. назад`
  } else {
    return date.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' })
  }
}

function getChangeReasonLabel(reason: string) {
  switch (reason) {
    case 'demper':
      return 'Демпинг'
    case 'manual':
      return 'Вручную'
    case 'competitor':
      return 'Конкурент'
    case 'sync':
      return 'Синхронизация'
    default:
      return reason
  }
}

function getChangeReasonVariant(reason: string): 'default' | 'secondary' | 'outline' {
  switch (reason) {
    case 'demper':
      return 'default'
    case 'manual':
      return 'secondary'
    default:
      return 'outline'
  }
}

interface PriceHistoryViewProps {
  history: PriceHistory[]
}

export function PriceHistoryView({ history }: PriceHistoryViewProps) {
  if (!history || history.length === 0) {
    return (
      <div className="text-center py-8 text-muted-foreground">
        <p>История изменений цен пока отсутствует</p>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div>
        <h3 className="text-sm font-medium mb-4">История изменений цен</h3>
        <div className="border rounded-lg">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Дата</TableHead>
                <TableHead>Старая цена</TableHead>
                <TableHead>Новая цена</TableHead>
                <TableHead>Изменение</TableHead>
                <TableHead>Конкурент</TableHead>
                <TableHead>Причина</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {history.map((h) => {
                const priceDiff = h.new_price - h.old_price
                const isIncrease = priceDiff > 0
                const isDecrease = priceDiff < 0

                return (
                  <TableRow key={h.id}>
                    <TableCell className="text-sm text-muted-foreground">
                      {formatDate(h.created_at)}
                    </TableCell>
                    <TableCell>{formatPrice(h.old_price)}</TableCell>
                    <TableCell className="font-medium">{formatPrice(h.new_price)}</TableCell>
                    <TableCell>
                      <div className="flex items-center gap-1">
                        {isIncrease && <TrendingUp className="h-4 w-4 text-green-500" />}
                        {isDecrease && <TrendingDown className="h-4 w-4 text-red-500" />}
                        {!isIncrease && !isDecrease && <Minus className="h-4 w-4 text-muted-foreground" />}
                        <span className={
                          isIncrease ? 'text-green-600' :
                          isDecrease ? 'text-red-600' :
                          'text-muted-foreground'
                        }>
                          {isIncrease && '+'}
                          {formatPrice(Math.abs(priceDiff))}
                        </span>
                      </div>
                    </TableCell>
                    <TableCell>
                      {h.competitor_price ? formatPrice(h.competitor_price) : '—'}
                    </TableCell>
                    <TableCell>
                      <Badge variant={getChangeReasonVariant(h.change_reason)}>
                        {getChangeReasonLabel(h.change_reason)}
                      </Badge>
                    </TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        </div>
      </div>

      {/* Statistics Summary */}
      <div className="grid grid-cols-3 gap-4">
        <div className="border rounded-lg p-4">
          <p className="text-sm text-muted-foreground">Всего изменений</p>
          <p className="text-2xl font-bold">{history.length}</p>
        </div>
        <div className="border rounded-lg p-4">
          <p className="text-sm text-muted-foreground">Снижений цены</p>
          <p className="text-2xl font-bold text-red-600">
            {history.filter(h => h.new_price < h.old_price).length}
          </p>
        </div>
        <div className="border rounded-lg p-4">
          <p className="text-sm text-muted-foreground">Повышений цены</p>
          <p className="text-2xl font-bold text-green-600">
            {history.filter(h => h.new_price > h.old_price).length}
          </p>
        </div>
      </div>
    </div>
  )
}
