'use client'

import { useEffect, useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { getReferralLeads, ReferralLeadsResponse } from '@/hooks/use-referral'
import { Loader2, Users } from 'lucide-react'
import { format } from 'date-fns'
import { ru } from 'date-fns/locale'

export default function PartnerLeadsPage() {
  const [data, setData] = useState<ReferralLeadsResponse | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function fetchData() {
      try {
        const result = await getReferralLeads(100)
        setData(result)
      } catch (error) {
        console.error('Failed to fetch leads:', error)
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

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Приведённые клиенты</h1>
        <p className="text-muted-foreground">
          Всего: {data?.total ?? 0} клиентов
        </p>
      </div>

      {data?.leads.length === 0 ? (
        <Card className="glass-card">
          <CardContent className="flex flex-col items-center justify-center py-12">
            <Users className="h-12 w-12 text-muted-foreground mb-4" />
            <p className="text-muted-foreground">Пока нет приведённых клиентов</p>
            <p className="text-sm text-muted-foreground mt-1">
              Поделитесь реферальной ссылкой, чтобы привлекать клиентов
            </p>
          </CardContent>
        </Card>
      ) : (
        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <Users className="h-5 w-5" />
              Клиенты
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-3 px-2 text-sm font-medium text-muted-foreground">
                      Email
                    </th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-muted-foreground">
                      Имя
                    </th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-muted-foreground">
                      Дата регистрации
                    </th>
                    <th className="text-left py-3 px-2 text-sm font-medium text-muted-foreground">
                      Статус
                    </th>
                    <th className="text-right py-3 px-2 text-sm font-medium text-muted-foreground">
                      Ваш заработок
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {data?.leads.map((lead) => (
                    <tr key={lead.id} className="border-b last:border-0">
                      <td className="py-3 px-2 text-sm">{lead.email}</td>
                      <td className="py-3 px-2 text-sm">{lead.full_name || '-'}</td>
                      <td className="py-3 px-2 text-sm text-muted-foreground">
                        {lead.registered_at
                          ? format(new Date(lead.registered_at), 'd MMM yyyy', { locale: ru })
                          : '-'}
                      </td>
                      <td className="py-3 px-2">
                        <Badge
                          variant={lead.status === 'paid' ? 'default' : 'secondary'}
                        >
                          {lead.status === 'paid' ? 'Оплатил' : 'Зарегистрирован'}
                        </Badge>
                      </td>
                      <td className="py-3 px-2 text-sm text-right font-medium">
                        {lead.partner_earned > 0 ? (
                          <span className="text-green-600">
                            +{(lead.partner_earned / 100).toLocaleString()} ₸
                          </span>
                        ) : (
                          '-'
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
