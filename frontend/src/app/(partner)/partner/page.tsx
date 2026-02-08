'use client'

import { useEffect, useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { getReferralStats, getReferralLink, ReferralStats, ReferralLink } from '@/hooks/use-referral'
import {
  MousePointer,
  UserPlus,
  CreditCard,
  Wallet,
  Copy,
  Check,
  Loader2,
} from 'lucide-react'

export default function PartnerDashboardPage() {
  const [stats, setStats] = useState<ReferralStats | null>(null)
  const [linkData, setLinkData] = useState<ReferralLink | null>(null)
  const [loading, setLoading] = useState(true)
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    async function fetchData() {
      try {
        const [statsData, linkResult] = await Promise.all([
          getReferralStats(),
          getReferralLink(),
        ])
        setStats(statsData)
        setLinkData(linkResult)
      } catch (error) {
        console.error('Failed to fetch referral data:', error)
      } finally {
        setLoading(false)
      }
    }
    fetchData()
  }, [])

  const copyToClipboard = async (text: string) => {
    await navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  const statCards = [
    {
      title: 'Клики по ссылке',
      value: stats?.clicks ?? 0,
      icon: MousePointer,
      color: 'text-blue-500',
    },
    {
      title: 'Регистрации',
      value: stats?.registrations ?? 0,
      icon: UserPlus,
      color: 'text-green-500',
    },
    {
      title: 'Оплатили',
      value: stats?.paid_users ?? 0,
      icon: CreditCard,
      color: 'text-purple-500',
    },
    {
      title: 'Доступно к выводу',
      value: `${((stats?.available_balance ?? 0) / 100).toLocaleString()} ₸`,
      icon: Wallet,
      color: 'text-orange-500',
    },
  ]

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Реферальная программа</h1>
        <p className="text-muted-foreground">Приглашайте друзей и зарабатывайте</p>
      </div>

      {/* Referral Link */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg">Ваша реферальная ссылка</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex gap-2">
            <code className="flex-1 px-3 py-2 bg-muted rounded-lg text-sm overflow-x-auto">
              {linkData?.referral_link || 'Загрузка...'}
            </code>
            <Button
              variant="outline"
              size="icon"
              onClick={() => linkData?.referral_link && copyToClipboard(linkData.referral_link)}
              disabled={!linkData?.referral_link}
            >
              {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
            </Button>
          </div>
          {linkData?.promo_code && (
            <p className="mt-2 text-sm text-muted-foreground">
              Промокод: <span className="font-mono font-medium">{linkData.promo_code}</span>
            </p>
          )}
        </CardContent>
      </Card>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {statCards.map((stat) => (
          <Card key={stat.title} className="glass-card">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                {stat.title}
              </CardTitle>
              <stat.icon className={`h-4 w-4 ${stat.color}`} />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stat.value}</div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Earnings Summary */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg">Заработок</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="p-4 bg-muted/50 rounded-lg">
              <p className="text-sm text-muted-foreground">Всего заработано</p>
              <p className="text-xl font-semibold mt-1">
                {((stats?.total_earned ?? 0) / 100).toLocaleString()} ₸
              </p>
            </div>
            <div className="p-4 bg-muted/50 rounded-lg">
              <p className="text-sm text-muted-foreground">Выведено</p>
              <p className="text-xl font-semibold mt-1">
                {((stats?.total_withdrawn ?? 0) / 100).toLocaleString()} ₸
              </p>
            </div>
            <div className="p-4 bg-primary/10 rounded-lg">
              <p className="text-sm text-primary">Доступно к выводу</p>
              <p className="text-xl font-semibold mt-1 text-primary">
                {((stats?.available_balance ?? 0) / 100).toLocaleString()} ₸
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* How it works */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg">Как это работает?</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="p-4 border rounded-lg">
              <div className="text-2xl font-bold text-primary mb-2">1</div>
              <p className="font-medium">Поделитесь ссылкой</p>
              <p className="text-sm text-muted-foreground mt-1">
                Отправьте реферальную ссылку друзьям или разместите в соцсетях
              </p>
            </div>
            <div className="p-4 border rounded-lg">
              <div className="text-2xl font-bold text-primary mb-2">2</div>
              <p className="font-medium">Друг регистрируется</p>
              <p className="text-sm text-muted-foreground mt-1">
                Когда кто-то переходит по ссылке и регистрируется, он становится вашим рефералом
              </p>
            </div>
            <div className="p-4 border rounded-lg">
              <div className="text-2xl font-bold text-primary mb-2">3</div>
              <p className="font-medium">Получайте комиссию</p>
              <p className="text-sm text-muted-foreground mt-1">
                Вы получаете 20% от каждой оплаты вашего реферала
              </p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
