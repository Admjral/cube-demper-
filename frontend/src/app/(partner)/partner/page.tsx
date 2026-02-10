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
  Share2,
} from 'lucide-react'

export default function PartnerDashboardPage() {
  const [stats, setStats] = useState<ReferralStats | null>(null)
  const [linkData, setLinkData] = useState<ReferralLink | null>(null)
  const [loading, setLoading] = useState(true)
  const [copied, setCopied] = useState<'link' | 'code' | null>(null)

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

  const copyToClipboard = async (text: string, type: 'link' | 'code') => {
    await navigator.clipboard.writeText(text)
    setCopied(type)
    setTimeout(() => setCopied(null), 2000)
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  return (
    <div className="space-y-4 sm:space-y-6">
      <div>
        <h1 className="text-xl sm:text-2xl font-semibold">Реферальная программа</h1>
        <p className="text-sm text-muted-foreground">Приглашайте друзей и зарабатывайте</p>
      </div>

      {/* Referral Link & Code */}
      <Card className="glass-card">
        <CardHeader className="pb-3">
          <CardTitle className="text-base sm:text-lg flex items-center gap-2">
            <Share2 className="h-4 w-4 sm:h-5 sm:w-5" />
            Ваша ссылка и промокод
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          {/* Link */}
          <div className="space-y-1.5">
            <p className="text-xs text-muted-foreground font-medium uppercase tracking-wide">Ссылка</p>
            <div className="flex gap-2">
              <code className="flex-1 px-3 py-2 bg-muted rounded-lg text-xs sm:text-sm overflow-x-auto break-all leading-relaxed">
                {linkData?.referral_link || 'Загрузка...'}
              </code>
              <Button
                variant="outline"
                size="icon"
                className="shrink-0 h-9 w-9"
                onClick={() => linkData?.referral_link && copyToClipboard(linkData.referral_link, 'link')}
                disabled={!linkData?.referral_link}
              >
                {copied === 'link' ? <Check className="h-4 w-4 text-green-500" /> : <Copy className="h-4 w-4" />}
              </Button>
            </div>
          </div>

          {/* Promo code */}
          {linkData?.promo_code && (
            <div className="space-y-1.5">
              <p className="text-xs text-muted-foreground font-medium uppercase tracking-wide">Промокод</p>
              <div className="flex gap-2 items-center">
                <code className="px-3 py-2 bg-muted rounded-lg text-sm sm:text-base font-mono font-bold tracking-widest">
                  {linkData.promo_code}
                </code>
                <Button
                  variant="outline"
                  size="icon"
                  className="shrink-0 h-9 w-9"
                  onClick={() => copyToClipboard(linkData.promo_code!, 'code')}
                >
                  {copied === 'code' ? <Check className="h-4 w-4 text-green-500" /> : <Copy className="h-4 w-4" />}
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Stats Grid */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
        {[
          {
            title: 'Клики',
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
            title: 'К выводу',
            value: `${((stats?.available_balance ?? 0) / 100).toLocaleString()} \u20B8`,
            icon: Wallet,
            color: 'text-orange-500',
          },
        ].map((stat) => (
          <Card key={stat.title} className="glass-card">
            <CardContent className="p-3 sm:p-4">
              <div className="flex items-center justify-between mb-1 sm:mb-2">
                <span className="text-xs sm:text-sm text-muted-foreground">{stat.title}</span>
                <stat.icon className={`h-3.5 w-3.5 sm:h-4 sm:w-4 ${stat.color}`} />
              </div>
              <p className="text-lg sm:text-2xl font-bold">{stat.value}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Earnings Summary */}
      <Card className="glass-card">
        <CardHeader className="pb-3">
          <CardTitle className="text-base sm:text-lg">Заработок</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            <div className="p-3 sm:p-4 bg-muted/50 rounded-lg">
              <p className="text-xs sm:text-sm text-muted-foreground">Всего заработано</p>
              <p className="text-lg sm:text-xl font-semibold mt-0.5">
                {((stats?.total_earned ?? 0) / 100).toLocaleString()} {'\u20B8'}
              </p>
            </div>
            <div className="p-3 sm:p-4 bg-muted/50 rounded-lg">
              <p className="text-xs sm:text-sm text-muted-foreground">Выведено</p>
              <p className="text-lg sm:text-xl font-semibold mt-0.5">
                {((stats?.total_withdrawn ?? 0) / 100).toLocaleString()} {'\u20B8'}
              </p>
            </div>
            <div className="p-3 sm:p-4 bg-primary/10 rounded-lg">
              <p className="text-xs sm:text-sm text-primary">Доступно к выводу</p>
              <p className="text-lg sm:text-xl font-semibold mt-0.5 text-primary">
                {((stats?.available_balance ?? 0) / 100).toLocaleString()} {'\u20B8'}
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* How it works */}
      <Card className="glass-card">
        <CardHeader className="pb-3">
          <CardTitle className="text-base sm:text-lg">Как это работает?</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            <div className="flex sm:flex-col items-start gap-3 sm:gap-0 p-3 sm:p-4 border rounded-lg">
              <div className="text-xl sm:text-2xl font-bold text-primary shrink-0">1</div>
              <div>
                <p className="font-medium text-sm sm:text-base">Поделитесь ссылкой</p>
                <p className="text-xs sm:text-sm text-muted-foreground mt-0.5">
                  Отправьте ссылку или промокод друзьям
                </p>
              </div>
            </div>
            <div className="flex sm:flex-col items-start gap-3 sm:gap-0 p-3 sm:p-4 border rounded-lg">
              <div className="text-xl sm:text-2xl font-bold text-primary shrink-0">2</div>
              <div>
                <p className="font-medium text-sm sm:text-base">Друг регистрируется</p>
                <p className="text-xs sm:text-sm text-muted-foreground mt-0.5">
                  Он вводит ваш промокод при регистрации
                </p>
              </div>
            </div>
            <div className="flex sm:flex-col items-start gap-3 sm:gap-0 p-3 sm:p-4 border rounded-lg">
              <div className="text-xl sm:text-2xl font-bold text-primary shrink-0">3</div>
              <div>
                <p className="font-medium text-sm sm:text-base">Получайте комиссию</p>
                <p className="text-xs sm:text-sm text-muted-foreground mt-0.5">
                  Вы получаете % от каждой оплаты реферала
                </p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
