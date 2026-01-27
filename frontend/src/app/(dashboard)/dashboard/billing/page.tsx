'use client'

import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { useSubscription, useCreatePayment } from '@/hooks/api/use-billing'
import { useAuth } from '@/hooks/use-auth'
import {
  CreditCard,
  Check,
  Sparkles,
  Crown,
  Zap,
  Loader2,
  AlertCircle,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { toast } from 'sonner'

const plans = [
  {
    id: 'bot-500',
    name: 'Бот 500',
    description: 'Для начинающих продавцов',
    price: 5000,
    features: [
      'Price Bot до 500 товаров',
      'Мониторинг конкурентов',
      'Автоматический демпинг',
      'Базовая аналитика',
    ],
    popular: false,
    icon: Zap,
  },
  {
    id: 'bot-1000',
    name: 'Бот 1000',
    description: 'Для растущего бизнеса',
    price: 20000,
    features: [
      'Price Bot до 1000 товаров',
      'Мониторинг конкурентов',
      'Автоматический демпинг',
      'Расширенная аналитика',
      'Приоритетная поддержка',
    ],
    popular: false,
    icon: Zap,
  },
  {
    id: 'combo-500',
    name: 'Комбо 500',
    description: 'Всё включено',
    price: 25000,
    features: [
      'Price Bot до 500 товаров',
      'Аналитика ниш',
      'Управление предзаказами',
      'WhatsApp рассылки',
      'Все базовые функции',
    ],
    popular: true,
    icon: Crown,
  },
  {
    id: 'combo-1000',
    name: 'Комбо 1000',
    description: 'Максимум возможностей',
    price: 30000,
    features: [
      'Price Bot до 1000 товаров',
      'Аналитика ниш',
      'Управление предзаказами',
      'WhatsApp рассылки',
      'Приоритетная поддержка',
    ],
    popular: false,
    icon: Crown,
  },
]

const addons = [
  { id: 'bot-4000', name: 'Бот 4000', price: 30000, description: 'Price Bot до 4000 товаров' },
  { id: 'analytics', name: 'Аналитика ниш', price: 15000, description: 'Отдельный модуль аналитики' },
  { id: 'ai-salesman', name: 'ИИ Продажник', price: 15000, description: 'Автоответы в WhatsApp через ИИ' },
]

export default function BillingPage() {
  const { user } = useAuth()
  const { data: subscription, isLoading: subscriptionLoading } = useSubscription()
  const { mutate: createPayment, isPending: paymentPending } = useCreatePayment()
  const [selectedPlan, setSelectedPlan] = useState<string | null>(null)

  const handleSelectPlan = (planId: string) => {
    if (!user) {
      toast.error('Войдите в аккаунт для оформления подписки')
      return
    }

    setSelectedPlan(planId)
    createPayment(planId, {
      onSuccess: (data) => {
        // In production, redirect to payment URL
        toast.success('Перенаправление на страницу оплаты...')
        // window.location.href = data.payment_url
        console.log('Payment URL:', data.payment_url)
      },
      onError: () => {
        toast.error('Ошибка при создании платежа')
        setSelectedPlan(null)
      },
    })
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold flex items-center gap-2">
          <CreditCard className="h-6 w-6" />
          Тарифы и подписка
        </h1>
        <p className="text-muted-foreground">
          Выберите план, который подходит вашему бизнесу
        </p>
      </div>

      {/* Current subscription */}
      {subscription && (
        <Card className="glass-card border-primary/20 bg-primary/5">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Текущий план</p>
                <p className="text-xl font-semibold text-foreground">{subscription.plan}</p>
                <p className="text-sm text-muted-foreground mt-1">
                  Активен до {new Date(subscription.current_period_end).toLocaleDateString('ru-RU')}
                </p>
              </div>
              <Badge variant={subscription.status === 'active' ? 'default' : 'secondary'}>
                {subscription.status === 'active' ? 'Активна' : subscription.status}
              </Badge>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Promo banner */}
      <Card className="glass-card border-warning/20 bg-warning/5">
        <CardContent className="pt-6">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-full bg-warning/20 flex items-center justify-center">
              <Sparkles className="h-5 w-5 text-warning" />
            </div>
            <div>
              <p className="font-medium text-foreground">Бонус для первых пользователей</p>
              <p className="text-sm text-muted-foreground">
                При оплате любого тарифа: ИИ Продажник + ИИ Юрист + ИИ Бухгалтер бесплатно на 1 месяц
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Plans */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {plans.map((plan) => (
          <Card
            key={plan.id}
            className={cn(
              'glass-card relative',
              plan.popular && 'ring-2 ring-primary'
            )}
          >
            {plan.popular && (
              <div className="absolute -top-3 left-1/2 -translate-x-1/2 px-3 py-1 bg-primary text-primary-foreground text-xs font-medium rounded-full">
                Популярный
              </div>
            )}
            <CardHeader className="pb-2">
              <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center mb-2">
                <plan.icon className="h-5 w-5 text-primary" />
              </div>
              <CardTitle className="text-lg">{plan.name}</CardTitle>
              <p className="text-sm text-muted-foreground">{plan.description}</p>
            </CardHeader>
            <CardContent>
              <div className="mb-4">
                <span className="text-3xl font-bold text-foreground">
                  {plan.price.toLocaleString()}
                </span>
                <span className="text-muted-foreground">₸/мес</span>
              </div>

              <ul className="space-y-2 mb-6">
                {plan.features.map((feature) => (
                  <li key={feature} className="flex items-start gap-2 text-sm">
                    <Check className="h-4 w-4 text-success mt-0.5 shrink-0" />
                    <span className="text-muted-foreground">{feature}</span>
                  </li>
                ))}
              </ul>

              <Button
                className="w-full"
                variant={plan.popular ? 'default' : 'outline'}
                disabled={paymentPending || !user}
                onClick={() => handleSelectPlan(plan.id)}
              >
                {paymentPending && selectedPlan === plan.id ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Обработка...
                  </>
                ) : subscription?.plan === plan.name ? (
                  'Текущий план'
                ) : (
                  'Выбрать'
                )}
              </Button>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Addons */}
      <div>
        <h2 className="text-lg font-semibold mb-4">Дополнительные модули</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {addons.map((addon) => (
            <Card key={addon.id} className="glass-card">
              <CardContent className="pt-6">
                <div className="text-center">
                  <h3 className="font-medium text-foreground">{addon.name}</h3>
                  <p className="text-2xl font-bold text-foreground mt-1">
                    {addon.price.toLocaleString()}
                    <span className="text-sm font-normal text-muted-foreground">₸/мес</span>
                  </p>
                  <p className="text-xs text-muted-foreground mt-1">{addon.description}</p>
                  <Button
                    variant="outline"
                    size="sm"
                    className="mt-4"
                    disabled={!user}
                    onClick={() => handleSelectPlan(addon.id)}
                  >
                    Добавить
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>

      {/* Info */}
      {!user && (
        <Card className="glass-card border-muted">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3 text-muted-foreground">
              <AlertCircle className="h-5 w-5" />
              <p className="text-sm">
                Войдите в аккаунт для оформления подписки
              </p>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
