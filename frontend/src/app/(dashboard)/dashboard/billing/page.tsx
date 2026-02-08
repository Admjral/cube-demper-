'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { useFeatures, usePlansV2, useAddons } from '@/hooks/api/use-features'
import { useAuth } from '@/hooks/use-auth'
import {
  CreditCard,
  Check,
  Sparkles,
  Crown,
  Zap,
  Loader2,
  AlertCircle,
  MessageSquare,
  BarChart3,
  ShoppingCart,
  Bot,
  Package,
  TrendingUp,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { format } from 'date-fns'
import { ru } from 'date-fns/locale'

// Feature labels for display
const featureLabels: Record<string, string> = {
  analytics: 'Аналитика',
  demping: 'Демпинг',
  exclude_own_stores: 'Не конкурировать со своими',
  invoice_glue: 'Склейка накладных',
  orders_view: 'Просмотр заказов',
  unit_economics: 'Юнит экономика',
  ai_lawyer: 'ИИ юрист',
  priority_support: 'Приоритетная поддержка 24/7',
  preorder: 'Предзаказы',
  whatsapp_auto: 'Авто рассылка WhatsApp',
  whatsapp_bulk: 'Массовая рассылка WhatsApp',
  ai_salesman: 'ИИ продажник',
}

// Plan icons and styles
const planConfig: Record<string, { icon: typeof Zap; color: string }> = {
  basic: { icon: Zap, color: 'text-blue-500' },
  standard: { icon: Crown, color: 'text-amber-500' },
  premium: { icon: Sparkles, color: 'text-purple-500' },
}

export default function BillingPage() {
  const { user } = useAuth()
  const { data: features, isLoading: featuresLoading } = useFeatures()
  const { data: plans, isLoading: plansLoading } = usePlansV2()
  const { data: addons, isLoading: addonsLoading } = useAddons()

  const isLoading = featuresLoading || plansLoading || addonsLoading

  const handleContactSupport = () => {
    // Open support widget or redirect to contact
    window.open('https://wa.me/77771234567', '_blank')
  }

  // Calculate limit usage (mock values - in production, get from backend)
  const analyticsUsed = 0 // TODO: get from backend
  const dempingUsed = 0 // TODO: get from backend

  const getLimitDisplay = (limit: number) => {
    return limit === -1 ? 'Безлимит' : limit.toString()
  }

  const getLimitProgress = (used: number, limit: number) => {
    if (limit === -1) return 0
    return Math.min((used / limit) * 100, 100)
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

      {isLoading ? (
        <div className="flex justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <>
          {/* Current subscription */}
          {features?.has_active_subscription ? (
            <Card className="glass-card border-primary/20 bg-primary/5">
              <CardContent className="pt-6">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                  <div>
                    <p className="text-sm text-muted-foreground">Текущий план</p>
                    <div className="flex items-center gap-2 mt-1">
                      <p className="text-xl font-semibold text-foreground">
                        {features.plan_name || 'Нет подписки'}
                      </p>
                      {features.is_trial && (
                        <Badge variant="outline" className="gap-1">
                          <Sparkles className="h-3 w-3" />
                          Пробный период
                        </Badge>
                      )}
                    </div>
                    {features.subscription_ends_at && (
                      <p className="text-sm text-muted-foreground mt-1">
                        Активен до {format(new Date(features.subscription_ends_at), 'd MMMM yyyy', { locale: ru })}
                      </p>
                    )}
                    {features.is_trial && features.trial_ends_at && (
                      <p className="text-sm text-muted-foreground">
                        Пробный период до {format(new Date(features.trial_ends_at), 'd MMMM yyyy', { locale: ru })}
                      </p>
                    )}
                  </div>
                  <Badge variant="default" className="self-start sm:self-center">
                    Активна
                  </Badge>
                </div>

                {/* Limits */}
                <div className="grid sm:grid-cols-2 gap-4 mt-6">
                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span className="text-muted-foreground flex items-center gap-2">
                        <BarChart3 className="h-4 w-4" />
                        Аналитика
                      </span>
                      <span className="font-medium">
                        {analyticsUsed} / {getLimitDisplay(features.analytics_limit)}
                      </span>
                    </div>
                    {features.analytics_limit !== -1 && (
                      <div className="h-2 bg-muted rounded-full overflow-hidden">
                        <div
                          className="h-full bg-primary rounded-full transition-all"
                          style={{ width: `${getLimitProgress(analyticsUsed, features.analytics_limit)}%` }}
                        />
                      </div>
                    )}
                  </div>
                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span className="text-muted-foreground flex items-center gap-2">
                        <TrendingUp className="h-4 w-4" />
                        Демпинг
                      </span>
                      <span className="font-medium">
                        {dempingUsed} / {features.demping_limit}
                      </span>
                    </div>
                    <div className="h-2 bg-muted rounded-full overflow-hidden">
                      <div
                        className={cn(
                          "h-full rounded-full transition-all",
                          getLimitProgress(dempingUsed, features.demping_limit) >= 80
                            ? "bg-yellow-500"
                            : "bg-primary"
                        )}
                        style={{ width: `${getLimitProgress(dempingUsed, features.demping_limit)}%` }}
                      />
                    </div>
                  </div>
                </div>

                {/* Features */}
                {features.features.length > 0 && (
                  <div className="mt-6 pt-4 border-t">
                    <p className="text-sm text-muted-foreground mb-3">Доступные функции:</p>
                    <div className="flex flex-wrap gap-2">
                      {features.features.map((feature) => (
                        <Badge key={feature} variant="secondary">
                          {featureLabels[feature] || feature}
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          ) : (
            <Card className="glass-card border-yellow-500/20 bg-yellow-500/5">
              <CardContent className="pt-6">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-full bg-yellow-500/20 flex items-center justify-center">
                    <AlertCircle className="h-5 w-5 text-yellow-500" />
                  </div>
                  <div>
                    <p className="font-medium text-foreground">Нет активной подписки</p>
                    <p className="text-sm text-muted-foreground">
                      Свяжитесь с поддержкой для оформления подписки
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Promo banner */}
          <Card className="glass-card border-green-500/20 bg-green-500/5">
            <CardContent className="pt-6">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-full bg-green-500/20 flex items-center justify-center">
                  <Sparkles className="h-5 w-5 text-green-500" />
                </div>
                <div>
                  <p className="font-medium text-foreground">Бета-тест бесплатно!</p>
                  <p className="text-sm text-muted-foreground">
                    Попробуйте тариф Базовый бесплатно в течение 3 дней
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Plans */}
          <div>
            <h2 className="text-lg font-semibold mb-4">Тарифные планы</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {plans?.map((plan) => {
                const config = planConfig[plan.code] || { icon: Zap, color: 'text-primary' }
                const Icon = config.icon
                const isCurrentPlan = features?.plan_code === plan.code

                return (
                  <Card
                    key={plan.id}
                    className={cn(
                      'glass-card relative',
                      plan.code === 'standard' && 'ring-2 ring-primary',
                      isCurrentPlan && 'border-primary bg-primary/5'
                    )}
                  >
                    {plan.code === 'standard' && (
                      <div className="absolute -top-3 left-1/2 -translate-x-1/2 px-3 py-1 bg-primary text-primary-foreground text-xs font-medium rounded-full">
                        Популярный
                      </div>
                    )}
                    <CardHeader className="pb-2">
                      <div className={cn("w-10 h-10 rounded-xl bg-muted flex items-center justify-center mb-2")}>
                        <Icon className={cn("h-5 w-5", config.color)} />
                      </div>
                      <CardTitle className="text-lg">{plan.name}</CardTitle>
                      <div className="flex items-center gap-2 text-sm text-muted-foreground">
                        <BarChart3 className="h-4 w-4" />
                        {plan.analytics_limit === -1 ? 'Безлимит' : plan.analytics_limit} товаров
                        <span className="mx-1">•</span>
                        <TrendingUp className="h-4 w-4" />
                        {plan.demping_limit} демпинг
                      </div>
                    </CardHeader>
                    <CardContent>
                      <div className="mb-4">
                        <span className="text-3xl font-bold text-foreground">
                          {(plan.price / 100).toLocaleString()}
                        </span>
                        <span className="text-muted-foreground"> ₸/мес</span>
                      </div>

                      <ul className="space-y-2 mb-6">
                        {plan.features.map((feature) => (
                          <li key={feature} className="flex items-start gap-2 text-sm">
                            <Check className="h-4 w-4 text-green-500 mt-0.5 shrink-0" />
                            <span className="text-muted-foreground">
                              {featureLabels[feature] || feature}
                            </span>
                          </li>
                        ))}
                      </ul>

                      {plan.trial_days > 0 && (
                        <p className="text-xs text-muted-foreground mb-4 text-center">
                          {plan.trial_days} дней бесплатно
                        </p>
                      )}

                      <Button
                        className="w-full"
                        variant={isCurrentPlan ? 'secondary' : plan.code === 'standard' ? 'default' : 'outline'}
                        disabled={isCurrentPlan}
                        onClick={handleContactSupport}
                      >
                        {isCurrentPlan ? (
                          'Текущий план'
                        ) : (
                          <>
                            <MessageSquare className="h-4 w-4 mr-2" />
                            Связаться
                          </>
                        )}
                      </Button>
                    </CardContent>
                  </Card>
                )
              })}
            </div>
          </div>

          {/* Addons */}
          <div>
            <h2 className="text-lg font-semibold mb-4">Дополнительные услуги</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {addons?.map((addon) => {
                const hasAddon = features?.features.some(f =>
                  addon.features.includes(f)
                )

                return (
                  <Card key={addon.id} className={cn("glass-card", hasAddon && "border-green-500/30")}>
                    <CardContent className="pt-6">
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1">
                          <div className="flex items-center gap-2">
                            <h3 className="font-medium text-foreground">{addon.name}</h3>
                            {hasAddon && (
                              <Badge variant="default" className="bg-green-500">
                                <Check className="h-3 w-3 mr-1" />
                                Активно
                              </Badge>
                            )}
                          </div>
                          <p className="text-sm text-muted-foreground mt-1">
                            {addon.description}
                          </p>
                          {addon.stackable && (
                            <Badge variant="outline" className="mt-2 text-xs">
                              <Package className="h-3 w-3 mr-1" />
                              Можно покупать несколько раз
                            </Badge>
                          )}
                        </div>
                        <div className="text-right">
                          <p className="text-xl font-bold text-foreground">
                            {(addon.price / 100).toLocaleString()}
                          </p>
                          <span className="text-xs text-muted-foreground">₸/мес</span>
                        </div>
                      </div>
                      {!hasAddon && (
                        <Button
                          variant="outline"
                          size="sm"
                          className="w-full mt-4"
                          onClick={handleContactSupport}
                        >
                          <MessageSquare className="h-4 w-4 mr-2" />
                          Подключить
                        </Button>
                      )}
                    </CardContent>
                  </Card>
                )
              })}
            </div>
          </div>

          {/* Contact info */}
          <Card className="glass-card">
            <CardContent className="pt-6">
              <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
                <div>
                  <h3 className="font-medium text-foreground">Как оформить подписку?</h3>
                  <p className="text-sm text-muted-foreground mt-1">
                    Свяжитесь с нами в WhatsApp для оформления подписки или активации бета-теста
                  </p>
                </div>
                <Button onClick={handleContactSupport}>
                  <MessageSquare className="h-4 w-4 mr-2" />
                  Написать в WhatsApp
                </Button>
              </div>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  )
}
