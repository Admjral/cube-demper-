'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { useFeatures, usePlansV2, useAddons, useActivateTrial } from '@/hooks/api/use-features'
import { useT } from '@/lib/i18n'
import {
  CreditCard,
  Check,
  Loader2,
  AlertCircle,
  MessageSquare,
  BarChart3,
  TrendingUp,
  Clock,
  Sparkles,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { featureLabels, planConfig, getDaysRemaining, getDaysColor, getDaysText } from '@/lib/features'
import { format } from 'date-fns'
import { ru } from 'date-fns/locale'
import { Zap } from 'lucide-react'

export default function BillingPage() {
  const t = useT()
  const { data: features, isLoading: featuresLoading } = useFeatures()
  const { data: plans, isLoading: plansLoading } = usePlansV2()
  const { data: addons, isLoading: addonsLoading } = useAddons()

  const isLoading = featuresLoading || plansLoading || addonsLoading

  const activateTrial = useActivateTrial()

  const handleContactSupport = () => {
    window.open('https://wa.me/77474576759', '_blank')
  }

  const daysRemaining = getDaysRemaining(features?.subscription_ends_at)
  const trialDaysRemaining = getDaysRemaining(features?.trial_ends_at)

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold flex items-center gap-2">
          <CreditCard className="h-6 w-6" />
          {t("billing.title")}
        </h1>
        <p className="text-muted-foreground">
          {t("billing.subtitle")}
        </p>
      </div>

      {isLoading ? (
        <div className="flex justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <>
          {/* Current subscription */}
          {features?.has_active_subscription && features?.plan_code && features.plan_code !== 'free' ? (
            <Card className="glass-card border-primary/20 bg-primary/5">
              <CardContent className="pt-6">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                  <div>
                    <p className="text-sm text-muted-foreground">{t("billing.currentPlan")}</p>
                    <div className="flex items-center gap-2 mt-1">
                      <p className="text-xl font-semibold text-foreground">
                        {features.plan_name || t("billing.plan")}
                      </p>
                      {features.is_trial && (
                        <Badge variant="outline" className="gap-1">
                          {t("billing.trial")}
                        </Badge>
                      )}
                    </div>
                    {features.subscription_ends_at && (
                      <div className="flex items-center gap-2 mt-1">
                        <p className="text-sm text-muted-foreground">
                          {t("billing.activeUntil")} {format(new Date(features.subscription_ends_at), 'd MMMM yyyy', { locale: ru })}
                        </p>
                        {daysRemaining !== null && (
                          <span className={cn("text-sm font-medium flex items-center gap-1", getDaysColor(daysRemaining))}>
                            <Clock className="h-3.5 w-3.5" />
                            {getDaysText(daysRemaining)}
                          </span>
                        )}
                      </div>
                    )}
                    {features.is_trial && features.trial_ends_at && trialDaysRemaining !== null && (
                      <p className="text-sm text-muted-foreground">
                        {t("billing.trialPeriod")} {getDaysText(trialDaysRemaining)} осталось
                      </p>
                    )}
                  </div>
                  <Badge variant="default" className="self-start sm:self-center">
                    {t("billing.active")}
                  </Badge>
                </div>

                {/* Limits */}
                <div className="grid sm:grid-cols-2 gap-4 mt-6">
                  <div className="flex items-center justify-between p-3 bg-muted/50 rounded-lg">
                    <span className="text-sm text-muted-foreground flex items-center gap-2">
                      <BarChart3 className="h-4 w-4" />
                      {t("billing.analytics")}
                    </span>
                    <span className="font-medium text-sm">
                      {features.analytics_limit === -1 ? t("billing.unlimited") : `до ${features.analytics_limit} ${t("common.products")}`}
                    </span>
                  </div>
                  <div className="flex items-center justify-between p-3 bg-muted/50 rounded-lg">
                    <span className="text-sm text-muted-foreground flex items-center gap-2">
                      <TrendingUp className="h-4 w-4" />
                      {t("billing.demping")}
                    </span>
                    <span className="font-medium text-sm">
                      до {features.demping_limit} {t("common.products")}
                    </span>
                  </div>
                </div>

                {/* Features */}
                {(features.features || []).length > 0 && (
                  <div className="mt-4 pt-4 border-t">
                    <p className="text-sm text-muted-foreground mb-2">{t("billing.features")}</p>
                    <div className="flex flex-wrap gap-2">
                      {(features.features || []).map((feature) => (
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
            <Card className="glass-card border-green-500/30 bg-green-500/5">
              <CardContent className="pt-6 pb-6">
                <div className="flex flex-col sm:flex-row items-center justify-between gap-4">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-full bg-green-500/20 flex items-center justify-center">
                      <Sparkles className="h-5 w-5 text-green-500" />
                    </div>
                    <div>
                      <p className="font-medium text-foreground">{t("billing.freePlan")}</p>
                      <p className="text-sm text-muted-foreground">
                        {t("billing.tryFree")}
                      </p>
                    </div>
                  </div>
                  <Button
                    className="bg-green-600 hover:bg-green-700 text-white shrink-0"
                    onClick={() => activateTrial.mutate()}
                    disabled={activateTrial.isPending}
                  >
                    {activateTrial.isPending ? (
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    ) : (
                      <Sparkles className="h-4 w-4 mr-2" />
                    )}
                    Активировать пробный период
                  </Button>
                </div>
                {activateTrial.isError && (
                  <p className="text-sm text-red-500 mt-2">
                    {(activateTrial.error as any)?.message || 'Не удалось активировать пробный период'}
                  </p>
                )}
              </CardContent>
            </Card>
          )}

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
                        {plan.analytics_limit === -1 ? t("billing.unlimited") : plan.analytics_limit} {t("common.products")}
                        <span className="mx-1">•</span>
                        <TrendingUp className="h-4 w-4" />
                        {plan.demping_limit} {t("billing.demping").toLowerCase()}
                      </div>
                    </CardHeader>
                    <CardContent>
                      <div className="mb-4">
                        <span className="text-3xl font-bold text-foreground">
                          {plan.price.toLocaleString()}
                        </span>
                        <span className="text-muted-foreground"> ₸/мес</span>
                      </div>

                      <ul className="space-y-2 mb-6">
                        {(plan.features || []).map((feature) => (
                          <li key={feature} className="flex items-start gap-2 text-sm">
                            <Check className="h-4 w-4 text-green-500 mt-0.5 shrink-0" />
                            <span className="text-muted-foreground">
                              {featureLabels[feature] || feature}
                            </span>
                          </li>
                        ))}
                      </ul>

                      {plan.trial_days > 0 && (!features?.plan_code || features.plan_code === 'free') && (
                        <p className="text-xs text-green-600 mb-4 text-center font-medium">
                          {plan.trial_days} дня бесплатного пробного периода
                        </p>
                      )}

                      <Button
                        className="w-full"
                        variant={isCurrentPlan ? 'secondary' : plan.code === 'standard' ? 'default' : 'outline'}
                        disabled={isCurrentPlan}
                        onClick={handleContactSupport}
                      >
                        {isCurrentPlan ? (
                          t("billing.currentPlan")
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
                const hasAddon = (features?.features || []).some(f =>
                  (addon.features || []).includes(f)
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
                        </div>
                        <div className="text-right">
                          <p className="text-xl font-bold text-foreground">
                            {addon.price.toLocaleString()}
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
        </>
      )}
    </div>
  )
}
