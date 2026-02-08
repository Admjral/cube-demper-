'use client'

import Link from 'next/link'
import { Button } from '@/components/ui/button'
import { Check, Sparkles } from 'lucide-react'
import { cn } from '@/lib/utils'

const plans = [
  {
    name: 'Standard',
    description: 'Базовый тариф для стабильных продаж и теста гипотез',
    price: '21 990',
    period: '/мес',
    features: [
      'Аналитика 500 товаров',
      'Демпинг 50 товаров',
      'Не конкурировать со своими магазинами',
      'Склейка накладных',
      'Управление заказами',
      'Юнит экономика',
      'ИИ юрист',
      'Приоритетная поддержка 24/7',
    ],
    cta: 'Выбрать',
    popular: false,
  },
  {
    name: 'Plus',
    description: 'Тариф для активного роста и масштабирования ассортимента',
    price: '27 990',
    period: '/мес',
    features: [
      'Аналитика 1000 товаров',
      'Демпинг 100 товаров',
      'Предзаказ',
      'Поиск ниш',
      'Авто рассылка',
      'Склейка накладных',
      'Управление заказами',
      'Юнит экономика',
      'ИИ юрист',
      'Приоритетная поддержка 24/7',
    ],
    cta: 'Выбрать',
    popular: true,
  },
  {
    name: 'Ultra',
    description: 'Полный набор инструментов для крупных и сетевых продавцов',
    price: '33 990',
    period: '/мес',
    features: [
      'Аналитика безлимит',
      'Демпинг 200 товаров',
      'Предзаказ',
      'Авто рассылка',
      'Склейка накладных',
      'Управление заказами',
      'Юнит экономика',
      'ИИ юрист',
      'Приоритетная поддержка 24/7',
      'Массовая рассылка',
    ],
    cta: 'Выбрать',
    popular: false,
  },
]

const addons = [
  {
    name: 'ИИ продажник',
    price: '15 000',
    description: 'ИИ-ассистент для продаж и общения с клиентами',
  },
  {
    name: 'Демпинг (каждые 100 товаров)',
    price: '10 000',
    description: 'Дополнительный пакет демпинга на каждые 100 товаров',
  },
  {
    name: 'Предзаказ',
    price: '10 000',
    description: 'Модуль управления предзаказами',
  },
  {
    name: 'WhatsApp рассылка',
    price: '15 000',
    description: 'Модуль рассылок и коммуникаций в WhatsApp',
  },
  {
    name: 'Аналитика безлимит',
    price: '20 000',
    description: 'Безлимитная аналитика по всем товарам',
  },
]

export function Pricing() {
  return (
    <section id="pricing" className="py-20 md:py-32 bg-muted/30">
      <div className="container mx-auto px-4">
        <div className="text-center max-w-2xl mx-auto mb-16">
          <h2 className="text-3xl md:text-4xl font-bold text-foreground mb-4">
            Простые и понятные тарифы
          </h2>
          <p className="text-lg text-muted-foreground">
            Выберите план, который подходит вашему бизнесу. Все цены в тенге.
          </p>
        </div>

        {/* Promo banner */}
        <div className="glass-card p-4 mb-12 max-w-2xl mx-auto text-center bg-primary/5 border-primary/20">
          <div className="flex items-center justify-center gap-2 text-primary font-medium">
            <Sparkles className="h-5 w-5" />
            <span>Бонус для первых пользователей</span>
          </div>
          <p className="text-sm text-muted-foreground mt-1">
            При оплате любого тарифа: ИИ Продажник + ИИ Юрист бесплатно на 1 месяц
          </p>
        </div>

        {/* Main plans */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-16">
          {plans.map((plan) => (
            <div
              key={plan.name}
              className={cn(
                'glass-card p-6 relative',
                plan.popular && 'ring-2 ring-primary'
              )}
            >
              {plan.popular && (
                <div className="absolute -top-3 left-1/2 -translate-x-1/2 px-3 py-1 bg-primary text-primary-foreground text-xs font-medium rounded-full">
                  Популярный
                </div>
              )}

              <div className="mb-4">
                <h3 className="text-lg font-semibold text-foreground">{plan.name}</h3>
                <p className="text-sm text-muted-foreground">{plan.description}</p>
              </div>

              <div className="mb-6">
                <span className="text-3xl font-bold text-foreground">{plan.price}</span>
                <span className="text-muted-foreground">₸{plan.period}</span>
              </div>

              <ul className="space-y-3 mb-6">
                {plan.features.map((feature) => (
                  <li key={feature} className="flex items-start gap-2 text-sm">
                    <Check className="h-4 w-4 text-success mt-0.5 shrink-0" />
                    <span className="text-muted-foreground">{feature}</span>
                  </li>
                ))}
              </ul>

              <Link href="/register">
                <Button
                  className="w-full"
                  variant={plan.popular ? 'default' : 'outline'}
                >
                  {plan.cta}
                </Button>
              </Link>
            </div>
          ))}
        </div>

        {/* Addons */}
        <div className="max-w-3xl mx-auto">
          <h3 className="text-xl font-semibold text-foreground text-center mb-6">
            Дополнительные модули
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {addons.map((addon) => (
              <div key={addon.name} className="glass-card p-4 text-center">
                <h4 className="font-medium text-foreground">{addon.name}</h4>
                <p className="text-2xl font-bold text-foreground mt-1">
                  {addon.price}
                  <span className="text-sm font-normal text-muted-foreground">₸</span>
                </p>
                <p className="text-xs text-muted-foreground mt-1">{addon.description}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}
