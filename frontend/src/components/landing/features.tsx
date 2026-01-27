import {
  Bot,
  BarChart3,
  Calculator,
  Package,
  MessageSquare,
  Link2,
  Scale,
  Users,
} from 'lucide-react'

const features = [
  {
    icon: Bot,
    title: 'Price Bot',
    description: 'Автоматический мониторинг цен конкурентов и демпинг. Настройте стратегию и бот будет поддерживать лучшую цену 24/7.',
  },
  {
    icon: BarChart3,
    title: 'Аналитика продаж',
    description: 'Детальная статистика по продажам, выручке и трендам. Топ товаров, графики по периодам.',
  },
  {
    icon: Calculator,
    title: 'Юнит-экономика',
    description: 'Расчёт маржинальности каждого товара с учётом всех затрат: комиссий, логистики, налогов.',
  },
  {
    icon: Package,
    title: 'Предзаказы',
    description: 'Управление предзаказами, контроль поставок и сроков. Автоматические уведомления клиентам.',
  },
  {
    icon: MessageSquare,
    title: 'WhatsApp рассылки',
    description: 'Интеграция с WhatsApp для массовых рассылок клиентам. Шаблоны сообщений, автоответы.',
  },
  {
    icon: Link2,
    title: 'Интеграции',
    description: 'Подключение нескольких магазинов Kaspi. Единый дашборд для всех ваших точек продаж.',
  },
  {
    icon: Scale,
    title: 'ИИ Юрист',
    description: 'Консультации по договорам, спорам с покупателями. Знает законодательство Казахстана.',
  },
  {
    icon: Users,
    title: 'ИИ Бухгалтер',
    description: 'Ответы на вопросы по налогам, отчётности и расчётам для ИП и ТОО в Казахстане.',
  },
]

export function Features() {
  return (
    <section id="features" className="py-20 md:py-32">
      <div className="container mx-auto px-4">
        <div className="text-center max-w-2xl mx-auto mb-16">
          <h2 className="text-3xl md:text-4xl font-bold text-foreground mb-4">
            Всё для успешных продаж
          </h2>
          <p className="text-lg text-muted-foreground">
            Полный набор инструментов для автоматизации и роста вашего бизнеса на Kaspi
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {features.map((feature) => (
            <div
              key={feature.title}
              className="glass-card p-6 glass-hover"
            >
              <div className="w-12 h-12 rounded-xl bg-primary/10 flex items-center justify-center mb-4">
                <feature.icon className="h-6 w-6 text-primary" />
              </div>
              <h3 className="text-lg font-semibold text-foreground mb-2">
                {feature.title}
              </h3>
              <p className="text-sm text-muted-foreground">
                {feature.description}
              </p>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
