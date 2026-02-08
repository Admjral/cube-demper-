import {
  Bot,
  BarChart3,
  Calculator,
  Package,
  MessageSquare,
  Link2,
  Scale,
} from 'lucide-react'

const features = [
  {
    icon: Bot,
    title: 'Демпинг‑бот',
    description:
      'Умный бот сам следит за конкурентами и двигает цену в рамках вашей маржи. Вы задаёте стратегию — бот 24/7 удерживает нужные позиции без ручных правок.',
  },
  {
    icon: BarChart3,
    title: 'Аналитика продаж и ниш',
    description:
      'Сводка по выручке, прибыли и оборачиваемости, топ‑товары и провалы. Быстро видно, какие категории тянут бизнес вверх, а какие «съедают» оборот.',
  },
  {
    icon: Calculator,
    title: 'Юнит‑экономика по каждому товару',
    description:
      'Автоматический учёт комиссий Kaspi, логистики, НДС и скидок. Показываем реальную прибыль по SKU, чтобы вы не продавали «в ноль» или в минус.',
  },
  {
    icon: Package,
    title: 'Гибкое управление предзаказами',
    description:
      'Управление предзаказами и поставками: когда привезти, сколько штук и что уже невыгодно заказывать. Меньше штрафов и срывов сроков для клиентов.',
  },
  {
    icon: MessageSquare,
    title: 'WhatsApp и массовые рассылки',
    description:
      'Теплые напоминания, допродажи и автоматические сообщения после заказа. Выстраивайте повторные покупки и собирайте отзывы без ручной рутины.',
  },
  {
    icon: Link2,
    title: 'Интеграция всех магазинов Kaspi',
    description:
      'Несколько магазинов — один кабинет. Объединённая аналитика, единые настройки демпинга и заказов, чтобы управлять сетью как одним бизнесом.',
  },
  {
    icon: Scale,
    title: 'ИИ‑юрист для селлеров',
    description:
      'Подскажет, как реагировать на претензии покупателей, спорные заказы и штрафы. Отвечает с учётом казахстанского законодательства и правил Kaspi. Также поможет с вопросами по налогам и отчётности для ИП/ТОО.',
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
