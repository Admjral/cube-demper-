import { Check, ArrowRight } from 'lucide-react'

const modules = [
  {
    id: 'demping',
    label: 'Демпинг‑бот',
    title: 'Цены под контролем бота, а не конкурентов',
    before: [
      'Ручные изменения цен по десяткам товаров каждый день',
      'Поздние реакции на агрессивный демпинг конкурентов',
      'Страх «улететь в минус» при любой скидке',
    ],
    after: [
      'Бот сам снижает и поднимает цены в заданных границах маржи',
      'Гибкие стратегии: держаться в топ‑3, не конкурировать со своими магазинами и др.',
      'Прозрачная история изменений цен и влияния на продажи',
    ],
  },
  {
    id: 'analytics',
    label: 'Аналитика и поиск ниш',
    title: 'Понимаете, где зарабатываете, а где теряете деньги',
    before: [
      'Работа вслепую по ощущениям: «кажется, этот товар продаётся»',
      'Ассортимент раздувается невыгодными SKU, которые съедают оборот',
      'Сложно объяснить, почему прибыль ниже, чем ожидалось',
    ],
    after: [
      'Отчёты по выручке, марже и оборачиваемости по каждому товару и категории',
      'Поиск ниш и товаров с высокой маржой и низкой конкуренцией',
      'Рекомендации по тому, какие позиции стоит расширять, а какие закрывать',
    ],
  },
  {
    id: 'unit-economy',
    label: 'Юнит‑экономика',
    title: 'Каждый товар проходит проверку на прибыльность',
    before: [
      'Учитываются только закупка и цена продажи, без логистики и комиссий',
      'Популярные товары могут работать в минус, пока это не видно в цифрах',
      'Решения по скидкам принимаются «на глаз»',
    ],
    after: [
      'Автоматический учёт комиссий Kaspi, доставки, налогов и скидок',
      'Понимание реальной прибыли по каждому SKU ещё до масштабирования',
      'Осознанные решения по акциям, распродажам и демпингу',
    ],
  },
  {
    id: 'whatsapp',
    label: 'Рассылки в WhatsApp',
    title: 'Из единичных покупок — в повторные',
    before: [
      'Клиент купил один раз и пропал, контакты не монетизируются',
      'Отзывы собираются хаотично, их мало и они не влияют на продажи',
      'Ручные сообщения занимают время и плохо масштабируются',
    ],
    after: [
      'Авто‑сообщения после заказа и доставки, напоминания и допродажи',
      'Массовые и целевые кампании по существующей базе в пару кликов',
      'Рост повторных заказов и количества 5‑звёздочных отзывов',
    ],
  },
  {
    id: 'ai-lawyer',
    label: 'ИИ‑юрист',
    title: 'Юридические и налоговые вопросы — без паники и гуглинга',
    before: [
      'Страх перед претензиями, штрафами и сложными кейсами с покупателями',
      'Постоянные вопросы по налогам и отчётности без понятных ответов',
      'Потеря времени на поиск информации в чатах и форумах',
    ],
    after: [
      'Поддержка по договорам, спорам и правилам Kaspi с учётом законодательства РК',
      'Ответы по налогам, режимам и отчётности для ИП и ТОО на простом языке',
      'Быстрые решения типичных ситуаций без ожидания живого консультанта',
    ],
  },
]

export function ModulesDetail() {
  return (
    <section className="py-20 md:py-28 bg-background">
      <div className="container mx-auto px-4">
        <div className="max-w-3xl mx-auto text-center mb-12 md:mb-16">
          <h2 className="text-3xl md:text-4xl font-bold text-foreground mb-4">
            Как модули Cube Demper меняют работу селлера
          </h2>
          <p className="text-lg text-muted-foreground">
            Мы не просто добавляем функции — каждый модуль закрывает конкретную боль селлера на Kaspi:
            от цен и маржи до отзывов и юридических вопросов.
          </p>
        </div>

        <div className="space-y-8">
          {modules.map((module) => (
            <div
              key={module.id}
              className="glass-card p-6 md:p-8 grid gap-6 md:gap-8 md:grid-cols-[minmax(0,1.1fr)_minmax(0,1.1fr)]"
            >
              <div>
                <span className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-primary/10 text-primary text-xs font-semibold mb-3">
                  <ArrowRight className="h-3 w-3" />
                  {module.label}
                </span>
                <h3 className="text-xl md:text-2xl font-semibold text-foreground mb-3">
                  {module.title}
                </h3>
                <p className="text-sm text-muted-foreground">
                  До подключения модуля процессы выглядят так:
                </p>
                <ul className="mt-3 space-y-2 text-sm text-muted-foreground">
                  {module.before.map((item) => (
                    <li key={item} className="flex items-start gap-2">
                      <span className="mt-1 h-1.5 w-1.5 rounded-full bg-muted-foreground/70" />
                      <span>{item}</span>
                    </li>
                  ))}
                </ul>
              </div>

              <div className="rounded-xl border border-primary/20 bg-primary/5 p-4 md:p-5">
                <p className="text-xs font-semibold uppercase tracking-wide text-primary mb-3">
                  После подключения модуля
                </p>
                <ul className="space-y-3">
                  {module.after.map((item) => (
                    <li key={item} className="flex items-start gap-2 text-sm text-foreground">
                      <Check className="h-4 w-4 text-success mt-0.5 shrink-0" />
                      <span>{item}</span>
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

