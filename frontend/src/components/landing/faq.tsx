'use client'

import { useState } from 'react'
import { ChevronDown } from 'lucide-react'
import { cn } from '@/lib/utils'

const faqs = [
  {
    question: 'Как подключить мой магазин Kaspi?',
    answer: 'После регистрации в личном кабинете перейдите в раздел "Интеграции" и введите данные от вашего аккаунта продавца Kaspi. Подключение занимает несколько минут.',
  },
  {
    question: 'Как работает Price Bot?',
    answer: 'Price Bot автоматически отслеживает цены конкурентов на ваши товары и корректирует ваши цены согласно заданной стратегии. Вы можете установить минимальную и максимальную цену, а также шаг изменения.',
  },
  {
    question: 'Безопасно ли подключать мой аккаунт?',
    answer: 'Да, мы используем защищённое соединение и не храним ваши пароли в открытом виде. Все данные передаются по зашифрованному каналу.',
  },
  {
    question: 'Можно ли подключить несколько магазинов?',
    answer: 'Да, вы можете подключить неограниченное количество магазинов Kaspi к одному аккаунту Demper и управлять ими из единого дашборда.',
  },
  {
    question: 'Есть ли тестовый период?',
    answer: 'Да, для новых пользователей доступен бесплатный период, в течение которого вы можете протестировать основные функции платформы.',
  },
  {
    question: 'Как работают ИИ-ассистенты?',
    answer: 'ИИ-ассистенты (Юрист, Бухгалтер, Продажник) используют современные языковые модели, обученные на казахстанском законодательстве и бизнес-практиках. Они помогают с консультациями и автоматизацией.',
  },
]

export function FAQ() {
  const [openIndex, setOpenIndex] = useState<number | null>(null)

  return (
    <section id="faq" className="py-20 md:py-32">
      <div className="container mx-auto px-4">
        <div className="text-center max-w-2xl mx-auto mb-16">
          <h2 className="text-3xl md:text-4xl font-bold text-foreground mb-4">
            Часто задаваемые вопросы
          </h2>
          <p className="text-lg text-muted-foreground">
            Ответы на популярные вопросы о платформе Demper
          </p>
        </div>

        <div className="max-w-3xl mx-auto space-y-4">
          {faqs.map((faq, index) => (
            <div
              key={index}
              className="glass-card overflow-hidden"
            >
              <button
                className="w-full p-4 text-left flex items-center justify-between gap-4"
                onClick={() => setOpenIndex(openIndex === index ? null : index)}
              >
                <span className="font-medium text-foreground">{faq.question}</span>
                <ChevronDown
                  className={cn(
                    'h-5 w-5 text-muted-foreground shrink-0 transition-transform',
                    openIndex === index && 'rotate-180'
                  )}
                />
              </button>
              <div
                className={cn(
                  'overflow-hidden transition-all duration-300',
                  openIndex === index ? 'max-h-96' : 'max-h-0'
                )}
              >
                <p className="px-4 pb-4 text-muted-foreground">
                  {faq.answer}
                </p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
