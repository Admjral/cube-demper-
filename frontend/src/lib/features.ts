import { Zap, Crown, Sparkles } from 'lucide-react'

export const featureLabels: Record<string, string> = {
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

export const planConfig: Record<string, { icon: typeof Zap; color: string }> = {
  free: { icon: Zap, color: 'text-gray-400' },
  basic: { icon: Zap, color: 'text-blue-500' },
  standard: { icon: Crown, color: 'text-amber-500' },
  premium: { icon: Sparkles, color: 'text-purple-500' },
}

/** Returns days remaining until date. Positive = days left, negative = overdue, null = no date. */
export function getDaysRemaining(dateStr: string | null | undefined): number | null {
  if (!dateStr) return null
  const diff = new Date(dateStr).getTime() - Date.now()
  return Math.ceil(diff / (1000 * 60 * 60 * 24))
}

/** Color class for days remaining badge */
export function getDaysColor(days: number): string {
  if (days <= 0) return 'text-red-500'
  if (days <= 3) return 'text-red-500'
  if (days <= 7) return 'text-yellow-500'
  return 'text-green-500'
}

/** Human-readable days remaining text */
export function getDaysText(days: number): string {
  if (days <= 0) return 'Истекла'
  if (days === 1) return '1 день'
  if (days >= 2 && days <= 4) return `${days} дня`
  return `${days} дней`
}
