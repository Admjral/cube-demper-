'use client'

import Link from 'next/link'
import { AlertTriangle, XCircle } from 'lucide-react'
import { useFeatures } from '@/hooks/api/use-features'
import { getDaysRemaining } from '@/lib/features'
import { useTranslation } from '@/lib/i18n'

/**
 * Banner that shows subscription expiration warnings in the dashboard layout.
 * - Yellow when <= 3 days remaining
 * - Red when expired
 * - Hidden otherwise
 */
export function SubscriptionBanner() {
  const { t } = useTranslation()
  const { data: features } = useFeatures()

  if (!features) return null

  // Only show for users who have/had a paid plan (not free)
  if (!features.plan_code || features.plan_code === 'free') return null

  const dateStr = features.is_trial ? features.trial_ends_at : features.subscription_ends_at
  const days = getDaysRemaining(dateStr)

  if (days === null) return null

  // Expired
  if (days <= 0) {
    return (
      <div className="bg-red-500/10 border-b border-red-500/20 px-4 py-2">
        <div className="flex items-center justify-center gap-2 text-sm text-red-600 dark:text-red-400">
          <XCircle className="h-4 w-4 shrink-0" />
          <span>
            {features.is_trial ? t('sub.trialExpired') : t('sub.subscriptionExpired')}{' '}
            {t('sub.accessLimited')}
          </span>
          <Link href="/dashboard/billing" className="underline font-medium ml-1">
            {t('sub.renew')}
          </Link>
        </div>
      </div>
    )
  }

  // Warning: 3 days or less
  if (days <= 3) {
    return (
      <div className="bg-yellow-500/10 border-b border-yellow-500/20 px-4 py-2">
        <div className="flex items-center justify-center gap-2 text-sm text-yellow-700 dark:text-yellow-400">
          <AlertTriangle className="h-4 w-4 shrink-0" />
          <span>
            {features.is_trial ? t('sub.trial') : t('sub.subscription')}{' '}
            {t('sub.expiresIn')}{' '}{days === 1 ? t('sub.day1') : `${days} ${t('sub.days')}`}.
          </span>
          <Link href="/dashboard/billing" className="underline font-medium ml-1">
            {t('sub.renew')}
          </Link>
        </div>
      </div>
    )
  }

  return null
}
