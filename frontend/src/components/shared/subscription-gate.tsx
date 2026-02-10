'use client'

import { ReactNode } from 'react'
import Link from 'next/link'
import { Lock, Loader2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { useFeatures } from '@/hooks/api/use-features'

interface SubscriptionGateProps {
  children: ReactNode
  showLoading?: boolean
}

/**
 * SubscriptionGate - blocks entire page content when user has no active subscription.
 * Use this to wrap feature pages that require any paid plan.
 *
 * Unlike FeatureGate (which checks individual features), this checks
 * `has_active_subscription` - whether the user has ANY active plan at all.
 */
export function SubscriptionGate({ children, showLoading = true }: SubscriptionGateProps) {
  const { data: features, isLoading } = useFeatures()

  if (isLoading) {
    if (!showLoading) return null
    return (
      <div className="flex items-center justify-center py-16">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (features?.has_active_subscription) {
    return <>{children}</>
  }

  return (
    <div className="flex items-center justify-center py-16">
      <Card className="max-w-md w-full">
        <CardContent className="pt-6 text-center space-y-4">
          <Lock className="h-12 w-12 mx-auto text-muted-foreground" />
          <div>
            <h3 className="text-lg font-semibold">Подписка неактивна</h3>
            <p className="text-sm text-muted-foreground mt-1">
              Для доступа к этому разделу необходима активная подписка
            </p>
          </div>
          <Link href="/dashboard/billing">
            <Button>Выбрать тариф</Button>
          </Link>
        </CardContent>
      </Card>
    </div>
  )
}
