'use client'

import { ReactNode } from 'react'
import Link from 'next/link'
import { Lock, Loader2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useFeatures, getFeatureUpgradeMessage } from '@/hooks/api/use-features'

interface FeatureGateProps {
  /** The feature code to check access for */
  feature: string
  /** Content to show if user has access */
  children: ReactNode
  /** Optional custom fallback content */
  fallback?: ReactNode
  /** Whether to show the upgrade prompt (default: true) */
  showUpgradePrompt?: boolean
  /** Whether to show loading state (default: true) */
  showLoading?: boolean
  /** Optional custom loading component */
  loadingComponent?: ReactNode
}

/**
 * FeatureGate component - Conditionally renders content based on user's subscription features.
 *
 * Usage:
 * ```tsx
 * <FeatureGate feature="preorder">
 *   <PreorderContent />
 * </FeatureGate>
 * ```
 */
export function FeatureGate({
  feature,
  children,
  fallback,
  showUpgradePrompt = true,
  showLoading = true,
  loadingComponent,
}: FeatureGateProps) {
  const { data: features, isLoading } = useFeatures()

  // Loading state
  if (isLoading) {
    if (!showLoading) return null

    if (loadingComponent) {
      return <>{loadingComponent}</>
    }

    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    )
  }

  // Check access
  const hasAccess = features?.features?.includes(feature) ?? false

  if (hasAccess) {
    return <>{children}</>
  }

  // Custom fallback
  if (fallback) {
    return <>{fallback}</>
  }

  // No upgrade prompt - just hide
  if (!showUpgradePrompt) {
    return null
  }

  // Default upgrade prompt
  const upgradeMessage = getFeatureUpgradeMessage(feature)

  return (
    <div className="border border-dashed border-muted-foreground/25 rounded-lg p-6 text-center bg-muted/30">
      <Lock className="h-8 w-8 mx-auto text-muted-foreground mb-3" />
      <p className="text-sm text-muted-foreground mb-4">
        {upgradeMessage}
      </p>
      <Link href="/dashboard/billing">
        <Button variant="outline" size="sm">
          Выбрать тариф
        </Button>
      </Link>
    </div>
  )
}

/**
 * Higher-Order Component version of FeatureGate.
 * Wraps an entire component with feature access check.
 *
 * Usage:
 * ```tsx
 * const ProtectedComponent = withFeatureGate('preorder', MyComponent)
 * ```
 */
export function withFeatureGate<P extends object>(
  feature: string,
  WrappedComponent: React.ComponentType<P>
) {
  return function FeatureGatedComponent(props: P) {
    return (
      <FeatureGate feature={feature}>
        <WrappedComponent {...props} />
      </FeatureGate>
    )
  }
}

/**
 * Component to show current usage limits with progress bar.
 */
interface LimitIndicatorProps {
  type: 'analytics' | 'demping'
  currentCount: number
  className?: string
}

export function LimitIndicator({ type, currentCount, className }: LimitIndicatorProps) {
  const { data: features } = useFeatures()

  if (!features) return null

  const limit = type === 'analytics' ? features.analytics_limit : features.demping_limit
  const label = type === 'analytics' ? 'Аналитика' : 'Демпинг'

  // Unlimited
  if (limit === -1) {
    return (
      <div className={className}>
        <span className="text-xs text-muted-foreground">
          {label}: {currentCount} / Безлимит
        </span>
      </div>
    )
  }

  const percentage = Math.min((currentCount / limit) * 100, 100)
  const isNearLimit = percentage >= 80
  const isAtLimit = percentage >= 100

  return (
    <div className={className}>
      <div className="flex justify-between text-xs mb-1">
        <span className="text-muted-foreground">{label}</span>
        <span className={
          isAtLimit ? 'text-destructive' :
          isNearLimit ? 'text-yellow-600 dark:text-yellow-500' :
          'text-muted-foreground'
        }>
          {currentCount} / {limit}
        </span>
      </div>
      <div className="h-1.5 bg-muted rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all ${
            isAtLimit ? 'bg-destructive' :
            isNearLimit ? 'bg-yellow-500' :
            'bg-primary'
          }`}
          style={{ width: `${percentage}%` }}
        />
      </div>
    </div>
  )
}
