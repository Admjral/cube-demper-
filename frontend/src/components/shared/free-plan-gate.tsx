'use client'

import Link from 'next/link'
import { Lock, Sparkles, Loader2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { useActivateTrial } from '@/hooks/api/use-features'
import { useTranslation } from '@/lib/i18n'

/**
 * FreePlanGate - shown when a free plan user tries to access a restricted page.
 * Prompts them to activate a 3-day trial or view available plans.
 */
export function FreePlanGate() {
  const { t } = useTranslation()
  const activateTrial = useActivateTrial()

  return (
    <div className="flex items-center justify-center py-16">
      <Card className="max-w-md w-full">
        <CardContent className="pt-8 pb-8 text-center space-y-5">
          <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mx-auto">
            <Lock className="h-8 w-8 text-primary" />
          </div>
          <div>
            <h3 className="text-xl font-semibold">{t('freePlan.locked')}</h3>
            <p className="text-sm text-muted-foreground mt-2">
              {t('freePlan.needPlan')}
            </p>
          </div>

          <div className="space-y-3">
            <Button
              className="w-full bg-green-600 hover:bg-green-700 text-white"
              onClick={() => activateTrial.mutate()}
              disabled={activateTrial.isPending}
            >
              {activateTrial.isPending ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Sparkles className="h-4 w-4 mr-2" />
              )}
              {t('freePlan.tryFree')}
            </Button>
            <Link href="/dashboard/billing" className="block">
              <Button variant="outline" className="w-full">
                {t('freePlan.choosePlan')}
              </Button>
            </Link>
          </div>

          {activateTrial.isError && (
            <p className="text-sm text-red-500">
              {(activateTrial.error as any)?.message || t('freePlan.trialError')}
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
