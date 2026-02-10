"use client"

import { useEffect } from "react"
import { useRouter, usePathname } from "next/navigation"
import { Sidebar } from "@/components/dashboard/sidebar"
import { Header } from "@/components/dashboard/header"
import { BottomNav } from "@/components/dashboard/bottom-nav"
import { SupportChatWidget } from "@/components/support/chat-widget"
import { SubscriptionBanner } from "@/components/shared/subscription-banner"
import { FreePlanGate } from "@/components/shared/free-plan-gate"
import { useAuth } from "@/hooks/use-auth"
import { useFeatures } from "@/hooks/api/use-features"

// Pages accessible on free plan (no paid subscription needed)
const FREE_PLAN_ALLOWED_PATHS = [
  '/dashboard',
  '/dashboard/billing',
  '/dashboard/profile',
  '/dashboard/settings',
  '/dashboard/integrations',
]

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const router = useRouter()
  const pathname = usePathname()
  const { user, loading } = useAuth()
  const { data: features, isLoading: featuresLoading } = useFeatures()

  // Redirect unverified phone users to verification page
  // Only for users with phone set (new users), not legacy users (phone=null)
  useEffect(() => {
    if (!loading && user && user.phone && !user.phone_verified) {
      router.push('/verify-phone')
    }
  }, [user, loading, router])

  // Check if user is on free plan and trying to access restricted page
  const isFreePlan = !featuresLoading && features && (features.plan_code === 'free' || (!features.plan_code && features.features?.length === 0))
  const isAllowedPath = FREE_PLAN_ALLOWED_PATHS.some(p => pathname === p)
  const showFreePlanGate = isFreePlan && !isAllowedPath

  return (
    <div className="min-h-screen bg-background flex">
      {/* Desktop sidebar */}
      <Sidebar />

      {/* Main content area */}
      <div className="flex-1 flex flex-col min-h-screen">
        {/* Header */}
        <Header />

        {/* Subscription expiration banner */}
        <SubscriptionBanner />

        {/* Page content */}
        <main className="flex-1 p-4 pb-20 lg:pb-4">
          {showFreePlanGate ? <FreePlanGate /> : children}
        </main>

        {/* Mobile bottom navigation */}
        <BottomNav />
      </div>

      {/* Support chat widget */}
      <SupportChatWidget />
    </div>
  )
}
