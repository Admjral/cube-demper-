"use client"

import { Sidebar } from "@/components/dashboard/sidebar"
import { Header } from "@/components/dashboard/header"
import { BottomNav } from "@/components/dashboard/bottom-nav"
import { SupportChatWidget } from "@/components/support/chat-widget"

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <div className="min-h-screen bg-background flex">
      {/* Desktop sidebar */}
      <Sidebar />

      {/* Main content area */}
      <div className="flex-1 flex flex-col min-h-screen">
        {/* Header */}
        <Header />

        {/* Page content */}
        <main className="flex-1 p-4 pb-20 lg:pb-4">
          {children}
        </main>

        {/* Mobile bottom navigation */}
        <BottomNav />
      </div>

      {/* Support chat widget */}
      <SupportChatWidget />
    </div>
  )
}
