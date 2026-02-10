'use client'

import Link from 'next/link'
import { usePathname, useRouter } from 'next/navigation'
import { cn } from '@/lib/utils'
import { ThemeToggle } from '@/components/shared/theme-toggle'
import { Button } from '@/components/ui/button'
import { useAuth } from '@/hooks/use-auth'
import {
  LayoutDashboard,
  Users,
  Receipt,
  CreditCard,
  LogOut,
  ArrowLeft,
} from 'lucide-react'

const navigation = [
  { name: 'Главная', href: '/partner', icon: LayoutDashboard },
  { name: 'Клиенты', href: '/partner/leads', icon: Users },
  { name: 'Транзакции', href: '/partner/transactions', icon: Receipt },
  { name: 'Вывод', href: '/partner/payout', icon: CreditCard },
]

export default function PartnerLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const pathname = usePathname()
  const router = useRouter()
  const { user, signOut, loading } = useAuth()

  const handleSignOut = async () => {
    await signOut()
    router.push('/login')
  }

  if (loading) return null

  if (!user) {
    router.push('/login')
    return null
  }

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="glass-header sticky top-0 z-50">
        <div className="container mx-auto px-4 h-14 sm:h-16 flex items-center justify-between">
          <div className="flex items-center gap-2 sm:gap-4 min-w-0">
            <Link
              href="/dashboard"
              className="text-muted-foreground hover:text-foreground shrink-0"
            >
              <ArrowLeft className="h-5 w-5" />
            </Link>
            <span className="font-semibold text-base sm:text-lg truncate">Реферальная программа</span>
          </div>
          <div className="flex items-center gap-2 sm:gap-3 shrink-0">
            <span className="text-sm text-muted-foreground hidden md:block truncate max-w-[200px]">
              {user?.email}
            </span>
            <ThemeToggle />
            <Button variant="ghost" size="icon" onClick={handleSignOut} className="h-8 w-8 sm:h-9 sm:w-9">
              <LogOut className="h-4 w-4" />
            </Button>
          </div>
        </div>
      </header>

      {/* Mobile navigation - horizontal scroll tabs */}
      <div className="lg:hidden border-b bg-background/95 backdrop-blur sticky top-14 sm:top-16 z-40">
        <div className="container mx-auto px-4">
          <nav className="flex gap-1 overflow-x-auto py-2 -mx-4 px-4 scrollbar-hide">
            {navigation.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  'flex items-center gap-1.5 px-3 py-1.5 rounded-full text-sm font-medium whitespace-nowrap transition-colors',
                  pathname === item.href
                    ? 'bg-primary text-primary-foreground'
                    : 'text-muted-foreground hover:text-foreground hover:bg-muted'
                )}
              >
                <item.icon className="h-3.5 w-3.5" />
                {item.name}
              </Link>
            ))}
          </nav>
        </div>
      </div>

      <div className="container mx-auto px-4 py-4 sm:py-6">
        <div className="flex gap-6">
          {/* Desktop sidebar */}
          <aside className="w-56 shrink-0 hidden lg:block">
            <nav className="glass-card p-2 sticky top-24 space-y-1">
              {navigation.map((item) => (
                <Link
                  key={item.href}
                  href={item.href}
                  className={cn(
                    'flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors',
                    pathname === item.href
                      ? 'bg-primary text-primary-foreground'
                      : 'text-muted-foreground hover:text-foreground hover:bg-muted'
                  )}
                >
                  <item.icon className="h-4 w-4" />
                  {item.name}
                </Link>
              ))}
            </nav>
          </aside>

          {/* Main content */}
          <main className="flex-1 min-w-0">{children}</main>
        </div>
      </div>
    </div>
  )
}
