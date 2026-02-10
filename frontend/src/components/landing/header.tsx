'use client'

import Image from 'next/image'
import Link from 'next/link'
import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { ThemeToggle } from '@/components/shared/theme-toggle'
import { Menu, X } from 'lucide-react'
import { useT } from '@/lib/i18n'

export function Header() {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)
  const t = useT()

  const navigation = [
    { name: t("landing.features"), href: '#features' },
    { name: t("landing.pricing"), href: '#pricing' },
    { name: t("landing.faq"), href: '#faq' },
  ]

  return (
    <header className="fixed top-0 left-0 right-0 z-50 glass-header">
      <nav className="container mx-auto px-4 h-16 flex items-center justify-between">
        {/* Logo */}
        <Link href="/" className="flex items-center">
          <Image src="/logodark.svg" alt="Demper" width={120} height={42} className="h-8 w-auto dark:hidden" priority />
          <Image src="/logowhite.svg" alt="Demper" width={120} height={42} className="h-8 w-auto hidden dark:block" priority />
        </Link>

        {/* Desktop Navigation */}
        <div className="hidden md:flex items-center gap-8">
          {navigation.map((item) => (
            <a
              key={item.name}
              href={item.href}
              className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors"
            >
              {item.name}
            </a>
          ))}
        </div>

        {/* Desktop Actions */}
        <div className="hidden md:flex items-center gap-4">
          <ThemeToggle />
          <Link href="/login">
            <Button variant="ghost" size="sm">
              {t("landing.signIn")}
            </Button>
          </Link>
          <Link href="/register">
            <Button size="sm">
              {t("landing.startFree")}
            </Button>
          </Link>
        </div>

        {/* Mobile menu button */}
        <div className="flex md:hidden items-center gap-2">
          <ThemeToggle />
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
          >
            {mobileMenuOpen ? (
              <X className="h-5 w-5" />
            ) : (
              <Menu className="h-5 w-5" />
            )}
          </Button>
        </div>
      </nav>

      {/* Mobile menu */}
      {mobileMenuOpen && (
        <div className="md:hidden glass-card mx-4 mb-4 p-4 animate-fade-in">
          <div className="flex flex-col gap-4">
            {navigation.map((item) => (
              <a
                key={item.name}
                href={item.href}
                className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors py-2"
                onClick={() => setMobileMenuOpen(false)}
              >
                {item.name}
              </a>
            ))}
            <hr className="border-border" />
            <Link href="/login" onClick={() => setMobileMenuOpen(false)}>
              <Button variant="ghost" className="w-full justify-start">
                {t("landing.signIn")}
              </Button>
            </Link>
            <Link href="/register" onClick={() => setMobileMenuOpen(false)}>
              <Button className="w-full">
                {t("landing.startFree")}
              </Button>
            </Link>
          </div>
        </div>
      )}
    </header>
  )
}
