'use client'

import Image from "next/image"
import { useTheme } from "next-themes"
import { ThemeToggle } from "@/components/shared/theme-toggle"
import { useEffect, useState } from "react"

export default function AuthLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const { resolvedTheme } = useTheme()
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
  }, [])

  return (
    <div className="min-h-screen flex flex-col items-center justify-center bg-background p-4 relative">
      {/* Background gradient */}
      <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-transparent to-primary/5 pointer-events-none" />

      {/* Theme toggle */}
      <div className="absolute top-4 right-4">
        <ThemeToggle />
      </div>

      {/* Logo */}
      <div className="mb-8 flex flex-col items-center">
        {mounted ? (
          <Image
            src={resolvedTheme === 'dark' ? '/logowhite.svg' : '/logodark.svg'}
            alt="Demper"
            width={180}
            height={62}
            className="h-12 w-auto"
            priority
          />
        ) : (
          <div className="h-12 w-[180px]" />
        )}
        <p className="text-muted-foreground mt-2">Kaspi Seller Panel</p>
      </div>

      {/* Auth card */}
      <div className="w-full max-w-md">
        {children}
      </div>

      {/* Footer */}
      <p className="mt-8 text-sm text-muted-foreground">
        {'\u00A9'} {new Date().getFullYear()} Demper. Все права защищены.
      </p>
    </div>
  )
}
