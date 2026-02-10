'use client'

import Link from 'next/link'
import { Button } from '@/components/ui/button'
import { ArrowRight, Play, Sparkles } from 'lucide-react'
import { useT } from '@/lib/i18n'

export function Hero() {
  const t = useT()
  return (
    <section className="relative pt-32 pb-20 md:pt-40 md:pb-32 overflow-hidden">
      {/* Background effects */}
      <div className="absolute inset-0 bg-gradient-to-b from-primary/5 via-transparent to-transparent pointer-events-none" />
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[800px] h-[800px] bg-primary/5 rounded-full blur-3xl pointer-events-none" />

      <div className="container mx-auto px-4 relative">
        <div className="max-w-4xl mx-auto text-center">
          {/* Badge */}
          <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-primary/10 text-primary text-sm font-medium mb-8">
            <Sparkles className="h-4 w-4" />
            <span>{t("landing.newAI")}</span>
          </div>

          {/* Heading */}
          <h1 className="text-4xl md:text-6xl lg:text-7xl font-bold text-foreground tracking-tight mb-6">
            {t("landing.heroTitle")}
          </h1>

          {/* Subheading */}
          <p className="text-lg md:text-xl text-muted-foreground max-w-2xl mx-auto mb-10">
            {t("landing.heroDesc")}
          </p>

          {/* CTA buttons */}
          <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
            <Link href="/register">
              <Button size="lg" className="w-full sm:w-auto text-base px-8">
                {t("landing.startFree")}
                <ArrowRight className="ml-2 h-5 w-5" />
              </Button>
            </Link>
            <Link href="#features">
              <Button variant="outline" size="lg" className="w-full sm:w-auto text-base px-8">
                <Play className="mr-2 h-5 w-5" />
                {t("landing.watchDemo")}
              </Button>
            </Link>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-3 gap-8 mt-16 pt-8 border-t border-border">
            <div>
              <p className="text-3xl md:text-4xl font-bold text-foreground">500+</p>
              <p className="text-sm text-muted-foreground mt-1">{t("landing.activeSellers")}</p>
            </div>
            <div>
              <p className="text-3xl md:text-4xl font-bold text-foreground">2M+</p>
              <p className="text-sm text-muted-foreground mt-1">{t("landing.productsProcessed")}</p>
            </div>
            <div>
              <p className="text-3xl md:text-4xl font-bold text-foreground">24/7</p>
              <p className="text-sm text-muted-foreground mt-1">{t("landing.automation")}</p>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
