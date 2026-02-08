import Link from 'next/link'
import { Button } from '@/components/ui/button'
import { ArrowRight } from 'lucide-react'

export function CTA() {
  return (
    <section className="py-20 md:py-32 bg-primary/5">
      <div className="container mx-auto px-4">
        <div className="max-w-3xl mx-auto text-center">
          <h2 className="text-3xl md:text-4xl font-bold text-foreground mb-4">
            Готовы увеличить продажи?
          </h2>
          <p className="text-lg text-muted-foreground mb-8">
            Присоединяйтесь к сотням продавцов, которые уже автоматизировали свой бизнес на Kaspi с помощью Demper
          </p>
          <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
            <Link href="/register">
              <Button size="lg" className="text-base px-8">
                Начать бесплатно
                <ArrowRight className="ml-2 h-5 w-5" />
              </Button>
            </Link>
            <Link href="https://wa.me/77476117623" target="_blank">
              <Button variant="outline" size="lg" className="text-base px-8">
                Связаться с нами
              </Button>
            </Link>
          </div>
        </div>
      </div>
    </section>
  )
}
