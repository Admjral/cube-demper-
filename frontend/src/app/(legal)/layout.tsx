import { Header } from '@/components/landing/header'
import { Footer } from '@/components/landing/footer'

export default function LegalLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <main className="min-h-screen flex flex-col">
      <Header />
      <div className="flex-1 pt-24 pb-16">
        <div className="container mx-auto px-4">
          <article className="max-w-3xl mx-auto prose prose-neutral dark:prose-invert prose-headings:text-foreground prose-p:text-muted-foreground prose-li:text-muted-foreground prose-strong:text-foreground prose-a:text-primary">
            {children}
          </article>
        </div>
      </div>
      <Footer />
    </main>
  )
}
