import { Header, Hero, Features, Pricing, FAQ, CTA, Footer } from '@/components/landing'

export default function HomePage() {
  return (
    <main className="min-h-screen">
      <Header />
      <Hero />
      <Features />
      <Pricing />
      <FAQ />
      <CTA />
      <Footer />
    </main>
  )
}
