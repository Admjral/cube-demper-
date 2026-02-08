import { Header, Hero, Features, ModulesDetail, Pricing, FAQ, CTA, Footer } from '@/components/landing'

export default function HomePage() {
  return (
    <main className="min-h-screen">
      <Header />
      <Hero />
      <Features />
      <ModulesDetail />
      <Pricing />
      <FAQ />
      <CTA />
      <Footer />
    </main>
  )
}
