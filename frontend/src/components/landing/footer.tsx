import Image from 'next/image'
import Link from 'next/link'
import { MessageSquare, Send, Instagram } from 'lucide-react'

const footerLinks = {
  product: [
    { name: 'Возможности', href: '#features' },
    { name: 'Тарифы', href: '#pricing' },
    { name: 'FAQ', href: '#faq' },
  ],
  legal: [
    { name: 'Условия использования', href: '/terms' },
    { name: 'Политика конфиденциальности', href: '/privacy' },
    { name: 'Оферта', href: '/offer' },
  ],
  support: [
    { name: 'Связаться с нами', href: 'https://wa.me/77476117623' },
    { name: 'Telegram', href: 'https://t.me/demper_support' },
    { name: 'Email', href: 'mailto:support@demper.kz' },
  ],
}

const socialLinks = [
  { name: 'WhatsApp', href: 'https://wa.me/77476117623', icon: MessageSquare },
  { name: 'Telegram', href: 'https://t.me/demper_kz', icon: Send },
  { name: 'Instagram', href: 'https://instagram.com/demper.kz', icon: Instagram },
]

export function Footer() {
  return (
    <footer className="border-t border-border bg-background">
      <div className="container mx-auto px-4 py-12">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
          {/* Brand */}
          <div className="col-span-2 md:col-span-1">
            <Link href="/" className="inline-block">
              <Image src="/logodark.svg" alt="Demper" width={120} height={42} className="h-8 w-auto dark:hidden" />
              <Image src="/logowhite.svg" alt="Demper" width={120} height={42} className="h-8 w-auto hidden dark:block" />
            </Link>
            <p className="text-sm text-muted-foreground mt-2">
              Автоматизация продаж на Kaspi.kz
            </p>
            <div className="flex items-center gap-4 mt-4">
              {socialLinks.map((social) => (
                <Link
                  key={social.name}
                  href={social.href}
                  target="_blank"
                  className="text-muted-foreground hover:text-foreground transition-colors"
                >
                  <social.icon className="h-5 w-5" />
                </Link>
              ))}
            </div>
          </div>

          {/* Product */}
          <div>
            <h4 className="font-semibold text-foreground mb-4">Продукт</h4>
            <ul className="space-y-2">
              {footerLinks.product.map((link) => (
                <li key={link.name}>
                  <Link
                    href={link.href}
                    className="text-sm text-muted-foreground hover:text-foreground transition-colors"
                  >
                    {link.name}
                  </Link>
                </li>
              ))}
            </ul>
          </div>

          {/* Legal */}
          <div>
            <h4 className="font-semibold text-foreground mb-4">Документы</h4>
            <ul className="space-y-2">
              {footerLinks.legal.map((link) => (
                <li key={link.name}>
                  <Link
                    href={link.href}
                    className="text-sm text-muted-foreground hover:text-foreground transition-colors"
                  >
                    {link.name}
                  </Link>
                </li>
              ))}
            </ul>
          </div>

          {/* Support */}
          <div>
            <h4 className="font-semibold text-foreground mb-4">Поддержка</h4>
            <ul className="space-y-2">
              {footerLinks.support.map((link) => (
                <li key={link.name}>
                  <Link
                    href={link.href}
                    target={link.href.startsWith('http') ? '_blank' : undefined}
                    className="text-sm text-muted-foreground hover:text-foreground transition-colors"
                  >
                    {link.name}
                  </Link>
                </li>
              ))}
            </ul>
          </div>
        </div>

        <div className="border-t border-border mt-8 pt-8 flex flex-col md:flex-row items-center justify-between gap-4">
          <p className="text-sm text-muted-foreground">
            &copy; {new Date().getFullYear()} Demper. Все права защищены.
          </p>
          <p className="text-sm text-muted-foreground">
            Сделано в Казахстане
          </p>
        </div>
      </div>
    </footer>
  )
}
