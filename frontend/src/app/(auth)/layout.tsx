import { ThemeToggle } from "@/components/shared/theme-toggle"

export default function AuthLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <div className="min-h-screen flex flex-col items-center justify-center bg-background p-4 relative">
      {/* Background gradient */}
      <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-transparent to-primary/5 pointer-events-none" />

      {/* Theme toggle */}
      <div className="absolute top-4 right-4">
        <ThemeToggle />
      </div>

      {/* Logo */}
      <div className="mb-8 text-center">
        <h1 className="text-3xl font-bold text-foreground">Demper</h1>
        <p className="text-muted-foreground mt-1">Kaspi Seller Panel</p>
      </div>

      {/* Auth card */}
      <div className="w-full max-w-md">
        {children}
      </div>

      {/* Footer */}
      <p className="mt-8 text-sm text-muted-foreground">
        &copy; {new Date().getFullYear()} Demper. Все права защищены.
      </p>
    </div>
  )
}
