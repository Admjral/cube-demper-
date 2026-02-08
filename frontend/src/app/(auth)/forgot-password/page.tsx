'use client'

import { useState } from 'react'
import Link from 'next/link'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useAuth } from '@/hooks/use-auth'
import { Loader2, Mail, ArrowLeft, Check } from 'lucide-react'

export default function ForgotPasswordPage() {
  const { resetPassword } = useAuth()

  const [email, setEmail] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState(false)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    setLoading(true)

    const { error } = await resetPassword(email)

    if (error) {
      setError(error.message)
      setLoading(false)
      return
    }

    setSuccess(true)
    setLoading(false)
  }

  if (success) {
    return (
      <div className="glass-card p-8 text-center">
        <div className="w-16 h-16 rounded-full bg-success/10 flex items-center justify-center mx-auto mb-4">
          <Check className="h-8 w-8 text-success" />
        </div>
        <h2 className="text-2xl font-semibold text-foreground">Письмо отправлено</h2>
        <p className="text-muted-foreground mt-2">
          Мы отправили инструкции по сбросу пароля на <strong>{email}</strong>.
          Проверьте почту и перейдите по ссылке.
        </p>
        <Link href="/login">
          <Button variant="outline" className="mt-6">
            <ArrowLeft className="mr-2 h-4 w-4" />
            Вернуться к входу
          </Button>
        </Link>
      </div>
    )
  }

  return (
    <div className="glass-card p-8">
      <div className="text-center mb-6">
        <h2 className="text-2xl font-semibold text-foreground">Восстановление пароля</h2>
        <p className="text-muted-foreground mt-1">
          Введите email для получения инструкций
        </p>
      </div>

      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="space-y-2">
          <Label htmlFor="email">Email</Label>
          <div className="relative">
            <Mail className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              id="email"
              type="email"
              placeholder="your@email.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="pl-10"
              required
              disabled={loading}
            />
          </div>
        </div>

        {error && (
          <div className="p-3 rounded-lg bg-destructive/10 text-destructive text-sm">
            {error}
          </div>
        )}

        <Button type="submit" className="w-full" disabled={loading}>
          {loading ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Отправка...
            </>
          ) : (
            'Отправить инструкции'
          )}
        </Button>
      </form>

      <div className="mt-6 text-center">
        <Link
          href="/login"
          className="text-sm text-muted-foreground hover:text-foreground inline-flex items-center"
        >
          <ArrowLeft className="mr-1 h-4 w-4" />
          Вернуться к входу
        </Link>
      </div>
    </div>
  )
}
