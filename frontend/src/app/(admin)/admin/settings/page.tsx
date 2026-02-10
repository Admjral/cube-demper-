'use client'

import { useEffect, useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { api } from '@/lib/api'
import { Loader2, Save, Percent } from 'lucide-react'

export default function AdminSettingsPage() {
  const [commissionPercent, setCommissionPercent] = useState('')
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)

  useEffect(() => {
    async function fetchSettings() {
      try {
        const data = await api.get<Record<string, string>>('/admin/settings')
        setCommissionPercent(data.referral_commission_percent || '20')
      } catch (error) {
        console.error('Failed to fetch settings:', error)
        setCommissionPercent('20')
      } finally {
        setLoading(false)
      }
    }
    fetchSettings()
  }, [])

  const handleSave = async () => {
    const value = parseInt(commissionPercent)
    if (isNaN(value) || value < 0 || value > 100) return

    setSaving(true)
    setSaved(false)
    try {
      await api.put('/admin/settings', {
        key: 'referral_commission_percent',
        value: String(value),
      })
      setSaved(true)
      setTimeout(() => setSaved(false), 3000)
    } catch (error) {
      console.error('Failed to save setting:', error)
    } finally {
      setSaving(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Настройки</h1>
        <p className="text-muted-foreground">Глобальные настройки платформы</p>
      </div>

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Percent className="h-5 w-5" />
            Реферальная программа
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2 max-w-xs">
            <Label htmlFor="commission">Процент комиссии реферера</Label>
            <div className="flex gap-2 items-center">
              <Input
                id="commission"
                type="number"
                min="0"
                max="100"
                value={commissionPercent}
                onChange={(e) => setCommissionPercent(e.target.value)}
                className="w-24"
              />
              <span className="text-muted-foreground">%</span>
            </div>
            <p className="text-xs text-muted-foreground">
              Процент от стоимости плана, который начисляется рефереру при оплате подписки приведённым пользователем
            </p>
          </div>

          <Button onClick={handleSave} disabled={saving} size="sm">
            {saving ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Save className="mr-2 h-4 w-4" />
            )}
            {saved ? 'Сохранено' : 'Сохранить'}
          </Button>
        </CardContent>
      </Card>
    </div>
  )
}
