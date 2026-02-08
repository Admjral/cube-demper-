"use client"

import { useState } from "react"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

interface ExtendSubscriptionDialogProps {
  subscriptionId: string
  open: boolean
  onOpenChange: (open: boolean) => void
  onExtend: (days: number) => void
}

export function ExtendSubscriptionDialog({
  subscriptionId, // Required for type, passed to parent via onExtend
  open,
  onOpenChange,
  onExtend,
}: ExtendSubscriptionDialogProps) {
  const [days, setDays] = useState("30")

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    const daysNum = parseInt(days)
    if (daysNum > 0) {
      onExtend(daysNum)
      setDays("30")
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Продлить подписку</DialogTitle>
          <DialogDescription>
            Введите количество дней для продления подписки
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={handleSubmit}>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label htmlFor="days">Дней</Label>
              <Input
                id="days"
                type="number"
                min="1"
                value={days}
                onChange={(e) => setDays(e.target.value)}
                required
              />
            </div>
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              Отмена
            </Button>
            <Button type="submit">Продлить</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
