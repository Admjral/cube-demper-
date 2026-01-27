"use client"

import { Card } from "@/components/ui/card"
import { ChatInterface } from "@/components/ai/chat-interface"
import { Receipt, Sparkles } from "lucide-react"

export default function AIAccountantPage() {
  return (
    <div className="h-[calc(100vh-12rem)] lg:h-[calc(100vh-8rem)] flex flex-col">
      {/* Header */}
      <div className="mb-4">
        <h1 className="text-2xl font-semibold flex items-center gap-2">
          <Receipt className="h-6 w-6" />
          ИИ Бухгалтер
        </h1>
        <p className="text-muted-foreground">
          Ответы на вопросы о налогах и расчётах для ИП и ТОО в РК
        </p>
      </div>

      {/* Chat area */}
      <Card className="glass-card flex-1 flex flex-col overflow-hidden">
        <ChatInterface
          assistantType="accountant"
          assistantName="ИИ Бухгалтер"
          assistantDescription="Консультации по налогам, расчётам и отчётности"
          placeholder="Задайте вопрос о налогах..."
          systemPromptPreview="Специализируется на налоговом законодательстве РК. Помогает с расчётами НДС, КПН, ИПН и отчётностью."
        />
      </Card>

      {/* Disclaimer */}
      <p className="text-xs text-muted-foreground mt-3 flex items-center gap-1 justify-center">
        <Sparkles className="h-3 w-3" />
        Powered by AI. Не является профессиональной бухгалтерской консультацией.
      </p>
    </div>
  )
}
