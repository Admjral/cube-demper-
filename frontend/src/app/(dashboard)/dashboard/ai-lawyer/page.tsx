"use client"

import { Card } from "@/components/ui/card"
import { ChatInterface } from "@/components/ai/chat-interface"
import { Scale, Sparkles } from "lucide-react"

export default function AILawyerPage() {
  return (
    <div className="h-[calc(100vh-12rem)] lg:h-[calc(100vh-8rem)] flex flex-col">
      {/* Header */}
      <div className="mb-4">
        <h1 className="text-2xl font-semibold flex items-center gap-2">
          <Scale className="h-6 w-6" />
          ИИ Юрист
        </h1>
        <p className="text-muted-foreground">
          Анализ договоров и правовая помощь по законодательству РК
        </p>
      </div>

      {/* Chat area */}
      <Card className="glass-card flex-1 flex flex-col overflow-hidden">
        <ChatInterface
          assistantType="lawyer"
          assistantName="ИИ Юрист"
          assistantDescription="Консультации по договорам, спорам и правовым вопросам"
          placeholder="Задайте юридический вопрос..."
          systemPromptPreview="Специализируется на законодательстве Казахстана. Помогает с договорами, претензиями и спорами."
        />
      </Card>

      {/* Disclaimer */}
      <p className="text-xs text-muted-foreground mt-3 flex items-center gap-1 justify-center">
        <Sparkles className="h-3 w-3" />
        Powered by AI. Не заменяет консультацию профессионального юриста.
      </p>
    </div>
  )
}
