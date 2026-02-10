"use client"

import { useState } from "react"
import { SubscriptionGate } from "@/components/shared/subscription-gate"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { 
  Scale, Sparkles, MessageSquare, FileText, Search, Calculator, 
  Receipt, Building, ChevronRight, Globe
} from "lucide-react"
import { LawyerChat } from "@/components/lawyer/lawyer-chat"
import { DocumentGenerator } from "@/components/lawyer/document-generator"
import { ContractAnalyzer } from "@/components/lawyer/contract-analyzer"
import { PenaltyCalculator } from "@/components/lawyer/penalty-calculator"
import { TaxCalculator } from "@/components/lawyer/tax-calculator"
import { FeeCalculator } from "@/components/lawyer/fee-calculator"
import { useLawyerLanguage, useSetLawyerLanguage, useLawyerFAQ, type LawyerLanguage } from "@/hooks/api/use-lawyer"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"

const features = [
  {
    id: 'consultation',
    title: 'Консультация',
    titleKk: 'Кеңес беру',
    description: 'Юридические вопросы по законам РК',
    icon: MessageSquare,
    color: 'text-blue-500',
    bgColor: 'bg-blue-500/10',
  },
  {
    id: 'documents',
    title: 'Документы',
    titleKk: 'Құжаттар',
    description: 'Генерация договоров и заявлений',
    icon: FileText,
    color: 'text-green-500',
    bgColor: 'bg-green-500/10',
  },
  {
    id: 'analysis',
    title: 'Анализ договора',
    titleKk: 'Шарт талдауы',
    description: 'Проверка рисков и условий',
    icon: Search,
    color: 'text-purple-500',
    bgColor: 'bg-purple-500/10',
  },
  {
    id: 'penalty',
    title: 'Калькулятор пени',
    titleKk: 'Өсімпұл калькуляторы',
    description: 'Расчёт пени по ставке НБ РК',
    icon: Calculator,
    color: 'text-orange-500',
    bgColor: 'bg-orange-500/10',
  },
  {
    id: 'taxes',
    title: 'Калькулятор налогов',
    titleKk: 'Салық калькуляторы',
    description: 'ИПН, КПН, НДС, соц. отчисления',
    icon: Receipt,
    color: 'text-red-500',
    bgColor: 'bg-red-500/10',
  },
  {
    id: 'fees',
    title: 'Калькулятор госпошлин',
    titleKk: 'Баж калькуляторы',
    description: 'Регистрация, суд, лицензии',
    icon: Building,
    color: 'text-cyan-500',
    bgColor: 'bg-cyan-500/10',
  },
]

export default function AILawyerPage() {
  const [activeTab, setActiveTab] = useState<string | null>(null)
  const { data: langData } = useLawyerLanguage()
  const { mutate: setLanguage } = useSetLawyerLanguage()
  const language = langData?.language || 'ru'
  const { data: faq } = useLawyerFAQ(language)

  const handleLanguageChange = (newLang: LawyerLanguage) => {
    setLanguage(newLang)
  }

  // If a feature is selected, show its content
  if (activeTab) {
    return (
      <SubscriptionGate>
      <div className="h-[calc(100vh-12rem)] lg:h-[calc(100vh-8rem)] flex flex-col">
        {/* Header */}
        <div className="mb-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Button 
              variant="ghost" 
              size="sm"
              onClick={() => setActiveTab(null)}
            >
              ← Назад
            </Button>
            <h1 className="text-xl font-semibold">
              {features.find(f => f.id === activeTab)?.title}
            </h1>
          </div>
          <LanguageSelector language={language} onChange={handleLanguageChange} />
        </div>

        {/* Content */}
        <Card className="glass-card flex-1 flex flex-col overflow-hidden">
          {activeTab === 'consultation' && <LawyerChat language={language} />}
          {activeTab === 'documents' && <DocumentGenerator language={language} />}
          {activeTab === 'analysis' && <ContractAnalyzer language={language} />}
          {activeTab === 'penalty' && <PenaltyCalculator />}
          {activeTab === 'taxes' && <TaxCalculator />}
          {activeTab === 'fees' && <FeeCalculator />}
        </Card>

        {/* Disclaimer */}
        <p className="text-xs text-muted-foreground mt-3 flex items-center gap-1 justify-center">
          <Sparkles className="h-3 w-3" />
          Powered by Gemini AI. Не заменяет консультацию профессионального юриста.
        </p>
      </div>
      </SubscriptionGate>
    )
  }

  // Main menu
  return (
    <SubscriptionGate>
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <Scale className="h-6 w-6" />
            ИИ-Юрист
          </h1>
          <p className="text-muted-foreground">
            Юридическая помощь для предпринимателей Казахстана
          </p>
        </div>
        <LanguageSelector language={language} onChange={handleLanguageChange} />
      </div>

      {/* Features Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {features.map((feature) => (
          <Card 
            key={feature.id}
            className="glass-card cursor-pointer hover:shadow-lg transition-all hover:scale-[1.02]"
            onClick={() => setActiveTab(feature.id)}
          >
            <CardHeader className="pb-2">
              <div className={`w-12 h-12 rounded-lg ${feature.bgColor} flex items-center justify-center mb-2`}>
                <feature.icon className={`h-6 w-6 ${feature.color}`} />
              </div>
              <CardTitle className="text-lg flex items-center justify-between">
                {language === 'kk' ? feature.titleKk : feature.title}
                <ChevronRight className="h-5 w-5 text-muted-foreground" />
              </CardTitle>
              <CardDescription>{feature.description}</CardDescription>
            </CardHeader>
          </Card>
        ))}
      </div>

      {/* FAQ Section */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg">Частые вопросы</CardTitle>
          <CardDescription>Нажмите, чтобы получить консультацию</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {faq?.slice(0, 5).map((item, idx) => (
              <Button
                key={idx}
                variant="ghost"
                className="w-full justify-start text-left h-auto py-3"
                onClick={() => setActiveTab('consultation')}
              >
                <span className="truncate">{item.question}</span>
              </Button>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Disclaimer */}
      <p className="text-xs text-muted-foreground flex items-center gap-1 justify-center">
        <Sparkles className="h-3 w-3" />
        Powered by Gemini AI. Не заменяет консультацию профессионального юриста.
      </p>
    </div>
    </SubscriptionGate>
  )
}

function LanguageSelector({ 
  language, 
  onChange 
}: { 
  language: LawyerLanguage
  onChange: (lang: LawyerLanguage) => void 
}) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="gap-2">
          <Globe className="h-4 w-4" />
          {language === 'ru' ? 'RU' : 'KZ'}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onClick={() => onChange('ru')}>
          🇷🇺 Русский
        </DropdownMenuItem>
        <DropdownMenuItem onClick={() => onChange('kk')}>
          🇰🇿 Қазақша
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
