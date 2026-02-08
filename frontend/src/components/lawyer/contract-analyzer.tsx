'use client'

import { useState, useCallback } from 'react'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import { useAnalyzeContract, type LawyerLanguage, type ContractAnalysis, type RiskLevel } from '@/hooks/api/use-lawyer'
import { Search, Upload, Loader2, AlertTriangle, AlertCircle, Info, CheckCircle, FileText } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useDropzone } from 'react-dropzone'

interface ContractAnalyzerProps {
  language: LawyerLanguage
}

const riskColors: Record<RiskLevel, { bg: string; text: string; icon: React.ComponentType<any> }> = {
  critical: { bg: 'bg-red-500/10', text: 'text-red-500', icon: AlertCircle },
  high: { bg: 'bg-orange-500/10', text: 'text-orange-500', icon: AlertTriangle },
  medium: { bg: 'bg-yellow-500/10', text: 'text-yellow-500', icon: Info },
  low: { bg: 'bg-green-500/10', text: 'text-green-500', icon: CheckCircle },
}

const riskLabels: Record<RiskLevel, string> = {
  critical: 'Критический',
  high: 'Высокий',
  medium: 'Средний',
  low: 'Низкий',
}

export function ContractAnalyzer({ language }: ContractAnalyzerProps) {
  const [file, setFile] = useState<File | null>(null)
  const [analysis, setAnalysis] = useState<ContractAnalysis | null>(null)

  const { mutate: analyze, isPending } = useAnalyzeContract()

  const onDrop = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      setFile(acceptedFiles[0])
      setAnalysis(null)
    }
  }, [])

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'application/pdf': ['.pdf'],
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document': ['.docx'],
      'text/plain': ['.txt'],
    },
    maxSize: 10 * 1024 * 1024, // 10MB
    multiple: false,
  })

  const handleAnalyze = () => {
    if (!file) return

    analyze({ file, language }, {
      onSuccess: (data) => {
        setAnalysis(data)
      }
    })
  }

  const getRiskStats = () => {
    if (!analysis) return { critical: 0, high: 0, medium: 0, low: 0 }
    
    return analysis.risks.reduce((acc, risk) => {
      acc[risk.level] = (acc[risk.level] || 0) + 1
      return acc
    }, { critical: 0, high: 0, medium: 0, low: 0 } as Record<RiskLevel, number>)
  }

  const riskStats = getRiskStats()
  const totalRisks = Object.values(riskStats).reduce((a, b) => a + b, 0)

  return (
    <div className="p-6 space-y-6 overflow-auto">
      <div className="max-w-3xl mx-auto">
        {/* Upload Area */}
        {!analysis && (
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Search className="h-5 w-5" />
                Анализ договора
              </CardTitle>
              <CardDescription>
                Загрузите договор для проверки рисков и ключевых условий
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div
                {...getRootProps()}
                className={cn(
                  "border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-colors",
                  isDragActive ? "border-primary bg-primary/5" : "border-muted-foreground/25 hover:border-primary/50",
                  file && "border-green-500 bg-green-500/5"
                )}
              >
                <input {...getInputProps()} />
                {file ? (
                  <div className="space-y-2">
                    <FileText className="h-12 w-12 mx-auto text-green-500" />
                    <p className="font-medium">{file.name}</p>
                    <p className="text-sm text-muted-foreground">
                      {(file.size / 1024 / 1024).toFixed(2)} MB
                    </p>
                  </div>
                ) : (
                  <div className="space-y-2">
                    <Upload className="h-12 w-12 mx-auto text-muted-foreground" />
                    <p className="font-medium">
                      {isDragActive ? 'Отпустите файл здесь' : 'Перетащите файл сюда'}
                    </p>
                    <p className="text-sm text-muted-foreground">
                      или нажмите для выбора (PDF, DOCX, TXT до 10MB)
                    </p>
                  </div>
                )}
              </div>

              <Button 
                onClick={handleAnalyze}
                disabled={!file || isPending}
                className="w-full"
              >
                {isPending ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    Анализируем договор...
                  </>
                ) : (
                  'Проанализировать'
                )}
              </Button>
            </CardContent>
          </Card>
        )}

        {/* Analysis Result */}
        {analysis && (
          <div className="space-y-6">
            {/* Header with overall risk */}
            <Card className={cn(
              "border-2",
              analysis.overall_risk_level === 'critical' && "border-red-500",
              analysis.overall_risk_level === 'high' && "border-orange-500",
              analysis.overall_risk_level === 'medium' && "border-yellow-500",
              analysis.overall_risk_level === 'low' && "border-green-500",
            )}>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle>Результат анализа</CardTitle>
                    <CardDescription>{file?.name}</CardDescription>
                  </div>
                  <Badge className={cn(
                    "text-lg px-4 py-2",
                    riskColors[analysis.overall_risk_level].bg,
                    riskColors[analysis.overall_risk_level].text
                  )}>
                    {riskLabels[analysis.overall_risk_level]} риск
                  </Badge>
                </div>
              </CardHeader>
              <CardContent>
                {/* Risk stats */}
                <div className="grid grid-cols-4 gap-4 mb-6">
                  {(['critical', 'high', 'medium', 'low'] as RiskLevel[]).map((level) => {
                    const RiskIcon = riskColors[level].icon
                    return (
                      <div key={level} className={cn("p-3 rounded-lg text-center", riskColors[level].bg)}>
                        <RiskIcon className={cn("h-5 w-5 mx-auto mb-1", riskColors[level].text)} />
                        <p className={cn("text-2xl font-bold", riskColors[level].text)}>
                          {riskStats[level]}
                        </p>
                        <p className="text-xs text-muted-foreground">{riskLabels[level]}</p>
                      </div>
                    )
                  })}
                </div>

                {/* Summary */}
                <div className="p-4 bg-muted rounded-lg">
                  <p className="font-medium mb-2">Краткое описание</p>
                  <p className="text-sm">{analysis.summary}</p>
                </div>

                <Button 
                  variant="outline" 
                  className="w-full mt-4"
                  onClick={() => {
                    setAnalysis(null)
                    setFile(null)
                  }}
                >
                  Загрузить другой договор
                </Button>
              </CardContent>
            </Card>

            {/* Key conditions */}
            {analysis.key_conditions.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle>Ключевые условия</CardTitle>
                </CardHeader>
                <CardContent>
                  <ul className="space-y-2">
                    {analysis.key_conditions.map((condition, idx) => (
                      <li key={idx} className="flex items-start gap-2">
                        <CheckCircle className="h-4 w-4 mt-0.5 text-green-500 shrink-0" />
                        <span className="text-sm">{condition}</span>
                      </li>
                    ))}
                  </ul>
                </CardContent>
              </Card>
            )}

            {/* Risks */}
            {analysis.risks.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle>Выявленные риски</CardTitle>
                </CardHeader>
                <CardContent>
                  <ScrollArea className="max-h-[400px]">
                    <div className="space-y-4">
                      {analysis.risks.map((risk, idx) => {
                        const RiskIcon = riskColors[risk.level].icon
                        return (
                          <div 
                            key={idx} 
                            className={cn(
                              "p-4 rounded-lg border-l-4",
                              riskColors[risk.level].bg,
                              risk.level === 'critical' && "border-l-red-500",
                              risk.level === 'high' && "border-l-orange-500",
                              risk.level === 'medium' && "border-l-yellow-500",
                              risk.level === 'low' && "border-l-green-500",
                            )}
                          >
                            <div className="flex items-start gap-3">
                              <RiskIcon className={cn("h-5 w-5 shrink-0", riskColors[risk.level].text)} />
                              <div className="space-y-2">
                                <div className="flex items-center gap-2">
                                  <p className="font-medium">{risk.title}</p>
                                  <Badge variant="outline" className="text-xs">
                                    {riskLabels[risk.level]}
                                  </Badge>
                                </div>
                                <p className="text-sm text-muted-foreground">{risk.description}</p>
                                {risk.clause && (
                                  <div className="p-2 bg-background/50 rounded text-xs font-mono">
                                    "{risk.clause}"
                                  </div>
                                )}
                                <div className="flex items-start gap-2 pt-2 border-t">
                                  <Info className="h-4 w-4 text-blue-500 shrink-0" />
                                  <p className="text-sm text-blue-600">{risk.recommendation}</p>
                                </div>
                              </div>
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  </ScrollArea>
                </CardContent>
              </Card>
            )}

            {/* General recommendations */}
            {analysis.recommendations.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle>Рекомендации</CardTitle>
                </CardHeader>
                <CardContent>
                  <ul className="space-y-2">
                    {analysis.recommendations.map((rec, idx) => (
                      <li key={idx} className="flex items-start gap-2">
                        <Info className="h-4 w-4 mt-0.5 text-blue-500 shrink-0" />
                        <span className="text-sm">{rec}</span>
                      </li>
                    ))}
                  </ul>
                </CardContent>
              </Card>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
