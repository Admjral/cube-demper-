'use client'

import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ScrollArea } from '@/components/ui/scroll-area'
import { useGenerateDocument, useLawyerDocuments, type DocumentType, type LawyerLanguage, type GeneratedDocument } from '@/hooks/api/use-lawyer'
import { FileText, Loader2, Download, Copy, Check, Clock } from 'lucide-react'

interface DocumentGeneratorProps {
  language: LawyerLanguage
}

const documentTypes: { value: DocumentType; label: string; category: string }[] = [
  { value: 'supply_contract', label: 'Договор поставки', category: 'contracts' },
  { value: 'sale_contract', label: 'Договор купли-продажи', category: 'contracts' },
  { value: 'service_contract', label: 'Договор оказания услуг', category: 'contracts' },
  { value: 'employment_contract', label: 'Трудовой договор', category: 'contracts' },
  { value: 'claim_to_supplier', label: 'Претензия поставщику', category: 'claims' },
  { value: 'claim_to_buyer', label: 'Претензия покупателю', category: 'claims' },
]

export function DocumentGenerator({ language }: DocumentGeneratorProps) {
  const [activeTab, setActiveTab] = useState('generate')
  const [docType, setDocType] = useState<DocumentType>('supply_contract')
  const [generatedDoc, setGeneratedDoc] = useState<GeneratedDocument | null>(null)
  const [copied, setCopied] = useState(false)
  
  // Form data for supply contract
  const [formData, setFormData] = useState({
    seller_type: 'ИП',
    seller_name: '',
    seller_bin: '',
    seller_address: '',
    buyer_type: 'ТОО',
    buyer_name: '',
    buyer_bin: '',
    buyer_address: '',
    goods_description: '',
    total_amount: '',
    delivery_date: '',
  })

  const { mutate: generateDocument, isPending } = useGenerateDocument()
  const { data: documents } = useLawyerDocuments()

  const handleInputChange = (field: string, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }))
  }

  const handleGenerate = () => {
    const data = {
      ...formData,
      total_amount: parseInt(formData.total_amount) || 0,
    }
    
    generateDocument({
      document_type: docType,
      language,
      data,
    }, {
      onSuccess: (doc) => {
        setGeneratedDoc(doc)
      }
    })
  }

  const handleCopy = () => {
    if (generatedDoc) {
      navigator.clipboard.writeText(generatedDoc.content)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }

  return (
    <div className="flex flex-col h-full">
      <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col">
        <div className="border-b px-4">
          <TabsList className="h-12">
            <TabsTrigger value="generate" className="gap-2">
              <FileText className="h-4 w-4" />
              Создать документ
            </TabsTrigger>
            <TabsTrigger value="history" className="gap-2">
              <Clock className="h-4 w-4" />
              История
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="generate" className="flex-1 p-4 overflow-auto">
          <div className="max-w-4xl mx-auto grid md:grid-cols-2 gap-6">
            {/* Form */}
            <Card>
              <CardHeader>
                <CardTitle>Параметры документа</CardTitle>
                <CardDescription>Заполните данные для генерации</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label>Тип документа</Label>
                  <Select value={docType} onValueChange={(v) => setDocType(v as DocumentType)}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {documentTypes.map((type) => (
                        <SelectItem key={type.value} value={type.value}>
                          {type.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {/* Supply contract fields */}
                {docType === 'supply_contract' && (
                  <>
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label>Тип поставщика</Label>
                        <Select value={formData.seller_type} onValueChange={(v) => handleInputChange('seller_type', v)}>
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="ИП">ИП</SelectItem>
                            <SelectItem value="ТОО">ТОО</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="space-y-2">
                        <Label>Наименование поставщика</Label>
                        <Input 
                          placeholder="Иванов И.И."
                          value={formData.seller_name}
                          onChange={(e) => handleInputChange('seller_name', e.target.value)}
                        />
                      </div>
                    </div>

                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label>Тип покупателя</Label>
                        <Select value={formData.buyer_type} onValueChange={(v) => handleInputChange('buyer_type', v)}>
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="ИП">ИП</SelectItem>
                            <SelectItem value="ТОО">ТОО</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="space-y-2">
                        <Label>Наименование покупателя</Label>
                        <Input 
                          placeholder="Ромашка"
                          value={formData.buyer_name}
                          onChange={(e) => handleInputChange('buyer_name', e.target.value)}
                        />
                      </div>
                    </div>

                    <div className="space-y-2">
                      <Label>Описание товара</Label>
                      <Textarea 
                        placeholder="Электроника: смартфоны, ноутбуки..."
                        value={formData.goods_description}
                        onChange={(e) => handleInputChange('goods_description', e.target.value)}
                      />
                    </div>

                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label>Сумма договора (тенге)</Label>
                        <Input 
                          type="number"
                          placeholder="1000000"
                          value={formData.total_amount}
                          onChange={(e) => handleInputChange('total_amount', e.target.value)}
                        />
                      </div>
                      <div className="space-y-2">
                        <Label>Дата поставки</Label>
                        <Input 
                          type="date"
                          value={formData.delivery_date}
                          onChange={(e) => handleInputChange('delivery_date', e.target.value)}
                        />
                      </div>
                    </div>
                  </>
                )}

                <Button 
                  onClick={handleGenerate}
                  disabled={isPending}
                  className="w-full"
                >
                  {isPending ? (
                    <>
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                      Генерация...
                    </>
                  ) : (
                    'Сгенерировать документ'
                  )}
                </Button>
              </CardContent>
            </Card>

            {/* Preview */}
            <Card>
              <CardHeader className="flex-row items-center justify-between space-y-0">
                <div>
                  <CardTitle>Предпросмотр</CardTitle>
                  <CardDescription>
                    {generatedDoc ? generatedDoc.title : 'Документ будет показан здесь'}
                  </CardDescription>
                </div>
                {generatedDoc && (
                  <div className="flex gap-2">
                    <Button variant="outline" size="sm" onClick={handleCopy}>
                      {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
                    </Button>
                    <Button variant="outline" size="sm">
                      <Download className="h-4 w-4" />
                    </Button>
                  </div>
                )}
              </CardHeader>
              <CardContent>
                <ScrollArea className="h-[250px] sm:h-[400px] w-full rounded border p-4">
                  {generatedDoc ? (
                    <pre className="text-sm whitespace-pre-wrap font-mono">
                      {generatedDoc.content}
                    </pre>
                  ) : (
                    <div className="h-full flex items-center justify-center text-muted-foreground">
                      <FileText className="h-12 w-12" />
                    </div>
                  )}
                </ScrollArea>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        <TabsContent value="history" className="flex-1 p-4 overflow-auto">
          <div className="max-w-2xl mx-auto">
            <Card>
              <CardHeader>
                <CardTitle>История документов</CardTitle>
                <CardDescription>Ранее сгенерированные документы</CardDescription>
              </CardHeader>
              <CardContent>
                {documents && documents.length > 0 ? (
                  <div className="space-y-2">
                    {documents.map((doc) => (
                      <div 
                        key={doc.id}
                        className="p-3 border rounded-lg hover:bg-muted cursor-pointer flex items-center justify-between"
                        onClick={() => setGeneratedDoc(doc)}
                      >
                        <div>
                          <p className="font-medium">{doc.title}</p>
                          <p className="text-xs text-muted-foreground">
                            {new Date(doc.created_at).toLocaleDateString('ru-RU')}
                          </p>
                        </div>
                        <FileText className="h-5 w-5 text-muted-foreground" />
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-center text-muted-foreground py-8">
                    Нет сохранённых документов
                  </p>
                )}
              </CardContent>
            </Card>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  )
}
