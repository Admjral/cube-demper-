"use client"

import { useState, useCallback } from "react"
import { useStore } from "@/store/use-store"
import { useProcessInvoices, LayoutType } from "@/hooks/api/use-invoices"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  FileArchive,
  Download,
  Loader2,
  Upload,
  Grid2X2,
  Grid3X3,
  LayoutGrid,
  CheckCircle,
  AlertCircle,
  Printer,
} from "lucide-react"
import { toast } from "sonner"
import { cn } from "@/lib/utils"

interface LayoutOption {
  value: LayoutType
  label: string
  description: string
  icon: React.ElementType
  grid: string
}

const LAYOUT_OPTIONS: LayoutOption[] = [
  {
    value: "4_on_1",
    label: "4 на 1",
    description: "Сетка 2×2 — 4 накладных на листе",
    icon: Grid2X2,
    grid: "2×2",
  },
  {
    value: "6_on_1",
    label: "6 на 1",
    description: "Сетка 2×3 — 6 накладных на листе",
    icon: LayoutGrid,
    grid: "2×3",
  },
  {
    value: "8_on_1",
    label: "8 на 1",
    description: "Сетка 2×4 — 8 накладных на листе",
    icon: LayoutGrid,
    grid: "2×4",
  },
  {
    value: "9_on_1",
    label: "9 на 1",
    description: "Сетка 3×3 — 9 накладных на листе",
    icon: Grid3X3,
    grid: "3×3",
  },
  {
    value: "16_on_1",
    label: "16 на 1",
    description: "Сетка 4×4 — 16 накладных на листе",
    icon: LayoutGrid,
    grid: "4×4",
  },
]

export default function InvoiceMergerPage() {
  const { locale } = useStore()
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [layout, setLayout] = useState<LayoutType>("4_on_1")
  const [isDragOver, setIsDragOver] = useState(false)

  const processInvoices = useProcessInvoices()

  const handleFileSelect = useCallback((file: File) => {
    if (!file.name.toLowerCase().endsWith(".zip")) {
      toast.error(
        locale === "ru"
          ? "Пожалуйста, выберите ZIP-архив"
          : "Please select a ZIP archive"
      )
      return
    }
    setSelectedFile(file)
  }, [locale])

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      setIsDragOver(false)

      const file = e.dataTransfer.files[0]
      if (file) {
        handleFileSelect(file)
      }
    },
    [handleFileSelect]
  )

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(true)
  }, [])

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(false)
  }, [])

  const handleFileInputChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0]
      if (file) {
        handleFileSelect(file)
      }
    },
    [handleFileSelect]
  )

  const handleProcess = async () => {
    if (!selectedFile) {
      toast.error(
        locale === "ru" ? "Выберите файл" : "Select a file"
      )
      return
    }

    try {
      const pdfBlob = await processInvoices.mutateAsync({
        file: selectedFile,
        layout,
      })

      // Создаём ссылку для скачивания
      const url = URL.createObjectURL(pdfBlob)
      const link = document.createElement("a")
      link.href = url
      link.download = `merged_invoices_${layout}.pdf`
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      URL.revokeObjectURL(url)

      toast.success(
        locale === "ru"
          ? "PDF успешно создан и скачан!"
          : "PDF successfully created and downloaded!"
      )

      // Очищаем выбранный файл
      setSelectedFile(null)
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error"
      toast.error(
        locale === "ru"
          ? `Ошибка обработки: ${message}`
          : `Processing error: ${message}`
      )
    }
  }

  const selectedLayoutOption = LAYOUT_OPTIONS.find((l) => l.value === layout)

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold">
          {locale === "ru" ? "Склейка накладных" : "Invoice Merger"}
        </h1>
        <p className="text-muted-foreground">
          {locale === "ru"
            ? "Объедините несколько накладных на одном листе A4"
            : "Merge multiple invoices onto a single A4 sheet"}
        </p>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        {/* Загрузка файла */}
        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <FileArchive className="h-5 w-5" />
              {locale === "ru" ? "Загрузка архива" : "Upload Archive"}
            </CardTitle>
            <CardDescription>
              {locale === "ru"
                ? "Загрузите ZIP-архив с PDF-накладными"
                : "Upload a ZIP archive with PDF invoices"}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div
              className={cn(
                "border-2 border-dashed rounded-xl p-8 text-center transition-colors cursor-pointer",
                isDragOver
                  ? "border-primary bg-primary/5"
                  : "border-muted-foreground/25 hover:border-primary/50",
                selectedFile && "border-green-500 bg-green-500/5"
              )}
              onDrop={handleDrop}
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onClick={() => document.getElementById("file-input")?.click()}
            >
              <input
                id="file-input"
                type="file"
                accept=".zip"
                className="hidden"
                onChange={handleFileInputChange}
              />

              {selectedFile ? (
                <div className="space-y-2">
                  <CheckCircle className="h-12 w-12 mx-auto text-green-500" />
                  <p className="font-medium">{selectedFile.name}</p>
                  <p className="text-sm text-muted-foreground">
                    {(selectedFile.size / 1024 / 1024).toFixed(2)} MB
                  </p>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={(e) => {
                      e.stopPropagation()
                      setSelectedFile(null)
                    }}
                  >
                    {locale === "ru" ? "Выбрать другой" : "Choose another"}
                  </Button>
                </div>
              ) : (
                <div className="space-y-2">
                  <Upload className="h-12 w-12 mx-auto text-muted-foreground" />
                  <p className="font-medium">
                    {locale === "ru"
                      ? "Перетащите ZIP-архив сюда"
                      : "Drag and drop ZIP archive here"}
                  </p>
                  <p className="text-sm text-muted-foreground">
                    {locale === "ru"
                      ? "или нажмите для выбора файла"
                      : "or click to select file"}
                  </p>
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Настройки сетки */}
        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <LayoutGrid className="h-5 w-5" />
              {locale === "ru" ? "Настройки сетки" : "Grid Settings"}
            </CardTitle>
            <CardDescription>
              {locale === "ru"
                ? "Выберите количество накладных на листе"
                : "Choose how many invoices per sheet"}
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label>
                {locale === "ru" ? "Тип сетки" : "Grid type"}
              </Label>
              <Select
                value={layout}
                onValueChange={(v: string) => setLayout(v as LayoutType)}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {LAYOUT_OPTIONS.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      <div className="flex items-center gap-2">
                        <option.icon className="h-4 w-4" />
                        <span>{option.label}</span>
                        <span className="text-muted-foreground">
                          ({option.grid})
                        </span>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Визуализация выбранной сетки */}
            <div className="p-4 bg-muted/30 rounded-xl">
              <div className="flex items-center justify-between mb-3">
                <span className="text-sm font-medium">
                  {locale === "ru" ? "Предпросмотр сетки" : "Grid preview"}
                </span>
                <span className="text-sm text-muted-foreground">
                  {selectedLayoutOption?.description}
                </span>
              </div>

              {/* Сетка-превью */}
              <div
                className="aspect-[1/1.414] bg-white rounded border max-w-[200px] mx-auto p-2"
                style={{
                  display: "grid",
                  gridTemplateColumns: `repeat(${
                    layout === "4_on_1" || layout === "6_on_1" || layout === "8_on_1" ? 2 : layout === "9_on_1" ? 3 : 4
                  }, 1fr)`,
                  gridTemplateRows: `repeat(${
                    layout === "4_on_1" ? 2 : layout === "6_on_1" ? 3 : layout === "8_on_1" ? 4 : layout === "9_on_1" ? 3 : 4
                  }, 1fr)`,
                  gap: "4px",
                }}
              >
                {Array.from({
                  length: layout === "4_on_1" ? 4 : layout === "6_on_1" ? 6 : layout === "8_on_1" ? 8 : layout === "9_on_1" ? 9 : 16,
                }).map((_, i) => (
                  <div
                    key={i}
                    className="bg-primary/20 rounded-sm border border-primary/30"
                  />
                ))}
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Кнопка обработки */}
      <Card className="glass-card">
        <CardContent className="p-6">
          <div className="flex flex-col sm:flex-row items-center justify-between gap-4">
            <div className="flex items-center gap-3">
              <Printer className="h-8 w-8 text-primary" />
              <div>
                <p className="font-medium">
                  {locale === "ru"
                    ? "Готово к обработке"
                    : "Ready to process"}
                </p>
                <p className="text-sm text-muted-foreground">
                  {selectedFile
                    ? locale === "ru"
                      ? `Файл: ${selectedFile.name} • Сетка: ${selectedLayoutOption?.label}`
                      : `File: ${selectedFile.name} • Grid: ${selectedLayoutOption?.label}`
                    : locale === "ru"
                    ? "Загрузите ZIP-архив с накладными"
                    : "Upload a ZIP archive with invoices"}
                </p>
              </div>
            </div>

            <Button
              size="lg"
              onClick={handleProcess}
              disabled={!selectedFile || processInvoices.isPending}
              className="min-w-[200px]"
            >
              {processInvoices.isPending ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  {locale === "ru" ? "Обработка..." : "Processing..."}
                </>
              ) : (
                <>
                  <Download className="h-4 w-4 mr-2" />
                  {locale === "ru" ? "Создать и скачать PDF" : "Create & Download PDF"}
                </>
              )}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Инструкция */}
      <Card className="glass-card border-dashed">
        <CardContent className="p-6">
          <div className="flex items-start gap-4">
            <AlertCircle className="h-5 w-5 text-muted-foreground mt-0.5" />
            <div className="space-y-2 text-sm text-muted-foreground">
              <p className="font-medium text-foreground">
                {locale === "ru" ? "Как это работает:" : "How it works:"}
              </p>
              <ol className="list-decimal list-inside space-y-1">
                <li>
                  {locale === "ru"
                    ? "Соберите все PDF-накладные в ZIP-архив"
                    : "Collect all PDF invoices into a ZIP archive"}
                </li>
                <li>
                  {locale === "ru"
                    ? "Загрузите архив и выберите тип сетки"
                    : "Upload the archive and select grid type"}
                </li>
                <li>
                  {locale === "ru"
                    ? "Нажмите «Создать и скачать PDF»"
                    : "Click 'Create & Download PDF'"}
                </li>
                <li>
                  {locale === "ru"
                    ? "Распечатайте готовый PDF на принтере"
                    : "Print the resulting PDF on your printer"}
                </li>
              </ol>
              <p className="mt-3">
                {locale === "ru"
                  ? "💡 Совет: для экономии бумаги используйте сетку 9 на 1 или 16 на 1"
                  : "💡 Tip: use 9 on 1 or 16 on 1 grid to save paper"}
              </p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
