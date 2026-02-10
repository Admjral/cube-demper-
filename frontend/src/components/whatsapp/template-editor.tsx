"use client"

import { useRef } from "react"
import { useStore } from "@/store/use-store"
import { useT } from "@/lib/i18n"
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from "@/components/ui/sheet"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
import { ArrowLeft, Loader2 } from "lucide-react"
import { VARIABLE_CHIPS } from "./template-constants"
import { TemplatePreview } from "./template-preview"

interface TemplateForm {
  name: string
  name_en: string
  message: string
  variables: string[]
  trigger_event: string
}

interface TemplateEditorProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  form: TemplateForm
  setForm: React.Dispatch<React.SetStateAction<TemplateForm>>
  onSave: () => void
  isSaving: boolean
  isEditing: boolean
  triggerLabel: string
}

export function TemplateEditor({
  open,
  onOpenChange,
  form,
  setForm,
  onSave,
  isSaving,
  isEditing,
  triggerLabel,
}: TemplateEditorProps) {
  const t = useT()
  const { locale } = useStore()
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  const insertVariable = (variable: string) => {
    const el = textareaRef.current
    if (!el) return
    const start = el.selectionStart
    const end = el.selectionEnd
    const text = form.message
    const newText = text.slice(0, start) + `{${variable}}` + text.slice(end)
    setForm((f) => ({ ...f, message: newText }))
    requestAnimationFrame(() => {
      const pos = start + variable.length + 2
      el.setSelectionRange(pos, pos)
      el.focus()
    })
  }

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        className="w-full sm:max-w-lg flex flex-col p-0 gap-0"
      >
        {/* Header */}
        <SheetHeader className="flex-row items-center gap-3 px-4 py-3 border-b space-y-0">
          <Button
            variant="ghost"
            size="icon"
            className="shrink-0 -ml-2"
            onClick={() => onOpenChange(false)}
          >
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div className="flex-1 min-w-0">
            <SheetTitle className="text-base truncate">
              {isEditing ? t("waTpl.editTemplate") : t("waTpl.newTemplate")}
            </SheetTitle>
            <SheetDescription className="sr-only">
              {t("waTpl.configureDesc")}
            </SheetDescription>
          </div>
          <Button
            size="sm"
            onClick={onSave}
            disabled={isSaving || !form.name || !form.message}
          >
            {isSaving && <Loader2 className="h-4 w-4 mr-1 animate-spin" />}
            {t("common.save")}
          </Button>
        </SheetHeader>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-4 py-4 space-y-4">
          {/* Name */}
          <div className="space-y-1.5">
            <Label className="text-xs">{t("waTpl.name")}</Label>
            <Input
              value={form.name}
              onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
              placeholder={t("waTpl.nameDefault")}
            />
          </div>

          {/* Name EN */}
          <div className="space-y-1.5">
            <Label className="text-xs">{t("waTpl.nameEn")}</Label>
            <Input
              value={form.name_en}
              onChange={(e) => setForm((f) => ({ ...f, name_en: e.target.value }))}
              placeholder="Order received"
            />
          </div>

          {/* Trigger badge */}
          {form.trigger_event && (
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground">
                {t("waTpl.trigger")}
              </span>
              <Badge variant="secondary" className="text-xs">
                {triggerLabel}
              </Badge>
            </div>
          )}

          {/* Message textarea */}
          <div className="space-y-1.5">
            <Label className="text-xs">{t("waTpl.messageText")}</Label>
            <Textarea
              ref={textareaRef}
              value={form.message}
              onChange={(e) => setForm((f) => ({ ...f, message: e.target.value }))}
              placeholder={t("waTpl.messagePlaceholder")}
              rows={6}
              className="resize-none"
            />
          </div>

          {/* Variable chips */}
          <div className="space-y-1.5">
            <p className="text-xs text-muted-foreground font-medium">
              {t("waTpl.insertVariable")}
            </p>
            <div className="flex gap-1.5 overflow-x-auto pb-1 -mx-1 px-1 scrollbar-none">
              {VARIABLE_CHIPS.map((chip) => (
                <button
                  key={chip.variable}
                  type="button"
                  onClick={() => insertVariable(chip.variable)}
                  className="shrink-0 px-2.5 py-1 rounded-full text-xs font-medium border bg-card hover:bg-accent/50 active:scale-95 transition-all"
                >
                  {locale === "ru" ? chip.labelRu : chip.labelEn}
                </button>
              ))}
            </div>
          </div>

          {/* Preview */}
          <div className="pt-2 border-t">
            <TemplatePreview message={form.message} />
          </div>
        </div>
      </SheetContent>
    </Sheet>
  )
}
