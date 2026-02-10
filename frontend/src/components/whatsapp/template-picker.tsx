import { useStore } from "@/store/use-store"
import { useT } from "@/lib/i18n"
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from "@/components/ui/sheet"
import { PRESET_TEMPLATES, type PresetTemplate } from "./template-constants"

interface TemplatePickerProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  onSelect: (preset: PresetTemplate) => void
}

export function TemplatePicker({ open, onOpenChange, onSelect }: TemplatePickerProps) {
  const t = useT()
  const { locale } = useStore()

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="bottom" className="max-h-[80vh] overflow-y-auto rounded-t-2xl">
        <SheetHeader className="pb-2">
          <SheetTitle>
            {t("waTpl.chooseTemplate")}
          </SheetTitle>
          <SheetDescription>
            {t("waTpl.chooseTemplateDesc")}
          </SheetDescription>
        </SheetHeader>
        <div className="grid grid-cols-2 gap-3 px-4 pb-6">
          {PRESET_TEMPLATES.map((preset) => (
            <button
              key={preset.id}
              onClick={() => onSelect(preset)}
              className="flex flex-col items-center gap-2 p-4 rounded-xl border bg-card hover:bg-accent/50 active:scale-[0.97] transition-all text-center"
            >
              <span className="text-3xl">{preset.icon}</span>
              <span className="text-sm font-medium leading-tight">
                {locale === "ru" ? preset.nameRu : preset.nameEn}
              </span>
            </button>
          ))}
        </div>
      </SheetContent>
    </Sheet>
  )
}
