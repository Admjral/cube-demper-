import { useT } from "@/lib/i18n"
import { SAMPLE_DATA } from "./template-constants"

function substituteVariables(message: string, data: Record<string, string>): string {
  return message.replace(/\{(\w+)\}/g, (match, key) => data[key] || match)
}

export function TemplatePreview({ message }: { message: string }) {
  const t = useT()

  if (!message.trim()) {
    return (
      <p className="text-xs text-muted-foreground text-center py-3">
        {t("waTpl.previewPlaceholder")}
      </p>
    )
  }

  const preview = substituteVariables(message, SAMPLE_DATA)

  return (
    <div className="space-y-1.5">
      <p className="text-xs text-muted-foreground font-medium">
        {t("waTpl.preview")}
      </p>
      <div className="bg-[#e7ffdb] dark:bg-[#005c4b] rounded-lg rounded-tr-none px-3 py-2 max-w-[85%] ml-auto">
        <p className="text-sm whitespace-pre-line break-words text-foreground">{preview}</p>
        <p className="text-[10px] text-muted-foreground text-right mt-1">12:00 ✓✓</p>
      </div>
    </div>
  )
}
