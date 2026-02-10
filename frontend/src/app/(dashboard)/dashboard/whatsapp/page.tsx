"use client"

import { useState } from "react"
import { useStore } from "@/store/use-store"
import { useT } from "@/lib/i18n"
import { SubscriptionGate } from "@/components/shared/subscription-gate"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Switch } from "@/components/ui/switch"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  MessageSquare,
  Wifi,
  WifiOff,
  Clock,
  FileText,
  Settings2,
  Plus,
  Edit,
  Trash2,
  Loader2,
  QrCode,
  Bot,
  BarChart3,
  Send,
  History,
  CheckCircle,
  XCircle,
  Eye,
  AlertCircle,
  TrendingUp,
  ShoppingBag,
  Phone,
} from "lucide-react"
import { toast } from "sonner"
import {
  useWhatsAppSessions,
  useWhatsAppTemplates,
  useWhatsAppSettings,
  useCreateSession,
  useDeleteSession,
  useSessionQRCode,
  usePairByPhone,
  useCreateTemplate,
  useUpdateTemplate,
  useDeleteTemplate,
  useUpdateWhatsAppSettings,
  useSendWhatsAppMessage,
  useWhatsAppMessages,
  useWhatsAppStats,
  useAISalesmanSettings,
  useUpdateAISalesmanSettings,
  useAISalesmanHistory,
  useAISalesmanStats,
  useOrdersPollingStatus,
  useToggleOrdersPolling,
  WhatsAppSession,
  WhatsAppTemplate,
  MessageHistoryFilters,
} from "@/hooks/api/use-whatsapp"
import { useStores } from "@/hooks/api/use-stores"
import { FeatureGate } from "@/components/shared/feature-gate"
import { TemplatePicker } from "@/components/whatsapp/template-picker"
import { TemplateEditor } from "@/components/whatsapp/template-editor"
import { PRESET_TEMPLATES, type PresetTemplate } from "@/components/whatsapp/template-constants"

// Orders Polling Toggle Component
function OrdersPollingToggle({
  storeId,
  storeName,
  onToggle,
}: {
  storeId: string
  storeName: string
  onToggle: (enabled: boolean) => void
}) {
  const t = useT()
  const { locale } = useStore()
  const { data: pollingStatus, isLoading } = useOrdersPollingStatus(storeId)

  if (isLoading) {
    return (
      <div className="flex items-center justify-between p-3 rounded-lg bg-muted/30">
        <span className="text-sm font-medium">{storeName}</span>
        <Loader2 className="h-4 w-4 animate-spin" />
      </div>
    )
  }

  const isEnabled = pollingStatus?.orders_polling_enabled || false
  const lastSync = pollingStatus?.last_orders_sync

  return (
    <div className="flex items-center justify-between p-3 rounded-lg bg-muted/30">
      <div className="flex-1">
        <span className="text-sm font-medium">{storeName}</span>
        {lastSync && (
          <p className="text-xs text-muted-foreground">
            {t("wa.lastSync")}{" "}
            {new Date(lastSync).toLocaleString(locale === "ru" ? "ru-RU" : "kk-KZ")}
          </p>
        )}
      </div>
      <Switch
        checked={isEnabled}
        onCheckedChange={onToggle}
      />
    </div>
  )
}

export default function WhatsAppPage() {
  const { locale } = useStore()
  const t = useT()
  const [activeTab, setActiveTab] = useState("sessions")

  // Session state
  const [newSessionName, setNewSessionName] = useState("")
  const [showQRDialog, setShowQRDialog] = useState(false)
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null)

  // Phone pairing state
  const [pairingTab, setPairingTab] = useState<"qr" | "phone">("qr")
  const [pairingPhone, setPairingPhone] = useState("")
  const [pairingCode, setPairingCode] = useState<string | null>(null)

  // Template state
  const [templateFlowStep, setTemplateFlowStep] = useState<"closed" | "picker" | "editor">("closed")
  const [editingTemplate, setEditingTemplate] = useState<WhatsAppTemplate | null>(null)
  const [templateForm, setTemplateForm] = useState({
    name: "",
    name_en: "",
    message: "",
    variables: [] as string[],
    trigger_event: "" as string,
  })

  // Order event types for templates
  const orderEventTypes = [
    { value: "order_approved", label: t("wa.orderPaid") },
    { value: "order_accepted_by_merchant", label: t("wa.orderAccepted") },
    { value: "order_shipped", label: t("wa.orderShipped") },
    { value: "order_delivered", label: t("wa.orderDelivered") },
    { value: "order_completed", label: t("wa.orderCompleted") },
    { value: "review_request", label: t("wa.reviewRequest") },
  ]

  // Message filters
  const [messageFilters, setMessageFilters] = useState<MessageHistoryFilters>({
    page: 1,
    per_page: 20,
  })

  // Manual send state
  const [manualSendPhone, setManualSendPhone] = useState("")
  const [manualSendMessage, setManualSendMessage] = useState("")

  // API hooks
  const { data: sessions, isLoading: sessionsLoading } = useWhatsAppSessions()
  const { data: templates, isLoading: templatesLoading } = useWhatsAppTemplates()
  const { data: settings, isLoading: settingsLoading } = useWhatsAppSettings()
  const { data: qrData, isLoading: qrLoading } = useSessionQRCode(
    selectedSessionId || "",
    showQRDialog && !!selectedSessionId
  )
  const { data: messagesData, isLoading: messagesLoading } = useWhatsAppMessages(messageFilters)
  const { data: waStats, isLoading: statsLoading } = useWhatsAppStats(7)
  const { data: salesmanSettings, isLoading: salesmanSettingsLoading } = useAISalesmanSettings()
  const { data: salesmanHistory, isLoading: salesmanHistoryLoading } = useAISalesmanHistory(50)
  const { data: salesmanStats, isLoading: salesmanStatsLoading } = useAISalesmanStats(7)

  const createSession = useCreateSession()
  const deleteSession = useDeleteSession()
  const pairByPhone = usePairByPhone()
  const createTemplate = useCreateTemplate()
  const updateTemplate = useUpdateTemplate()
  const deleteTemplateM = useDeleteTemplate()
  const updateSettings = useUpdateWhatsAppSettings()
  const sendMessage = useSendWhatsAppMessage()
  const updateSalesmanSettings = useUpdateAISalesmanSettings()

  // Kaspi stores for orders polling
  const { data: kaspiStores } = useStores()
  const toggleOrdersPolling = useToggleOrdersPolling()

  // Get active session
  const activeSession = sessions?.find((s) => s.status === "connected")
  const isConnected = !!activeSession

  // Session handlers
  const handleCreateSession = async () => {
    if (!newSessionName.trim()) {
      toast.error(t("wa.enterSessionName"))
      return
    }
    try {
      const session = await createSession.mutateAsync(newSessionName)
      setSelectedSessionId(session.id)
      setShowQRDialog(true)
      setNewSessionName("")
      toast.success(locale === "ru" ? "Сессия создана. Отсканируйте QR-код" : "Session created. Scan QR code")
    } catch {
      toast.error(t("wa.sessionCreateError"))
    }
  }

  const handleDeleteSession = async (sessionId: string) => {
    if (!confirm(t("wa.deleteSession"))) return
    try {
      await deleteSession.mutateAsync(sessionId)
      toast.success(t("wa.sessionDeleted"))
    } catch {
      toast.error(t("wa.deleteError"))
    }
  }

  // Template handlers
  const handleSaveTemplate = async () => {
    if (!templateForm.name || !templateForm.message) {
      toast.error(t("wa.fillNameAndText"))
      return
    }
    try {
      if (editingTemplate) {
        await updateTemplate.mutateAsync({
          id: editingTemplate.id,
          name: templateForm.name,
          name_en: templateForm.name_en,
          message: templateForm.message,
          variables: templateForm.variables,
          trigger_event: templateForm.trigger_event || undefined,
        })
        toast.success(t("wa.templateUpdated"))
      } else {
        await createTemplate.mutateAsync({
          name: templateForm.name,
          name_en: templateForm.name_en,
          message: templateForm.message,
          variables: templateForm.variables,
          trigger_event: templateForm.trigger_event || undefined,
        })
        toast.success(t("wa.templateCreated"))
      }
      setTemplateFlowStep("closed")
      setEditingTemplate(null)
      setTemplateForm({ name: "", name_en: "", message: "", variables: [], trigger_event: "" })
    } catch {
      toast.error(t("wa.saveError"))
    }
  }

  const handleDeleteTemplate = async (templateId: string) => {
    if (!confirm(t("wa.deleteTemplate"))) return
    try {
      await deleteTemplateM.mutateAsync(templateId)
      toast.success(t("wa.templateDeleted"))
    } catch {
      toast.error(t("wa.deleteError"))
    }
  }

  const handleToggleTemplate = async (template: WhatsAppTemplate) => {
    try {
      await updateTemplate.mutateAsync({
        id: template.id,
        is_active: !template.is_active,
      })
    } catch {
      toast.error(t("wa.updateError"))
    }
  }

  const openEditTemplate = (template: WhatsAppTemplate) => {
    setEditingTemplate(template)
    setTemplateForm({
      name: template.name,
      name_en: template.name_en || "",
      message: template.message,
      variables: template.variables,
      trigger_event: template.trigger_event || "",
    })
    setTemplateFlowStep("editor")
  }

  // Handle preset selection from picker
  const handlePresetSelect = (preset: PresetTemplate) => {
    setEditingTemplate(null)
    setTemplateForm({
      name: locale === "ru" ? preset.nameRu : preset.nameEn,
      name_en: preset.nameEn,
      message: locale === "ru" ? preset.messageRu : preset.messageEn,
      variables: [],
      trigger_event: preset.triggerEvent,
    })
    setTemplateFlowStep("editor")
  }

  // Get trigger event label by value
  const getTriggerEventLabel = (value: string) => {
    const event = orderEventTypes.find(e => e.value === value)
    return event?.label || value
  }

  // Manual send handler
  const handleManualSend = async () => {
    if (!manualSendPhone || !manualSendMessage) {
      toast.error(locale === "ru" ? "Заполните номер и сообщение" : "Fill in phone and message")
      return
    }
    if (!activeSession) {
      toast.error(locale === "ru" ? "Нет активной сессии WhatsApp" : "No active WhatsApp session")
      return
    }
    try {
      await sendMessage.mutateAsync({
        session_id: activeSession.id,
        phone: manualSendPhone,
        message: manualSendMessage,
      })
      toast.success(locale === "ru" ? "Сообщение отправлено" : "Message sent")
      setManualSendPhone("")
      setManualSendMessage("")
    } catch {
      toast.error(locale === "ru" ? "Ошибка отправки" : "Error sending")
    }
  }

  // Status badge helper
  const getStatusBadge = (status: string) => {
    switch (status) {
      case "sent":
        return <Badge variant="secondary" className="gap-1"><CheckCircle className="h-3 w-3" />{t("wa.sent")}</Badge>
      case "delivered":
        return <Badge variant="default" className="gap-1"><CheckCircle className="h-3 w-3" />{t("wa.delivered")}</Badge>
      case "read":
        return <Badge className="gap-1 bg-green-500"><Eye className="h-3 w-3" />{t("wa.read")}</Badge>
      case "failed":
        return <Badge variant="destructive" className="gap-1"><XCircle className="h-3 w-3" />{t("wa.failed")}</Badge>
      default:
        return <Badge variant="outline" className="gap-1"><AlertCircle className="h-3 w-3" />{locale === "ru" ? "Ожидает" : "Pending"}</Badge>
    }
  }

  return (
    <SubscriptionGate>
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">WhatsApp</h1>
          <p className="text-muted-foreground">
            {t("wa.subtitle")}
          </p>
        </div>
        <Badge
          variant={isConnected ? "default" : "destructive"}
          className="flex items-center gap-2 px-3 py-1.5"
        >
          {isConnected ? (
            <><Wifi className="h-4 w-4" />{t("wa.connected")}</>
          ) : (
            <><WifiOff className="h-4 w-4" />{t("wa.disconnected")}</>
          )}
        </Badge>
      </div>

      {/* Main Tabs */}
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="w-full grid grid-cols-2 sm:grid-cols-4 lg:w-auto lg:inline-grid">
          <TabsTrigger value="sessions" className="gap-2">
            <MessageSquare className="h-4 w-4 hidden sm:block" />
            {t("wa.sessions")}
          </TabsTrigger>
          <TabsTrigger value="salesman" className="gap-2">
            <Bot className="h-4 w-4 hidden sm:block" />
            {t("wa.aiSalesman")}
          </TabsTrigger>
          <TabsTrigger value="messages" className="gap-2">
            <History className="h-4 w-4 hidden sm:block" />
            {t("wa.messages")}
          </TabsTrigger>
          <TabsTrigger value="stats" className="gap-2">
            <BarChart3 className="h-4 w-4 hidden sm:block" />
            {t("wa.stats")}
          </TabsTrigger>
        </TabsList>

        {/* Sessions Tab */}
        <TabsContent value="sessions" className="space-y-6 mt-6">
          {/* Sessions Card */}
          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="flex items-center justify-between">
                <span className="flex items-center gap-2">
                  <MessageSquare className="h-5 w-5" />
                  {t("wa.whatsappSessions")}
                </span>
                <Dialog>
                  <DialogTrigger asChild>
                    <Button size="sm">
                      <Plus className="h-4 w-4 mr-2" />
                      {t("common.add")}
                    </Button>
                  </DialogTrigger>
                  <DialogContent>
                    <DialogHeader>
                      <DialogTitle>{t("wa.newSession")}</DialogTitle>
                      <DialogDescription>
                        {t("wa.newSessionDesc")}
                      </DialogDescription>
                    </DialogHeader>
                    <div className="space-y-4 pt-4">
                      <div className="space-y-2">
                        <Label>{t("wa.sessionName")}</Label>
                        <Input
                          placeholder={t("wa.sessionDefault")}
                          value={newSessionName}
                          onChange={(e) => setNewSessionName(e.target.value)}
                        />
                      </div>
                      <Button onClick={handleCreateSession} disabled={createSession.isPending} className="w-full">
                        {createSession.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                        {t("common.create")}
                      </Button>
                    </div>
                  </DialogContent>
                </Dialog>
              </CardTitle>
            </CardHeader>
            <CardContent>
              {sessionsLoading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                </div>
              ) : sessions && sessions.length > 0 ? (
                <div className="space-y-3">
                  {sessions.map((session) => (
                    <div key={session.id} className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 p-3 rounded-lg bg-muted/30">
                      <div className="flex items-center gap-3">
                        <div className={`w-3 h-3 rounded-full shrink-0 ${
                          session.status === "connected" ? "bg-green-500" :
                          session.status === "qr_pending" ? "bg-yellow-500 animate-pulse" : "bg-red-500"
                        }`} />
                        <div className="min-w-0">
                          <p className="font-medium truncate">{session.session_name}</p>
                          <p className="text-sm text-muted-foreground">{session.phone_number || session.status}</p>
                        </div>
                      </div>
                      <div className="flex gap-2 self-end sm:self-auto">
                        {session.status === "qr_pending" && (
                          <Button variant="outline" size="sm" onClick={() => { setSelectedSessionId(session.id); setShowQRDialog(true) }}>
                            <QrCode className="h-4 w-4 mr-2" />QR
                          </Button>
                        )}
                        <Button variant="ghost" size="icon" className="text-destructive" onClick={() => handleDeleteSession(session.id)}>
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-center text-muted-foreground py-8">
                  {t("wa.noSessions")}
                </p>
              )}
            </CardContent>
          </Card>

          {/* Manual Send Card */}
          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Send className="h-5 w-5" />
                {t("wa.sendMessage")}
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid sm:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label>{t("wa.phoneNumber")}</Label>
                  <Input
                    placeholder="+7 777 123 4567"
                    value={manualSendPhone}
                    onChange={(e) => setManualSendPhone(e.target.value)}
                  />
                </div>
              </div>
              <div className="space-y-2">
                <Label>{t("wa.message")}</Label>
                <Textarea
                  placeholder={t("wa.messagePlaceholder")}
                  value={manualSendMessage}
                  onChange={(e) => setManualSendMessage(e.target.value)}
                  rows={3}
                />
              </div>
              <Button onClick={handleManualSend} disabled={sendMessage.isPending || !isConnected}>
                {sendMessage.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                <Send className="h-4 w-4 mr-2" />
                {t("common.send")}
              </Button>
              {!isConnected && (
                <p className="text-sm text-destructive">
                  {t("wa.connectToSend")}
                </p>
              )}
            </CardContent>
          </Card>

          {/* Orders Monitoring */}
          <FeatureGate feature="whatsapp_auto">
          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <ShoppingBag className="h-5 w-5" />
                {t("wa.ordersMonitoring")}
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground mb-4">
                {t("wa.ordersMonitoringDesc")}
              </p>
              {kaspiStores && kaspiStores.length > 0 ? (
                <div className="space-y-3">
                  {kaspiStores.map((store) => (
                    <OrdersPollingToggle
                      key={store.id}
                      storeId={store.id}
                      storeName={store.name}
                      onToggle={(enabled) => {
                        toggleOrdersPolling.mutate(
                          { storeId: store.id, enabled },
                          {
                            onSuccess: () => {
                              toast.success(
                                `${t(enabled ? "wa.monitoringEnabled" : "wa.monitoringDisabled")} ${t("wa.for")} ${store.name}`
                              )
                            },
                            onError: () => {
                              toast.error(t("common.error"))
                            },
                          }
                        )
                      }}
                    />
                  ))}
                </div>
              ) : (
                <p className="text-sm text-muted-foreground text-center py-4">
                  {t("wa.noConnectedStores")}
                </p>
              )}
            </CardContent>
          </Card>
          </FeatureGate>

          {/* Templates */}
          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="flex items-center justify-between">
                <span className="flex items-center gap-2">
                  <FileText className="h-5 w-5" />
                  {t("wa.messageTemplates")}
                </span>
                <Button size="sm" onClick={() => { setEditingTemplate(null); setTemplateForm({ name: "", name_en: "", message: "", variables: [], trigger_event: "" }); setTemplateFlowStep("picker") }}>
                  <Plus className="h-4 w-4 mr-2" />
                  {t("common.add")}
                </Button>
              </CardTitle>
            </CardHeader>
            <CardContent>
              {templatesLoading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                </div>
              ) : templates && templates.length > 0 ? (
                <div className="space-y-4">
                  {templates.map((template) => (
                    <div key={template.id} className="p-4 rounded-lg bg-muted/30">
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1">
                          <div className="flex items-center gap-3 flex-wrap">
                            <h3 className="font-semibold">{locale === "ru" ? template.name : template.name_en || template.name}</h3>
                            {template.trigger_event && (
                              <Badge variant="secondary" className="text-xs gap-1">
                                <Clock className="h-3 w-3" />
                                {getTriggerEventLabel(template.trigger_event)}
                              </Badge>
                            )}
                            <Switch checked={template.is_active} onCheckedChange={() => handleToggleTemplate(template)} />
                          </div>
                          <p className="text-sm text-muted-foreground mt-2 p-3 bg-background/50 rounded-lg">{template.message}</p>
                          <div className="flex flex-wrap gap-2 mt-3">
                            {template.variables.map((v) => (
                              <Badge key={v} variant="outline" className="text-xs">{`{{${v}}}`}</Badge>
                            ))}
                          </div>
                        </div>
                        <div className="flex gap-2">
                          <Button variant="ghost" size="icon" onClick={() => openEditTemplate(template)}>
                            <Edit className="h-4 w-4" />
                          </Button>
                          <Button variant="ghost" size="icon" className="text-destructive" onClick={() => handleDeleteTemplate(template.id)}>
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-center text-muted-foreground py-8">
                  {t("wa.noTemplates")}
                </p>
              )}
            </CardContent>
          </Card>

          {/* Settings */}
          {settings && (
            <Card className="glass-card">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Settings2 className="h-5 w-5" />
                  {t("wa.settings")}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-4">
                  <div className="space-y-2">
                    <Label>{t("wa.dailyLimit")}</Label>
                    <Input
                      type="number"
                      value={settings.daily_limit}
                      onChange={(e) => updateSettings.mutate({ daily_limit: Number(e.target.value) })}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>{t("wa.interval")}</Label>
                    <Input
                      type="number"
                      value={settings.interval_seconds}
                      onChange={(e) => updateSettings.mutate({ interval_seconds: Number(e.target.value) })}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>{t("wa.workStart")}</Label>
                    <Input type="time" value={settings.work_hours_start} onChange={(e) => updateSettings.mutate({ work_hours_start: e.target.value })} />
                  </div>
                  <div className="space-y-2">
                    <Label>{t("wa.workEnd")}</Label>
                    <Input type="time" value={settings.work_hours_end} onChange={(e) => updateSettings.mutate({ work_hours_end: e.target.value })} />
                  </div>
                </div>
              </CardContent>
            </Card>
          )}
        </TabsContent>

        {/* AI Salesman Tab */}
        <TabsContent value="salesman" className="space-y-6 mt-6">
          <FeatureGate feature="ai_salesman">
          {/* AI Settings */}
          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Bot className="h-5 w-5" />
                {t("wa.aiSettings")}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {salesmanSettingsLoading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                </div>
              ) : salesmanSettings && salesmanSettings.length > 0 ? (
                <div className="space-y-6">
                  {salesmanSettings.map((store) => (
                    <div key={store.store_id} className="p-4 rounded-lg bg-muted/30 space-y-4">
                      <div className="flex items-center justify-between">
                        <h3 className="font-semibold">{store.store_name}</h3>
                        <div className="flex items-center gap-2">
                          <span className="text-sm text-muted-foreground">
                            {t("wa.aiEnabled")}
                          </span>
                          <Switch
                            checked={store.ai_enabled}
                            onCheckedChange={(checked) => updateSalesmanSettings.mutate({ storeId: store.store_id, ai_enabled: checked })}
                          />
                        </div>
                      </div>

                      {store.ai_enabled && (
                        <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
                          <div className="space-y-2">
                            <Label>{t("wa.tone")}</Label>
                            <Select
                              value={store.ai_tone || "friendly"}
                              onValueChange={(value) => updateSalesmanSettings.mutate({ storeId: store.store_id, ai_tone: value })}
                            >
                              <SelectTrigger>
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="friendly">{t("wa.friendly")}</SelectItem>
                                <SelectItem value="professional">{t("wa.professional")}</SelectItem>
                                <SelectItem value="casual">{t("wa.casual")}</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>
                          <div className="space-y-2">
                            <Label>{t("wa.maxDiscount")}</Label>
                            <Input
                              type="number"
                              value={store.ai_discount_percent || ""}
                              placeholder="0"
                              onChange={(e) => updateSalesmanSettings.mutate({ storeId: store.store_id, ai_discount_percent: Number(e.target.value) || null })}
                            />
                          </div>
                          <div className="space-y-2">
                            <Label>{t("wa.promoCode")}</Label>
                            <Input
                              value={store.ai_promo_code || ""}
                              placeholder="PROMO10"
                              onChange={(e) => updateSalesmanSettings.mutate({ storeId: store.store_id, ai_promo_code: e.target.value || null })}
                            />
                          </div>
                          <div className="space-y-2">
                            <Label>{t("wa.reviewBonus")}</Label>
                            <Input
                              value={store.ai_review_bonus || ""}
                              placeholder={t("wa.reviewBonusDefault")}
                              onChange={(e) => updateSalesmanSettings.mutate({ storeId: store.store_id, ai_review_bonus: e.target.value || null })}
                            />
                          </div>
                          <div className="space-y-2">
                            <Label>{t("wa.delay")}</Label>
                            <Input
                              type="number"
                              value={store.ai_send_delay_minutes}
                              onChange={(e) => updateSalesmanSettings.mutate({ storeId: store.store_id, ai_send_delay_minutes: Number(e.target.value) })}
                            />
                          </div>
                          <div className="space-y-2">
                            <Label>{t("wa.messagesLimit")}</Label>
                            <Input
                              type="number"
                              value={store.ai_max_messages_per_day}
                              onChange={(e) => updateSalesmanSettings.mutate({ storeId: store.store_id, ai_max_messages_per_day: Number(e.target.value) })}
                            />
                          </div>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-center text-muted-foreground py-8">
                  {t("wa.noStoresAI")}
                </p>
              )}
            </CardContent>
          </Card>

          {/* AI Stats */}
          {salesmanStats && (
            <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <Card className="glass-card">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-4">
                    <div className="p-3 rounded-full bg-primary/10">
                      <Send className="h-6 w-6 text-primary" />
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">{t("wa.totalMessages")}</p>
                      <p className="text-2xl font-bold">{salesmanStats.total_messages}</p>
                    </div>
                  </div>
                </CardContent>
              </Card>
              <Card className="glass-card">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-4">
                    <div className="p-3 rounded-full bg-green-500/10">
                      <TrendingUp className="h-6 w-6 text-green-500" />
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">{t("wa.newOrders")}</p>
                      <p className="text-2xl font-bold">{salesmanStats.by_trigger?.new_order || 0}</p>
                    </div>
                  </div>
                </CardContent>
              </Card>
              <Card className="glass-card">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-4">
                    <div className="p-3 rounded-full bg-blue-500/10">
                      <History className="h-6 w-6 text-blue-500" />
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">{t("wa.repeatCustomers")}</p>
                      <p className="text-2xl font-bold">{salesmanStats.by_trigger?.repeat_customer || 0}</p>
                    </div>
                  </div>
                </CardContent>
              </Card>
              <Card className="glass-card">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-4">
                    <div className="p-3 rounded-full bg-yellow-500/10">
                      <MessageSquare className="h-6 w-6 text-yellow-500" />
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">{t("wa.reviewRequests")}</p>
                      <p className="text-2xl font-bold">{salesmanStats.by_trigger?.review_request || 0}</p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          )}

          {/* AI History */}
          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <History className="h-5 w-5" />
                {t("wa.aiMessageHistory")}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {salesmanHistoryLoading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                </div>
              ) : salesmanHistory?.messages && salesmanHistory.messages.length > 0 ? (
                <div className="space-y-3">
                  {salesmanHistory.messages.map((msg) => (
                    <div key={msg.id} className="p-4 rounded-lg bg-muted/30">
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1">
                          <div className="flex items-center gap-2 mb-2">
                            <span className="font-medium">{msg.customer_name || msg.customer_phone}</span>
                            <Badge variant="outline">{msg.trigger}</Badge>
                          </div>
                          <p className="text-sm text-muted-foreground">{msg.text}</p>
                          {msg.products_suggested.length > 0 && (
                            <div className="flex flex-wrap gap-1 mt-2">
                              {msg.products_suggested.map((p) => (
                                <Badge key={p} variant="secondary" className="text-xs">{p}</Badge>
                              ))}
                            </div>
                          )}
                        </div>
                        <span className="text-xs text-muted-foreground whitespace-nowrap">
                          {new Date(msg.created_at).toLocaleDateString()}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-center text-muted-foreground py-8">
                  {t("wa.noAIMessages")}
                </p>
              )}
            </CardContent>
          </Card>
          </FeatureGate>
        </TabsContent>

        {/* Messages Tab */}
        <TabsContent value="messages" className="space-y-6 mt-6">
          {/* Filters */}
          <Card className="glass-card">
            <CardContent className="pt-6">
              <div className="grid sm:grid-cols-2 lg:grid-cols-5 gap-4">
                <div className="space-y-2">
                  <Label>{t("common.status")}</Label>
                  <Select
                    value={messageFilters.status_filter || "all"}
                    onValueChange={(v) => setMessageFilters({ ...messageFilters, status_filter: v === "all" ? undefined : v, page: 1 })}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">{t("common.all")}</SelectItem>
                      <SelectItem value="sent">{t("wa.sent")}</SelectItem>
                      <SelectItem value="delivered">{t("wa.delivered")}</SelectItem>
                      <SelectItem value="read">{t("wa.read")}</SelectItem>
                      <SelectItem value="failed">{t("wa.failed")}</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label>{t("wa.phone")}</Label>
                  <Input
                    placeholder="+7 777..."
                    value={messageFilters.phone || ""}
                    onChange={(e) => setMessageFilters({ ...messageFilters, phone: e.target.value || undefined, page: 1 })}
                  />
                </div>
                <div className="space-y-2">
                  <Label>{t("wa.dateFrom")}</Label>
                  <Input
                    type="date"
                    value={messageFilters.date_from || ""}
                    onChange={(e) => setMessageFilters({ ...messageFilters, date_from: e.target.value || undefined, page: 1 })}
                  />
                </div>
                <div className="space-y-2">
                  <Label>{t("wa.dateTo")}</Label>
                  <Input
                    type="date"
                    value={messageFilters.date_to || ""}
                    onChange={(e) => setMessageFilters({ ...messageFilters, date_to: e.target.value || undefined, page: 1 })}
                  />
                </div>
                <div className="flex items-end">
                  <Button variant="outline" onClick={() => setMessageFilters({ page: 1, per_page: 20 })} className="w-full">
                    {t("common.reset")}
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Messages List */}
          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="flex items-center justify-between">
                <span className="flex items-center gap-2">
                  <History className="h-5 w-5" />
                  {t("wa.messageHistory")}
                </span>
                {messagesData && (
                  <span className="text-sm font-normal text-muted-foreground">
                    {t("wa.total")} {messagesData.total}
                  </span>
                )}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {messagesLoading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                </div>
              ) : messagesData?.messages && messagesData.messages.length > 0 ? (
                <div className="space-y-3">
                  {messagesData.messages.map((msg) => (
                    <div key={msg.id} className="p-4 rounded-lg bg-muted/30">
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1">
                          <div className="flex items-center gap-2 mb-2 flex-wrap">
                            <span className="font-medium">{msg.recipient_name || msg.recipient_phone}</span>
                            {getStatusBadge(msg.status)}
                            {msg.template_name && <Badge variant="secondary">{msg.template_name}</Badge>}
                          </div>
                          <p className="text-sm text-muted-foreground line-clamp-2">{msg.message_content}</p>
                          {msg.error_message && (
                            <p className="text-xs text-destructive mt-1">{msg.error_message}</p>
                          )}
                        </div>
                        <span className="text-xs text-muted-foreground whitespace-nowrap">
                          {new Date(msg.created_at).toLocaleString()}
                        </span>
                      </div>
                    </div>
                  ))}

                  {/* Pagination */}
                  {messagesData.total > messageFilters.per_page! && (
                    <div className="flex justify-center gap-2 pt-4">
                      <Button
                        variant="outline"
                        size="sm"
                        disabled={messageFilters.page === 1}
                        onClick={() => setMessageFilters({ ...messageFilters, page: (messageFilters.page || 1) - 1 })}
                      >
                        {t("wa.previous")}
                      </Button>
                      <span className="flex items-center px-4 text-sm text-muted-foreground">
                        {messageFilters.page} / {Math.ceil(messagesData.total / (messageFilters.per_page || 20))}
                      </span>
                      <Button
                        variant="outline"
                        size="sm"
                        disabled={(messageFilters.page || 1) >= Math.ceil(messagesData.total / (messageFilters.per_page || 20))}
                        onClick={() => setMessageFilters({ ...messageFilters, page: (messageFilters.page || 1) + 1 })}
                      >
                        {t("common.next")}
                      </Button>
                    </div>
                  )}
                </div>
              ) : (
                <p className="text-center text-muted-foreground py-8">
                  {t("wa.noMessages")}
                </p>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Stats Tab */}
        <TabsContent value="stats" className="space-y-6 mt-6">
          {statsLoading ? (
            <div className="flex justify-center py-8">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : waStats && (
            <>
              {/* Summary Cards */}
              <div className="grid sm:grid-cols-2 lg:grid-cols-5 gap-4">
                <Card className="glass-card">
                  <CardContent className="pt-6">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">{t("wa.sent")}</p>
                      <p className="text-3xl font-bold text-primary">{waStats.total_sent}</p>
                    </div>
                  </CardContent>
                </Card>
                <Card className="glass-card">
                  <CardContent className="pt-6">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">{t("wa.delivered")}</p>
                      <p className="text-3xl font-bold text-green-500">{waStats.total_delivered}</p>
                      <p className="text-xs text-muted-foreground">{waStats.delivery_rate}%</p>
                    </div>
                  </CardContent>
                </Card>
                <Card className="glass-card">
                  <CardContent className="pt-6">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">{t("wa.read")}</p>
                      <p className="text-3xl font-bold text-blue-500">{waStats.total_read}</p>
                      <p className="text-xs text-muted-foreground">{waStats.read_rate}%</p>
                    </div>
                  </CardContent>
                </Card>
                <Card className="glass-card">
                  <CardContent className="pt-6">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">{t("wa.errors")}</p>
                      <p className="text-3xl font-bold text-destructive">{waStats.total_failed}</p>
                    </div>
                  </CardContent>
                </Card>
                <Card className="glass-card">
                  <CardContent className="pt-6">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">{t("wa.today")}</p>
                      <p className="text-3xl font-bold">{waStats.today_sent}</p>
                      <p className="text-xs text-muted-foreground">/ {waStats.today_limit}</p>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Chart */}
              <Card className="glass-card">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <BarChart3 className="h-5 w-5" />
                    {t("wa.weekMessages")}
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {waStats.messages_by_day.length > 0 ? (
                    <div className="space-y-4">
                      {waStats.messages_by_day.map((day) => (
                        <div key={day.date} className="space-y-2">
                          <div className="flex items-center justify-between text-sm">
                            <span>{new Date(day.date).toLocaleDateString(locale === "ru" ? "ru-RU" : "kk-KZ", { weekday: "short", day: "numeric", month: "short" })}</span>
                            <span className="text-muted-foreground">{day.sent} {t("wa.msgs")}</span>
                          </div>
                          <div className="flex h-4 rounded-full overflow-hidden bg-muted">
                            {day.sent > 0 && (
                              <>
                                <div
                                  className="bg-green-500"
                                  style={{ width: `${(day.read / day.sent) * 100}%` }}
                                  title={`${t("wa.read")}: ${day.read}`}
                                />
                                <div
                                  className="bg-blue-500"
                                  style={{ width: `${((day.delivered - day.read) / day.sent) * 100}%` }}
                                  title={`${t("wa.delivered")}: ${day.delivered - day.read}`}
                                />
                                <div
                                  className="bg-gray-400"
                                  style={{ width: `${((day.sent - day.delivered) / day.sent) * 100}%` }}
                                  title={`${t("wa.sent")}: ${day.sent - day.delivered}`}
                                />
                                {day.failed > 0 && (
                                  <div
                                    className="bg-red-500"
                                    style={{ width: `${(day.failed / (day.sent + day.failed)) * 100}%` }}
                                    title={`${t("wa.errors")}: ${day.failed}`}
                                  />
                                )}
                              </>
                            )}
                          </div>
                        </div>
                      ))}
                      <div className="flex items-center gap-4 text-xs text-muted-foreground pt-2">
                        <div className="flex items-center gap-1"><div className="w-3 h-3 rounded bg-green-500" />{t("wa.read")}</div>
                        <div className="flex items-center gap-1"><div className="w-3 h-3 rounded bg-blue-500" />{t("wa.delivered")}</div>
                        <div className="flex items-center gap-1"><div className="w-3 h-3 rounded bg-gray-400" />{t("wa.sent")}</div>
                        <div className="flex items-center gap-1"><div className="w-3 h-3 rounded bg-red-500" />{t("wa.errors")}</div>
                      </div>
                    </div>
                  ) : (
                    <p className="text-center text-muted-foreground py-8">
                      {locale === "ru" ? "Нет данных за эту неделю" : "No data for this week"}
                    </p>
                  )}
                </CardContent>
              </Card>
            </>
          )}
        </TabsContent>
      </Tabs>

      {/* QR / Phone Pairing Dialog */}
      <Dialog open={showQRDialog} onOpenChange={(open) => {
        setShowQRDialog(open)
        if (!open) {
          setPairingCode(null)
          setPairingPhone("")
          setPairingTab("qr")
        }
      }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t("wa.scanQR")}</DialogTitle>
            <DialogDescription>
              {t("wa.scanQRDesc")}
            </DialogDescription>
          </DialogHeader>

          {/* Tabs: QR / Phone */}
          <div className="flex gap-2 border-b pb-2">
            <Button
              variant={pairingTab === "qr" ? "default" : "ghost"}
              size="sm"
              onClick={() => setPairingTab("qr")}
            >
              <QrCode className="h-4 w-4 mr-2" />
              {t("wa.qrTab")}
            </Button>
            <Button
              variant={pairingTab === "phone" ? "default" : "ghost"}
              size="sm"
              onClick={() => setPairingTab("phone")}
            >
              <Phone className="h-4 w-4 mr-2" />
              {t("wa.phoneTab")}
            </Button>
          </div>

          {pairingTab === "qr" ? (
            <div className="flex flex-col items-center py-4">
              {qrLoading ? (
                <Loader2 className="h-8 w-8 animate-spin" />
              ) : qrData?.qr_code ? (
                <img src={`data:image/png;base64,${qrData.qr_code}`} alt="QR Code" className="w-64 h-64" />
              ) : qrData?.status === "connected" ? (
                <div className="text-center">
                  <Wifi className="h-16 w-16 text-green-500 mx-auto mb-4" />
                  <p className="text-green-500 font-medium">{t("wa.connectedBang")}</p>
                </div>
              ) : (
                <p className="text-muted-foreground">{t("wa.qrLoading")}</p>
              )}
            </div>
          ) : (
            <div className="space-y-4 py-4">
              <div className="space-y-2">
                <Label>{t("wa.enterPhone")}</Label>
                <Input
                  placeholder="+7 700 123 4567"
                  value={pairingPhone}
                  onChange={(e) => setPairingPhone(e.target.value)}
                  disabled={pairByPhone.isPending}
                />
              </div>

              {!pairingCode ? (
                <Button
                  className="w-full"
                  onClick={async () => {
                    if (!selectedSessionId || !pairingPhone.trim()) return
                    try {
                      const result = await pairByPhone.mutateAsync({
                        sessionId: selectedSessionId,
                        phoneNumber: pairingPhone,
                      })
                      setPairingCode(result.code)
                    } catch {
                      toast.error(t("wa.pairingError"))
                    }
                  }}
                  disabled={pairByPhone.isPending || !pairingPhone.trim()}
                >
                  {pairByPhone.isPending ? (
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  ) : (
                    <Phone className="h-4 w-4 mr-2" />
                  )}
                  {t("wa.getCode")}
                </Button>
              ) : (
                <div className="text-center space-y-3">
                  <p className="text-sm text-muted-foreground">{t("wa.pairingCode")}</p>
                  <p className="text-3xl font-mono font-bold tracking-widest">{pairingCode}</p>
                  <p className="text-xs text-muted-foreground">
                    {t("wa.pairingInstruction")}
                  </p>
                </div>
              )}
            </div>
          )}
        </DialogContent>
      </Dialog>

      {/* Template Picker (bottom sheet) */}
      <TemplatePicker
        open={templateFlowStep === "picker"}
        onOpenChange={(open) => { if (!open) setTemplateFlowStep("closed") }}
        onSelect={handlePresetSelect}
      />

      {/* Template Editor (right sheet, fullscreen on mobile) */}
      <TemplateEditor
        open={templateFlowStep === "editor"}
        onOpenChange={(open) => { if (!open) setTemplateFlowStep("closed") }}
        form={templateForm}
        setForm={setTemplateForm}
        onSave={handleSaveTemplate}
        isSaving={createTemplate.isPending || updateTemplate.isPending}
        isEditing={!!editingTemplate}
        triggerLabel={templateForm.trigger_event ? getTriggerEventLabel(templateForm.trigger_event) : ""}
      />
    </div>
    </SubscriptionGate>
  )
}
