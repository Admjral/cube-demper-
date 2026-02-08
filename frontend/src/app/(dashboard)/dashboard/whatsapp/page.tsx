"use client"

import { useState } from "react"
import { useStore } from "@/store/use-store"
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
} from "lucide-react"
import { toast } from "sonner"
import {
  useWhatsAppSessions,
  useWhatsAppTemplates,
  useWhatsAppSettings,
  useCreateSession,
  useDeleteSession,
  useSessionQRCode,
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

// Orders Polling Toggle Component
function OrdersPollingToggle({
  storeId,
  storeName,
  onToggle,
  locale,
}: {
  storeId: string
  storeName: string
  onToggle: (enabled: boolean) => void
  locale: string
}) {
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
            {locale === "ru" ? "Последняя синхронизация: " : "Last sync: "}
            {new Date(lastSync).toLocaleString(locale === "ru" ? "ru-RU" : "en-US")}
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
  const [activeTab, setActiveTab] = useState("sessions")

  // Session state
  const [newSessionName, setNewSessionName] = useState("")
  const [showQRDialog, setShowQRDialog] = useState(false)
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null)

  // Template state
  const [showTemplateDialog, setShowTemplateDialog] = useState(false)
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
    { value: "order_approved", label: locale === "ru" ? "Заказ оплачен" : "Order paid" },
    { value: "order_accepted_by_merchant", label: locale === "ru" ? "Заказ принят" : "Order accepted" },
    { value: "order_shipped", label: locale === "ru" ? "Заказ отправлен" : "Order shipped" },
    { value: "order_delivered", label: locale === "ru" ? "Заказ доставлен" : "Order delivered" },
    { value: "order_completed", label: locale === "ru" ? "Заказ завершён" : "Order completed" },
    { value: "review_request", label: locale === "ru" ? "Запрос отзыва" : "Review request" },
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
      toast.error(locale === "ru" ? "Введите имя сессии" : "Enter session name")
      return
    }
    try {
      const session = await createSession.mutateAsync(newSessionName)
      setSelectedSessionId(session.id)
      setShowQRDialog(true)
      setNewSessionName("")
      toast.success(locale === "ru" ? "Сессия создана. Отсканируйте QR-код" : "Session created. Scan QR code")
    } catch {
      toast.error(locale === "ru" ? "Ошибка создания сессии" : "Error creating session")
    }
  }

  const handleDeleteSession = async (sessionId: string) => {
    if (!confirm(locale === "ru" ? "Удалить сессию?" : "Delete session?")) return
    try {
      await deleteSession.mutateAsync(sessionId)
      toast.success(locale === "ru" ? "Сессия удалена" : "Session deleted")
    } catch {
      toast.error(locale === "ru" ? "Ошибка удаления" : "Error deleting")
    }
  }

  // Template handlers
  const handleSaveTemplate = async () => {
    if (!templateForm.name || !templateForm.message) {
      toast.error(locale === "ru" ? "Заполните название и текст шаблона" : "Fill in name and message")
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
        toast.success(locale === "ru" ? "Шаблон обновлён" : "Template updated")
      } else {
        await createTemplate.mutateAsync({
          name: templateForm.name,
          name_en: templateForm.name_en,
          message: templateForm.message,
          variables: templateForm.variables,
          trigger_event: templateForm.trigger_event || undefined,
        })
        toast.success(locale === "ru" ? "Шаблон создан" : "Template created")
      }
      setShowTemplateDialog(false)
      setEditingTemplate(null)
      setTemplateForm({ name: "", name_en: "", message: "", variables: [], trigger_event: "" })
    } catch {
      toast.error(locale === "ru" ? "Ошибка сохранения" : "Error saving")
    }
  }

  const handleDeleteTemplate = async (templateId: string) => {
    if (!confirm(locale === "ru" ? "Удалить шаблон?" : "Delete template?")) return
    try {
      await deleteTemplateM.mutateAsync(templateId)
      toast.success(locale === "ru" ? "Шаблон удалён" : "Template deleted")
    } catch {
      toast.error(locale === "ru" ? "Ошибка удаления" : "Error deleting")
    }
  }

  const handleToggleTemplate = async (template: WhatsAppTemplate) => {
    try {
      await updateTemplate.mutateAsync({
        id: template.id,
        is_active: !template.is_active,
      })
    } catch {
      toast.error(locale === "ru" ? "Ошибка обновления" : "Error updating")
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
    setShowTemplateDialog(true)
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
        return <Badge variant="secondary" className="gap-1"><CheckCircle className="h-3 w-3" />{locale === "ru" ? "Отправлено" : "Sent"}</Badge>
      case "delivered":
        return <Badge variant="default" className="gap-1"><CheckCircle className="h-3 w-3" />{locale === "ru" ? "Доставлено" : "Delivered"}</Badge>
      case "read":
        return <Badge className="gap-1 bg-green-500"><Eye className="h-3 w-3" />{locale === "ru" ? "Прочитано" : "Read"}</Badge>
      case "failed":
        return <Badge variant="destructive" className="gap-1"><XCircle className="h-3 w-3" />{locale === "ru" ? "Ошибка" : "Failed"}</Badge>
      default:
        return <Badge variant="outline" className="gap-1"><AlertCircle className="h-3 w-3" />{locale === "ru" ? "Ожидает" : "Pending"}</Badge>
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">WhatsApp</h1>
          <p className="text-muted-foreground">
            {locale === "ru" ? "Автоматизация сообщений и AI продажник" : "Message automation and AI Salesman"}
          </p>
        </div>
        <Badge
          variant={isConnected ? "default" : "destructive"}
          className="flex items-center gap-2 px-3 py-1.5"
        >
          {isConnected ? (
            <><Wifi className="h-4 w-4" />{locale === "ru" ? "Подключено" : "Connected"}</>
          ) : (
            <><WifiOff className="h-4 w-4" />{locale === "ru" ? "Отключено" : "Disconnected"}</>
          )}
        </Badge>
      </div>

      {/* Main Tabs */}
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="w-full grid grid-cols-4 lg:w-auto lg:inline-grid">
          <TabsTrigger value="sessions" className="gap-2">
            <MessageSquare className="h-4 w-4 hidden sm:block" />
            {locale === "ru" ? "Сессии" : "Sessions"}
          </TabsTrigger>
          <TabsTrigger value="salesman" className="gap-2">
            <Bot className="h-4 w-4 hidden sm:block" />
            {locale === "ru" ? "AI Продажник" : "AI Salesman"}
          </TabsTrigger>
          <TabsTrigger value="messages" className="gap-2">
            <History className="h-4 w-4 hidden sm:block" />
            {locale === "ru" ? "Сообщения" : "Messages"}
          </TabsTrigger>
          <TabsTrigger value="stats" className="gap-2">
            <BarChart3 className="h-4 w-4 hidden sm:block" />
            {locale === "ru" ? "Статистика" : "Stats"}
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
                  {locale === "ru" ? "Сессии WhatsApp" : "WhatsApp Sessions"}
                </span>
                <Dialog>
                  <DialogTrigger asChild>
                    <Button size="sm">
                      <Plus className="h-4 w-4 mr-2" />
                      {locale === "ru" ? "Добавить" : "Add"}
                    </Button>
                  </DialogTrigger>
                  <DialogContent>
                    <DialogHeader>
                      <DialogTitle>{locale === "ru" ? "Новая сессия" : "New Session"}</DialogTitle>
                      <DialogDescription>
                        {locale === "ru" ? "Введите имя для новой WhatsApp сессии" : "Enter a name for the new WhatsApp session"}
                      </DialogDescription>
                    </DialogHeader>
                    <div className="space-y-4 pt-4">
                      <div className="space-y-2">
                        <Label>{locale === "ru" ? "Имя сессии" : "Session name"}</Label>
                        <Input
                          placeholder={locale === "ru" ? "Мой WhatsApp" : "My WhatsApp"}
                          value={newSessionName}
                          onChange={(e) => setNewSessionName(e.target.value)}
                        />
                      </div>
                      <Button onClick={handleCreateSession} disabled={createSession.isPending} className="w-full">
                        {createSession.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                        {locale === "ru" ? "Создать" : "Create"}
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
                    <div key={session.id} className="flex items-center justify-between p-3 rounded-lg bg-muted/30">
                      <div className="flex items-center gap-3">
                        <div className={`w-3 h-3 rounded-full ${
                          session.status === "connected" ? "bg-green-500" :
                          session.status === "qr_pending" ? "bg-yellow-500 animate-pulse" : "bg-red-500"
                        }`} />
                        <div>
                          <p className="font-medium">{session.session_name}</p>
                          <p className="text-sm text-muted-foreground">{session.phone_number || session.status}</p>
                        </div>
                      </div>
                      <div className="flex gap-2">
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
                  {locale === "ru" ? "Нет сессий. Создайте первую сессию WhatsApp." : "No sessions. Create your first WhatsApp session."}
                </p>
              )}
            </CardContent>
          </Card>

          {/* Manual Send Card */}
          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Send className="h-5 w-5" />
                {locale === "ru" ? "Отправить сообщение" : "Send Message"}
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid sm:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label>{locale === "ru" ? "Номер телефона" : "Phone number"}</Label>
                  <Input
                    placeholder="+7 777 123 4567"
                    value={manualSendPhone}
                    onChange={(e) => setManualSendPhone(e.target.value)}
                  />
                </div>
              </div>
              <div className="space-y-2">
                <Label>{locale === "ru" ? "Сообщение" : "Message"}</Label>
                <Textarea
                  placeholder={locale === "ru" ? "Введите текст сообщения..." : "Enter message text..."}
                  value={manualSendMessage}
                  onChange={(e) => setManualSendMessage(e.target.value)}
                  rows={3}
                />
              </div>
              <Button onClick={handleManualSend} disabled={sendMessage.isPending || !isConnected}>
                {sendMessage.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                <Send className="h-4 w-4 mr-2" />
                {locale === "ru" ? "Отправить" : "Send"}
              </Button>
              {!isConnected && (
                <p className="text-sm text-destructive">
                  {locale === "ru" ? "Подключите WhatsApp для отправки сообщений" : "Connect WhatsApp to send messages"}
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
                {locale === "ru" ? "Мониторинг заказов" : "Orders Monitoring"}
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground mb-4">
                {locale === "ru"
                  ? "Включите мониторинг для автоматической отправки WhatsApp при изменении статуса заказа"
                  : "Enable monitoring to automatically send WhatsApp when order status changes"}
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
                                locale === "ru"
                                  ? `Мониторинг ${enabled ? "включён" : "выключен"} для ${store.name}`
                                  : `Monitoring ${enabled ? "enabled" : "disabled"} for ${store.name}`
                              )
                            },
                            onError: () => {
                              toast.error(locale === "ru" ? "Ошибка" : "Error")
                            },
                          }
                        )
                      }}
                      locale={locale}
                    />
                  ))}
                </div>
              ) : (
                <p className="text-sm text-muted-foreground text-center py-4">
                  {locale === "ru" ? "Нет подключенных магазинов" : "No connected stores"}
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
                  {locale === "ru" ? "Шаблоны сообщений" : "Message Templates"}
                </span>
                <Button size="sm" onClick={() => { setEditingTemplate(null); setTemplateForm({ name: "", name_en: "", message: "", variables: [], trigger_event: "" }); setShowTemplateDialog(true) }}>
                  <Plus className="h-4 w-4 mr-2" />
                  {locale === "ru" ? "Добавить" : "Add"}
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
                  {locale === "ru" ? "Нет шаблонов. Создайте первый шаблон." : "No templates. Create your first template."}
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
                  {locale === "ru" ? "Настройки" : "Settings"}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-4">
                  <div className="space-y-2">
                    <Label>{locale === "ru" ? "Лимит в день" : "Daily limit"}</Label>
                    <Input
                      type="number"
                      value={settings.daily_limit}
                      onChange={(e) => updateSettings.mutate({ daily_limit: Number(e.target.value) })}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>{locale === "ru" ? "Интервал (сек)" : "Interval (sec)"}</Label>
                    <Input
                      type="number"
                      value={settings.interval_seconds}
                      onChange={(e) => updateSettings.mutate({ interval_seconds: Number(e.target.value) })}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>{locale === "ru" ? "Начало работы" : "Work start"}</Label>
                    <Input type="time" value={settings.work_hours_start} onChange={(e) => updateSettings.mutate({ work_hours_start: e.target.value })} />
                  </div>
                  <div className="space-y-2">
                    <Label>{locale === "ru" ? "Конец работы" : "Work end"}</Label>
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
                {locale === "ru" ? "Настройки AI Продажника" : "AI Salesman Settings"}
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
                            {locale === "ru" ? "AI включен" : "AI enabled"}
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
                            <Label>{locale === "ru" ? "Тон общения" : "Tone"}</Label>
                            <Select
                              value={store.ai_tone || "friendly"}
                              onValueChange={(value) => updateSalesmanSettings.mutate({ storeId: store.store_id, ai_tone: value })}
                            >
                              <SelectTrigger>
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="friendly">{locale === "ru" ? "Дружелюбный" : "Friendly"}</SelectItem>
                                <SelectItem value="professional">{locale === "ru" ? "Профессиональный" : "Professional"}</SelectItem>
                                <SelectItem value="casual">{locale === "ru" ? "Неформальный" : "Casual"}</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>
                          <div className="space-y-2">
                            <Label>{locale === "ru" ? "Макс. скидка %" : "Max discount %"}</Label>
                            <Input
                              type="number"
                              value={store.ai_discount_percent || ""}
                              placeholder="0"
                              onChange={(e) => updateSalesmanSettings.mutate({ storeId: store.store_id, ai_discount_percent: Number(e.target.value) || null })}
                            />
                          </div>
                          <div className="space-y-2">
                            <Label>{locale === "ru" ? "Промокод" : "Promo code"}</Label>
                            <Input
                              value={store.ai_promo_code || ""}
                              placeholder="PROMO10"
                              onChange={(e) => updateSalesmanSettings.mutate({ storeId: store.store_id, ai_promo_code: e.target.value || null })}
                            />
                          </div>
                          <div className="space-y-2">
                            <Label>{locale === "ru" ? "Бонус за отзыв" : "Review bonus"}</Label>
                            <Input
                              value={store.ai_review_bonus || ""}
                              placeholder={locale === "ru" ? "Скидка 5% на след. заказ" : "5% off next order"}
                              onChange={(e) => updateSalesmanSettings.mutate({ storeId: store.store_id, ai_review_bonus: e.target.value || null })}
                            />
                          </div>
                          <div className="space-y-2">
                            <Label>{locale === "ru" ? "Задержка (мин)" : "Delay (min)"}</Label>
                            <Input
                              type="number"
                              value={store.ai_send_delay_minutes}
                              onChange={(e) => updateSalesmanSettings.mutate({ storeId: store.store_id, ai_send_delay_minutes: Number(e.target.value) })}
                            />
                          </div>
                          <div className="space-y-2">
                            <Label>{locale === "ru" ? "Лимит сообщ./день" : "Messages/day limit"}</Label>
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
                  {locale === "ru" ? "Нет магазинов. Добавьте магазин Kaspi для настройки AI." : "No stores. Add a Kaspi store to configure AI."}
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
                      <p className="text-sm text-muted-foreground">{locale === "ru" ? "Всего сообщений" : "Total messages"}</p>
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
                      <p className="text-sm text-muted-foreground">{locale === "ru" ? "Новые заказы" : "New orders"}</p>
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
                      <p className="text-sm text-muted-foreground">{locale === "ru" ? "Повторные" : "Repeat customers"}</p>
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
                      <p className="text-sm text-muted-foreground">{locale === "ru" ? "Запросы отзывов" : "Review requests"}</p>
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
                {locale === "ru" ? "История AI сообщений" : "AI Message History"}
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
                  {locale === "ru" ? "Нет сообщений AI" : "No AI messages yet"}
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
                  <Label>{locale === "ru" ? "Статус" : "Status"}</Label>
                  <Select
                    value={messageFilters.status_filter || "all"}
                    onValueChange={(v) => setMessageFilters({ ...messageFilters, status_filter: v === "all" ? undefined : v, page: 1 })}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">{locale === "ru" ? "Все" : "All"}</SelectItem>
                      <SelectItem value="sent">{locale === "ru" ? "Отправлено" : "Sent"}</SelectItem>
                      <SelectItem value="delivered">{locale === "ru" ? "Доставлено" : "Delivered"}</SelectItem>
                      <SelectItem value="read">{locale === "ru" ? "Прочитано" : "Read"}</SelectItem>
                      <SelectItem value="failed">{locale === "ru" ? "Ошибка" : "Failed"}</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label>{locale === "ru" ? "Телефон" : "Phone"}</Label>
                  <Input
                    placeholder="+7 777..."
                    value={messageFilters.phone || ""}
                    onChange={(e) => setMessageFilters({ ...messageFilters, phone: e.target.value || undefined, page: 1 })}
                  />
                </div>
                <div className="space-y-2">
                  <Label>{locale === "ru" ? "Дата от" : "Date from"}</Label>
                  <Input
                    type="date"
                    value={messageFilters.date_from || ""}
                    onChange={(e) => setMessageFilters({ ...messageFilters, date_from: e.target.value || undefined, page: 1 })}
                  />
                </div>
                <div className="space-y-2">
                  <Label>{locale === "ru" ? "Дата до" : "Date to"}</Label>
                  <Input
                    type="date"
                    value={messageFilters.date_to || ""}
                    onChange={(e) => setMessageFilters({ ...messageFilters, date_to: e.target.value || undefined, page: 1 })}
                  />
                </div>
                <div className="flex items-end">
                  <Button variant="outline" onClick={() => setMessageFilters({ page: 1, per_page: 20 })} className="w-full">
                    {locale === "ru" ? "Сбросить" : "Reset"}
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
                  {locale === "ru" ? "История сообщений" : "Message History"}
                </span>
                {messagesData && (
                  <span className="text-sm font-normal text-muted-foreground">
                    {locale === "ru" ? `Всего: ${messagesData.total}` : `Total: ${messagesData.total}`}
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
                        {locale === "ru" ? "Назад" : "Previous"}
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
                        {locale === "ru" ? "Далее" : "Next"}
                      </Button>
                    </div>
                  )}
                </div>
              ) : (
                <p className="text-center text-muted-foreground py-8">
                  {locale === "ru" ? "Нет сообщений" : "No messages"}
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
                      <p className="text-sm text-muted-foreground">{locale === "ru" ? "Отправлено" : "Sent"}</p>
                      <p className="text-3xl font-bold text-primary">{waStats.total_sent}</p>
                    </div>
                  </CardContent>
                </Card>
                <Card className="glass-card">
                  <CardContent className="pt-6">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">{locale === "ru" ? "Доставлено" : "Delivered"}</p>
                      <p className="text-3xl font-bold text-green-500">{waStats.total_delivered}</p>
                      <p className="text-xs text-muted-foreground">{waStats.delivery_rate}%</p>
                    </div>
                  </CardContent>
                </Card>
                <Card className="glass-card">
                  <CardContent className="pt-6">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">{locale === "ru" ? "Прочитано" : "Read"}</p>
                      <p className="text-3xl font-bold text-blue-500">{waStats.total_read}</p>
                      <p className="text-xs text-muted-foreground">{waStats.read_rate}%</p>
                    </div>
                  </CardContent>
                </Card>
                <Card className="glass-card">
                  <CardContent className="pt-6">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">{locale === "ru" ? "Ошибки" : "Failed"}</p>
                      <p className="text-3xl font-bold text-destructive">{waStats.total_failed}</p>
                    </div>
                  </CardContent>
                </Card>
                <Card className="glass-card">
                  <CardContent className="pt-6">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">{locale === "ru" ? "Сегодня" : "Today"}</p>
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
                    {locale === "ru" ? "Сообщения за неделю" : "Messages this week"}
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {waStats.messages_by_day.length > 0 ? (
                    <div className="space-y-4">
                      {waStats.messages_by_day.map((day) => (
                        <div key={day.date} className="space-y-2">
                          <div className="flex items-center justify-between text-sm">
                            <span>{new Date(day.date).toLocaleDateString(locale === "ru" ? "ru-RU" : "en-US", { weekday: "short", day: "numeric", month: "short" })}</span>
                            <span className="text-muted-foreground">{day.sent} {locale === "ru" ? "сообщ." : "msgs"}</span>
                          </div>
                          <div className="flex h-4 rounded-full overflow-hidden bg-muted">
                            {day.sent > 0 && (
                              <>
                                <div
                                  className="bg-green-500"
                                  style={{ width: `${(day.read / day.sent) * 100}%` }}
                                  title={`${locale === "ru" ? "Прочитано" : "Read"}: ${day.read}`}
                                />
                                <div
                                  className="bg-blue-500"
                                  style={{ width: `${((day.delivered - day.read) / day.sent) * 100}%` }}
                                  title={`${locale === "ru" ? "Доставлено" : "Delivered"}: ${day.delivered - day.read}`}
                                />
                                <div
                                  className="bg-gray-400"
                                  style={{ width: `${((day.sent - day.delivered) / day.sent) * 100}%` }}
                                  title={`${locale === "ru" ? "Отправлено" : "Sent"}: ${day.sent - day.delivered}`}
                                />
                                {day.failed > 0 && (
                                  <div
                                    className="bg-red-500"
                                    style={{ width: `${(day.failed / (day.sent + day.failed)) * 100}%` }}
                                    title={`${locale === "ru" ? "Ошибки" : "Failed"}: ${day.failed}`}
                                  />
                                )}
                              </>
                            )}
                          </div>
                        </div>
                      ))}
                      <div className="flex items-center gap-4 text-xs text-muted-foreground pt-2">
                        <div className="flex items-center gap-1"><div className="w-3 h-3 rounded bg-green-500" />{locale === "ru" ? "Прочитано" : "Read"}</div>
                        <div className="flex items-center gap-1"><div className="w-3 h-3 rounded bg-blue-500" />{locale === "ru" ? "Доставлено" : "Delivered"}</div>
                        <div className="flex items-center gap-1"><div className="w-3 h-3 rounded bg-gray-400" />{locale === "ru" ? "Отправлено" : "Sent"}</div>
                        <div className="flex items-center gap-1"><div className="w-3 h-3 rounded bg-red-500" />{locale === "ru" ? "Ошибки" : "Failed"}</div>
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

      {/* QR Dialog */}
      <Dialog open={showQRDialog} onOpenChange={setShowQRDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{locale === "ru" ? "Отсканируйте QR-код" : "Scan QR Code"}</DialogTitle>
            <DialogDescription>
              {locale === "ru" ? "Откройте WhatsApp на телефоне и отсканируйте код" : "Open WhatsApp on your phone and scan this code"}
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col items-center py-4">
            {qrLoading ? (
              <Loader2 className="h-8 w-8 animate-spin" />
            ) : qrData?.qr_code ? (
              <img src={`data:image/png;base64,${qrData.qr_code}`} alt="QR Code" className="w-64 h-64" />
            ) : qrData?.status === "connected" ? (
              <div className="text-center">
                <Wifi className="h-16 w-16 text-green-500 mx-auto mb-4" />
                <p className="text-green-500 font-medium">{locale === "ru" ? "Подключено!" : "Connected!"}</p>
              </div>
            ) : (
              <p className="text-muted-foreground">{locale === "ru" ? "QR-код загружается..." : "Loading QR code..."}</p>
            )}
          </div>
        </DialogContent>
      </Dialog>

      {/* Template Dialog */}
      <Dialog open={showTemplateDialog} onOpenChange={setShowTemplateDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {editingTemplate
                ? locale === "ru" ? "Редактировать шаблон" : "Edit Template"
                : locale === "ru" ? "Новый шаблон" : "New Template"}
            </DialogTitle>
          </DialogHeader>
          <div className="space-y-4 pt-4">
            <div className="space-y-2">
              <Label>{locale === "ru" ? "Название" : "Name"}</Label>
              <Input
                value={templateForm.name}
                onChange={(e) => setTemplateForm({ ...templateForm, name: e.target.value })}
                placeholder={locale === "ru" ? "Заказ принят" : "Order received"}
              />
            </div>
            <div className="space-y-2">
              <Label>{locale === "ru" ? "Название (EN)" : "Name (EN)"}</Label>
              <Input
                value={templateForm.name_en}
                onChange={(e) => setTemplateForm({ ...templateForm, name_en: e.target.value })}
                placeholder="Order received"
              />
            </div>
            <div className="space-y-2">
              <Label>{locale === "ru" ? "Текст сообщения" : "Message text"}</Label>
              <Textarea
                value={templateForm.message}
                onChange={(e) => setTemplateForm({ ...templateForm, message: e.target.value })}
                placeholder={locale === "ru" ? "Здравствуйте, {customer_name}! Ваш заказ #{order_code} принят." : "Hello, {customer_name}! Your order #{order_code} is received."}
                rows={4}
              />
              <div className="text-xs text-muted-foreground space-y-1">
                <p className="font-medium">{locale === "ru" ? "Доступные переменные:" : "Available variables:"}</p>
                <p>{locale === "ru" ? "Клиент:" : "Customer:"} {"{customer_name}"}, {"{customer_first_name}"}</p>
                <p>{locale === "ru" ? "Заказ:" : "Order:"} {"{order_code}"}, {"{order_total}"}, {"{items_list}"}, {"{items_count}"}, {"{first_item}"}</p>
                <p>{locale === "ru" ? "Доставка:" : "Delivery:"} {"{delivery_address}"}, {"{delivery_city}"}</p>
                <p>{locale === "ru" ? "Магазин:" : "Store:"} {"{store_name}"}, {"{promo_code}"}</p>
              </div>
            </div>
            <div className="space-y-2">
              <Label>{locale === "ru" ? "Автоматический триггер" : "Auto trigger"}</Label>
              <Select
                value={templateForm.trigger_event || "none"}
                onValueChange={(v) => setTemplateForm({ ...templateForm, trigger_event: v === "none" ? "" : v })}
              >
                <SelectTrigger>
                  <SelectValue placeholder={locale === "ru" ? "Выберите событие" : "Select event"} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">{locale === "ru" ? "Без триггера (вручную)" : "No trigger (manual)"}</SelectItem>
                  {orderEventTypes.map((event) => (
                    <SelectItem key={event.value} value={event.value}>{event.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                {locale === "ru"
                  ? "Сообщение отправится автоматически при наступлении события"
                  : "Message will be sent automatically when event occurs"}
              </p>
            </div>
            <Button onClick={handleSaveTemplate} disabled={createTemplate.isPending || updateTemplate.isPending} className="w-full">
              {(createTemplate.isPending || updateTemplate.isPending) && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
              {locale === "ru" ? "Сохранить" : "Save"}
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}
