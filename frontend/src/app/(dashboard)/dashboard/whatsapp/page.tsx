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
  Plus,
  Trash2,
  Loader2,
  QrCode,
  Send,
  Phone,
  Users,
  Megaphone,
  Ban,
  Play,
  Search,
  XCircle,
} from "lucide-react"
import { toast } from "sonner"
import {
  useWhatsAppSessions,
  useCreateSession,
  useDeleteSession,
  useSessionQRCode,
  usePairByPhone,
  useSendWhatsAppMessage,
  useCustomerContacts,
  useBlockContact,
  useUnblockContact,
  useBroadcasts,
  useCreateBroadcast,
  useStartBroadcast,
  useCancelBroadcast,
} from "@/hooks/api/use-whatsapp"
import { useStores } from "@/hooks/api/use-stores"

export default function WhatsAppPage() {
  const { locale } = useStore()
  const t = useT()
  const [activeTab, setActiveTab] = useState("broadcasts")

  // Session state
  const [newSessionName, setNewSessionName] = useState("")
  const [showQRDialog, setShowQRDialog] = useState(false)
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null)

  // Phone pairing state
  const [pairingTab, setPairingTab] = useState<"qr" | "phone">("qr")
  const [pairingPhone, setPairingPhone] = useState("")
  const [pairingCode, setPairingCode] = useState<string | null>(null)

  // Manual send state
  const [manualSendPhone, setManualSendPhone] = useState("")
  const [manualSendMessage, setManualSendMessage] = useState("")

  // API hooks
  const { data: sessions, isLoading: sessionsLoading } = useWhatsAppSessions()
  const { data: qrData, isLoading: qrLoading } = useSessionQRCode(
    selectedSessionId || "",
    showQRDialog && !!selectedSessionId
  )

  const createSession = useCreateSession()
  const deleteSession = useDeleteSession()
  const pairByPhone = usePairByPhone()
  const sendMessage = useSendWhatsAppMessage()

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

  return (
    <SubscriptionGate>
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">{t("wa.broadcastTitle")}</h1>
          <p className="text-muted-foreground">
            {t("wa.broadcastSubtitle")}
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
        <TabsList className="w-full grid grid-cols-3 lg:w-auto lg:inline-grid">
          <TabsTrigger value="broadcasts" className="gap-2">
            <Megaphone className="h-4 w-4 hidden sm:block" />
            {t("wa.broadcasts")}
          </TabsTrigger>
          <TabsTrigger value="contacts" className="gap-2">
            <Users className="h-4 w-4 hidden sm:block" />
            {t("wa.contacts")}
          </TabsTrigger>
          <TabsTrigger value="connection" className="gap-2">
            <MessageSquare className="h-4 w-4 hidden sm:block" />
            {t("wa.connection")}
          </TabsTrigger>
        </TabsList>

        {/* Broadcasts Tab */}
        <TabsContent value="broadcasts" className="space-y-6 mt-6">
          <BroadcastsTab />
        </TabsContent>

        {/* Contacts Tab */}
        <TabsContent value="contacts" className="space-y-6 mt-6">
          <ContactsTab />
        </TabsContent>

        {/* Connection Tab */}
        <TabsContent value="connection" className="space-y-6 mt-6">
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
    </div>
    </SubscriptionGate>
  )
}


// ==================== Contacts Tab ====================

function ContactsTab() {
  const t = useT()
  const { selectedStore } = useStore()
  const [searchQuery, setSearchQuery] = useState("")
  const [page, setPage] = useState(1)

  const { data, isLoading } = useCustomerContacts({
    store_id: selectedStore?.id,
    search: searchQuery || undefined,
    page,
    per_page: 30,
  })

  const blockContact = useBlockContact()
  const unblockContact = useUnblockContact()

  const contacts = data?.contacts || []
  const total = data?.total || 0

  return (
    <div className="space-y-4">
      {/* Header with search */}
      <Card className="glass-card">
        <CardContent className="pt-6">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
            <div>
              <h3 className="font-semibold flex items-center gap-2">
                <Users className="h-5 w-5" />
                {t("wa.contacts")}
              </h3>
              <p className="text-sm text-muted-foreground">
                {t("wa.contactsDesc")}
              </p>
            </div>
            <Badge variant="secondary" className="text-sm shrink-0">
              {t("wa.totalContacts")}: {total}
            </Badge>
          </div>
          <div className="mt-4 relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder={t("wa.searchContacts")}
              value={searchQuery}
              onChange={(e) => { setSearchQuery(e.target.value); setPage(1) }}
              className="pl-10"
            />
          </div>
        </CardContent>
      </Card>

      {/* Contacts list */}
      {isLoading ? (
        <div className="space-y-3">
          {[1, 2, 3].map(i => (
            <Card key={i} className="glass-card">
              <CardContent className="p-4">
                <div className="h-12 bg-muted/50 rounded animate-pulse" />
              </CardContent>
            </Card>
          ))}
        </div>
      ) : contacts.length === 0 ? (
        <Card className="glass-card">
          <CardContent className="p-8 text-center">
            <Users className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
            <h3 className="text-lg font-semibold mb-2">{t("wa.noContacts")}</h3>
            <p className="text-muted-foreground">{t("wa.noContactsDesc")}</p>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-2">
          {contacts.map(contact => (
            <Card key={contact.id} className="glass-card">
              <CardContent className="p-4">
                <div className="flex items-center justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <p className="font-medium truncate">
                        {contact.name || contact.phone}
                      </p>
                      {contact.store_name && (
                        <Badge variant="outline" className="text-xs shrink-0">
                          {contact.store_name}
                        </Badge>
                      )}
                    </div>
                    <div className="flex items-center gap-3 text-sm text-muted-foreground mt-1">
                      <span className="flex items-center gap-1">
                        <Phone className="h-3 w-3" />
                        {contact.phone}
                      </span>
                      <span>{contact.orders_count} {t("wa.orders")}</span>
                    </div>
                  </div>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      if (contact.is_blocked) {
                        unblockContact.mutate(contact.id)
                      } else {
                        blockContact.mutate(contact.id)
                      }
                    }}
                  >
                    <Ban className="h-4 w-4 mr-1" />
                    {contact.is_blocked ? t("wa.unblock") : t("wa.block")}
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}

          {/* Pagination */}
          {total > 30 && (
            <div className="flex justify-center gap-2 pt-4">
              <Button
                variant="outline"
                size="sm"
                disabled={page <= 1}
                onClick={() => setPage(p => p - 1)}
              >
                {t("wa.previous")}
              </Button>
              <span className="flex items-center text-sm text-muted-foreground px-3">
                {page} / {Math.ceil(total / 30)}
              </span>
              <Button
                variant="outline"
                size="sm"
                disabled={page >= Math.ceil(total / 30)}
                onClick={() => setPage(p => p + 1)}
              >
                &rarr;
              </Button>
            </div>
          )}
        </div>
      )}
    </div>
  )
}


// ==================== Broadcasts Tab ====================

function BroadcastsTab() {
  const t = useT()
  const { selectedStore } = useStore()
  const { data: stores } = useStores()
  const [showCreate, setShowCreate] = useState(false)
  const [newCampaign, setNewCampaign] = useState({
    name: "",
    message_text: "",
    filter_min_orders: 0,
    filter_store_id: "",
  })

  const { data: broadcasts, isLoading } = useBroadcasts()
  const createBroadcast = useCreateBroadcast()
  const startBroadcast = useStartBroadcast()
  const cancelBroadcast = useCancelBroadcast()

  const statusBadge = (status: string) => {
    const map: Record<string, { label: string; className: string }> = {
      draft: { label: t("wa.broadcastDraft"), className: "bg-muted text-muted-foreground" },
      sending: { label: t("wa.broadcastSending"), className: "bg-blue-500/20 text-blue-600" },
      completed: { label: t("wa.broadcastCompleted"), className: "bg-green-500/20 text-green-600" },
      failed: { label: t("wa.broadcastFailed"), className: "bg-red-500/20 text-red-600" },
      cancelled: { label: t("wa.broadcastCancelled"), className: "bg-yellow-500/20 text-yellow-600" },
    }
    const s = map[status] || map.draft
    return <Badge className={s.className}>{s.label}</Badge>
  }

  const handleCreate = async () => {
    if (!newCampaign.name || !newCampaign.message_text) {
      toast.error("Fill in name and message")
      return
    }
    try {
      await createBroadcast.mutateAsync({
        name: newCampaign.name,
        message_text: newCampaign.message_text,
        filter_min_orders: newCampaign.filter_min_orders,
        filter_store_id: newCampaign.filter_store_id || undefined,
      })
      toast.success("Broadcast created")
      setShowCreate(false)
      setNewCampaign({ name: "", message_text: "", filter_min_orders: 0, filter_store_id: "" })
    } catch {
      toast.error("Failed to create broadcast")
    }
  }

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className="font-semibold flex items-center gap-2">
            <Megaphone className="h-5 w-5" />
            {t("wa.broadcasts")}
          </h3>
        </div>
        <Button onClick={() => setShowCreate(true)}>
          <Plus className="h-4 w-4 mr-2" />
          {t("wa.newBroadcast")}
        </Button>
      </div>

      {/* Create dialog */}
      <Dialog open={showCreate} onOpenChange={setShowCreate}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t("wa.newBroadcast")}</DialogTitle>
            <DialogDescription>{t("wa.contactsDesc")}</DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label>{t("wa.broadcastName")}</Label>
              <Input
                value={newCampaign.name}
                onChange={e => setNewCampaign(c => ({ ...c, name: e.target.value }))}
                placeholder="Акция февраль"
              />
            </div>
            <div className="space-y-2">
              <Label>{t("wa.broadcastMessage")}</Label>
              <Textarea
                value={newCampaign.message_text}
                onChange={e => setNewCampaign(c => ({ ...c, message_text: e.target.value }))}
                placeholder="Здравствуйте! У нас скидки до 30%..."
                rows={4}
              />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label>{t("wa.broadcastMinOrders")}</Label>
                <Input
                  type="number"
                  min={0}
                  value={newCampaign.filter_min_orders}
                  onChange={e => setNewCampaign(c => ({ ...c, filter_min_orders: parseInt(e.target.value) || 0 }))}
                />
              </div>
              <div className="space-y-2">
                <Label>{t("wa.broadcastStore")}</Label>
                <Select
                  value={newCampaign.filter_store_id}
                  onValueChange={v => setNewCampaign(c => ({ ...c, filter_store_id: v === "all" ? "" : v }))}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={t("wa.broadcastAllStores")} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">{t("wa.broadcastAllStores")}</SelectItem>
                    {stores?.map(s => (
                      <SelectItem key={s.id} value={s.id}>{s.name}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
            <Button
              className="w-full"
              onClick={handleCreate}
              disabled={createBroadcast.isPending}
            >
              {createBroadcast.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
              {t("wa.broadcastCreate")}
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      {/* Broadcasts list */}
      {isLoading ? (
        <div className="space-y-3">
          {[1, 2].map(i => (
            <Card key={i} className="glass-card">
              <CardContent className="p-4">
                <div className="h-16 bg-muted/50 rounded animate-pulse" />
              </CardContent>
            </Card>
          ))}
        </div>
      ) : !broadcasts?.length ? (
        <Card className="glass-card">
          <CardContent className="p-8 text-center">
            <Megaphone className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
            <h3 className="text-lg font-semibold mb-2">{t("wa.noBroadcasts")}</h3>
            <p className="text-muted-foreground">{t("wa.noBroadcastsDesc")}</p>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-3">
          {broadcasts.map(campaign => (
            <Card key={campaign.id} className="glass-card">
              <CardContent className="p-4">
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <h4 className="font-medium truncate">{campaign.name}</h4>
                      {statusBadge(campaign.status)}
                    </div>
                    <p className="text-sm text-muted-foreground truncate">
                      {campaign.message_text.substring(0, 80)}...
                    </p>
                    <div className="flex items-center gap-4 mt-2 text-xs text-muted-foreground">
                      <span>{t("wa.broadcastRecipients")}: {campaign.total_recipients}</span>
                      {campaign.status === "sending" && (
                        <span className="text-blue-600">
                          {t("wa.broadcastProgress")}: {campaign.sent_count}/{campaign.total_recipients}
                        </span>
                      )}
                      {campaign.status === "completed" && (
                        <span className="text-green-600">
                          {t("wa.sent")}: {campaign.sent_count}, {t("wa.failed")}: {campaign.failed_count}
                        </span>
                      )}
                    </div>
                  </div>
                  <div className="flex gap-2 shrink-0">
                    {campaign.status === "draft" && (
                      <Button
                        size="sm"
                        onClick={() => startBroadcast.mutate(campaign.id)}
                        disabled={startBroadcast.isPending}
                      >
                        <Play className="h-4 w-4 mr-1" />
                        {t("wa.broadcastStart")}
                      </Button>
                    )}
                    {(campaign.status === "draft" || campaign.status === "sending") && (
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => cancelBroadcast.mutate(campaign.id)}
                        disabled={cancelBroadcast.isPending}
                      >
                        <XCircle className="h-4 w-4 mr-1" />
                        {t("wa.broadcastCancel")}
                      </Button>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  )
}
