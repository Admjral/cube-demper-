'use client'

import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  useAdminUsers,
  useBlockUser,
  useUnblockUser,
  useUpdateUserRole,
  useDeleteUser,
  useUserSubscriptionDetails,
  useAssignSubscription,
  useAssignAddon,
  useRemoveAddon,
  useCancelSubscription,
} from '@/hooks/api/use-admin'
import { usePlansV2, useAddons } from '@/hooks/api/use-features'
import {
  Users,
  Search,
  MoreHorizontal,
  Loader2,
  Mail,
  Calendar,
  Shield,
  Ban,
  CheckCircle,
  Package,
  Store,
  CreditCard,
  Plus,
  Trash2,
  Sparkles,
} from 'lucide-react'
import { format } from 'date-fns'
import { ru } from 'date-fns/locale'
import { toast } from 'sonner'

export default function AdminUsersPage() {
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(1)
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null)
  const [showSubscriptionDialog, setShowSubscriptionDialog] = useState(false)
  const [subscriptionForm, setSubscriptionForm] = useState({
    planCode: '',
    days: 30,
    isTrial: false,
    notes: '',
    endsAt: '',
  })
  const [addonForm, setAddonForm] = useState({
    addonCode: '',
    quantity: 1,
    days: 30,
  })

  const { data, isLoading, refetch } = useAdminUsers(page, 50)
  const { data: plans } = usePlansV2()
  const { data: addons } = useAddons()
  const { data: subscriptionDetails, isLoading: subscriptionLoading } = useUserSubscriptionDetails(
    selectedUserId || '',
    showSubscriptionDialog && !!selectedUserId
  )

  const blockUserMutation = useBlockUser()
  const unblockUserMutation = useUnblockUser()
  const updateRoleMutation = useUpdateUserRole()
  const deleteUserMutation = useDeleteUser()
  const assignSubscription = useAssignSubscription()
  const assignAddon = useAssignAddon()
  const removeAddon = useRemoveAddon()
  const cancelSubscription = useCancelSubscription()

  const users = data?.users ?? []
  const total = data?.total ?? 0

  const filteredUsers = users.filter(
    (user) =>
      user.email.toLowerCase().includes(search.toLowerCase()) ||
      user.full_name?.toLowerCase().includes(search.toLowerCase())
  )

  const handleBlockUser = async (userId: string) => {
    if (confirm('Заблокировать пользователя?')) {
      await blockUserMutation.mutateAsync({ userId })
    }
  }

  const handleUnblockUser = async (userId: string) => {
    await unblockUserMutation.mutateAsync(userId)
  }

  const handleMakeAdmin = async (userId: string) => {
    if (confirm('Сделать администратором?')) {
      await updateRoleMutation.mutateAsync({ userId, role: 'admin' })
    }
  }

  const handleMakeSupport = async (userId: string) => {
    if (confirm('Сделать саппортом?')) {
      await updateRoleMutation.mutateAsync({ userId, role: 'support' })
    }
  }

  const handleRemoveRole = async (userId: string) => {
    await updateRoleMutation.mutateAsync({ userId, role: 'user' })
  }

  const handleDeleteUser = async (userId: string) => {
    if (confirm('Удалить пользователя? Это действие необратимо!')) {
      await deleteUserMutation.mutateAsync(userId)
    }
  }

  const openSubscriptionDialog = (userId: string) => {
    setSelectedUserId(userId)
    setSubscriptionForm({ planCode: '', days: 30, isTrial: false, notes: '', endsAt: '' })
    setAddonForm({ addonCode: '', quantity: 1, days: 30 })
    setShowSubscriptionDialog(true)
  }

  const handleAssignSubscription = async () => {
    if (!selectedUserId || !subscriptionForm.planCode) return
    try {
      await assignSubscription.mutateAsync({
        userId: selectedUserId,
        planCode: subscriptionForm.planCode,
        days: subscriptionForm.days,
        isTrial: subscriptionForm.isTrial,
        notes: subscriptionForm.notes || undefined,
        endsAt: subscriptionForm.endsAt ? new Date(subscriptionForm.endsAt).toISOString() : undefined,
      })
      toast.success('Подписка назначена')
      setSubscriptionForm({ planCode: '', days: 30, isTrial: false, notes: '', endsAt: '' })
    } catch {
      toast.error('Ошибка назначения подписки')
    }
  }

  const handleCancelSubscription = async () => {
    if (!selectedUserId) return
    if (!confirm('Отменить подписку пользователя?')) return
    try {
      await cancelSubscription.mutateAsync(selectedUserId)
      toast.success('Подписка отменена')
    } catch {
      toast.error('Ошибка отмены подписки')
    }
  }

  const handleAssignAddon = async () => {
    if (!selectedUserId || !addonForm.addonCode) return
    try {
      await assignAddon.mutateAsync({
        userId: selectedUserId,
        addonCode: addonForm.addonCode,
        quantity: addonForm.quantity,
        days: addonForm.days,
      })
      toast.success('Доп. услуга добавлена')
      setAddonForm({ addonCode: '', quantity: 1, days: 30 })
    } catch {
      toast.error('Ошибка добавления')
    }
  }

  const handleRemoveAddon = async (addonCode: string) => {
    if (!selectedUserId) return
    if (!confirm('Удалить доп. услугу?')) return
    try {
      await removeAddon.mutateAsync({ userId: selectedUserId, addonCode })
      toast.success('Доп. услуга удалена')
    } catch {
      toast.error('Ошибка удаления')
    }
  }

  const selectedUser = users.find(u => u.id === selectedUserId)

  const getRoleBadge = (role: string) => {
    switch (role) {
      case 'admin':
        return (
          <Badge className="bg-red-500/10 text-red-500 border-0">
            <Shield className="h-3 w-3 mr-1" />
            Админ
          </Badge>
        )
      case 'support':
        return (
          <Badge className="bg-blue-500/10 text-blue-500 border-0">
            <Shield className="h-3 w-3 mr-1" />
            Саппорт
          </Badge>
        )
      default:
        return <Badge variant="secondary">Пользователь</Badge>
    }
  }

  const getSubscriptionBadge = (status: string | null) => {
    switch (status) {
      case 'active':
        return (
          <Badge className="bg-green-500/10 text-green-500 border-0">
            <CheckCircle className="h-3 w-3 mr-1" />
            Активна
          </Badge>
        )
      case 'canceled':
        return <Badge variant="secondary">Отменена</Badge>
      case 'past_due':
        return <Badge variant="destructive">Просрочена</Badge>
      default:
        return <span className="text-muted-foreground text-sm">—</span>
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <Users className="h-6 w-6" />
            Пользователи
          </h1>
          <p className="text-muted-foreground">
            Всего: {total} пользователей
          </p>
        </div>
      </div>

      {/* Search */}
      <div className="relative max-w-md">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
        <Input
          placeholder="Поиск по email или имени..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="pl-10"
        />
      </div>

      {/* Users table */}
      <Card className="glass-card">
        <CardContent className="p-0">
          {isLoading ? (
            <div className="flex items-center justify-center h-64">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Пользователь
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Роль
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Подписка
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Магазины / Товары
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Дата регистрации
                    </th>
                    <th className="text-right p-4 text-sm font-medium text-muted-foreground">
                      Действия
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredUsers.map((user) => (
                    <tr
                      key={user.id}
                      className={`border-b border-border last:border-0 hover:bg-muted/50 ${
                        user.is_blocked ? 'bg-red-500/5' : ''
                      }`}
                    >
                      <td className="p-4">
                        <div>
                          <div className="flex items-center gap-2">
                            <p className="font-medium text-foreground">
                              {user.full_name || 'Без имени'}
                            </p>
                            {user.is_blocked && (
                              <Badge variant="destructive" className="text-xs">
                                <Ban className="h-3 w-3 mr-1" />
                                Заблокирован
                              </Badge>
                            )}
                          </div>
                          <p className="text-sm text-muted-foreground flex items-center gap-1">
                            <Mail className="h-3 w-3" />
                            {user.email}
                          </p>
                          {user.partner_name && (
                            <p className="text-xs text-muted-foreground mt-1">
                              Партнёр: {user.partner_name}
                            </p>
                          )}
                        </div>
                      </td>
                      <td className="p-4">{getRoleBadge(user.role)}</td>
                      <td className="p-4">
                        {user.subscription_plan ? (
                          <div>
                            <p className="text-sm font-medium">{user.subscription_plan}</p>
                            {getSubscriptionBadge(user.subscription_status)}
                            {user.subscription_end_date && (
                              <p className="text-xs text-muted-foreground mt-1">
                                до {format(new Date(user.subscription_end_date), 'd MMM yyyy', { locale: ru })}
                              </p>
                            )}
                          </div>
                        ) : (
                          <span className="text-muted-foreground text-sm">—</span>
                        )}
                      </td>
                      <td className="p-4">
                        <div className="flex items-center gap-4 text-sm">
                          <span className="flex items-center gap-1">
                            <Store className="h-3 w-3 text-muted-foreground" />
                            {user.stores_count}
                          </span>
                          <span className="flex items-center gap-1">
                            <Package className="h-3 w-3 text-muted-foreground" />
                            {user.products_count}
                          </span>
                        </div>
                      </td>
                      <td className="p-4">
                        <span className="text-sm text-muted-foreground flex items-center gap-1">
                          <Calendar className="h-3 w-3" />
                          {user.created_at
                            ? format(new Date(user.created_at), 'd MMM yyyy', { locale: ru })
                            : '-'}
                        </span>
                      </td>
                      <td className="p-4 text-right">
                        <DropdownMenu>
                          <DropdownMenuTrigger asChild>
                            <Button variant="ghost" size="icon">
                              <MoreHorizontal className="h-4 w-4" />
                            </Button>
                          </DropdownMenuTrigger>
                          <DropdownMenuContent align="end">
                            <DropdownMenuItem onClick={() => openSubscriptionDialog(user.id)}>
                              <CreditCard className="h-4 w-4 mr-2" />
                              Управление подпиской
                            </DropdownMenuItem>
                            <DropdownMenuSeparator />
                            {user.role === 'user' && (
                              <>
                                <DropdownMenuItem onClick={() => handleMakeAdmin(user.id)}>
                                  Сделать админом
                                </DropdownMenuItem>
                                <DropdownMenuItem onClick={() => handleMakeSupport(user.id)}>
                                  Сделать саппортом
                                </DropdownMenuItem>
                              </>
                            )}
                            {(user.role === 'admin' || user.role === 'support') && (
                              <DropdownMenuItem onClick={() => handleRemoveRole(user.id)}>
                                Убрать роль
                              </DropdownMenuItem>
                            )}
                            <DropdownMenuSeparator />
                            {user.is_blocked ? (
                              <DropdownMenuItem onClick={() => handleUnblockUser(user.id)}>
                                Разблокировать
                              </DropdownMenuItem>
                            ) : (
                              <DropdownMenuItem
                                onClick={() => handleBlockUser(user.id)}
                                className="text-orange-600"
                              >
                                Заблокировать
                              </DropdownMenuItem>
                            )}
                            <DropdownMenuItem
                              onClick={() => handleDeleteUser(user.id)}
                              className="text-destructive"
                            >
                              Удалить
                            </DropdownMenuItem>
                          </DropdownMenuContent>
                        </DropdownMenu>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Pagination */}
      {total > 50 && (
        <div className="flex items-center justify-center gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={page === 1}
            onClick={() => setPage(page - 1)}
          >
            Назад
          </Button>
          <span className="text-sm text-muted-foreground">
            Страница {page} из {Math.ceil(total / 50)}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= Math.ceil(total / 50)}
            onClick={() => setPage(page + 1)}
          >
            Вперёд
          </Button>
        </div>
      )}

      {/* Subscription Management Dialog */}
      <Dialog open={showSubscriptionDialog} onOpenChange={setShowSubscriptionDialog}>
        <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <CreditCard className="h-5 w-5" />
              Управление подпиской
            </DialogTitle>
            <DialogDescription>
              {selectedUser?.full_name || selectedUser?.email || 'Пользователь'}
            </DialogDescription>
          </DialogHeader>

          {subscriptionLoading ? (
            <div className="flex justify-center py-8">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : (
            <div className="space-y-6 pt-4">
              {/* Current subscription info */}
              {subscriptionDetails?.subscription && (
                <Card>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base">Текущая подписка</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-2">
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Тариф:</span>
                      <span className="font-medium">
                        {subscriptionDetails.subscription.plan_name || 'Не выбран'}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Статус:</span>
                      <Badge variant={subscriptionDetails.subscription.status === 'active' ? 'default' : 'secondary'}>
                        {subscriptionDetails.subscription.status === 'active' ? 'Активна' :
                         subscriptionDetails.subscription.status || 'Нет'}
                      </Badge>
                    </div>
                    {subscriptionDetails.subscription.is_trial && (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">Пробный период:</span>
                        <Badge variant="outline" className="gap-1">
                          <Sparkles className="h-3 w-3" />
                          До {subscriptionDetails.subscription.trial_ends_at
                            ? format(new Date(subscriptionDetails.subscription.trial_ends_at), 'd MMM yyyy', { locale: ru })
                            : '—'}
                        </Badge>
                      </div>
                    )}
                    {subscriptionDetails.subscription.ends_at && (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">Действует до:</span>
                        <span>{format(new Date(subscriptionDetails.subscription.ends_at), 'd MMM yyyy', { locale: ru })}</span>
                      </div>
                    )}
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Аналитика:</span>
                      <span>
                        {subscriptionDetails.computed_limits.analytics_limit === -1
                          ? 'Безлимит'
                          : subscriptionDetails.computed_limits.analytics_limit}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Демпинг:</span>
                      <span>{subscriptionDetails.computed_limits.demping_limit}</span>
                    </div>
                    {subscriptionDetails.subscription.notes && (
                      <div className="pt-2 border-t">
                        <span className="text-muted-foreground text-sm">
                          Заметка: {subscriptionDetails.subscription.notes}
                        </span>
                      </div>
                    )}
                    {subscriptionDetails.subscription.status === 'active' && (
                      <div className="pt-3 border-t">
                        <Button
                          variant="destructive"
                          size="sm"
                          className="w-full"
                          onClick={handleCancelSubscription}
                          disabled={cancelSubscription.isPending}
                        >
                          {cancelSubscription.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                          Отменить подписку
                        </Button>
                      </div>
                    )}
                  </CardContent>
                </Card>
              )}

              {/* Active add-ons */}
              {subscriptionDetails?.addons && subscriptionDetails.addons.length > 0 && (
                <Card>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base">Активные доп. услуги</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2">
                      {subscriptionDetails.addons.map((addon) => (
                        <div key={addon.id} className="flex items-center justify-between p-2 rounded-lg bg-muted/30">
                          <div>
                            <span className="font-medium">{addon.name}</span>
                            {addon.quantity > 1 && (
                              <Badge variant="outline" className="ml-2">x{addon.quantity}</Badge>
                            )}
                            {addon.expires_at && (
                              <span className="text-xs text-muted-foreground ml-2">
                                до {format(new Date(addon.expires_at), 'd MMM', { locale: ru })}
                              </span>
                            )}
                          </div>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="text-destructive h-8 w-8"
                            onClick={() => handleRemoveAddon(addon.code)}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Assign subscription form */}
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-base">Назначить/изменить подписку</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label>Тариф</Label>
                      <Select
                        value={subscriptionForm.planCode}
                        onValueChange={(v) => setSubscriptionForm({ ...subscriptionForm, planCode: v })}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Выберите тариф" />
                        </SelectTrigger>
                        <SelectContent>
                          {plans?.map((plan) => (
                            <SelectItem key={plan.id} value={plan.code}>
                              {plan.name} — {plan.price.toLocaleString()} ₸
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>Срок (дни)</Label>
                      <Input
                        type="number"
                        min={1}
                        value={subscriptionForm.days}
                        onChange={(e) => setSubscriptionForm({ ...subscriptionForm, days: parseInt(e.target.value) || 30, endsAt: '' })}
                      />
                    </div>
                  </div>
                  <div className="space-y-2">
                    <Label>Или дата окончания</Label>
                    <Input
                      type="date"
                      value={subscriptionForm.endsAt}
                      onChange={(e) => setSubscriptionForm({ ...subscriptionForm, endsAt: e.target.value, days: 0 })}
                    />
                    {subscriptionForm.endsAt && (
                      <p className="text-xs text-muted-foreground">
                        Дата окончания имеет приоритет над количеством дней
                      </p>
                    )}
                  </div>
                  <div className="flex items-center gap-4">
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={subscriptionForm.isTrial}
                        onChange={(e) => setSubscriptionForm({ ...subscriptionForm, isTrial: e.target.checked })}
                        className="w-4 h-4"
                      />
                      <span className="text-sm">Пробный период (бета-тест)</span>
                    </label>
                  </div>
                  <div className="space-y-2">
                    <Label>Заметка (опционально)</Label>
                    <Input
                      value={subscriptionForm.notes}
                      onChange={(e) => setSubscriptionForm({ ...subscriptionForm, notes: e.target.value })}
                      placeholder="Причина выдачи, комментарий..."
                    />
                  </div>
                  <Button
                    onClick={handleAssignSubscription}
                    disabled={!subscriptionForm.planCode || assignSubscription.isPending}
                    className="w-full"
                  >
                    {assignSubscription.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                    Назначить подписку
                  </Button>
                </CardContent>
              </Card>

              {/* Add addon form */}
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-base">Добавить доп. услугу</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid grid-cols-3 gap-4">
                    <div className="space-y-2">
                      <Label>Услуга</Label>
                      <Select
                        value={addonForm.addonCode}
                        onValueChange={(v) => setAddonForm({ ...addonForm, addonCode: v })}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Выберите" />
                        </SelectTrigger>
                        <SelectContent>
                          {addons?.map((addon) => (
                            <SelectItem key={addon.id} value={addon.code}>
                              {addon.name} — {addon.price.toLocaleString()} ₸
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>Кол-во</Label>
                      <Input
                        type="number"
                        min={1}
                        value={addonForm.quantity}
                        onChange={(e) => setAddonForm({ ...addonForm, quantity: parseInt(e.target.value) || 1 })}
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Дни</Label>
                      <Input
                        type="number"
                        min={1}
                        value={addonForm.days}
                        onChange={(e) => setAddonForm({ ...addonForm, days: parseInt(e.target.value) || 30 })}
                      />
                    </div>
                  </div>
                  <Button
                    onClick={handleAssignAddon}
                    disabled={!addonForm.addonCode || assignAddon.isPending}
                    variant="outline"
                    className="w-full"
                  >
                    {assignAddon.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                    <Plus className="h-4 w-4 mr-2" />
                    Добавить услугу
                  </Button>
                </CardContent>
              </Card>

              {/* Features list */}
              {subscriptionDetails?.computed_features && subscriptionDetails.computed_features.length > 0 && (
                <Card>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base">Доступные функции</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="flex flex-wrap gap-2">
                      {subscriptionDetails.computed_features.map((feature) => (
                        <Badge key={feature} variant="secondary">
                          {feature}
                        </Badge>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
