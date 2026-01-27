'use client'

import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { useAdminUsers } from '@/hooks/api/use-admin'
import {
  Users,
  Search,
  MoreHorizontal,
  Loader2,
  Mail,
  Calendar,
  Shield,
} from 'lucide-react'

export default function AdminUsersPage() {
  const [search, setSearch] = useState('')
  const { data: users, isLoading } = useAdminUsers()

  // Mock data for demo
  const mockUsers = users || [
    { id: '1', email: 'seller1@mail.kz', full_name: 'Асхат Нурланов', role: 'user', subscription_plan: 'Комбо 500', subscription_status: 'active', stores_count: 2, created_at: '2024-01-15' },
    { id: '2', email: 'shop2@gmail.com', full_name: 'Мария Иванова', role: 'user', subscription_plan: 'Бот 1000', subscription_status: 'active', stores_count: 1, created_at: '2024-01-10' },
    { id: '3', email: 'admin@demper.kz', full_name: 'Admin', role: 'admin', subscription_plan: null, subscription_status: null, stores_count: 0, created_at: '2024-01-01' },
    { id: '4', email: 'store3@inbox.kz', full_name: null, role: 'user', subscription_plan: 'Бот 500', subscription_status: 'canceled', stores_count: 1, created_at: '2024-01-20' },
    { id: '5', email: 'newuser@test.kz', full_name: 'Новый Пользователь', role: 'user', subscription_plan: null, subscription_status: null, stores_count: 0, created_at: '2024-01-25' },
  ]

  const filteredUsers = mockUsers.filter(
    (user) =>
      user.email.toLowerCase().includes(search.toLowerCase()) ||
      user.full_name?.toLowerCase().includes(search.toLowerCase())
  )

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <Users className="h-6 w-6" />
            Пользователи
          </h1>
          <p className="text-muted-foreground">
            Управление пользователями платформы
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
                      Магазины
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
                      className="border-b border-border last:border-0 hover:bg-muted/50"
                    >
                      <td className="p-4">
                        <div>
                          <p className="font-medium text-foreground">
                            {user.full_name || 'Без имени'}
                          </p>
                          <p className="text-sm text-muted-foreground flex items-center gap-1">
                            <Mail className="h-3 w-3" />
                            {user.email}
                          </p>
                        </div>
                      </td>
                      <td className="p-4">
                        <Badge variant={user.role === 'admin' ? 'default' : 'secondary'}>
                          {user.role === 'admin' ? (
                            <Shield className="h-3 w-3 mr-1" />
                          ) : null}
                          {user.role}
                        </Badge>
                      </td>
                      <td className="p-4">
                        {user.subscription_plan ? (
                          <div>
                            <p className="text-sm font-medium">{user.subscription_plan}</p>
                            <Badge
                              variant={
                                user.subscription_status === 'active'
                                  ? 'default'
                                  : 'secondary'
                              }
                              className="text-xs mt-1"
                            >
                              {user.subscription_status}
                            </Badge>
                          </div>
                        ) : (
                          <span className="text-muted-foreground text-sm">—</span>
                        )}
                      </td>
                      <td className="p-4">
                        <span className="text-sm">{user.stores_count}</span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm text-muted-foreground flex items-center gap-1">
                          <Calendar className="h-3 w-3" />
                          {new Date(user.created_at).toLocaleDateString('ru-RU')}
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
                            <DropdownMenuItem>Редактировать</DropdownMenuItem>
                            <DropdownMenuItem>Изменить подписку</DropdownMenuItem>
                            <DropdownMenuItem>Сделать админом</DropdownMenuItem>
                            <DropdownMenuItem className="text-destructive">
                              Заблокировать
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
    </div>
  )
}
