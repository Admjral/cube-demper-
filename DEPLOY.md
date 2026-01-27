# Инструкция по деплою Demper

## Требования
- Docker и Docker Compose
- Домен с DNS записями (A записи для основного домена и api.домена)
- SSL сертификаты (Let's Encrypt)

## Шаг 1: Подготовка сервера

```bash
# Установка Docker (Ubuntu)
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Установка Docker Compose
sudo apt install docker-compose-plugin
```

## Шаг 2: Клонирование репозитория

```bash
git clone https://github.com/your-repo/demper.git
cd demper
```

## Шаг 3: Настройка переменных окружения

### Backend (.env)
```bash
cp backend/.env.example backend/.env
nano backend/.env
```

Заполните:
- `SUPABASE_URL` - URL вашего Supabase проекта
- `SUPABASE_KEY` - Service role key из Supabase
- `OPENAI_API_KEY` - API ключ OpenAI для ИИ-ассистентов
- `FRONTEND_URL` - https://ваш-домен.kz

### Frontend (.env.local)
```bash
cp frontend/.env.example frontend/.env.local
nano frontend/.env.local
```

Заполните:
- `NEXT_PUBLIC_SUPABASE_URL` - URL вашего Supabase проекта
- `NEXT_PUBLIC_SUPABASE_ANON_KEY` - Anon key из Supabase
- `NEXT_PUBLIC_API_URL` - https://api.ваш-домен.kz

## Шаг 4: SSL сертификаты (Let's Encrypt)

```bash
# Установка Certbot
sudo apt install certbot

# Получение сертификатов
sudo certbot certonly --standalone -d ваш-домен.kz -d api.ваш-домен.kz

# Копирование сертификатов
mkdir -p ssl
sudo cp /etc/letsencrypt/live/ваш-домен.kz/fullchain.pem ssl/
sudo cp /etc/letsencrypt/live/ваш-домен.kz/privkey.pem ssl/
sudo chown -R $USER:$USER ssl/
```

## Шаг 5: Настройка Nginx

```bash
cp nginx.conf.example nginx.conf
# Замените your-domain.kz на ваш домен
nano nginx.conf
```

Раскомментируйте секцию nginx в docker-compose.yml.

## Шаг 6: Запуск

```bash
# Сборка и запуск
docker compose up -d --build

# Просмотр логов
docker compose logs -f

# Проверка статуса
docker compose ps
```

## Шаг 7: Supabase настройка

В Supabase Dashboard:

1. **Authentication** → Settings → URL Configuration:
   - Site URL: https://ваш-домен.kz
   - Redirect URLs: https://ваш-домен.kz/*

2. **Создайте таблицу profiles**:
```sql
CREATE TABLE profiles (
  id UUID REFERENCES auth.users(id) PRIMARY KEY,
  email TEXT,
  full_name TEXT,
  avatar_url TEXT,
  role TEXT DEFAULT 'user',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- RLS
ALTER TABLE profiles ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can view own profile" ON profiles
  FOR SELECT USING (auth.uid() = id);

CREATE POLICY "Users can update own profile" ON profiles
  FOR UPDATE USING (auth.uid() = id);

-- Trigger to create profile on signup
CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO public.profiles (id, email, full_name)
  VALUES (NEW.id, NEW.email, NEW.raw_user_meta_data->>'full_name');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE TRIGGER on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE FUNCTION public.handle_new_user();
```

3. **Создайте таблицу subscriptions**:
```sql
CREATE TABLE subscriptions (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id UUID REFERENCES auth.users(id),
  plan TEXT NOT NULL,
  status TEXT DEFAULT 'active',
  current_period_start TIMESTAMP WITH TIME ZONE,
  current_period_end TIMESTAMP WITH TIME ZONE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

ALTER TABLE subscriptions ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can view own subscription" ON subscriptions
  FOR SELECT USING (auth.uid() = user_id);
```

## Обновление

```bash
# Остановка
docker compose down

# Получение обновлений
git pull

# Пересборка и запуск
docker compose up -d --build
```

## Мониторинг

```bash
# Логи всех сервисов
docker compose logs -f

# Логи конкретного сервиса
docker compose logs -f backend
docker compose logs -f frontend

# Использование ресурсов
docker stats
```

## Troubleshooting

### Backend не запускается
```bash
docker compose logs backend
# Проверьте .env файл
```

### Frontend показывает 500 ошибку
```bash
docker compose logs frontend
# Проверьте NEXT_PUBLIC_* переменные
```

### SSL проблемы
```bash
# Обновление сертификатов
sudo certbot renew
# Перезапуск nginx
docker compose restart nginx
```
