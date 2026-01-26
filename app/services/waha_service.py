"""
WAHA Service - прямая работа с WAHA API (без Railway)
Использует один общий WAHA контейнер из docker-compose
"""
import httpx
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class WahaSessionStatus(str, Enum):
    """Статусы сессии WAHA"""
    STARTING = "STARTING"
    SCAN_QR_CODE = "SCAN_QR_CODE"
    WORKING = "WORKING"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


class WahaError(Exception):
    """Базовая ошибка WAHA"""
    def __init__(self, message: str, status_code: int = None, details: dict = None):
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)


class WahaConnectionError(WahaError):
    """Ошибка подключения к WAHA"""
    pass


class WahaSessionError(WahaError):
    """Ошибка сессии WAHA"""
    pass


class WahaMessageError(WahaError):
    """Ошибка отправки сообщения"""
    pass


@dataclass
class WahaConfig:
    """Конфигурация WAHA"""
    base_url: str = "http://waha:3000"
    api_key: Optional[str] = None
    default_session: str = "default"
    webhook_url: Optional[str] = None
    timeout: float = 30.0


class WahaService:
    """
    Сервис для работы с WAHA API.
    Использует общий WAHA контейнер (docker-compose).
    """

    def __init__(self, config: WahaConfig = None):
        self.config = config or WahaConfig()
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def headers(self) -> Dict[str, str]:
        """HTTP заголовки для запросов"""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.config.api_key:
            headers["X-Api-Key"] = self.config.api_key
        return headers

    async def _get_client(self) -> httpx.AsyncClient:
        """Получить HTTP клиент (lazy initialization)"""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.config.base_url,
                headers=self.headers,
                timeout=self.config.timeout,
            )
        return self._client

    async def close(self):
        """Закрыть HTTP клиент"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def _request(
        self,
        method: str,
        endpoint: str,
        json_data: dict = None,
        params: dict = None,
    ) -> Dict[str, Any]:
        """
        Выполнить HTTP запрос к WAHA API
        
        Args:
            method: HTTP метод (GET, POST, DELETE, etc.)
            endpoint: Путь API (например /api/sessions)
            json_data: JSON тело запроса
            params: Query параметры
            
        Returns:
            Ответ API как dict
            
        Raises:
            WahaConnectionError: Ошибка соединения
            WahaError: Ошибка API
        """
        client = await self._get_client()
        
        try:
            response = await client.request(
                method=method,
                url=endpoint,
                json=json_data,
                params=params,
            )
            
            # Логируем запрос
            logger.debug(f"WAHA {method} {endpoint} -> {response.status_code}")
            
            # Проверяем успешность
            if response.status_code >= 400:
                error_detail = response.text
                try:
                    error_json = response.json()
                    error_detail = error_json.get("message", error_detail)
                except Exception:
                    pass
                    
                logger.error(f"WAHA API error: {response.status_code} - {error_detail}")
                raise WahaError(
                    message=f"WAHA API error: {error_detail}",
                    status_code=response.status_code,
                    details={"endpoint": endpoint, "response": error_detail}
                )
            
            # Возвращаем JSON или пустой dict
            if response.headers.get("content-type", "").startswith("application/json"):
                return response.json()
            return {"raw": response.text}
            
        except httpx.ConnectError as e:
            logger.error(f"WAHA connection error: {e}")
            raise WahaConnectionError(
                message=f"Cannot connect to WAHA at {self.config.base_url}. Is WAHA container running?",
                details={"error": str(e)}
            )
        except httpx.TimeoutException as e:
            logger.error(f"WAHA timeout: {e}")
            raise WahaConnectionError(
                message="WAHA request timeout",
                details={"error": str(e)}
            )

    # ==================== SESSION MANAGEMENT ====================

    async def create_session(
        self,
        name: str = None,
        webhook_url: str = None,
        webhook_events: List[str] = None,
    ) -> Dict[str, Any]:
        """
        Создать новую сессию WhatsApp
        
        Args:
            name: Имя сессии (по умолчанию "default")
            webhook_url: URL для webhook уведомлений
            webhook_events: Список событий для webhook
            
        Returns:
            Информация о созданной сессии
        """
        session_name = name or self.config.default_session
        
        payload = {
            "name": session_name,
            "start": True,
        }
        
        # Добавляем webhook конфигурацию если указана
        if webhook_url or self.config.webhook_url:
            payload["config"] = {
                "webhooks": [{
                    "url": webhook_url or self.config.webhook_url,
                    "events": webhook_events or ["message", "session.status"],
                }]
            }
        
        logger.info(f"Creating WAHA session: {session_name}")
        
        try:
            result = await self._request("POST", "/api/sessions", json_data=payload)
            logger.info(f"WAHA session created: {session_name}")
            return result
        except WahaError as e:
            # Если сессия уже существует, это не ошибка
            if e.status_code == 422 and "already exists" in str(e.details).lower():
                logger.info(f"WAHA session already exists: {session_name}")
                return await self.get_session(session_name)
            raise WahaSessionError(
                message=f"Failed to create session: {e.message}",
                status_code=e.status_code,
                details=e.details
            )

    async def get_session(self, name: str = None) -> Dict[str, Any]:
        """
        Получить информацию о сессии
        
        Args:
            name: Имя сессии
            
        Returns:
            Информация о сессии (name, status, etc.)
        """
        session_name = name or self.config.default_session
        return await self._request("GET", f"/api/sessions/{session_name}")

    async def list_sessions(self) -> List[Dict[str, Any]]:
        """
        Получить список всех сессий
        
        Returns:
            Список сессий
        """
        return await self._request("GET", "/api/sessions")

    async def stop_session(self, name: str = None) -> Dict[str, Any]:
        """
        Остановить сессию
        
        Args:
            name: Имя сессии
        """
        session_name = name or self.config.default_session
        logger.info(f"Stopping WAHA session: {session_name}")
        return await self._request("POST", f"/api/sessions/{session_name}/stop")

    async def start_session(self, name: str = None) -> Dict[str, Any]:
        """
        Запустить сессию
        
        Args:
            name: Имя сессии
        """
        session_name = name or self.config.default_session
        logger.info(f"Starting WAHA session: {session_name}")
        return await self._request("POST", f"/api/sessions/{session_name}/start")

    async def delete_session(self, name: str = None) -> Dict[str, Any]:
        """
        Удалить сессию (logout)
        
        Args:
            name: Имя сессии
        """
        session_name = name or self.config.default_session
        logger.info(f"Deleting WAHA session: {session_name}")
        return await self._request("DELETE", f"/api/sessions/{session_name}")

    # ==================== QR CODE & AUTH ====================

    async def get_qr_code(self, name: str = None, format: str = "image") -> bytes | Dict:  # noqa: A002
        """
        Получить QR код для авторизации
        
        Args:
            name: Имя сессии
            format: "image" для PNG, "raw" для base64 строки
            
        Returns:
            QR код как bytes (image) или dict с base64 (raw)
        """
        session_name = name or self.config.default_session
        qr_format = format  # Alias to avoid shadowing built-in
        
        client = await self._get_client()
        
        try:
            response = await client.get(
                f"/api/{session_name}/auth/qr",
                params={"format": qr_format},
            )
            
            if response.status_code == 200:
                if qr_format == "image":
                    return response.content  # PNG bytes
                return response.json()
            elif response.status_code == 404:
                raise WahaSessionError(
                    message=f"Session '{session_name}' not found or not ready for QR",
                    status_code=404
                )
            else:
                raise WahaError(
                    message=f"Failed to get QR code: {response.text}",
                    status_code=response.status_code
                )
                
        except httpx.ConnectError as e:
            raise WahaConnectionError(message=f"Cannot connect to WAHA: {e}")

    async def get_screenshot(self, name: str = None) -> bytes:
        """
        Получить скриншот WhatsApp Web (для отладки)
        
        Args:
            name: Имя сессии
            
        Returns:
            PNG изображение как bytes
        """
        session_name = name or self.config.default_session
        
        client = await self._get_client()
        response = await client.get(f"/api/{session_name}/auth/screenshot")
        
        if response.status_code == 200:
            return response.content
        raise WahaError(
            message=f"Failed to get screenshot: {response.text}",
            status_code=response.status_code
        )

    # ==================== MESSAGING ====================

    @staticmethod
    def format_chat_id(phone: str) -> str:
        """
        Форматировать номер телефона в chat_id для WAHA
        
        Args:
            phone: Номер телефона (77001234567)
            
        Returns:
            Chat ID (77001234567@c.us)
        """
        # Убираем все нецифровые символы
        phone_clean = "".join(filter(str.isdigit, phone))
        
        # Убираем начальный + если есть
        if phone_clean.startswith("+"):
            phone_clean = phone_clean[1:]
            
        return f"{phone_clean}@c.us"

    async def send_text(
        self,
        phone: str,
        text: str,
        session: str = None,
    ) -> Dict[str, Any]:
        """
        Отправить текстовое сообщение
        
        Args:
            phone: Номер телефона получателя
            text: Текст сообщения
            session: Имя сессии
            
        Returns:
            Информация об отправленном сообщении
        """
        session_name = session or self.config.default_session
        chat_id = self.format_chat_id(phone)
        
        payload = {
            "chatId": chat_id,
            "text": text,
            "session": session_name,
        }
        
        logger.info(f"Sending text to {chat_id} via session {session_name}")
        
        try:
            result = await self._request("POST", "/api/sendText", json_data=payload)
            logger.info(f"Message sent to {chat_id}, id: {result.get('id')}")
            return result
        except WahaError as e:
            raise WahaMessageError(
                message=f"Failed to send message: {e.message}",
                status_code=e.status_code,
                details={"phone": phone, "chat_id": chat_id, **e.details}
            )

    async def send_seen(
        self,
        phone: str,
        session: str = None,
    ) -> Dict[str, Any]:
        """
        Пометить сообщения как прочитанные
        
        Args:
            phone: Номер телефона
            session: Имя сессии
        """
        session_name = session or self.config.default_session
        chat_id = self.format_chat_id(phone)
        
        payload = {
            "chatId": chat_id,
            "session": session_name,
        }
        
        return await self._request("POST", "/api/sendSeen", json_data=payload)

    async def send_image(
        self,
        phone: str,
        image_url: str,
        caption: str = None,
        session: str = None,
    ) -> Dict[str, Any]:
        """
        Отправить изображение
        
        Args:
            phone: Номер телефона
            image_url: URL изображения
            caption: Подпись к изображению
            session: Имя сессии
        """
        session_name = session or self.config.default_session
        chat_id = self.format_chat_id(phone)
        
        payload = {
            "chatId": chat_id,
            "session": session_name,
            "file": {
                "url": image_url,
            },
        }
        
        if caption:
            payload["caption"] = caption
        
        logger.info(f"Sending image to {chat_id}")
        return await self._request("POST", "/api/sendImage", json_data=payload)

    async def send_file(
        self,
        phone: str,
        file_url: str,
        filename: str = None,
        caption: str = None,
        session: str = None,
    ) -> Dict[str, Any]:
        """
        Отправить файл
        
        Args:
            phone: Номер телефона
            file_url: URL файла
            filename: Имя файла
            caption: Подпись
            session: Имя сессии
        """
        session_name = session or self.config.default_session
        chat_id = self.format_chat_id(phone)
        
        payload = {
            "chatId": chat_id,
            "session": session_name,
            "file": {
                "url": file_url,
            },
        }
        
        if filename:
            payload["file"]["filename"] = filename
        if caption:
            payload["caption"] = caption
        
        logger.info(f"Sending file to {chat_id}")
        return await self._request("POST", "/api/sendFile", json_data=payload)

    async def send_poll(
        self,
        phone: str,
        question: str,
        options: List[str],
        multiple_answers: bool = False,
        session: str = None,
    ) -> Dict[str, Any]:
        """
        Отправить опрос
        
        Args:
            phone: Номер телефона
            question: Текст вопроса
            options: Варианты ответов
            multiple_answers: Разрешить множественный выбор
            session: Имя сессии
        """
        session_name = session or self.config.default_session
        chat_id = self.format_chat_id(phone)
        
        payload = {
            "chatId": chat_id,
            "session": session_name,
            "poll": {
                "name": question,
                "options": options,
                "multipleAnswers": multiple_answers,
            },
        }
        
        logger.info(f"Sending poll to {chat_id}")
        return await self._request("POST", "/api/sendPoll", json_data=payload)

    async def send_location(
        self,
        phone: str,
        latitude: float,
        longitude: float,
        name: str = None,
        address: str = None,
        session: str = None,
    ) -> Dict[str, Any]:
        """
        Отправить локацию
        
        Args:
            phone: Номер телефона
            latitude: Широта
            longitude: Долгота
            name: Название места
            address: Адрес
            session: Имя сессии
        """
        session_name = session or self.config.default_session
        chat_id = self.format_chat_id(phone)
        
        payload = {
            "chatId": chat_id,
            "session": session_name,
            "latitude": latitude,
            "longitude": longitude,
        }
        
        if name:
            payload["name"] = name
        if address:
            payload["address"] = address
        
        logger.info(f"Sending location to {chat_id}")
        return await self._request("POST", "/api/sendLocation", json_data=payload)

    async def send_contact(
        self,
        phone: str,
        contact_name: str,
        contact_phone: str,
        session: str = None,
    ) -> Dict[str, Any]:
        """
        Отправить контакт (vCard)
        
        Args:
            phone: Номер телефона получателя
            contact_name: Имя контакта
            contact_phone: Телефон контакта
            session: Имя сессии
        """
        session_name = session or self.config.default_session
        recipient_chat_id = self.format_chat_id(phone)
        
        payload = {
            "chatId": recipient_chat_id,
            "session": session_name,
            "contacts": [{
                "fullName": contact_name,
                "phoneNumber": contact_phone,
            }],
        }
        
        logger.info("Sending contact to %s", recipient_chat_id)
        return await self._request("POST", "/api/sendContacts", json_data=payload)

    # ==================== CHAT INFO ====================

    async def get_chats(self, session: str = None) -> List[Dict[str, Any]]:
        """
        Получить список чатов
        
        Args:
            session: Имя сессии
            
        Returns:
            Список чатов
        """
        session_name = session or self.config.default_session
        return await self._request("GET", f"/api/{session_name}/chats")

    async def get_messages(
        self,
        phone: str,
        session: str = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Получить историю сообщений чата
        
        Args:
            phone: Номер телефона
            session: Имя сессии
            limit: Количество сообщений
            
        Returns:
            Список сообщений
        """
        session_name = session or self.config.default_session
        chat_id = self.format_chat_id(phone)
        
        return await self._request(
            "GET",
            f"/api/{session_name}/chats/{chat_id}/messages",
            params={"limit": limit}
        )

    async def check_number_exists(
        self,
        phone: str,
        session: str = None,
    ) -> bool:
        """
        Проверить существует ли номер в WhatsApp
        
        Args:
            phone: Номер телефона
            session: Имя сессии
            
        Returns:
            True если номер зарегистрирован в WhatsApp
        """
        session_name = session or self.config.default_session
        
        try:
            result = await self._request(
                "GET",
                "/api/contacts/check-exists",
                params={"phone": phone, "session": session_name}
            )
            return result.get("exists", False)
        except WahaError:
            return False

    # ==================== HEALTH CHECK ====================

    async def health_check(self) -> bool:
        """
        Проверить доступность WAHA
        
        Returns:
            True если WAHA доступен
        """
        try:
            await self._request("GET", "/api/version")
            return True
        except Exception as e:
            logger.warning(f"WAHA health check failed: {e}")
            return False

    async def get_version(self) -> Dict[str, Any]:
        """
        Получить версию WAHA
        
        Returns:
            Информация о версии
        """
        return await self._request("GET", "/api/version")


# Singleton instance (будет инициализирован с настройками из config)
_waha_service: Optional[WahaService] = None


def get_waha_service() -> WahaService:
    """
    Получить singleton instance WahaService
    
    Использование:
        from app.services.waha_service import get_waha_service
        waha = get_waha_service()
        await waha.send_text("77001234567", "Hello!")
    """
    global _waha_service
    
    if _waha_service is None:
        from ..config import settings
        
        config = WahaConfig(
            base_url=getattr(settings, 'waha_url', 'http://waha:3000'),
            api_key=getattr(settings, 'waha_api_key', None),
            webhook_url=getattr(settings, 'waha_webhook_url', None),
        )
        _waha_service = WahaService(config)
    
    return _waha_service
