"""
Railway WAHA Service - создает WAHA контейнеры через Railway API
вместо Docker daemon
"""
import os
import httpx
import asyncpg
import secrets
from uuid import UUID
from typing import Optional, Dict

RAILWAY_API_URL = "https://backboard.railway.app/graphql"
RAILWAY_API_TOKEN = os.getenv("RAILWAY_API_TOKEN")  # Personal Access Token
PROJECT_ID = os.getenv("RAILWAY_PROJECT_ID")  # Cube Demper project ID
BACKEND_URL = os.getenv("BACKEND_URL")


class RailwayWahaService:
    """Управление WAHA контейнерами через Railway API"""

    async def create_user_session(self, user_id: UUID, pool: asyncpg.Pool):
        """Создать WAHA service для пользователя через Railway API"""
        # 1. Generate unique API key
        api_key = secrets.token_urlsafe(32)

        # 2. Create new service via Railway GraphQL API
        service_name = f"waha-user-{user_id}"

        # GraphQL mutation to create service
        mutation = """
        mutation CreateService($input: ServiceCreateInput!) {
          serviceCreate(input: $input) {
            id
            name
          }
        }
        """

        variables = {
            "input": {
                "projectId": PROJECT_ID,
                "name": service_name,
                "source": {
                    "image": "devlikeapro/waha:latest"
                },
                "variables": {
                    "WAHA_API_KEY": api_key,
                    "WHATSAPP_HOOK_URL": f"{BACKEND_URL}/whatsapp/webhook",
                    "WHATSAPP_HOOK_EVENTS": "message,session.status"
                }
            }
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                RAILWAY_API_URL,
                json={"query": mutation, "variables": variables},
                headers={"Authorization": f"Bearer {RAILWAY_API_TOKEN}"}
            )
            result = response.json()
            service_id = result["data"]["serviceCreate"]["id"]

        # 3. Save to database
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO whatsapp_sessions
                (user_id, waha_container_name, waha_api_key, status, railway_service_id)
                VALUES ($1, $2, $3, 'STARTING', $4)
            """, user_id, service_name, api_key, service_id)

        return {"service_id": service_id, "api_key": api_key}

    async def get_service_url(self, user_id: UUID, pool: asyncpg.Pool) -> str:
        """Получить Railway URL для WAHA service пользователя"""
        async with pool.acquire() as conn:
            session = await conn.fetchrow(
                "SELECT railway_service_id FROM whatsapp_sessions WHERE user_id = $1",
                user_id
            )

            if not session:
                raise ValueError("No WAHA session found")

            # Query Railway API для service URL
            query = """
            query GetService($id: String!) {
              service(id: $id) {
                domains {
                  serviceDomains {
                    domain
                  }
                }
              }
            }
            """

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    RAILWAY_API_URL,
                    json={"query": query, "variables": {"id": session["railway_service_id"]}},
                    headers={"Authorization": f"Bearer {RAILWAY_API_TOKEN}"}
                )
                result = response.json()
                domain = result["data"]["service"]["domains"]["serviceDomains"][0]["domain"]
                return f"https://{domain}"

    async def delete_user_session(self, user_id: UUID, pool: asyncpg.Pool):
        """Удалить WAHA service пользователя через Railway API"""
        async with pool.acquire() as conn:
            session = await conn.fetchrow(
                "SELECT railway_service_id FROM whatsapp_sessions WHERE user_id = $1",
                user_id
            )

            if not session:
                raise ValueError("No WAHA session found")

            # GraphQL mutation to delete service
            mutation = """
            mutation DeleteService($id: String!) {
              serviceDelete(id: $id)
            }
            """

            async with httpx.AsyncClient() as client:
                await client.post(
                    RAILWAY_API_URL,
                    json={"query": mutation, "variables": {"id": session["railway_service_id"]}},
                    headers={"Authorization": f"Bearer {RAILWAY_API_TOKEN}"}
                )

            # Remove from database
            await conn.execute(
                "DELETE FROM whatsapp_sessions WHERE user_id = $1",
                user_id
            )
