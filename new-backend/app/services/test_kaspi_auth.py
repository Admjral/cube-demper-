"""
Test file for Kaspi Authentication Service

This demonstrates the usage patterns for the dual-flow authentication:
1. Direct login (no SMS)
2. Login with SMS verification

Run with: pytest app/services/test_kaspi_auth.py -v
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime

from .kaspi_auth_service import (
    authenticate_kaspi,
    verify_sms_code,
    validate_session,
    refresh_session,
    get_active_session,
    KaspiAuthError,
    KaspiSMSRequiredError,
    KaspiInvalidCredentialsError,
)


@pytest.mark.asyncio
class TestKaspiAuthService:
    """Test suite for Kaspi authentication service"""

    async def test_direct_login_success(self):
        """Test successful login without SMS verification"""
        with patch('app.services.kaspi_auth_service.get_browser_farm') as mock_farm:
            # Mock browser farm
            mock_page = AsyncMock()
            mock_context = AsyncMock()
            mock_shard = AsyncMock()

            mock_shard.initialize = AsyncMock()
            mock_shard.get_context = AsyncMock(return_value=mock_context)
            mock_context.new_page = AsyncMock(return_value=mock_page)

            mock_farm_instance = AsyncMock()
            mock_farm_instance.shards = [mock_shard]
            mock_farm.return_value = mock_farm_instance

            # Mock successful login
            with patch('app.services.kaspi_auth_service._login_to_kaspi_page') as mock_login:
                mock_cookies = [
                    {'name': 'session', 'value': 'abc123', 'domain': '.kaspi.kz'}
                ]
                mock_login.return_value = (True, mock_cookies, False)

                with patch('app.services.kaspi_auth_service._get_merchant_info') as mock_merchant:
                    mock_merchant.return_value = ('MERCH123', 'Test Shop')

                    # Execute
                    result = await authenticate_kaspi('test@example.com', 'password123')

                    # Verify
                    assert result['merchant_uid'] == 'MERCH123'
                    assert result['shop_name'] == 'Test Shop'
                    assert result['requires_sms'] is False
                    assert 'guid' in result
                    assert isinstance(result['guid'], str)  # Should be encrypted

    async def test_login_with_sms_required(self):
        """Test login that requires SMS verification"""
        with patch('app.services.kaspi_auth_service.get_browser_farm') as mock_farm:
            # Mock browser farm
            mock_page = AsyncMock()
            mock_page.url = 'https://idmc.shop.kaspi.kz/verify'
            mock_context = AsyncMock()
            mock_shard = AsyncMock()

            mock_shard.initialize = AsyncMock()
            mock_shard.get_context = AsyncMock(return_value=mock_context)
            mock_context.new_page = AsyncMock(return_value=mock_page)

            mock_farm_instance = AsyncMock()
            mock_farm_instance.shards = [mock_shard]
            mock_farm.return_value = mock_farm_instance

            # Mock login requiring SMS
            with patch('app.services.kaspi_auth_service._login_to_kaspi_page') as mock_login:
                mock_cookies = [
                    {'name': 'temp_session', 'value': 'xyz789', 'domain': '.kaspi.kz'}
                ]
                mock_login.return_value = (True, mock_cookies, True)  # SMS required

                # Execute and expect exception
                with pytest.raises(KaspiSMSRequiredError) as exc_info:
                    await authenticate_kaspi('test@example.com', 'password123')

                # Verify exception contains partial session
                exception = exc_info.value
                assert exception.partial_session is not None
                assert 'cookies' in exception.partial_session
                assert 'email' in exception.partial_session
                assert exception.partial_session['email'] == 'test@example.com'

    async def test_sms_verification_success(self):
        """Test successful SMS verification"""
        partial_session = {
            'cookies': [
                {'name': 'temp_session', 'value': 'xyz789', 'domain': '.kaspi.kz'}
            ],
            'email': 'test@example.com',
            'password': 'password123',
            'page_url': 'https://idmc.shop.kaspi.kz/verify'
        }

        with patch('app.services.kaspi_auth_service.get_browser_farm') as mock_farm:
            # Mock browser farm
            mock_page = AsyncMock()
            mock_context = AsyncMock()
            mock_shard = AsyncMock()

            mock_shard.initialize = AsyncMock()
            mock_shard.get_context = AsyncMock(return_value=mock_context)
            mock_context.new_page = AsyncMock(return_value=mock_page)

            mock_farm_instance = AsyncMock()
            mock_farm_instance.shards = [mock_shard]
            mock_farm.return_value = mock_farm_instance

            # Mock SMS verification
            with patch('app.services.kaspi_auth_service._verify_sms_on_page') as mock_verify:
                mock_cookies = [
                    {'name': 'session', 'value': 'abc123', 'domain': '.kaspi.kz'}
                ]
                mock_verify.return_value = (True, mock_cookies)

                with patch('app.services.kaspi_auth_service._get_merchant_info') as mock_merchant:
                    mock_merchant.return_value = ('MERCH123', 'Test Shop')

                    with patch('app.services.kaspi_auth_service.get_db_pool') as mock_pool:
                        mock_conn = AsyncMock()
                        mock_pool.return_value.acquire = AsyncMock(return_value=mock_conn)
                        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
                        mock_conn.__aexit__ = AsyncMock()

                        # Execute
                        result = await verify_sms_code('MERCH123', '123456', partial_session)

                        # Verify
                        assert result['merchant_uid'] == 'MERCH123'
                        assert result['shop_name'] == 'Test Shop'
                        assert 'guid' in result
                        assert isinstance(result['guid'], str)

    async def test_invalid_credentials(self):
        """Test login with invalid credentials"""
        with patch('app.services.kaspi_auth_service.get_browser_farm') as mock_farm:
            # Mock browser farm
            mock_page = AsyncMock()
            mock_context = AsyncMock()
            mock_shard = AsyncMock()

            mock_shard.initialize = AsyncMock()
            mock_shard.get_context = AsyncMock(return_value=mock_context)
            mock_context.new_page = AsyncMock(return_value=mock_page)

            mock_farm_instance = AsyncMock()
            mock_farm_instance.shards = [mock_shard]
            mock_farm.return_value = mock_farm_instance

            # Mock invalid credentials
            with patch('app.services.kaspi_auth_service._login_to_kaspi_page') as mock_login:
                mock_login.side_effect = KaspiInvalidCredentialsError("Invalid credentials")

                # Execute and expect exception
                with pytest.raises(KaspiInvalidCredentialsError):
                    await authenticate_kaspi('test@example.com', 'wrongpassword')

    async def test_validate_session_valid(self):
        """Test session validation with valid session"""
        from app.core.security import encrypt_session

        session_data = {
            'cookies': [
                {'name': 'session', 'value': 'abc123', 'domain': '.kaspi.kz'}
            ],
            'email': 'test@example.com',
            'merchant_uid': 'MERCH123'
        }

        encrypted_guid = encrypt_session(session_data)

        with patch('httpx.AsyncClient') as mock_client:
            # Mock successful API call
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                'merchants': [{'uid': 'MERCH123', 'name': 'Test Shop'}]
            }

            mock_client_instance = AsyncMock()
            mock_client_instance.get = AsyncMock(return_value=mock_response)
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock()
            mock_client.return_value = mock_client_instance

            # Execute
            is_valid = await validate_session(encrypted_guid)

            # Verify
            assert is_valid is True

    async def test_validate_session_expired(self):
        """Test session validation with expired session"""
        from app.core.security import encrypt_session

        session_data = {
            'cookies': [
                {'name': 'session', 'value': 'expired123', 'domain': '.kaspi.kz'}
            ],
            'email': 'test@example.com'
        }

        encrypted_guid = encrypt_session(session_data)

        with patch('httpx.AsyncClient') as mock_client:
            # Mock failed API call
            mock_response = AsyncMock()
            mock_response.status_code = 401

            mock_client_instance = AsyncMock()
            mock_client_instance.get = AsyncMock(return_value=mock_response)
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock()
            mock_client.return_value = mock_client_instance

            # Execute
            is_valid = await validate_session(encrypted_guid)

            # Verify
            assert is_valid is False

    async def test_get_active_session(self):
        """Test getting active session from database"""
        from app.core.security import encrypt_session

        session_data = {
            'cookies': [
                {'name': 'session', 'value': 'abc123', 'domain': '.kaspi.kz'}
            ],
            'email': 'test@example.com',
            'merchant_uid': 'MERCH123'
        }

        encrypted_guid = encrypt_session(session_data)

        with patch('app.services.kaspi_auth_service.get_db_pool') as mock_pool:
            # Mock database
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value={
                'guid': {'encrypted': encrypted_guid}
            })

            mock_pool_instance = AsyncMock()
            mock_pool_instance.acquire = AsyncMock(return_value=mock_conn)
            mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_conn.__aexit__ = AsyncMock()
            mock_pool.return_value = mock_pool_instance

            with patch('app.services.kaspi_auth_service.validate_session') as mock_validate:
                mock_validate.return_value = True

                # Execute
                result = await get_active_session('MERCH123')

                # Verify
                assert result is not None
                assert result['email'] == 'test@example.com'
                assert result['merchant_uid'] == 'MERCH123'


# Integration test example (requires actual credentials)
@pytest.mark.integration
@pytest.mark.skip(reason="Requires actual Kaspi credentials")
async def test_full_auth_flow_integration():
    """
    Full integration test - requires real credentials.

    Usage:
        1. Set environment variables:
           export KASPI_TEST_EMAIL=your@email.com
           export KASPI_TEST_PASSWORD=yourpassword
        2. Run: pytest app/services/test_kaspi_auth.py::test_full_auth_flow_integration -v
    """
    import os

    email = os.getenv('KASPI_TEST_EMAIL')
    password = os.getenv('KASPI_TEST_PASSWORD')

    if not email or not password:
        pytest.skip("KASPI_TEST_EMAIL and KASPI_TEST_PASSWORD must be set")

    try:
        # Attempt authentication
        result = await authenticate_kaspi(email, password)

        print(f"Authentication successful!")
        print(f"Merchant: {result['shop_name']} ({result['merchant_uid']})")
        print(f"GUID encrypted: {result['guid'][:50]}...")

        # Validate the session
        is_valid = await validate_session(result['guid'])
        assert is_valid, "Session should be valid immediately after auth"

    except KaspiSMSRequiredError as e:
        print("SMS verification required!")
        print("In a real scenario, you would:")
        print("1. Store partial_session from exception")
        print("2. Wait for user to enter SMS code")
        print("3. Call verify_sms_code(merchant_id, sms_code, partial_session)")

        # For testing, we can't complete this without actual SMS
        pytest.skip("Cannot complete SMS verification in automated test")
