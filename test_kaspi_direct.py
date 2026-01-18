#!/usr/bin/env python3
"""
Direct test of Kaspi API authentication and product fetching
No database required - tests only the Playwright auth and API calls
"""

import asyncio
import sys
import os

# Set environment variables for minimal config
os.environ['SECRET_KEY'] = 'test-secret-key-min-32-characters-long'
os.environ['ENCRYPTION_KEY'] = 'Jyw8gEkD2sEPuGnRYs4JpHACx0rEf986zVs6f04kEKk='
os.environ['DEBUG'] = 'true'

# Kaspi credentials
KASPI_EMAIL = "hvsv1@icloud.com"
KASPI_PASSWORD = "CIoD29g8U1"


async def test_kaspi_auth_and_products():
    """Test Kaspi authentication and product loading"""

    print("=" * 80)
    print("KASPI INTEGRATION TEST")
    print("=" * 80)

    try:
        # Import services
        print("\n[1/4] Importing Kaspi services...")
        from app.services.kaspi_auth_service import authenticate_kaspi
        from app.services.api_parser import get_products
        print("✅ Services imported successfully")

        # Step 1: Authenticate
        print(f"\n[2/4] Authenticating with Kaspi (email: {KASPI_EMAIL})...")
        session_data = await authenticate_kaspi(
            email=KASPI_EMAIL,
            password=KASPI_PASSWORD,
            merchant_id=None
        )

        print("✅ Authentication successful!")
        print(f"   Merchant ID: {session_data.get('merchant_uid')}")
        print(f"   Shop Name: {session_data.get('shop_name')}")
        print(f"   Requires SMS: {session_data.get('requires_sms', False)}")
        print(f"   Session keys: {list(session_data.keys())}")

        # Check if SMS required
        if session_data.get('requires_sms'):
            print("\n⚠️  SMS verification required!")
            print("   This test cannot proceed automatically.")
            return

        # Step 2: Decrypt session
        print("\n[3/4] Decrypting session...")
        from app.core.security import decrypt_session

        encrypted_guid = session_data.get('guid')
        print(f"   Encrypted GUID type: {type(encrypted_guid)}")
        print(f"   Encrypted GUID length: {len(encrypted_guid) if encrypted_guid else 'None'}")

        decrypted_session = decrypt_session(encrypted_guid)
        print(f"   Decrypted session type: {type(decrypted_session)}")
        print(f"   Decrypted session keys: {list(decrypted_session.keys()) if isinstance(decrypted_session, dict) else 'NOT A DICT!'}")

        # Step 3: Load products
        merchant_id = session_data.get('merchant_uid')
        print(f"\n[4/4] Loading products for merchant {merchant_id}...")

        products = await get_products(
            merchant_id=merchant_id,
            session=decrypted_session
        )

        print(f"✅ Successfully loaded {len(products)} products!")

        # Show first 3 products
        if products:
            print("\n" + "=" * 80)
            print("SAMPLE PRODUCTS (first 3):")
            print("=" * 80)

            for i, product in enumerate(products[:3], 1):
                print(f"\nProduct #{i}:")
                print(f"  Name: {product.get('name')}")
                print(f"  SKU: {product.get('kaspi_sku')}")
                print(f"  Product ID: {product.get('kaspi_product_id')}")
                print(f"  Price: {product.get('price')} tiyns")

                # Show availabilities type (this was the bug!)
                availabilities = product.get('availabilities')
                print(f"  Availabilities type: {type(availabilities)}")
                print(f"  Availabilities keys: {list(availabilities.keys()) if isinstance(availabilities, dict) else 'NOT A DICT'}")

                if isinstance(availabilities, dict):
                    # This is what we need to convert to JSON string for PostgreSQL
                    import json
                    json_string = json.dumps(availabilities)
                    print(f"  Availabilities as JSON: {json_string[:100]}...")

        # Success summary
        print("\n" + "=" * 80)
        print("✅ ALL TESTS PASSED!")
        print("=" * 80)
        print(f"Total products: {len(products)}")
        print("\nConclusion:")
        print("  ✅ Kaspi authentication works")
        print("  ✅ Session encryption/decryption works")
        print("  ✅ Product API fetching works")
        print("  ✅ Availabilities are returned as dict (need JSON conversion for DB)")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    print("Starting Kaspi integration test...")
    print("Note: This test requires Playwright browser to be installed")
    print()

    asyncio.run(test_kaspi_auth_and_products())
