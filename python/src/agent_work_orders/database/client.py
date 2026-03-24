"""Supabase client for Agent Work Orders.

Provides database connection management and health checks for work order state persistence.
Reuses same Supabase credentials as main Archon server (SUPABASE_URL, SUPABASE_SERVICE_KEY).
"""

import os
from typing import Any

from supabase import Client, create_client

from ..utils.structured_logger import get_logger

logger = get_logger(__name__)


def get_agent_work_orders_client() -> Client:
    """Get Supabase client for agent work orders.

    Reuses same credentials as main Archon server (SUPABASE_URL, SUPABASE_SERVICE_KEY).
    The service key provides full access and bypasses Row Level Security policies.

    Returns:
        Supabase client instance configured for work order operations

    Raises:
        ValueError: If SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables are not set

    Example:
        >>> client = get_agent_work_orders_client()
        >>> response = client.table("archon_agent_work_orders").select("*").execute()
    """
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")

    if not url or not key:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in environment variables. "
            "These should match the credentials used by the main Archon server."
        )

    try:
        # Some versions of supabase-py don't support the new sb_secret_ format yet.
        # If the key doesn't look like a JWT (doesn't contain '.'),
        # we use a dummy JWT to initialize and then override the auth headers.
        try:
            client = create_client(url, key)
        except Exception as e:
            if "Invalid API key" in str(e) or (key and "sb_secret_" in key):
                logger.info("New Supabase secret format detected in agent_work_orders. Using initialization override.")
                # Use a valid-looking dummy JWT to bypass the library's internal regex check
                dummy_jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.s0"
                client = create_client(url, dummy_jwt)
                
                # Override the key for data operations (PostgREST)
                client.postgrest.auth(key)
                
                # Crucial: New Supabase keys also need the 'apikey' header updated
                if hasattr(client.postgrest, "session"):
                    client.postgrest.session.headers["apikey"] = key
                else:
                    # Fallback for newer postgrest versions
                    client.postgrest.headers["apikey"] = key
                    
                # Set the key on the client instance itself
                client.supabase_key = key
            else:
                raise e
        return client
    except Exception as e:
        logger.error(f"Failed to create Supabase client for agent_work_orders: {e}")
        raise


async def check_database_health() -> dict[str, Any]:
    """Check if agent work orders tables exist and are accessible.

    Verifies that both archon_agent_work_orders and archon_agent_work_order_steps
    tables exist and can be queried. This is a lightweight check using limit(0)
    to avoid fetching actual data.

    Returns:
        Dictionary with health check results:
        - status: "healthy" or "unhealthy"
        - tables_exist: True if both tables are accessible, False otherwise
        - error: Error message if check failed (only present when unhealthy)

    Example:
        >>> health = await check_database_health()
        >>> if health["status"] == "healthy":
        ...     print("Database is ready")
    """
    try:
        client = get_agent_work_orders_client()

        # Try to query both tables (limit 0 to avoid fetching data)
        client.table("archon_agent_work_orders").select("agent_work_order_id").limit(0).execute()
        client.table("archon_agent_work_order_steps").select("id").limit(0).execute()

        logger.info("database_health_check_passed", tables=["archon_agent_work_orders", "archon_agent_work_order_steps"])
        return {"status": "healthy", "tables_exist": True}
    except Exception as e:
        logger.error("database_health_check_failed", error=str(e), exc_info=True)
        return {"status": "unhealthy", "tables_exist": False, "error": str(e)}
