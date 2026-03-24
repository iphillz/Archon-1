"""
Client Manager Service

Manages database and API client connections. Supports new Supabase secret format.
"""

import os
import re
import logging
from supabase import Client, create_client
from ..config.logfire_config import search_logger

def get_supabase_client() -> Client:
    """
    Get or create a properly configured Supabase client using environment variables.
    Supports both legacy JWT service role keys and new sb_secret_ service role keys.
    """
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")

    if not url or not key:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in environment variables"
        )

    try:
        # Some versions of supabase-py don't support the new sb_secret_ format yet.
        # If the key doesn't look like a JWT (doesn't contain '.'),
        # we use a dummy JWT to initialize and then override the auth headers.
        try:
            client = create_client(url, key)
        except Exception as e:
            if "Invalid API key" in str(e) or "sb_secret_" in key:
                search_logger.info("New Supabase secret format detected in client_manager. Using initialization override.")
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

        # Extract project ID from URL for logging purposes only
        match = re.match(r"https://([^.]+)\.supabase\.co", url)
        if match:
            project_id = match.group(1)
            search_logger.debug(f"Supabase client initialized - project_id={project_id}")

        return client
    except Exception as e:
        search_logger.error(f"Failed to create Supabase client: {e}")
        raise
