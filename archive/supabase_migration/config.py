"""
Supabase Configuration Module.

Handles Supabase connection configuration from environment variables.
"""

import os
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class SupabaseConfig:
    """Supabase configuration from environment variables."""
    
    def __init__(self):
        """Initialize Supabase configuration."""
        self.url: Optional[str] = os.getenv("SUPABASE_URL")
        self.anon_key: Optional[str] = os.getenv("SUPABASE_KEY")
        self.service_key: Optional[str] = os.getenv("SUPABASE_SERVICE_KEY")
        self.db_password: Optional[str] = os.getenv("SUPABASE_DB_PASSWORD")
        
        # Validate required fields
        if not self.url:
            raise ValueError("SUPABASE_URL environment variable is required")
        if not self.anon_key:
            raise ValueError("SUPABASE_KEY environment variable is required")
    
    def is_configured(self) -> bool:
        """Check if Supabase is properly configured."""
        return bool(self.url and self.anon_key)
    
    def get_connection_string(self) -> str:
        """
        Get PostgreSQL connection string for direct database access.
        
        Note: This requires the database password and is typically used for
        PySpark JDBC connections or direct psycopg2 connections.
        """
        if not self.db_password:
            raise ValueError("SUPABASE_DB_PASSWORD is required for connection string")
        
        # Extract project reference from URL
        # URL format: https://xxxxx.supabase.co
        project_ref = self.url.replace("https://", "").replace(".supabase.co", "")
        
        return f"postgresql://postgres:{self.db_password}@db.{project_ref}.supabase.co:5432/postgres"
    
    def __repr__(self) -> str:
        """String representation (hides sensitive keys)."""
        url_display = self.url if self.url else "Not set"
        key_display = f"{self.anon_key[:10]}..." if self.anon_key else "Not set"
        return f"SupabaseConfig(url={url_display}, key={key_display})"
