"""
Run Supabase migrations automatically.

This script reads SQL migration files from the migrations/ directory
and executes them against the Supabase PostgreSQL database.

Requirements:
- SUPABASE_SERVICE_ROLE_KEY or SUPABASE_DB_URL in .env file
- psycopg2 or psycopg2-binary package installed
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_db_connection():
    """Get PostgreSQL database connection."""
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 not installed. Install it with: pip install psycopg2-binary")
        return None
    
    # Try to get connection from SUPABASE_DB_URL
    db_url = os.getenv("SUPABASE_DB_URL")
    if db_url:
        try:
            return psycopg2.connect(db_url)
        except Exception as e:
            logger.error(f"Failed to connect using SUPABASE_DB_URL: {e}")
    
    # Try to construct connection from Supabase URL and service role key
    supabase_url = os.getenv("SUPABASE_URL", "")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    
    if not supabase_url or not service_role_key:
        logger.error(
            "Missing Supabase credentials. Set either:\n"
            "  - SUPABASE_DB_URL (PostgreSQL connection string)\n"
            "  - SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY"
        )
        return None
    
    # Extract project reference from Supabase URL
    # Example: https://xxxxx.supabase.co -> xxxxx
    try:
        project_ref = supabase_url.replace("https://", "").replace(".supabase.co", "").split("//")[0]
        db_host = f"db.{project_ref}.supabase.co"
        db_port = 5432
        db_name = "postgres"
        db_user = "postgres"
        # Note: Service role key is not the database password
        # You need the actual database password
        db_password = os.getenv("SUPABASE_DB_PASSWORD", "")
        
        if not db_password:
            logger.error(
                "Missing SUPABASE_DB_PASSWORD. "
                "Get it from Supabase Dashboard → Settings → Database → Connection string"
            )
            return None
        
        return psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
    except Exception as e:
        logger.error(f"Failed to construct database connection: {e}")
        return None

def run_migration(conn, migration_file: Path):
    """Run a single migration file."""
    logger.info(f"Running migration: {migration_file.name}")
    
    try:
        with open(migration_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Split by semicolons, but preserve function definitions
        # Simple approach: execute the entire file as one transaction
        with conn.cursor() as cur:
            cur.execute(sql_content)
            conn.commit()
        
        logger.info(f"✅ Successfully executed: {migration_file.name}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to execute {migration_file.name}: {e}")
        conn.rollback()
        return False

def main():
    """Main entry point."""
    migrations_dir = Path(__file__).parent.parent / "migrations"
    
    if not migrations_dir.exists():
        logger.error(f"Migrations directory not found: {migrations_dir}")
        return 1
    
    # Get connection
    conn = get_db_connection()
    if not conn:
        logger.error("Failed to establish database connection")
        return 1
    
    try:
        # Find migration files in order
        migration_files = sorted([
            f for f in migrations_dir.glob("*.sql")
            if f.name.startswith("010_") or f.name.startswith("011_") or f.name.startswith("012_")
        ])
        
        if not migration_files:
            logger.warning("No migration files found (010_*, 011_*, 012_*)")
            return 0
        
        logger.info(f"Found {len(migration_files)} migration file(s)")
        
        # Run migrations
        success_count = 0
        for migration_file in migration_files:
            if run_migration(conn, migration_file):
                success_count += 1
            else:
                logger.error(f"Migration failed: {migration_file.name}")
                logger.info("Stopping migration process due to error")
                return 1
        
        logger.info(f"✅ All migrations completed successfully ({success_count}/{len(migration_files)})")
        return 0
        
    finally:
        conn.close()

if __name__ == "__main__":
    sys.exit(main())
