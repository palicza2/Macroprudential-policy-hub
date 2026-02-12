"""
Run Supabase migrations using Supabase CLI.

This script uses npx supabase to run migrations, which is the recommended
method for Supabase projects.

Requirements:
- npm/node installed
- Supabase project linked (run: npx supabase link --project-ref [PROJECT_REF])
"""

import os
import sys
import subprocess
import logging
from pathlib import Path
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def run_supabase_cli_command(command: list) -> bool:
    """Run a Supabase CLI command using npx."""
    try:
        full_command = ["npx", "supabase"] + command
        logger.info(f"Running: {' '.join(full_command)}")
        
        result = subprocess.run(
            full_command,
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode == 0:
            if result.stdout:
                logger.info(result.stdout)
            return True
        else:
            if result.stderr:
                logger.error(result.stderr)
            if result.stdout:
                logger.error(result.stdout)
            return False
    except FileNotFoundError:
        logger.error("npm/npx not found. Please install Node.js and npm.")
        return False
    except Exception as e:
        logger.error(f"Error running Supabase CLI: {e}")
        return False

def check_supabase_linked() -> bool:
    """Check if Supabase project is linked."""
    result = run_supabase_cli_command(["status"])
    return result

def link_supabase_project(project_ref: str = None, password: str = None) -> bool:
    """Link Supabase project."""
    if not project_ref:
        # Try to get from environment
        supabase_url = os.getenv("SUPABASE_URL", "")
        if supabase_url:
            # Extract project ref from URL
            project_ref = supabase_url.replace("https://", "").replace(".supabase.co", "").split("/")[0]
    
    if not project_ref:
        logger.error(
            "Project reference not found. Provide it as argument or set SUPABASE_URL in .env"
        )
        return False
    
    command = ["link", "--project-ref", project_ref]
    if password:
        command.extend(["--password", password])
    
    return run_supabase_cli_command(command)

def push_migrations() -> bool:
    """Push all migrations to Supabase."""
    return run_supabase_cli_command(["db", "push"])

def list_migrations() -> bool:
    """List migration status."""
    return run_supabase_cli_command(["migration", "list"])

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run Supabase migrations using CLI")
    parser.add_argument(
        "--link",
        action="store_true",
        help="Link Supabase project first"
    )
    parser.add_argument(
        "--project-ref",
        type=str,
        help="Supabase project reference (for linking)"
    )
    parser.add_argument(
        "--password",
        type=str,
        help="Database password (for linking)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List migration status"
    )
    
    args = parser.parse_args()
    
    # Check if linked
    if not check_supabase_linked():
        if args.link:
            logger.info("Linking Supabase project...")
            if link_supabase_project(args.project_ref, args.password):
                logger.info("✅ Successfully linked Supabase project")
            else:
                logger.error("❌ Failed to link Supabase project")
                return 1
        else:
            logger.error(
                "Supabase project not linked. Run with --link flag or manually:\n"
                "  npx supabase link --project-ref [PROJECT_REF]"
            )
            return 1
    
    if args.list:
        logger.info("Listing migration status...")
        return 0 if list_migrations() else 1
    
    # Push migrations
    logger.info("Pushing migrations to Supabase...")
    if push_migrations():
        logger.info("✅ All migrations pushed successfully")
        return 0
    else:
        logger.error("❌ Failed to push migrations")
        return 1

if __name__ == "__main__":
    sys.exit(main())
