"""
Script to run Supabase migration.

Usage:
    python scripts/run_supabase_migration.py [--dry-run] [--skip-validation]
"""

import sys
import logging
import argparse
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import from archived supabase_migration
import sys
from pathlib import Path
_base_dir = Path(__file__).parent.parent
_archive_dir = _base_dir / "archive"
if _archive_dir.exists():
    sys.path.insert(0, str(_archive_dir))
from supabase_migration import SupabaseMigrator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main migration script."""
    parser = argparse.ArgumentParser(description='Run Supabase migration')
    parser.add_argument('--dry-run', action='store_true', help='Only validate and transform, do not insert')
    parser.add_argument('--skip-validation', action='store_true', help='Skip data validation')
    parser.add_argument('--use-service-key', action='store_true', default=True, help='Use service role key (bypasses RLS, default: True)')
    args = parser.parse_args()
    
    try:
        logger.info("Initializing Supabase migrator...")
        migrator = SupabaseMigrator(use_service_key=args.use_service_key)
        
        if args.dry_run:
            logger.info("\n" + "=" * 60)
            logger.info("DRY RUN MODE - No data will be inserted")
            logger.info("=" * 60)
        
        # Run migration
        results = migrator.migrate_all(
            validate=not args.skip_validation,
            dry_run=args.dry_run
        )
        
        logger.info("\n" + "=" * 60)
        logger.info("Migration Results:")
        logger.info("=" * 60)
        for key, value in results.items():
            logger.info(f"  {key}: {value}")
        
        if args.dry_run:
            logger.info("\n" + "=" * 60)
            logger.info("Dry run completed. Run without --dry-run to perform actual migration.")
            logger.info("=" * 60)
        else:
            logger.info("\n" + "=" * 60)
            logger.info("✅ Migration completed successfully!")
            logger.info("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error(f"\n❌ Migration failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
