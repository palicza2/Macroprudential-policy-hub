"""
Main entry point for the macroprudential hub pipeline.
Uses stage-based architecture via PipelineOrchestrator.
"""

import logging
import os

from pipeline.orchestrator import main as orchestrator_main

logging.basicConfig(level=logging.INFO, format='%(message)s')
for noisy_lib in ['kaleido', 'urllib3', 'matplotlib', 'chromies', 'werkzeug']:
    logging.getLogger(noisy_lib).setLevel(logging.CRITICAL)

logger = logging.getLogger("MAIN")


def main():
    """Main entry point - delegates to orchestrator."""
    orchestrator_main()

if __name__ == "__main__":
    main()