"""
Output utilities.
Suppresses stdout/stderr for noisy libraries.
"""

import os
import sys
from contextlib import contextmanager


@contextmanager
def SuppressOutput():
    """
    Operációs rendszer szintű kimenet elnémítása (stdout és stderr).
    Ez elkapja a C-szintű könyvtárak (pl. Kaleido/Chromium) zaját is.
    """
    # 1. Mentjük az eredeti csatornákat
    original_stdout_fd = sys.stdout.fileno()
    original_stderr_fd = sys.stderr.fileno()

    saved_stdout_fd = os.dup(original_stdout_fd)
    saved_stderr_fd = os.dup(original_stderr_fd)

    # 2. Megnyitjuk a "semmit" (null device)
    devnull = os.open(os.devnull, os.O_RDWR)

    try:
        # 3. Átirányítjuk a kimeneteket a null-ba
        sys.stdout.flush()
        sys.stderr.flush()
        os.dup2(devnull, original_stdout_fd)
        os.dup2(devnull, original_stderr_fd)
        yield
    finally:
        # 4. Visszaállítjuk az eredeti állapotot
        os.dup2(saved_stdout_fd, original_stdout_fd)
        os.dup2(saved_stderr_fd, original_stderr_fd)
        
        # 5. Takarítás
        os.close(saved_stdout_fd)
        os.close(saved_stderr_fd)
        os.close(devnull)
