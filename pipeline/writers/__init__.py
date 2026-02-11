"""
Pipeline Writers Module.
Contains writers for persisting data to external systems (e.g., Supabase).
"""

from pipeline.writers.supabase_writer import SupabaseWriter

__all__ = ['SupabaseWriter']
