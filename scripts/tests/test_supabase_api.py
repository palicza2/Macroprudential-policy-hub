"""
Supabase REST API Tesztelés

Ez a script teszteli a Supabase REST API-t különböző táblák lekérdezésével.
"""

import os
import sys
from typing import Dict, Any, List
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from supabase import create_client, Client
from supabase_migration.config import SupabaseConfig


def test_connection(supabase: Client) -> bool:
    """Teszteli a Supabase kapcsolatot."""
    print("=" * 60)
    print("1. Kapcsolat tesztelése...")
    print("=" * 60)
    
    try:
        # Egyszerű lekérdezés a countries táblából
        response = supabase.table("countries").select("iso2, country_name").limit(1).execute()
        
        if response.data:
            print(f"OK: Kapcsolat sikeres!")
            print(f"   Első ország: {response.data[0]}")
            return True
        else:
            print("WARNING: Kapcsolat sikeres, de nincs adat.")
            return True
    except Exception as e:
        print(f"ERROR: Kapcsolat hiba: {e}")
        return False


def test_ccyb_decisions(supabase: Client) -> None:
    """Teszteli a CCyB decisions táblát."""
    print("\n" + "=" * 60)
    print("2. CCyB Decisions tesztelése...")
    print("=" * 60)
    
    try:
        # Magyarország legfrissebb CCyB döntései
        response = supabase.table("ccyb_decisions") \
            .select("*") \
            .eq("country_iso2", "HU") \
            .order("effective_date", desc=True) \
            .limit(3) \
            .execute()
        
        print(f"OK: {len(response.data)} rekord található Magyarországról:")
        for record in response.data:
            print(f"   - {record.get('effective_date')}: {record.get('rate')}% "
                  f"(Status: {record.get('status')})")
        
        # Összesített statisztika
        all_response = supabase.table("ccyb_decisions") \
            .select("country_iso2", count="exact") \
            .execute()
        
        print(f"\n   Összesen {all_response.count} CCyB döntés az adatbázisban.")
        
    except Exception as e:
        print(f"ERROR: Hiba: {e}")


def test_syrb_measures(supabase: Client) -> None:
    """Teszteli a SyRB measures táblát."""
    print("\n" + "=" * 60)
    print("3. SyRB Measures tesztelése...")
    print("=" * 60)
    
    try:
        # Aktív SyRB intézkedések
        response = supabase.table("syrb_measures") \
            .select("*") \
            .eq("status", "Active") \
            .limit(5) \
            .execute()
        
        print(f"OK: {len(response.data)} aktív SyRB intézkedés:")
        for record in response.data:
            print(f"   - {record.get('country_iso2')}: {record.get('measure_type')} "
                  f"({record.get('rate')}%)")
        
    except Exception as e:
        print(f"ERROR: Hiba: {e}")


def test_bbm_measures(supabase: Client) -> None:
    """Teszteli a BBM measures táblát."""
    print("\n" + "=" * 60)
    print("4. BBM Measures tesztelése...")
    print("=" * 60)
    
    try:
        # LTV intézkedések
        response = supabase.table("bbm_measures") \
            .select("*") \
            .eq("measure_type", "LTV") \
            .eq("status", "Active") \
            .limit(5) \
            .execute()
        
        print(f"OK: {len(response.data)} aktív LTV intézkedés:")
        for record in response.data:
            print(f"   - {record.get('country_iso2')}: {record.get('measure_short', 'N/A')}")
        
    except Exception as e:
        print(f"ERROR: Hiba: {e}")


def test_dti_lti_rules(supabase: Client) -> None:
    """Teszteli a DTI/LTI rules táblát."""
    print("\n" + "=" * 60)
    print("5. DTI/LTI Rules tesztelése...")
    print("=" * 60)
    
    try:
        # Összes DTI/LTI szabály
        response = supabase.table("dti_lti_rules") \
            .select("*") \
            .execute()
        
        print(f"OK: {len(response.data)} DTI/LTI szabály:")
        for record in response.data:
            limit = record.get('limit_standard', 'N/A')
            print(f"   - {record.get('country_iso2')}: {limit}x "
                  f"({record.get('legal_form')}, {record.get('implementation_status')})")
        
    except Exception as e:
        print(f"ERROR: Hiba: {e}")


def test_snapshots(supabase: Client) -> None:
    """Teszteli a snapshot táblákat."""
    print("\n" + "=" * 60)
    print("6. Snapshots tesztelése...")
    print("=" * 60)
    
    try:
        # CCyB snapshot
        ccyb_response = supabase.table("latest_ccyb_snapshot") \
            .select("*") \
            .eq("country_iso2", "HU") \
            .execute()
        
        if ccyb_response.data:
            record = ccyb_response.data[0]
            print(f"OK: Magyarország CCyB snapshot:")
            print(f"   - Rate: {record.get('rate')}%")
            print(f"   - Effective Date: {record.get('effective_date')}")
            print(f"   - Credit Gap: {record.get('credit_gap')}")
        
        # SyRB snapshot
        syrb_response = supabase.table("latest_syrb_snapshot") \
            .select("*") \
            .eq("country_iso2", "HU") \
            .execute()
        
        if syrb_response.data:
            record = syrb_response.data[0]
            print(f"\nOK: Magyarország SyRB snapshot:")
            print(f"   - General Rate: {record.get('general_rate')}%")
            print(f"   - Sectoral Rate: {record.get('sectoral_rate')}%")
            print(f"   - Total Rate: {record.get('total_rate')}%")
        
    except Exception as e:
        print(f"ERROR: Hiba: {e}")


def test_trends(supabase: Client) -> None:
    """Teszteli a trend táblákat."""
    print("\n" + "=" * 60)
    print("7. Trends tesztelése...")
    print("=" * 60)
    
    try:
        # CCyB trend - legfrissebb 5 nap
        response = supabase.table("ccyb_diffusion_trend") \
            .select("*") \
            .order("date", desc=True) \
            .limit(5) \
            .execute()
        
        print(f"OK: CCyB trend - legfrissebb 5 nap:")
        for record in response.data:
            print(f"   - {record.get('date')}: {record.get('countries_with_buffer')} ország, "
                  f"átlag: {record.get('avg_rate')}%")
        
        # BBM trend
        bbm_response = supabase.table("bbm_diffusion_trend") \
            .select("*") \
            .order("date", desc=True) \
            .limit(5) \
            .execute()
        
        print(f"\nOK: BBM trend - legfrissebb 5 nap:")
        for record in bbm_response.data:
            print(f"   - {record.get('date')}: {record.get('countries_with_bbm')} ország "
                  f"(LTV: {record.get('ltv_count')}, DTI/LTI: {record.get('dti_lti_count')})")
        
    except Exception as e:
        print(f"ERROR: Hiba: {e}")


def test_complex_queries(supabase: Client) -> None:
    """Teszteli összetett lekérdezéseket."""
    print("\n" + "=" * 60)
    print("8. Összetett lekérdezések tesztelése...")
    print("=" * 60)
    
    try:
        # JOIN példa: CCyB decisions + countries
        response = supabase.table("ccyb_decisions") \
            .select("*, countries(country_name, region)") \
            .eq("status", "Active") \
            .limit(3) \
            .execute()
        
        print(f"OK: JOIN teszt (CCyB + Countries):")
        for record in response.data:
            country_info = record.get('countries', {})
            print(f"   - {country_info.get('country_name', 'N/A')} "
                  f"({record.get('country_iso2')}): {record.get('rate')}%")
        
        # Aggregáció: országok száma CCyB-vel
        count_response = supabase.table("latest_ccyb_snapshot") \
            .select("country_iso2", count="exact") \
            .execute()
        
        print(f"\nOK: Összesen {count_response.count} ország van CCyB snapshot-ban.")
        
    except Exception as e:
        print(f"ERROR: Hiba: {e}")


def test_filtering_and_sorting(supabase: Client) -> None:
    """Teszteli a szűrést és rendezést."""
    print("\n" + "=" * 60)
    print("9. Szűrés és rendezés tesztelése...")
    print("=" * 60)
    
    try:
        # Több feltétel: aktív, magas rate
        response = supabase.table("ccyb_decisions") \
            .select("*") \
            .eq("status", "Active") \
            .gte("rate", 2.0) \
            .order("rate", desc=True) \
            .limit(5) \
            .execute()
        
        print(f"OK: Aktív CCyB döntések, rate >= 2.0% (csökkenő sorrendben):")
        for record in response.data:
            print(f"   - {record.get('country_iso2')}: {record.get('rate')}% "
                  f"({record.get('effective_date')})")
        
    except Exception as e:
        print(f"ERROR: Hiba: {e}")


def main():
    """Fő függvény."""
    print("\n" + "=" * 60)
    print("Supabase REST API Tesztelés")
    print("=" * 60)
    print(f"Dátum: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Supabase client inicializálása
    try:
        config = SupabaseConfig()
        # Használjuk az anon key-t a REST API olvasáshoz
        supabase = create_client(config.url, config.anon_key)
        print(f"\nSupabase URL: {config.url}")
        print(f"API Key típus: Anon (read-only)")
    except Exception as e:
        print(f"ERROR: Supabase inicializálási hiba: {e}")
        print("\nEllenőrizd a környezeti változókat:")
        print("  - SUPABASE_URL")
        print("  - SUPABASE_KEY")
        return
    
    # Tesztek futtatása
    if not test_connection(supabase):
        print("\nERROR: A kapcsolat teszt sikertelen. További tesztek kihagyva.")
        return
    
    test_ccyb_decisions(supabase)
    test_syrb_measures(supabase)
    test_bbm_measures(supabase)
    test_dti_lti_rules(supabase)
    test_snapshots(supabase)
    test_trends(supabase)
    test_complex_queries(supabase)
    test_filtering_and_sorting(supabase)
    
    print("\n" + "=" * 60)
    print("OK: Összes teszt befejezve!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
