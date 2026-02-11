"""
Supabase Render Stage Tesztelés

Ez a script teszteli a Supabase-alapú render stage működését.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import SUPABASE_RENDER_CONFIG
from pipeline.stages.render_stage import RenderStage
from pipeline.orchestrator import PipelineOrchestrator


def test_config():
    """Teszteli a konfigurációt."""
    print("=" * 60)
    print("1. Konfiguráció tesztelése")
    print("=" * 60)
    
    print(f"USE_SUPABASE_FOR_RENDER: {SUPABASE_RENDER_CONFIG['enabled']}")
    print(f"SUPABASE_URL: {SUPABASE_RENDER_CONFIG['url'][:30] + '...' if SUPABASE_RENDER_CONFIG['url'] else 'not set'}")
    print(f"SUPABASE_KEY: {'set' if SUPABASE_RENDER_CONFIG['anon_key'] else 'not set'}")
    
    if SUPABASE_RENDER_CONFIG['enabled']:
        print("OK: Supabase render engedélyezve")
    else:
        print("INFO: Supabase render nincs engedélyezve (használja a pipeline adatokat)")
    
    print()


def test_render_stage_init():
    """Teszteli a RenderStage inicializálását."""
    print("=" * 60)
    print("2. Render Stage inicializálás tesztelése")
    print("=" * 60)
    
    try:
        from config import BASE_DIR, REPORTS_DIR, NEWS_CONFIG
        
        # Teszt 1: Supabase nélkül
        print("Teszt 1: Supabase nélkül...")
        render_stage_no_supabase = RenderStage(
            BASE_DIR,
            REPORTS_DIR,
            NEWS_CONFIG,
            use_supabase=False
        )
        print(f"OK: RenderStage inicializálva (use_supabase=False)")
        print(f"   Supabase client: {render_stage_no_supabase.supabase_client}")
        
        # Teszt 2: Supabase-pel (ha engedélyezve van)
        if SUPABASE_RENDER_CONFIG['enabled']:
            print("\nTeszt 2: Supabase-pel...")
            render_stage_with_supabase = RenderStage(
                BASE_DIR,
                REPORTS_DIR,
                NEWS_CONFIG,
                use_supabase=True,
                supabase_config=SUPABASE_RENDER_CONFIG
            )
            print(f"OK: RenderStage inicializálva (use_supabase=True)")
            print(f"   Supabase client: {'initialized' if render_stage_with_supabase.supabase_client else 'None'}")
        else:
            print("\nTeszt 2: Kihagyva (USE_SUPABASE_FOR_RENDER=false)")
        
        print()
        return True
        
    except Exception as e:
        print(f"ERROR: Hiba a RenderStage inicializálásakor: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_supabase_fetch():
    """Teszteli a Supabase adatok lekérdezését."""
    print("=" * 60)
    print("3. Supabase adatok lekérdezés tesztelése")
    print("=" * 60)
    
    if not SUPABASE_RENDER_CONFIG['enabled']:
        print("INFO: Kihagyva (USE_SUPABASE_FOR_RENDER=false)")
        print()
        return True
    
    try:
        from config import BASE_DIR, REPORTS_DIR, NEWS_CONFIG
        
        render_stage = RenderStage(
            BASE_DIR,
            REPORTS_DIR,
            NEWS_CONFIG,
            use_supabase=True,
            supabase_config=SUPABASE_RENDER_CONFIG
        )
        
        if not render_stage.supabase_client:
            print("ERROR: Supabase client nincs inicializálva")
            return False
        
        print("Lekérdezés Supabase-ből...")
        countries_data = render_stage._fetch_countries_data_from_supabase()
        
        if countries_data:
            print(f"OK: {len(countries_data)} ország lekérdezve")
            
            # Néhány ország ellenőrzése
            sample_countries = list(countries_data.keys())[:3]
            for country in sample_countries:
                profile = countries_data[country]
                print(f"\n   {country} ({profile.get('iso2', 'N/A')}):")
                print(f"      CCyB: {profile.get('current_status', {}).get('ccyb', {}).get('rate', 'N/A')}%")
                print(f"      SyRB: {profile.get('current_status', {}).get('syrb', {}).get('rate', 'N/A')}%")
                print(f"      BBM: {len(profile.get('current_status', {}).get('bbm', []))} típus")
                print(f"      CCyB history: {len(profile.get('historical_evolution', {}).get('ccyb', []))} rekord")
        else:
            print("WARNING: Üres adatok érkeztek Supabase-ből")
        
        print()
        return True
        
    except Exception as e:
        print(f"ERROR: Hiba a Supabase lekérdezéskor: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_orchestrator_init():
    """Teszteli az Orchestrator inicializálását."""
    print("=" * 60)
    print("4. Orchestrator inicializálás tesztelése")
    print("=" * 60)
    
    try:
        orchestrator = PipelineOrchestrator()
        print("OK: PipelineOrchestrator inicializálva")
        print(f"   Render stage use_supabase: {orchestrator.render_stage.use_supabase}")
        print()
        return True
        
    except Exception as e:
        print(f"ERROR: Hiba az Orchestrator inicializálásakor: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Fő függvény."""
    print("\n" + "=" * 60)
    print("Supabase Render Stage Tesztelés")
    print("=" * 60)
    print()
    
    results = []
    
    # 1. Konfiguráció tesztelése
    test_config()
    
    # 2. Render Stage inicializálás
    results.append(("Render Stage Init", test_render_stage_init()))
    
    # 3. Supabase adatok lekérdezése
    results.append(("Supabase Fetch", test_supabase_fetch()))
    
    # 4. Orchestrator inicializálás
    results.append(("Orchestrator Init", test_orchestrator_init()))
    
    # Összefoglaló
    print("=" * 60)
    print("Összefoglaló")
    print("=" * 60)
    
    for test_name, result in results:
        status = "OK" if result else "ERROR"
        print(f"{status}: {test_name}")
    
    all_passed = all(result for _, result in results)
    print()
    if all_passed:
        print("OK: Minden teszt sikeres!")
    else:
        print("ERROR: Néhány teszt sikertelen")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
