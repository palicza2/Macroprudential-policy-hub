"""
Teljes Supabase Render Tesztelés

Ez a script teszteli a teljes pipeline-t Supabase-pel és anélkül.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_fallback_mode():
    """Teszt 1: Fallback mód (Supabase nélkül)"""
    print("=" * 60)
    print("TESZT 1: Fallback mód (USE_SUPABASE_FOR_RENDER=false)")
    print("=" * 60)
    
    # Töröljük a USE_SUPABASE_FOR_RENDER változót
    if "USE_SUPABASE_FOR_RENDER" in os.environ:
        del os.environ["USE_SUPABASE_FOR_RENDER"]
    
    # Újratöltjük a config-ot
    import importlib
    import config
    importlib.reload(config)
    
    from config import SUPABASE_RENDER_CONFIG
    from pipeline.stages.render_stage import RenderStage
    from config import BASE_DIR, REPORTS_DIR, NEWS_CONFIG
    
    print(f"USE_SUPABASE_FOR_RENDER: {SUPABASE_RENDER_CONFIG['enabled']}")
    
    render_stage = RenderStage(
        BASE_DIR,
        REPORTS_DIR,
        NEWS_CONFIG,
        use_supabase=SUPABASE_RENDER_CONFIG['enabled'],
        supabase_config=SUPABASE_RENDER_CONFIG
    )
    
    print(f"Render stage use_supabase: {render_stage.use_supabase}")
    print(f"Supabase client: {render_stage.supabase_client}")
    
    if not render_stage.use_supabase and render_stage.supabase_client is None:
        print("OK: Fallback mód működik (Supabase nincs használva)")
        return True
    else:
        print("ERROR: Fallback mód nem működik")
        return False


def test_supabase_mode():
    """Teszt 2: Supabase mód"""
    print("\n" + "=" * 60)
    print("TESZT 2: Supabase mód (USE_SUPABASE_FOR_RENDER=true)")
    print("=" * 60)
    
    # Beállítjuk a USE_SUPABASE_FOR_RENDER változót
    os.environ["USE_SUPABASE_FOR_RENDER"] = "true"
    
    # Újratöltjük a config-ot
    import importlib
    import config
    importlib.reload(config)
    
    from config import SUPABASE_RENDER_CONFIG
    from pipeline.stages.render_stage import RenderStage
    from config import BASE_DIR, REPORTS_DIR, NEWS_CONFIG
    
    print(f"USE_SUPABASE_FOR_RENDER: {SUPABASE_RENDER_CONFIG['enabled']}")
    
    render_stage = RenderStage(
        BASE_DIR,
        REPORTS_DIR,
        NEWS_CONFIG,
        use_supabase=SUPABASE_RENDER_CONFIG['enabled'],
        supabase_config=SUPABASE_RENDER_CONFIG
    )
    
    print(f"Render stage use_supabase: {render_stage.use_supabase}")
    print(f"Supabase client: {'initialized' if render_stage.supabase_client else 'None'}")
    
    if render_stage.use_supabase and render_stage.supabase_client:
        # Teszteljük az adatok lekérdezését
        print("\nAdatok lekérdezése Supabase-ből...")
        countries_data = render_stage._fetch_countries_data_from_supabase()
        
        if countries_data and len(countries_data) > 0:
            print(f"OK: {len(countries_data)} ország lekérdezve")
            print("OK: Supabase mód működik")
            return True
        else:
            print("WARNING: Üres adatok érkeztek Supabase-ből")
            return False
    else:
        print("ERROR: Supabase mód nem működik")
        return False


def test_template_variables():
    """Teszt 3: Template változók ellenőrzése"""
    print("\n" + "=" * 60)
    print("TESZT 3: Template változók ellenőrzése")
    print("=" * 60)
    
    # Beállítjuk a USE_SUPABASE_FOR_RENDER változót
    os.environ["USE_SUPABASE_FOR_RENDER"] = "true"
    
    # Újratöltjük a config-ot
    import importlib
    import config
    importlib.reload(config)
    
    from config import SUPABASE_RENDER_CONFIG
    
    print(f"SUPABASE_URL: {SUPABASE_RENDER_CONFIG['url'][:30] + '...' if SUPABASE_RENDER_CONFIG['url'] else 'not set'}")
    print(f"SUPABASE_KEY: {'set' if SUPABASE_RENDER_CONFIG['anon_key'] else 'not set'}")
    
    # Szimuláljuk a template renderelést
    supabase_url = SUPABASE_RENDER_CONFIG.get("url", "") if SUPABASE_RENDER_CONFIG['enabled'] else ""
    supabase_key = SUPABASE_RENDER_CONFIG.get("anon_key", "") if SUPABASE_RENDER_CONFIG['enabled'] else ""
    
    print(f"\nTemplate változók:")
    print(f"  supabase_url: {supabase_url[:30] + '...' if supabase_url else 'empty'}")
    print(f"  supabase_key: {'set' if supabase_key else 'empty'}")
    
    if SUPABASE_RENDER_CONFIG['enabled']:
        if supabase_url and supabase_key:
            print("OK: Template változók be vannak állítva")
            return True
        else:
            print("ERROR: Template változók hiányoznak")
            return False
    else:
        if not supabase_url and not supabase_key:
            print("OK: Template változók üresek (Supabase nincs engedélyezve)")
            return True
        else:
            print("ERROR: Template változók nem üresek (Supabase nincs engedélyezve)")
            return False


def main():
    """Fő függvény."""
    print("\n" + "=" * 60)
    print("Teljes Supabase Render Tesztelés")
    print("=" * 60)
    print()
    
    results = []
    
    # Teszt 1: Fallback mód
    results.append(("Fallback mód", test_fallback_mode()))
    
    # Teszt 2: Supabase mód
    results.append(("Supabase mód", test_supabase_mode()))
    
    # Teszt 3: Template változók
    results.append(("Template változók", test_template_variables()))
    
    # Összefoglaló
    print("\n" + "=" * 60)
    print("Összefoglaló")
    print("=" * 60)
    
    for test_name, result in results:
        status = "OK" if result else "ERROR"
        print(f"{status}: {test_name}")
    
    all_passed = all(result for _, result in results)
    print()
    if all_passed:
        print("OK: Minden teszt sikeres!")
        print("\nKövetkező lépések:")
        print("1. Futtasd a teljes pipeline-t: python main.py")
        print("2. Nyisd meg a generált index.html-t böngészőben")
        print("3. Teszteld a country profile betöltést Supabase-ből")
    else:
        print("ERROR: Néhány teszt sikertelen")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
