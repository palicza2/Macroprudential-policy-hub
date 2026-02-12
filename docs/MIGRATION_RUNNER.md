# Migration Runner Script

## Automatikus Migration Futtatás

A `scripts/run_migrations.py` script automatikusan futtatja a Supabase migration fájlokat.

### Előfeltételek

1. **psycopg2 telepítése:**
   ```bash
   pip install psycopg2-binary
   ```

2. **Environment változók beállítása (.env fájlban):**

   **Opció 1: PostgreSQL connection string (ajánlott)**
   ```env
   SUPABASE_DB_URL=postgresql://postgres:[PASSWORD]@db.[PROJECT_REF].supabase.co:5432/postgres
   ```
   
   A connection string megtalálható a Supabase Dashboard-ban:
   - Settings → Database → Connection string → Connection pooling
   - Vagy: Settings → Database → Connection string → Direct connection

   **Opció 2: Supabase URL + Service Role Key + Database Password**
   ```env
   SUPABASE_URL=https://[PROJECT_REF].supabase.co
   SUPABASE_SERVICE_ROLE_KEY=[SERVICE_ROLE_KEY]
   SUPABASE_DB_PASSWORD=[DATABASE_PASSWORD]
   ```

### Futtatás

```bash
python scripts/run_migrations.py
```

A script:
1. Összekapcsolódik a Supabase PostgreSQL adatbázissal
2. Megkeresi a migration fájlokat (`010_*`, `011_*`, `012_*`)
3. Sorrendben futtatja őket
4. Ha valamelyik hibázik, megáll és rollback-et csinál

### Migration Fájlok Sorrendje

1. `010_create_materialized_views.sql` - Materialized View-k létrehozása
2. `010_fix_syrb_snapshot.sql` - SyRB snapshot javítás
3. `011_add_foreign_keys_bbm.sql` - Foreign key-k hozzáadása
4. `012_drop_old_snapshot_trend_tables.sql` - Régi táblák törlése (opcionális)

### Biztonsági Megjegyzések

⚠️ **Fontos:**
- A `.env` fájlt **NE** commit-old a git-be!
- A service role key teljes hozzáférést ad az adatbázishoz
- Használd csak biztonságos környezetben

### Alternatív: Manuális Futtatás

Ha nem szeretnéd az automatikus scriptet használni, futtasd manuálisan a Supabase SQL Editor-ben:

1. Nyisd meg a Supabase Dashboard-ot
2. Menj a **SQL Editor**-be
3. Másold be és futtasd a migration fájlokat sorrendben
