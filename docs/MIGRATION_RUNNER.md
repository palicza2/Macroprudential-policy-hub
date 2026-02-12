# Migration Runner Script

## Automatikus Migration Futtatás

Két módszer áll rendelkezésre a migration-ök futtatásához:

### 1. Supabase CLI (Ajánlott) 🚀

A `scripts/run_migrations_cli.py` script a Supabase CLI-t használja (`npx supabase`).

### 2. Direct PostgreSQL Connection

A `scripts/run_migrations.py` script közvetlenül PostgreSQL kapcsolaton keresztül futtatja a migration-öket.

## 1. Supabase CLI Módszer (Ajánlott)

### Előfeltételek

1. **Node.js és npm telepítve** (már megvan ✅)

2. **Supabase projekt linkelése:**
   ```bash
   npx supabase link --project-ref [PROJECT_REF]
   ```
   
   A `PROJECT_REF` megtalálható a Supabase Dashboard URL-jében:
   - `https://[PROJECT_REF].supabase.co`
   
   Vagy használd a scriptet:
   ```bash
   python scripts/run_migrations_cli.py --link --project-ref [PROJECT_REF]
   ```

### Futtatás

```bash
# Migration-ök futtatása
python scripts/run_migrations_cli.py

# Vagy közvetlenül a CLI-vel
npx supabase db push
```

### Előnyök

- ✅ Hivatalos Supabase módszer
- ✅ Automatikus migration tracking
- ✅ Könnyű használat
- ✅ Nincs szükség database password-re

---

## 2. Direct PostgreSQL Connection Módszer

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

### Előnyök

- ✅ Nincs szükség Supabase CLI-ra
- ✅ Közvetlen PostgreSQL kapcsolat
- ✅ Teljes kontroll a migration folyamat felett

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
