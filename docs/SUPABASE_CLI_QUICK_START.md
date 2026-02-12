# Supabase CLI Gyors Útmutató

## ✅ Telepítés

A Supabase CLI-t **npx**-en keresztül használjuk (nincs szükség globális telepítésre):

```bash
npx supabase --version
```

✅ **Már telepítve és működik!** (verzió: 2.76.8)

---

## 🚀 Gyors Kezdés

### 1. Projekt Linkelése

Linkeld a lokális projektet a Supabase projektedhez:

```bash
npx supabase link --project-ref [PROJECT_REF]
```

A `PROJECT_REF` megtalálható:
- Supabase Dashboard URL: `https://[PROJECT_REF].supabase.co`
- Vagy a `.env` fájlban: `SUPABASE_URL` változóból

**Példa:**
```bash
npx supabase link --project-ref abcdefghijklmnop
```

### 2. Migration Státusz Ellenőrzése

```bash
npx supabase migration list
```

### 3. Migration Futtatás

```bash
# Összes új migration futtatása
npx supabase db push

# Vagy használd a Python scriptet
python scripts/run_migrations_cli.py
```

---

## 📋 Gyakori Feladatok

### Migration Létrehozása

```bash
npx supabase migration new [migration_name]
```

Ez létrehoz egy új fájlt a `supabase/migrations/` könyvtárban.

### Schema Diff

```bash
# Lokális és remote schema összehasonlítása
npx supabase db diff

# Schema dump
npx supabase db dump
```

### Projekt Státusz

```bash
npx supabase status
```

---

## 🔧 Jelenlegi Migration Fájlok

A migration-ök már lefutottak:

✅ `migrations/010_create_materialized_views.sql`  
✅ `migrations/010_fix_syrb_snapshot.sql`  
✅ `migrations/011_add_foreign_keys_bbm.sql`

Ha a Supabase CLI-t szeretnéd használni a jövőben, akkor:

1. **Vagy** hozd létre a `supabase/` könyvtárat és mozgasd át a migration fájlokat
2. **Vagy** használd a jelenlegi `migrations/` könyvtárat és futtasd manuálisan a SQL fájlokat

---

## 📝 Hasznos Parancsok

```bash
# Verzió ellenőrzés
npx supabase --version

# Segítség
npx supabase --help

# Database reset (VIGYÁZAT: törli az adatokat!)
npx supabase db reset

# Migration státusz
npx supabase migration list
```

---

## 🔗 További Információk

- [Supabase CLI Dokumentáció](https://supabase.com/docs/reference/cli)
- [Migration Guide](https://supabase.com/docs/guides/cli/local-development#database-migrations)
- `docs/SUPABASE_CLI_SETUP.md` - Részletes dokumentáció
