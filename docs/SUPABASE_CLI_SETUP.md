# Supabase CLI Telepítés és Használat

## ✅ Telepítés

A Supabase CLI-t **npx**-en keresztül használjuk (nincs szükség globális telepítésre):

```bash
npx supabase --version
```

Ez automatikusan letölti és futtatja a legújabb Supabase CLI-t.

## 🚀 Használat

### 1. Supabase Projekt Linkelése

Először linkeld a lokális projektet a Supabase projektedhez:

```bash
npx supabase link --project-ref [PROJECT_REF]
```

A `PROJECT_REF` megtalálható a Supabase Dashboard URL-jében:
- `https://[PROJECT_REF].supabase.co`

Vagy használd a Supabase URL-t:

```bash
npx supabase link --project-ref [PROJECT_REF] --password [DATABASE_PASSWORD]
```

### 2. Migration Futtatás

A Supabase CLI-vel futtathatod a migration fájlokat:

```bash
# Összes migration futtatása
npx supabase db push

# Vagy egy konkrét migration fájl
npx supabase migration up [migration_name]
```

### 3. Migration Létrehozása

Új migration létrehozása:

```bash
npx supabase migration new [migration_name]
```

Ez létrehoz egy új fájlt a `supabase/migrations/` könyvtárban.

### 4. Lokális Fejlesztés

Ha lokális Supabase-t szeretnél futtatni:

```bash
# Lokális Supabase indítása (Docker szükséges)
npx supabase start

# Lokális Supabase leállítása
npx supabase stop
```

### 5. Schema Diff

Schema változások összehasonlítása:

```bash
# Lokális és remote schema összehasonlítása
npx supabase db diff

# Schema dump
npx supabase db dump
```

## 📁 Projekt Struktúra

A Supabase CLI egy `supabase/` könyvtárat hoz létre a projektben:

```
supabase/
├── config.toml          # Supabase konfiguráció
├── migrations/          # Migration fájlok
│   ├── 20240101000000_initial.sql
│   └── ...
└── seed.sql            # Seed adatok (opcionális)
```

## 🔧 Migration Fájlok Kezelése

A jelenlegi migration fájlok a `migrations/` könyvtárban vannak:

```
migrations/
├── 010_create_materialized_views.sql
├── 010_fix_syrb_snapshot.sql
├── 011_add_foreign_keys_bbm.sql
└── 012_drop_old_snapshot_trend_tables.sql
```

Ha a Supabase CLI-t szeretnéd használni, akkor ezeket a fájlokat át kell mozgatni a `supabase/migrations/` könyvtárba, vagy a Supabase CLI-t úgy konfigurálni, hogy a `migrations/` könyvtárat használja.

## 📝 Hasznos Parancsok

```bash
# Supabase CLI verzió ellenőrzése
npx supabase --version

# Segítség
npx supabase --help

# Projekt linkelés
npx supabase link --project-ref [PROJECT_REF]

# Migration státusz
npx supabase migration list

# Database reset (VIGYÁZAT: törli az adatokat!)
npx supabase db reset
```

## ⚠️ Fontos Megjegyzések

1. **npx használata**: Az `npx` minden futtatáskor letölti a Supabase CLI-t, ha nincs lokálisan. Ez lassabb lehet, de nem igényel telepítést.

2. **Globális telepítés**: Ha gyakran használod, érdemes lehet globálisan telepíteni Scoop-pal vagy Chocolatey-vel, de az npx is tökéletesen működik.

3. **Docker**: A lokális fejlesztéshez Docker szükséges. Ha csak a remote adatbázist használod, nincs rá szükség.

## 🔗 További Információk

- [Supabase CLI Dokumentáció](https://supabase.com/docs/reference/cli)
- [Supabase Migration Guide](https://supabase.com/docs/guides/cli/local-development#database-migrations)
