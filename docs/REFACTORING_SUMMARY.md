# 🔧 Projekt Refaktorálási Összefoglaló

## 📊 Főbb Problémák

### 1. Duplikáció és Elavult Fájlok

- ✅ **`bbm.py` vs `bbm/` mappa** - Circular import problémák
- ✅ **`utils.py` vs `utils/` mappa** - Kód duplikáció
- ✅ **43 markdown fájl** a `docs/` mappában - Túlsúly
- ✅ **Sok test script** a `scripts/` mappában - Csoportosítás szükséges

### 2. Használatban Lévő, de Dokumentálatlan Modulok

- ⚠️ **`supabase_migration/`** - Használatban van (`supabase_writer.py`), de:
  - Különbözik a `migrations/` SQL fájloktól
  - Python-based data migration vs SQL schema migration
  - **Javaslat:** Dokumentálás vagy konszolidálás

---

## 🎯 Prioritások

### ⭐⭐⭐ KRITIKUS (Azonnal)

1. **`bbm.py` refaktorálás** - Circular import problémák megoldása
2. **`utils.py` refaktorálás** - Modulokra bontás

### ⭐⭐ FONTOS (Rövid távon)

3. **Dokumentáció konszolidálás** - 43 fájl → csoportosítás
4. **Test scriptek csoportosítása** - `scripts/tests/` mappába

### ⭐ KÖZEPES (Hosszú távon)

5. **Elavult fájlok archiválása** - `run_all.R`, `output/` (ha elavult)

---

## 📋 Részletes Terv

Lásd: `docs/REFACTORING_PLAN.md`

---

## 🚀 Gyors Kezdés

### 1. Elavult Fájlok Archiválása (30 perc)

```bash
# Hozz létre archive mappát
mkdir archive

# Mozgasd az elavult fájlokat
# (ellenőrizd előtte, hogy valóban elavultak-e)
```

### 2. `bbm.py` Refaktorálás (2-3 óra)

- Mozgasd át a függvényeket a `bbm/` mappába
- Frissítsd az importokat
- Teszteld

### 3. `utils.py` Refaktorálás (1-2 óra)

- Mozgasd át a függvényeket a `utils/` mappába
- Frissítsd az importokat
- Teszteld

---

## ⚠️ Figyelmeztetések

1. **Backward Compatibility:** Tartsd meg deprecated-ként vagy hozz létre `__init__.py` fájlokat
2. **Tesztelés:** Minden refaktorálás után futtasd a teljes pipeline-t
3. **Git History:** Használj `git mv` parancsokat
