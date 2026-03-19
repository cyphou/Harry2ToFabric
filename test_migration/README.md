# Kit de test — Migration Harry2 → Fabric Lakehouse

## Objectif

Ce dossier contient un **jeu de test complet** simulant les tables IBM Harry2 (DB2 for i / AS/400) avec toutes les spécificités IBM i, pour valider chaque étape de la migration vers Microsoft Fabric Lakehouse.

## Contenu des fichiers

| # | Fichier | Rôle | Exécutable sur |
|---|---------|------|----------------|
| 01 | `01_harry2_source_ddl.sql` | Schéma source (DDL) simulant DB2 for i | PostgreSQL, SQL Server, tout SGBD |
| 02 | `02_harry2_sample_data.sql` | Données réalistes (édition française) | Idem |
| 03 | `03_bronze_ingestion.py` | Ingestion Bronze (PySpark) | Fabric Notebook, Databricks |
| 04 | `04_silver_transform.py` | Transformation Silver (CYYMMDD, EBCDIC, ISBN) | Fabric Notebook, Databricks |
| 05 | `05_gold_model_vorder.py` | Modèle étoile Gold + V-Order + Z-Order | Fabric Notebook, Databricks |
| 06 | `06_quality_gate.py` | Contrôles qualité automatisés (6 checks) | Fabric Notebook, Databricks |
| 07 | `07_extraction_queries_db2.sql` | Requêtes pour le vrai IBM i (production) | DB2 for i via ODBC |

## Scénario de test

### Étape 1 : Créer la source simulée (optionnel — test local)

```sql
-- Sur PostgreSQL ou SQL Server local
\i 01_harry2_source_ddl.sql
\i 02_harry2_sample_data.sql
```

### Étape 2 : Ingestion Bronze (Fabric Notebook)

Coller le contenu de `03_bronze_ingestion.py` dans un Fabric Notebook.
Il crée directement les tables Bronze depuis des données en mémoire (pas besoin d'ODBC pour le test).

### Étape 3 : Transformation Silver

Exécuter `04_silver_transform.py` dans un 2e notebook.
Vérifie automatiquement :
- Conversion dates CYYMMDD → ISO 8601
- Trim des champs CHAR à longueur fixe
- Validation ISBN-13
- Préservation des accents (é, è, ç, García Márquez...)

### Étape 4 : Modèle Gold + V-Order

Exécuter `05_gold_model_vorder.py`.
Construit le star schema et applique OPTIMIZE ZORDER.

### Étape 5 : Quality Gate

Exécuter `06_quality_gate.py`.
Rapport automatique couvrant 6 familles de checks.

## Spécificités IBM i simulées

| Spécificité | Comment c'est simulé |
|-------------|---------------------|
| **Dates CYYMMDD** | `1250315` = 2025-03-15 (C=1→20xx), `0780301` = 1978-03-01 (C=0→19xx) |
| **Packed decimal** | Colonnes DECIMAL(9,2) comme OVPXHT (prix HT) |
| **Noms courts** | OUVRGP, AUTURP, VENTEP... (max 10 car. style RPG) |
| **CHAR fixe** | Espaces trailing sur tous les champs CHAR |
| **EBCDIC CCSID 297** | Accents français (é,è,ê,ç,à,ù) + espagnols (á,í) pour test |
| **FK implicites** | Pas de FOREIGN KEY déclarées — liens via code dans RPG |
| **ISBN invalide** | `9782070000000` volontairement faux pour tester le Quality Gate |
| **Table BLU** | VENTEP simulée comme table column-organized |

## Données de test

- **10 ouvrages** (dont 1 ISBN invalide) — auteurs français réels
- **10 auteurs** — Nothomb, Houellebecq, Musso, Lévy, García Márquez...
- **8 éditeurs** — Grasset, Fayard, Gallimard, Flammarion...
- **25 lignes de ventes** — dont retours et date ancienne (2010)
- **6 commandes** — divers statuts (livrée, partielle, en cours)
- **11 positions de stock** — multi-entrepôts

## Résultats attendus du Quality Gate

| Check | Résultat attendu |
|-------|-----------------|
| Complétude | ✅ PASS (sauf ISBN invalide filtré en Gold) |
| ISBN-13 | ⚠️ WARN — 1 ISBN invalide sur 9 (~11%) |
| Dates CYYMMDD | ✅ PASS — y compris L'étranger 1978 (C=0) |
| Montants | ✅ PASS — équilibre Bronze = Silver |
| Accents EBCDIC | ✅ PASS — é, è, á, í préservés |
| Intégrité FK | ✅ PASS — tous les liens résolus |
