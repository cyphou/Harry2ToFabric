<div align="center">

# 📦 Migration IBM Harry / Harry2 → Microsoft Fabric Lakehouse

![IBM i](https://img.shields.io/badge/Source-IBM%20i%20%2F%20AS400-054ADA?style=for-the-badge&logo=ibm&logoColor=white)
![DB2](https://img.shields.io/badge/DB2%20for%20i-Column%20BLU-054ADA?style=for-the-badge&logo=ibm&logoColor=white)
![Fabric](https://img.shields.io/badge/Cible-Microsoft%20Fabric-742774?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Delta](https://img.shields.io/badge/Format-Delta%20Lake%20%2B%20V--Order-00ADD8?style=for-the-badge&logo=databricks&logoColor=white)
![Status](https://img.shields.io/badge/Statut-Étude%20de%20faisabilité-orange?style=for-the-badge)

</div>

> 📅 **Date** : 18 mars 2026
> 👤 **Auteur** : Équipe Data & Analytics
> 🏷️ **Version** : 2.0 — avec kit de test, multi-agent et gains Copilot

---

## 📑 Table des matières

| # | Section | Description |
|:-:|---------|-------------|
| 1 | [🏢 Contexte Harry2](#-1-contexte--ibm-harry--harry2) | Présentation du système source IBM i |
| 2 | [🎯 Pourquoi Fabric](#-2-pourquoi-migrer-vers-fabric-lakehouse-) | Justification business et technique |
| 3 | [🏗️ Architecture Medallion](#️-3-architecture-cible--medallion-bronze--silver--gold) | Bronze / Silver / Gold |
| 4 | [🔌 Extraction](#-4-stratégie-dextraction-des-données) | ODBC, CSV, Parquet, Shortcuts |
| 5 | [⚠️ Risques techniques](#️-5-points-dattention-techniques) | EBCDIC, packed decimal, CYYMMDD |
| 6 | [📊 Périmètre données](#-6-estimation-du-périmètre-données) | Volumes par domaine |
| 6bis | [🧊 BLU → V-Order](#-6bis-stockage-vectoriel--colonnes--de-harry2-blu-à-fabric-v-order) | Migration du stockage colonnes |
| 7 | [📅 Planning 8 semaines](#-7-planning-macro-accéléré--8-semaines) | Gantt accéléré |
| 7bis | [🧮 Complexité](#-7bis-analyse-de-complexité-de-la-migration) | Score 3.3/5, matrice de risques |
| 7ter | [🤖 Gains Copilot](#-7ter-gains-via-copilot--outils-ia) | −35 % effort, −25 % planning |
| 8 | [🧪 Kit de test](#-8-kit-de-test-de-migration) | 7 fichiers SQL + PySpark prêts |
| 8bis | [✅ Prochaines étapes](#-8bis-prochaines-étapes) | Checklist d'actions |
| 9 | [🤖 Multi-Agent](#-9-plan-de-migration-multi-agent) | 6 agents spécialisés + orchestrateur |
| A | [📚 Annexes](#-annexes) | Ressources Fabric, IBM i, Multi-Agent |

---

## 🏢 1. Contexte : IBM Harry / Harry2

### 1.1 Qu'est-ce que Harry2 ?

> 📖 **Harry** et **Harry2** sont des systèmes de gestion éditoriale (ERP éditorial) utilisés historiquement par de grands groupes d'édition français.

Ce sont des applications IBM mainframe/midrange qui gèrent :

- 📚 Le **catalogue des ouvrages** (titres, ISBN, auteurs, collections, séries)
- 📝 Les **contrats auteurs** et les droits
- 📦 La **gestion commerciale** (commandes, stocks, distribution)
- 🏭 Les **flux de production** éditoriaux (manuscrits → publication)
- 🌐 Les **métadonnées ONIX** pour la diffusion
- 💰 La **comptabilité des droits d'auteur** (royalties)

### 1.2 Stack technique Harry2

| Composant | Technologie typique |
|-----------|-------------------|
| 🖥️ Serveur | IBM i (AS/400) ou mainframe |
| 🗄️ Base de données | DB2 for i / DB2 z/OS / Oracle |
| 🔠 Encodage | EBCDIC (IBM i) ou Latin-1 |
| 🔗 Interfaces | Fichiers plats, ONIX XML, écrans 5250 |
| 🌐 Protocoles | SNA, ODBC iSeries Access, FTP |

---

## 🎯 2. Pourquoi migrer vers Fabric Lakehouse ?

### 2.1 Limites du système actuel

- ❌ Compétences IBM i en déclin sur le marché
- ❌ Interfaces utilisateur obsolètes (écrans verts 5250)
- ❌ Difficulté d'intégration avec les outils modernes (BI, Data Science, IA)
- ❌ Coûts de maintenance élevés
- ❌ Pas de capacité de vectorisation / recherche sémantique native

### 2.2 Avantages du Lakehouse Fabric

| Critère | Lakehouse Fabric |
|---------|:----------------:|
| Données structurées ET semi-structurées | ✅ Delta Lake supporte les deux |
| Métadonnées ONIX (XML) | ✅ Stockage natif dans Files + parsing Spark |
| Volume historique important | ✅ Scalabilité élastique OneLake |
| Requêtes SQL pour le reporting | ✅ SQL analytics endpoint automatique |
| Data Science / IA / Vectorisation | ✅ Spark notebooks intégrés |
| Power BI | ✅ Intégration directe, semantic model auto-généré |
| Coût | ✅ Pay-as-you-go, pas de licence mainframe |

---

## 🏗️ 3. Architecture cible : Medallion (Bronze / Silver / Gold)

```
┌─────────────────────────────────────────────────────────────────┐
│                      FABRIC WORKSPACE                           │
│                                                                 │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐            │
│  │   BRONZE    │───▶│   SILVER    │───▶│    GOLD     │           │
│  │  (Raw/Brut) │    │ (Nettoyé)  │    │ (Métier)   │           │
│  └────────────┘    └────────────┘    └────────────┘            │
│       ▲                                      │                  │
│       │                                      ▼                  │
│  Data Pipeline                          Power BI                │
│  (Copy Activity)                     Semantic Model             │
│       ▲                                      │                  │
│       │                                      ▼                  │
│  On-Premises                           Rapports &               │
│  Data Gateway                          Dashboards               │
└───────┼─────────────────────────────────────────────────────────┘
        │
   ┌────┴─────────┐
   │   Harry2      │
   │   IBM i       │
   │   DB2/Oracle  │
   └──────────────┘
```

### 3.1 Couche Bronze (Raw)

> 🟥 Tables brutes extraites telles quelles de Harry2. Aucune transformation, conservation de l'historique complet.

**Tables attendues :**
- 🟧 `bronze_ouvrages` — Catalogue complet des ouvrages
- 🟧 `bronze_auteurs` — Référentiel auteurs
- 🟧 `bronze_contrats` — Contrats et droits
- 🟧 `bronze_commandes` — Historique des commandes
- 🟧 `bronze_stocks` — Positions de stock
- 🟧 `bronze_ventes` — Données de ventes
- 🟧 `bronze_editeurs` — Maisons d'édition / imprints
- 🟧 `bronze_collections` — Collections éditoriales
- 🟧 `bronze_onix_metadata` — Métadonnées ONIX parsées

### 3.2 Couche Silver (Cleaned)

> ⚪ Données nettoyées, typées, dédupliquées, avec encodage UTF-8 uniforme.

**Transformations :**
- 🔄 Conversion EBCDIC → UTF-8
- ✂️ Trim des champs à longueur fixe
- 📆 Parsing des dates (formats IBM `yyyyMMdd`, `CYYMMDD`)
- 🔍 Résolution des codes métier via tables de lookup
- 🧹 Déduplication par clés naturelles (ISBN, code auteur, etc.)
- ✅ Validation des ISBN-13 (checksum)

### 3.3 Couche Gold (Business)

> 🟡 Modèle dimensionnel optimisé pour le reporting et l'analyse.

**Tables dimensionnelles :**

| Table | Type | Description |
|-------|:----:|-------------|
| `dim_ouvrages` | 🟦 Dim | Catalogue (titre, ISBN, collection, format, prix, statut) |
| `dim_auteurs` | 🟦 Dim | Référentiel auteurs avec infos contrats |
| `dim_editeurs` | 🟦 Dim | Maisons d'édition, imprints, marques |
| `dim_collections` | 🟦 Dim | Collections et séries |
| `dim_dates` | 🟦 Dim | Calendrier éditorial |
| `dim_entrepots` | 🟦 Dim | Sites de stockage / distribution |
| `fact_ventes` | 🟥 Fait | Volumes et CA par ouvrage/jour/canal |
| `fact_droits` | 🟥 Fait | Comptabilité des droits d'auteur |
| `fact_stocks` | 🟥 Fait | Position de stock quotidienne |
| `fact_commandes` | 🟥 Fait | Détail des commandes / livraisons |

---

## 🔌 4. Stratégie d'extraction des données

### 4.1 Options de connectivité

| Source Harry2 | Connecteur Fabric | Mode | Prérequis |
|---------------|-------------------|------|-----------|
| DB2 sur IBM i | ODBC + On-Premises Gateway | Copy Activity | IBM i Access ODBC driver + Gateway |
| Oracle | Oracle connector natif | Copy Activity | Oracle client + Gateway |
| Export CSV sur SFTP | SFTP connector | Copy Activity | Accès SFTP au serveur |
| Fichiers sur ADLS Gen2 | Shortcut OneLake | Direct (pas de copie) | Dépôt préalable sur ADLS |
| API REST (si dispo) | REST connector | Copy Activity / Dataflow Gen2 | Endpoint API documenté |
| Export Parquet depuis ETL | Upload direct Lakehouse | Manual / Pipeline | Script d'export côté IBM i |

### 4.2 Recommandation

**Option privilégiée** : Export CSV/Parquet depuis IBM i → ADLS Gen2 → Shortcut OneLake

Cette approche :
- Minimise la charge sur le système Harry2 en production
- Permet un contrôle total sur le format d'export
- Utilise les raccourcis OneLake (pas de duplication de données)
- Peut être automatisée via un job IBM i + Azure Data Factory Pipeline

**Option alternative** : Connecteur ODBC direct si la latence réseau et la charge sont acceptables.

### 4.3 Extraction incrémentale

Si Harry2 dispose de colonnes de type `date_modification` ou `timestamp` :

```sql
-- Extraction incrémentale DB2
SELECT * FROM BIBLIO.OUVRAGES
WHERE DATE_MAJ > :dernier_watermark
ORDER BY DATE_MAJ
```

Sinon, envisager un **Change Data Capture (CDC)** via les journaux IBM i (journal receivers) ou une extraction full périodique avec détection de delta côté Spark.

---

## ⚠️ 5. Points d'attention techniques

| Risque | Impact | Mitigation |
|--------|:------:|-----------|
| 🔠 **Encodage EBCDIC** | 🔴 Caractères corrompus (accents) | Conversion explicite EBCDIC → UTF-8 à l'extraction |
| 💾 **Packed decimal (COMP-3)** | 🔴 Valeurs numériques illisibles | Mapping ODBC correct ou conversion côté IBM i |
| ✂️ **Champs à longueur fixe** | 🟡 Espaces parasites dans les données | `TRIM()` systématique en couche Silver |
| 🔑 **Codes métier cryptiques** | 🟡 Données incompréhensibles sans context | Extraire toutes les tables de codes/lookup en parallèle |
| 📆 **Format date IBM** (`CYYMMDD`) | 🔴 Dates mal interprétées | Parsing spécifique : C=siècle (0=19xx, 1=20xx) |
| 📦 **Volumes historiques** | 🟡 Extraction longue, timeout | Extraction par tranches (par année, par éditeur) |
| 📄 **ONIX XML volumineux** | 🟡 Parsing lent | Spark XML (`com.databricks:spark-xml`) en mode distribué |
| 🇫🇷 **Accents français** | 🔴 Perte de caractères spéciaux | Vérifier encodage à chaque étape (Latin-1 → UTF-8) |
| 📚 **ISBN doubles formats** | 🟠 ISBN-10 vs ISBN-13 mélangés | Normaliser en ISBN-13 systématiquement |

---

## 📊 6. Estimation du périmètre données

> **À compléter lors de l'inventaire Harry2**

| Domaine | Tables estimées | Volume estimé | Fréquence MAJ |
|---------|----------------|---------------|----------------|
| Catalogue ouvrages | 5-10 | 500K - 2M lignes | Quotidien |
| Auteurs / Contrats | 3-5 | 50K - 200K lignes | Hebdomadaire |
| Commandes / Ventes | 5-8 | 10M - 50M lignes | Quotidien |
| Stocks | 2-3 | 1M - 5M lignes | Quotidien |
| Métadonnées ONIX | 1-2 | 500K - 2M fichiers XML | Sur événement |
| Codes / Référentiels | 10-20 | 1K - 50K lignes | Mensuel |

---

## 🧊 6bis. Stockage vectoriel / colonnes : de Harry2 BLU à Fabric V-Order

> <div align="center">
>
> ![BLU](https://img.shields.io/badge/DB2%20BLU-Column%20Organized-054ADA?style=for-the-badge&logo=ibm&logoColor=white)
> ➡️
> ![V-Order](https://img.shields.io/badge/Fabric-V--Order%20%2B%20Z--Order-742774?style=for-the-badge&logo=microsoftazure&logoColor=white)
>
> </div>

### 6bis.1 Le stockage vectoriel dans Harry2 (DB2 BLU Acceleration)

Harry2, lorsqu'il s'appuie sur **DB2 LUW** (Linux/Unix/Windows), peut exploiter la technologie **IBM BLU Acceleration** introduite avec DB2 10.5 (2013). Cette technologie repose sur 4 piliers :

| Pilier BLU | Description |
|-----------|-------------|
| **Stockage orienté colonnes** | Les données sont stockées par colonne (column-organized tables) plutôt que par ligne. Chaque colonne est compressée indépendamment, ce qui améliore drastiquement la compression et les performances de scan analytique. |
| **Traitement vectorisé (SIMD)** | Les instructions processeur SIMD (Single Instruction Multiple Data) traitent plusieurs valeurs d'une colonne en une seule opération CPU. C'est le "vectoriel" au sens matériel. |
| **Requêtes sur données compressées** | Les données restent compressées en mémoire et pendant le traitement — pas de décompression préalable nécessaire ("actionable compression"). |
| **Data Skipping** | Les synopsis (min/max par page) permettent de sauter les blocs de données non pertinents sans les lire. |

Dans Harry2, les tables de **ventes**, **stocks** et **historiques de commandes** sont les candidats typiques au stockage colonnes BLU, car ce sont des tables volumineuses consultées en mode analytique (agrégations, filtres sur dates/produits).

#### Création d'une table column-organized dans DB2 Harry2 :

```sql
-- DB2 BLU : table orientée colonnes
CREATE TABLE HARRY2.FACT_VENTES (
    ID_VENTE        BIGINT NOT NULL,
    ISBN13          CHAR(13),
    DATE_VENTE      DATE,
    QTE_VENDUE      INTEGER,
    CA_HT           DECIMAL(15,2),
    CODE_CANAL      CHAR(3),
    CODE_EDITEUR    CHAR(5)
) ORGANIZE BY COLUMN;
-- Compression automatique par colonne, pas d'index classique nécessaire
```

### 6bis.2 Équivalent dans Fabric Lakehouse : Delta Parquet + V-Order

Le Lakehouse Fabric stocke nativement en **format Parquet** (qui est un format colonnes) via **Delta Lake**. L'équivalent optimisé de BLU dans Fabric s'appelle **V-Order** :

| Fonctionnalité BLU (Harry2/DB2) | Équivalent Fabric Lakehouse |
|----------------------------------|----------------------------|
| Column-organized tables | **Parquet** (format colonnes natif de Delta Lake) |
| Compression vectorisée SIMD | **V-Order** (réorganisation at-write du layout Parquet) |
| Actionable compression | Parquet + encodage dictionnaire + Run-Length Encoding |
| Data Skipping (synopsis) | **Z-Order** + statistiques min/max par row group |
| Pas d'index nécessaire | Delta Lake statistics + bloom filters |

#### Correspondance directe :

```
┌─────────────────────────────┐    ┌─────────────────────────────┐
│     Harry2 / DB2 BLU        │    │   Fabric Lakehouse          │
│                             │    │                             │
│  ORGANIZE BY COLUMN         │───▶│  Format Parquet (natif)     │
│  Compression SIMD           │───▶│  V-Order write optimization │
│  Data Skipping              │───▶│  Z-Order + Delta stats      │
│  In-memory columnar         │───▶│  Spark columnar processing  │
│  Synopsis tables            │───▶│  Row group min/max stats    │
└─────────────────────────────┘    └─────────────────────────────┘
```

### 6bis.3 Activer V-Order dans le Lakehouse (équivalent BLU)

Le **V-Order** est une optimisation at-write qui réorganise le layout Parquet pour des lectures plus rapides. C'est l'équivalent fonctionnel du stockage vectorisé colonnes de BLU.

#### Au niveau session Spark :

```sql
%%sql
-- Activer V-Order pour la session (équivalent de ORGANIZE BY COLUMN)
SET spark.sql.parquet.vorder.default=TRUE
```

#### Au niveau table :

```sql
%%sql
-- Créer une table Delta avec V-Order (équivalent de CREATE TABLE ... ORGANIZE BY COLUMN)
CREATE TABLE gold_fact_ventes (
    id_vente        BIGINT,
    isbn13          STRING,
    date_vente      DATE,
    qte_vendue      INT,
    ca_ht           DECIMAL(15,2),
    code_canal      STRING,
    code_editeur    STRING
) USING DELTA
TBLPROPERTIES("delta.parquet.vorder.enabled" = "true");
```

#### Combiner V-Order + Z-Order (équivalent BLU + index colonnes fréquentes) :

```sql
%%sql
-- Optimiser avec V-Order + Z-Order sur les colonnes de filtre fréquentes
OPTIMIZE gold_fact_ventes
  ZORDER BY (date_vente, isbn13)
  VORDER;
```

### 6bis.4 Comparaison de performance attendue

| Scénario | Harry2 BLU | Fabric V-Order + Z-Order | Commentaire |
|----------|-----------|-------------------------|-------------|
| Scan analytique full table | 5-10x vs row-store | Comparable (Parquet colonnes natif) | Compression colonne dans les deux cas |
| Filtre sélectif (date, ISBN) | Data Skipping efficace | Z-Order + Delta stats skip | Z-Order cible les colonnes de filtre |
| Agrégation (SUM CA par mois) | SIMD vectorisé | Spark vectorized reader | Traitement par batch de colonnes |
| Écriture / INSERT | ~15% plus lent (compression) | ~15% plus lent avec V-Order | Même trade-off lecture/écriture |
| Compression ratio | 8-10x typique | 5-8x (Parquet + Snappy/ZStd) | BLU légèrement meilleur en compression pure |

### 6bis.5 Recommandation de migration du stockage vectoriel

| Table Harry2 | Type BLU | Recommandation Fabric |
|-------------|----------|----------------------|
| `FACT_VENTES` | ORGANIZE BY COLUMN | Delta + V-Order + Z-Order(date_vente, isbn13) |
| `FACT_STOCKS` | ORGANIZE BY COLUMN | Delta + V-Order + Z-Order(date_stock, code_entrepot) |
| `FACT_COMMANDES` | ORGANIZE BY COLUMN | Delta + V-Order + Z-Order(date_commande, code_editeur) |
| `FACT_DROITS` | ORGANIZE BY COLUMN | Delta + V-Order + Z-Order(periode, code_auteur) |
| `DIM_OUVRAGES` | ROW (petite table) | Delta standard (pas besoin de V-Order) |
| `DIM_AUTEURS` | ROW (petite table) | Delta standard |
| `DIM_EDITEURS` | ROW (petite table) | Delta standard |

> **Règle** : Activer V-Order + Z-Order sur les **tables de faits** (volumineuses, accès analytique). Les **tables de dimensions** (petites, accès lookup) n'en ont pas besoin.

### 6bis.6 Comment migrer concrètement le format BLU colonnes ?

Le point essentiel à comprendre : **le format BLU est un format de stockage interne à DB2**. Il n'est **pas exportable tel quel**. Quand on lit une table `ORGANIZE BY COLUMN` via SQL, ODBC ou un export CSV, les données sortent en **lignes classiques** — DB2 fait la conversion automatiquement.

La migration se fait donc en **3 étapes** :

```
┌───────────────────┐     ┌──────────────────┐     ┌──────────────────────┐
│  ÉTAPE 1          │     │  ÉTAPE 2          │     │  ÉTAPE 3              │
│  EXTRACTION       │────▶│  INGESTION        │────▶│  OPTIMISATION         │
│                   │     │                   │     │                       │
│  DB2 BLU → lignes │     │  lignes → Bronze  │     │  Bronze → Gold        │
│  (SQL/ODBC/CSV)   │     │  (Delta Parquet)  │     │  (V-Order + Z-Order)  │
│                   │     │                   │     │                       │
│  Le format BLU    │     │  Parquet stocke   │     │  On recrée l'optim.   │
│  est TRANSPARENT  │     │  déjà en colonnes │     │  vectorielle côté     │
│  à l'extraction   │     │  automatiquement  │     │  Fabric               │
└───────────────────┘     └──────────────────┘     └──────────────────────┘
```

#### ÉTAPE 1 — Extraction depuis Harry2 (BLU transparent)

Le format BLU colonnes est **invisible** lors de l'extraction. Trois options :

**Option A : Export SQL via ODBC (recommandé pour tables < 50M lignes)**

```sql
-- Depuis DB2 : la table BLU est lue comme n'importe quelle table
-- DB2 convertit automatiquement colonnes → lignes pour le client ODBC
SELECT 
    ID_VENTE, ISBN13, DATE_VENTE, QTE_VENDUE, 
    CA_HT, CODE_CANAL, CODE_EDITEUR
FROM HARRY2.FACT_VENTES
WHERE DATE_VENTE >= '2020-01-01'  -- extraction incrémentale si possible
ORDER BY DATE_VENTE;
```

Configuration du Data Gateway + ODBC dans Fabric Pipeline :
```json
{
    "type": "Copy",
    "source": {
        "type": "OdbcSource",
        "connectionString": "Driver={IBM i Access ODBC Driver};System=HARRY2_HOST;DBQ=HARRY2LIB;",
        "query": "SELECT * FROM HARRY2.FACT_VENTES WHERE DATE_VENTE >= ?"
    },
    "sink": {
        "type": "LakehouseSink",
        "tableName": "bronze_fact_ventes"
    }
}
```

**Option B : Export fichier plat depuis IBM i (recommandé pour tables volumineuses)**

```sql
-- Sur le système IBM i / AS400, exporter via CPYTOIMPF (commande CL)
-- Génère un CSV avec séparateur pipe, encodage UTF-8
CPYTOIMPF FROMFILE(HARRY2LIB/FACTVENTES) 
          TOSTMF('/export/harry2/fact_ventes.csv')
          MBROPT(*REPLACE) 
          STMFCCSID(1208)      -- UTF-8
          RCDDLM(*CRLF) 
          DTAFMT(*DLM) 
          FLDDLM('|')
          ADDCOLNAM(*SQL);
```

Puis upload vers ADLS Gen2 ou directement dans le Lakehouse Files :
```bash
# Via azcopy vers Azure Data Lake
azcopy copy "/export/harry2/fact_ventes.csv" \
  "https://<storage>.dfs.core.windows.net/harry2-landing/fact_ventes.csv"
```

**Option C : Export Parquet directement (si Spark ou Python dispo côté source)**

```python
# Si un environnement Python/Spark est disponible côté source
# Installer ibm_db + pyarrow
import ibm_db
import pandas as pd

sql = "SELECT * FROM HARRY2.FACT_VENTES"
conn = ibm_db.connect("DATABASE=HARRY2;HOSTNAME=host;PORT=50000;PROTOCOL=TCPIP;UID=user;PWD=pass;", "", "")
df = pd.read_sql(sql, conn)
df.to_parquet("/export/harry2/fact_ventes.parquet", engine="pyarrow")
```

> **Point clé** : Quelle que soit l'option, les données BLU sortent en lignes. Le format colonnes est **recréé côté Fabric** (Parquet le fait automatiquement, V-Order l'optimise).

#### ÉTAPE 2 — Ingestion dans le Lakehouse Bronze

```python
# Notebook Fabric — Ingestion Bronze
# Les données arrivent en CSV ou Parquet → on les charge dans une table Delta Bronze

# Option CSV
df_csv = (spark.read
    .option("header", "true")
    .option("delimiter", "|")
    .option("encoding", "UTF-8")
    .csv("Files/harry2-landing/fact_ventes.csv")
)

# Option Parquet (déjà en format colonnes !)
df_parquet = spark.read.parquet("Files/harry2-landing/fact_ventes.parquet")

# Écriture en Delta Bronze (Parquet colonnes automatique)
df_csv.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_fact_ventes")

# À ce stade, les données sont DÉJÀ stockées en colonnes (Parquet)
# mais sans l'optimisation V-Order
```

#### ÉTAPE 3 — Recréer l'optimisation vectorielle (V-Order = équivalent BLU)

C'est ici qu'on recrée l'équivalent du stockage BLU optimisé dans Fabric :

```python
# Notebook Fabric — Transformation Bronze → Gold avec V-Order
from pyspark.sql.functions import col, to_date, trim

# 1. Lire le Bronze
df_raw = spark.read.format("delta").table("bronze_fact_ventes")

# 2. Nettoyer (Silver)
df_clean = (df_raw
    .withColumn("isbn13", trim(col("ISBN13")).cast("string"))
    .withColumn("date_vente", to_date(col("DATE_VENTE"), "yyyy-MM-dd"))
    .withColumn("qte_vendue", col("QTE_VENDUE").cast("int"))
    .withColumn("ca_ht", col("CA_HT").cast("decimal(15,2)"))
    .withColumn("code_canal", trim(col("CODE_CANAL")))
    .withColumn("code_editeur", trim(col("CODE_EDITEUR")))
    .filter(col("isbn13").isNotNull())
    .dropDuplicates(["isbn13", "date_vente", "code_canal"])
)

# 3. Écrire en Gold avec V-Order ACTIVÉ
#    ⟵ C'EST ICI qu'on recrée le BLU côté Fabric
df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("parquet.vorder.enabled", "true") \
    .saveAsTable("gold_fact_ventes")

# 4. Z-Order + V-Order pour optimiser les colonnes de filtre
#    ⟵ Équivalent des synopsis BLU orientés sur les colonnes fréquentes
spark.sql("""
    OPTIMIZE gold_fact_ventes 
    ZORDER BY (date_vente, isbn13) 
    VORDER
""")

print("✓ Migration BLU → V-Order terminée")
print("  - Stockage colonnes : Parquet natif")
print("  - Optimisation vectorielle : V-Order activé")
print("  - Data skipping : Z-Order sur date_vente + isbn13")
```

#### Vérification post-migration

```python
# Vérifier que V-Order est bien activé sur la table Gold
spark.sql("DESCRIBE DETAIL gold_fact_ventes").show(truncate=False)

# Vérifier les propriétés de la table
spark.sql("SHOW TBLPROPERTIES gold_fact_ventes").show(truncate=False)
# Doit montrer : delta.parquet.vorder.enabled = true

# Comparer la taille avant/après V-Order
spark.sql("""
    SELECT 
        COUNT(*) as nb_lignes,
        COUNT(DISTINCT isbn13) as nb_isbn,
        MIN(date_vente) as date_min,
        MAX(date_vente) as date_max,
        SUM(ca_ht) as ca_total
    FROM gold_fact_ventes
""").show()
```

### 6bis.7 Résumé : ce qui change et ce qui ne change pas

| Aspect | Change ? | Détail |
|--------|----------|--------|
| **Données** | NON | Les données sont identiques — BLU est transparent à l'extraction |
| **Format de stockage** | AUTOMATIQUE | Parquet = colonnes natif. Pas besoin de recréer manuellement |
| **Optimisation vectorielle** | À RECRÉER | V-Order remplace SIMD/BLU — à activer explicitement |
| **Data Skipping** | À RECRÉER | Z-Order remplace les synopsis BLU — à configurer par table |
| **Compression** | AUTOMATIQUE | Parquet compresse par colonne nativement (Snappy ou ZStd) |
| **Types de données** | À MAPPER | `DECIMAL`, `CHAR(n)` → `DECIMAL`, `STRING` dans Spark |
| **Encodage** | À CONVERTIR | EBCDIC/Latin-1 → UTF-8 à l'extraction |
| **Schéma SQL** | À ADAPTER | Syntaxe DB2 → syntaxe Spark SQL / Delta |

> **En résumé** : Le format BLU n'est **pas un obstacle** à la migration. Il est transparent à l'extraction. Le vrai travail est de recréer les optimisations équivalentes côté Fabric avec V-Order + Z-Order, ce qui se fait en quelques lignes de configuration.

---

## 📅 7. Planning macro (accéléré — 8 semaines)

Grâce à la parallélisation multi-agent, les phases classiquement séquentielles peuvent se chevaucher :

```
Semaine     1    2    3    4    5    6    7    8
            ├────┤
Phase 0     ████ Inventaire Harry2 + connexion ODBC
            ├─────────┤
Phase 1          ██████ PoC extraction 3-5 tables → Bronze
                 ├─────────┤
Phase 2               ██████ Pipeline Bronze→Silver (en flux)
            ├────┤     ├────┤
Agent Dev   ████ Scaffolding agents (en parallèle de Phase 0-1)
                       ├─────────┤
Phase 3                     ██████ Gold + V-Order + Power BI
                            ├─────────┤
Phase 4                          ██████ Industrialisation + monitoring
                                      ├────┤
Phase 5                                    ██ Vectorisation (optionnel)
```

| Phase | Description | Durée | Parallèle avec |
|:-----:|-------------|:-----:|----------------|
| 🔍 **Phase 0** | Inventaire Harry2 + test connectivité ODBC | 1 semaine | Agent scaffolding |
| 🟧 **Phase 1** | PoC extraction 3-5 tables → Bronze | 2 semaines | Phase 0 (fin) |
| ⚪ **Phase 2** | Pipeline Bronze → Silver (en flux dès premières tables) | 2 semaines | Phase 1 (overlap) |
| 🟡 **Phase 3** | Gold + V-Order + Power BI | 2 semaines | Phase 2 (overlap) |
| ⚙️ **Phase 4** | Industrialisation, monitoring, orchestrateur complet | 2 semaines | Phase 3 (overlap) |
| 🧠 **Phase 5** | Vectorisation / recherche sémantique (optionnel) | 1 semaine | Phase 4 (fin) |

**🚀 Clés de l'accélération :**
- ⚡ **Flux continu** : dès qu'une table est en Bronze, l'Agent Transform démarre sans attendre les autres
- 🔨 **Scaffolding agents en parallèle** : les agents Discovery/Extract sont développés pendant l'inventaire
- 🤖 **Quality Gate automatisé** : pas d'attente humaine sauf exception (notifications Teams asynchrones)
- ⚡ **V-Order appliqué au fil de l'eau** : l'Agent Gold traite chaque table dès sa sortie de Silver

---

## 🧮 7bis. Analyse de complexité de la migration

### 7bis.1 Score de complexité global

| Axe de complexité | Score (1-5) | Justification |
|-------------------|:-----------:|---------------|
| **Accessibilité source** | 🔴 4/5 | IBM i / AS/400 = système fermé, compétences rares, ODBC via gateway nécessaire |
| **Volume de données** | 🟡 3/5 | 10-50M lignes sur les tables de faits — significatif mais pas extrême |
| **Variété de formats** | 🟡 3/5 | Données relationnelles + ONIX XML + fichiers plats. Pas de streaming ou IoT |
| **Encodage / caractères** | 🔴 4/5 | EBCDIC (CCSID 297 France), packed decimal, dates CYYMMDD — spécificités IBM i |
| **Logique métier embarquée** | 🔴 4/5 | Règles de gestion dans RPG/CL, triggers DB2, procédures stockées |
| **Transformation de schéma** | 🟡 3/5 | DB2 → Delta : mapping de types direct pour la plupart, packed decimal à gérer |
| **Stockage vectoriel (BLU)** | 🟢 2/5 | Transparent à l'extraction. V-Order le remplace côté Fabric facilement |
| **Dépendances inter-tables** | 🟡 3/5 | Clés étrangères implicites (pas toujours déclarées dans DB2 for i) |
| **Disponibilité du système source** | 🔴 4/5 | Harry2 souvent en production 24/7, fenêtre d'extraction limitée |
| **Compétences équipe** | 🟡 3/5 | Mix IBM i (rare) + Spark/Fabric (courant) — deux profils différents nécessaires |

**Score moyen : 3.3/5 — Complexité moyenne-haute**

> <div align="center">
>
> ![Score](https://img.shields.io/badge/Complexit%C3%A9-3.3%20%2F%205-orange?style=for-the-badge&logo=target&logoColor=white)
> ![Effort](https://img.shields.io/badge/Effort-80--110%20j%2Fh-blue?style=for-the-badge)
> ![Planning](https://img.shields.io/badge/Planning-8%20semaines-green?style=for-the-badge)
> ![Avec Copilot](https://img.shields.io/badge/Avec%20Copilot-6%20sem%20%7C%2055--75%20j%2Fh-purple?style=for-the-badge&logo=github&logoColor=white)
>
> </div>

### 7bis.2 Matrice de risques

```
        Impact
  Élevé │  ④       ①
        │     ③
  Moyen │  ⑥    ②  ⑤
        │
 Faible │  ⑧  ⑦
        └──────────────
         Faible  Moyen  Élevé
                Probabilité

① Encodage EBCDIC corrompu     ④ Fenêtre extraction insuffisante
② Packed decimal mal converti  ⑤ Compétences IBM i indisponibles
③ Règles métier RPG manquées   ⑥ Volume sous-estimé
⑦ BLU → V-Order incomplet     ⑧ ISBN invalides en volume
```

| # | Risque | Prob. | Impact | Mitigation |
|---|--------|:-----:|:------:|------------|
| ① | Encodage EBCDIC → UTF-8 corrompu (accents perdus) | Élevée | Élevé | Conversion explicite CCSID 297→UTF-8 à l'extraction. Test sur échantillon avec accents (é, è, ê, ç, à, ù) |
| ② | Packed decimal (COMP-3) mal interprété | Moyenne | Moyen | Utiliser le driver ODBC IBM i Access qui gère nativement la conversion. Vérifier montants/prix sur échantillon |
| ③ | Règles métier dans RPG/CL non migrées | Moyenne | Élevé | Atelier avec l'équipe Harry2 pour documenter les triggers, programmes CL et procédures stockées. Les réécrire en PySpark |
| ④ | Fenêtre d'extraction trop courte (système 24/7) | Faible | Élevé | Extraction incrémentale par journal IBM i (CDC). Ou extraction sur réplique/backup si disponible |
| ⑤ | Expert IBM i / RPG indisponible | Élevée | Moyen | Identifier et réserver la ressource dès Sprint 0. Documenter toute connaissance acquise immédiatement |
| ⑥ | Volume réel > estimations | Faible | Moyen | Extraction partitionnée par date. Ajuster le parallélisme de l'Agent Extraction dynamiquement |
| ⑦ | V-Order non appliqué sur toutes les tables BLU | Faible | Faible | L'Agent Gold vérifie automatiquement le flag `is_blu_source` du manifeste |
| ⑧ | ISBN invalides en volume dans le source | Faible | Faible | L'Agent Quality Gate les détecte. Nettoyage ou exclusion en Silver |

### 7bis.3 Complexité par domaine fonctionnel

| Domaine Harry2 | Tables | Complexité | Raisons |
|----------------|:------:|:----------:|---------|
| **Catalogue ouvrages** | 5-10 | 🟡 Moyenne | Métadonnées riches, ONIX XML, ISBN multi-format |
| **Auteurs / Contrats** | 3-5 | 🔴 Haute | Logique de droits d'auteur complexe, calculs de royalties dans RPG |
| **Ventes / Commandes** | 5-8 | 🟡 Moyenne | Volume important mais structure simple. Tables BLU → V-Order |
| **Stocks** | 2-3 | 🟢 Faible | Structure plate, mise à jour fréquente mais simple |
| **Métadonnées ONIX** | 1-2 | 🔴 Haute | XML complexe (ONIX 2.1/3.0), parsing lourd, mapping vers relationnel |
| **Référentiels / Codes** | 10-20 | 🟢 Faible | Petites tables de lookup, extraction triviale |

### 7bis.4 Comparaison avec d'autres migrations

| Type de migration | Complexité | Notre cas |
|-------------------|:----------:|:---------:|
| Oracle → Fabric Lakehouse | 🟡 3/5 | — |
| SQL Server → Fabric Warehouse | 🟢 2/5 | — |
| **IBM i Harry2 → Fabric Lakehouse** | **🟠 3.3/5** | **← Notre cas** |
| Mainframe COBOL/VSAM → Cloud | 🔴 4.5/5 | — |
| SAP ERP → Fabric | 🔴 4/5 | — |
| MongoDB → Cosmos DB | 🟢 2/5 | — |

**Notre migration est plus complexe qu'un Oracle/SQL Server** (à cause de l'IBM i, EBCDIC, RPG), **mais nettement moins complexe qu'un mainframe COBOL** (pas de fichiers VSAM, pas de JCL, DB2 for i est relativement moderne).

### 7bis.5 Facteurs de réduction de complexité

Ce qui **simplifie** cette migration :

| Facteur | Impact |
|---------|--------|
| ✅ DB2 for i supporte SQL standard | Extraction par requêtes SQL classiques, pas de langage propriétaire pour lire les données |
| ✅ Format BLU transparent | Pas de conversion de format spécial — les données sortent en lignes via SQL |
| ✅ Parquet = colonnes natif | Le format cible est déjà columnar — pas de perte de performance architecturale |
| ✅ Fabric Pipeline + ODBC | Connecteur générique, pas besoin de développement custom |
| ✅ Delta Lake = schéma évolutif | `mergeSchema` / `overwriteSchema` gèrent les changements de structure |
| Multi-agent automatisé | L'Agent Discovery catalogue automatiquement, l'Agent Quality Gate détecte les problèmes |

### 7bis.6 Effort estimé par rôle

| Rôle | Effort (j/h) | Phase principale |
|------|:------------:|-----------------|
| Expert IBM i / RPG | 15-20 jours | Discovery + Extraction + documentation règles métier |
| Data Engineer Fabric/Spark | 25-30 jours | Extraction → Silver → Gold, pipelines, V-Order |
| Data Architect | 10-15 jours | Modélisation Gold, partitioning, Z-Order strategy |
| Développeur Agent IA | 15-20 jours | Développement 6 agents Foundry + Semantic Kernel |
| Analyste métier éditorial | 10-15 jours | Validation qualité, mapping codes métier, UAT |
| Développeur Power BI | 5-10 jours | Rapports et dashboards sur couche Gold |
| **TOTAL** | **80-110 j/h** | **~2-3 ETP pendant 8 semaines** |

---

## 🤖 7ter. Gains via Copilot & outils IA

> <div align="center">
>
> ![GitHub Copilot](https://img.shields.io/badge/GitHub%20Copilot-Agent%20%2B%20Code-24292e?style=for-the-badge&logo=github&logoColor=white)
> ![Fabric Copilot](https://img.shields.io/badge/Fabric%20Copilot-Notebooks%20%2B%20Pipelines-742774?style=for-the-badge&logo=microsoftazure&logoColor=white)
> ![Power BI Copilot](https://img.shields.io/badge/Power%20BI%20Copilot-Rapports%20auto-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
> ![Gain](https://img.shields.io/badge/Gain%20total-−35%25%20effort-success?style=for-the-badge)
>
> </div>

### 7ter.1 Vue d'ensemble — Copilot dans chaque phase

| Phase | Outil Copilot | Gain estimé | Exemple concret |
|-------|--------------|:-----------:|-----------------|
| 🔍 **Discovery** | GitHub Copilot (code) + Fabric Copilot (notebooks) | **-40 %** sur le temps d'inventaire | Copilot génère les requêtes `SYSCOLUMNS` / `SYSTABLES` pour cataloguer Harry2 automatiquement |
| 🔌 **Extraction** | Fabric Copilot dans Data Factory | **-30 %** sur la création de pipelines | Prompt : *« Crée un pipeline Copy Activity ODBC → Bronze Lakehouse pour la table OUVRAGES »* → pipeline généré |
| ⚪ **Transform (Silver)** | Fabric Copilot dans Notebooks Spark | **-50 %** sur le code PySpark | Prompt : *« Convertis le champ CYYMMDD en date ISO, décimal packé en float, EBCDIC en UTF-8 »* → cellule PySpark prête |
| 🛡️ **Quality Gate** | GitHub Copilot | **-40 %** sur le code de validation | Copilot auto-complète les checks ISBN, row count, null ratio depuis le contexte du notebook |
| 🟡 **Gold / V-Order** | Fabric Copilot + GitHub Copilot | **-35 %** sur la modélisation | Prompt : *« Génère le DDL Delta pour une table de faits ventes avec V-Order et Z-Order sur date_vente »* |
| 📊 **Power BI** | Copilot in Power BI | **-60 %** sur le prototypage rapports | *« Crée un rapport de ventes par éditeur, auteur, mois avec un filtre sur ISBN »* → rapport généré sur le SQL endpoint |
| **Agents IA** | GitHub Copilot | **-45 %** sur le code des agents | Copilot génère le scaffold Semantic Kernel, les prompts agents, les appels API Fabric REST |
| **Documentation** | Microsoft 365 Copilot | **-70 %** sur la rédaction | Résumé automatique des résultats de migration, génération des rapports de recette |

### 7ter.2 Impact sur l'effort total

**Sans Copilot** (estimation §7bis.6) :

| Rôle | Effort initial |
|------|:--------------:|
| Expert IBM i / RPG | 15-20 j/h |
| Data Engineer Fabric/Spark | 25-30 j/h |
| Data Architect | 10-15 j/h |
| Développeur Agent IA | 15-20 j/h |
| Analyste métier | 10-15 j/h |
| Développeur Power BI | 5-10 j/h |
| **TOTAL** | **80-110 j/h** |

**Avec Copilot** (gains appliqués par rôle) :

| Rôle | Effort initial | Gain Copilot | Effort réduit | Comment |
|------|:--------------:|:------------:|:-------------:|---------|
| Expert IBM i / RPG | 15-20 j/h | **~15 %** | 13-17 j/h | Copilot n'aide pas sur RPG/CL ni sur la connaissance métier IBM i. Gain limité à la documentation |
| Data Engineer Fabric/Spark | 25-30 j/h | **~40 %** | 15-18 j/h | Fabric Copilot génère pipelines + notebooks PySpark. Le plus gros gain du projet |
| Data Architect | 10-15 j/h | **~25 %** | 8-11 j/h | Copilot assiste le DDL Delta, Z-Order strategy, mais le design reste humain |
| Développeur Agent IA | 15-20 j/h | **~45 %** | 8-11 j/h | GitHub Copilot excels sur le code Python/Semantic Kernel, génère les prompts agents |
| Analyste métier | 10-15 j/h | **~10 %** | 9-14 j/h | La validation métier reste essentiellement humaine |
| Développeur Power BI | 5-10 j/h | **~60 %** | 2-4 j/h | Copilot in Power BI génère les visuels et les mesures DAX depuis le prompt |
| **TOTAL** | **80-110 j/h** | **~35 % moyen** | **55-75 j/h** | **Économie de 25-35 j/h** |

### 7ter.3 Impact sur le planning 8 semaines

```
SANS Copilot                          AVEC Copilot
━━━━━━━━━━━━━━━━━━━━━━━━━━━━        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
S1-S2  Discovery + Infra              S1     Discovery + Infra ← Copilot accélère inventaire
S3-S4  Extraction Bronze              S2-S3  Extraction Bronze ← Pipelines auto-générés
S5-S6  Silver + Gold + Agents         S3-S4  Silver + Gold + Agents ← PySpark auto-généré
S7     Power BI + Tests               S5     Power BI auto + Tests ← Copilot PBI
S8     Industrialisation              S5-S6  Industrialisation
━━━━━━━━━━━━━━━━━━━━━━━━━━━━        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
       8 semaines                            6 semaines (-25%)
```

### 7ter.4 Détail des usages Copilot par outil

#### Fabric Copilot dans Notebooks

```python
# Prompt dans le notebook Fabric :
# "Lis la table Bronze ouvrages, convertis les dates CYYMMDD en ISO 8601,
#  nettoie les champs EBCDIC, et écris en Silver Delta avec schéma strict"

# ⇒ Copilot génère :
from pyspark.sql import functions as F

df = spark.read.format("delta").load("Tables/bronze_ouvrages")

df_silver = (df
    .withColumn("date_publication", 
        F.to_date(
            F.concat(
                (F.col("DTPUB").cast("int") / 10000 + 1900).cast("int").cast("string"),
                F.lpad((F.col("DTPUB") % 10000 / 100).cast("int").cast("string"), 2, "0"),
                F.lpad((F.col("DTPUB") % 100).cast("int").cast("string"), 2, "0")
            ), "yyyyMMdd"))
    .withColumn("prix_public", F.col("PXPUB").cast("decimal(10,2)"))
    .withColumn("titre", F.trim(F.col("TITRE")))
    .drop("DTPUB", "PXPUB")
)

df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_ouvrages")
```

#### Copilot in Power BI

```
Prompt utilisateur :
"Crée un dashboard des ventes Harry2 avec :
 - Ventes par mois (bar chart)
 - Top 10 auteurs par CA (table)
 - Répartition par format (camembert)
 - Filtre sur éditeur et année"

⇒ Copilot génère le rapport automatiquement depuis le SQL analytics endpoint
   du Lakehouse Gold, y compris les mesures DAX :

   Total Ventes = SUM(gold_fact_ventes[montant_ttc])
   CA Auteur = CALCULATE([Total Ventes], ALLEXCEPT(gold_dim_auteurs, gold_dim_auteurs[nom_auteur]))
```

#### GitHub Copilot pour les Agents

```python
# GitHub Copilot génère le squelette d'un agent Semantic Kernel
# à partir du commentaire :

# Agent de transformation Silver qui lit le manifeste Bronze,
# applique les conversions EBCDIC/packed decimal/CYYMMDD,
# et écrit en Delta Silver avec quality checks intégrés

from semantic_kernel.agents import ChatCompletionAgent
from semantic_kernel.connectors.ai.open_ai import AzureChatCompletion

agent_transform = ChatCompletionAgent(
    name="TransformAgent",
    instructions="""Tu es un agent de transformation de données.
    Pour chaque table du manifeste Bronze :
    1. Lis le schéma source (types DB2 for i)
    2. Applique les conversions : CYYMMDD→date, packed decimal→float, EBCDIC→UTF-8
    3. Renomme les colonnes en snake_case
    4. Écris en Delta Silver avec mode overwrite
    5. Retourne le compte de lignes transformées""",
    service=AzureChatCompletion(
        deployment_name="gpt-4o",
        endpoint=AZURE_OPENAI_ENDPOINT,
        api_key=AZURE_OPENAI_KEY
    )
)
```

### 7ter.5 Limites de Copilot sur ce projet

| Domaine | Limite | Conséquence |
|---------|--------|-------------|
| **RPG / CL** | GitHub Copilot a une connaissance limitée de RPG IV et CL IBM i | L'expert IBM i reste indispensable pour documenter la logique métier |
| **EBCDIC** | Copilot ne connaît pas les subtilités de CCSID 297 (France) | Les conversions de caractères doivent être testées manuellement sur échantillon |
| **Validation métier** | Copilot ne peut pas valider si un ISBN ou un code éditeur est correct métier | L'analyste métier éditorial reste essentiel |
| **Architecture** | Copilot génère du code, pas des décisions d'architecture (partitioning, Z-Order strategy) | Le Data Architect reste dans la boucle |
| **Sécurité / RGPD** | Copilot ne connaît pas les contraintes de sécurité spécifiques de l'entreprise | Revue sécurité humaine obligatoire |

### 7ter.6 Synthèse des gains

```
┌─────────────────────────────────────────────────────────┐
│              GAINS COPILOT — SYNTHÈSE                   │
├─────────────────────────────────────────────────────────┤
│  Effort : 80-110 j/h  →  55-75 j/h    (−35 %)         │
│  Planning : 8 semaines →  6 semaines   (−25 %)         │
│  Coût ETP : 2-3 ETP   →  1.5-2 ETP    (−30 %)         │
├─────────────────────────────────────────────────────────┤
│  Plus gros gains :                                      │
│   • Data Engineer PySpark    −40 %                      │
│   • Développeur Power BI     −60 %                      │
│   • Développeur Agents IA    −45 %                      │
│  Plus faibles gains :                                   │
│   • Expert IBM i             −15 %  (connaissance rare) │
│   • Analyste métier          −10 %  (validation humaine)│
└─────────────────────────────────────────────────────────┘
```

---

## 🧪 8. Kit de test de migration

> 🎯 Un **jeu de test complet** est disponible dans le dossier `test_migration/` pour valider chaque étape de la migration sans accès au vrai système IBM i.

| # | Fichier | Rôle |
|:-:|---------|------|
| 🗂️ | `01_harry2_source_ddl.sql` | Schéma source DB2 for i simulé (10 tables, noms courts RPG, CYYMMDD, packed decimal) |
| 💾 | `02_harry2_sample_data.sql` | Données réalistes : 10 ouvrages, 10 auteurs, 8 éditeurs, 25 ventes, accents EBCDIC |
| 🟧 | `03_bronze_ingestion.py` | Notebook PySpark — ingestion Bronze (tables Delta brutes) |
| ⚪ | `04_silver_transform.py` | Notebook PySpark — Silver (conversion CYYMMDD, trim, ISBN, accents) |
| 🟡 | `05_gold_model_vorder.py` | Notebook PySpark — Modèle étoile Gold + OPTIMIZE ZORDER |
| 🛡️ | `06_quality_gate.py` | Notebook PySpark — 6 familles de checks automatisés |
| 🔌 | `07_extraction_queries_db2.sql` | Requêtes SQL pour le vrai IBM i (inventaire, extraction, vérification EBCDIC) |

**🚫 Pièges volontaires intégrés** (pour tester la robustesse) :
- 🚨 1 ISBN invalide (`9782070000000` — checksum faux)
- 📆 1 date ancienne C=0 (1978 — `0780301`)
- ↩️ Retours de ventes (quantités négatives)
- 🇪🇸 Accents espagnols (García Márquez) en plus des français

> 🚀 **Pour tester** : copier les notebooks `.py` dans un Fabric Workspace et les exécuter dans l'ordre `03 → 04 → 05 → 06`.

---

## ✅ 8bis. Prochaines étapes

- [ ] 🔍 **Inventaire des tables Harry2** — Lister toutes les tables/fichiers physiques avec volumes et fréquences
- [ ] 🔌 **Test de connectivité** — Valider l'accès ODBC depuis un gateway on-premises vers DB2/IBM i
- [ ] 🧪 **Exécuter le kit de test** — Valider les notebooks 03→06 sur un Fabric Workspace de développement
- [ ] 🟧 **PoC Bronze** — Extraire ouvrages, auteurs, ventes vers un Lakehouse de développement
- [ ] 🧑‍💼 **Validation métier** — Faire valider la qualité des données par les équipes éditoriales
- [ ] ⚪ **Notebook de transformation** — Développer les transformations Bronze → Silver → Gold
- [ ] 📊 **Rapports Power BI** — Prototyper les premiers dashboards éditoriaux

---

## 🤖 9. Plan de migration multi-agent

> <div align="center">
>
> ![Foundry](https://img.shields.io/badge/Foundry-Agent%20Service-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
> ![Semantic Kernel](https://img.shields.io/badge/Semantic%20Kernel-Orchestration-5C2D91?style=for-the-badge&logo=dotnet&logoColor=white)
> ![Agents](https://img.shields.io/badge/6%20Agents-Sp%C3%A9cialis%C3%A9s-success?style=for-the-badge)
>
> </div>

### 9.1 Pourquoi un mode multi-agent ?

La migration Harry2 → Fabric Lakehouse implique des domaines d'expertise très différents (IBM i, DB2, Spark, Power BI, métier éditorial). Un **système multi-agent** permet de :

- **Paralléliser** les tâches d'extraction, transformation et validation
- **Spécialiser** chaque agent sur son domaine d'expertise
- **Automatiser** les décisions de mapping, qualité et routage
- **Orchestrer** le flux de bout en bout avec un agent coordinateur
- **Maintenir un humain dans la boucle** pour les validations critiques

### 9.2 Architecture multi-agent

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    AGENT ORCHESTRATEUR (Workflow Agent)                  │
│         Coordonne l'ensemble du flux de migration Harry2 → Fabric       │
│         Pattern : Sequential + Handoff + Human-in-the-loop              │
└──────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┘
       │          │          │          │          │          │
  ┌────▼───┐ ┌───▼────┐ ┌──▼───┐ ┌───▼────┐ ┌──▼───┐ ┌───▼────┐
  │ AGENT  │ │ AGENT  │ │AGENT │ │ AGENT  │ │AGENT │ │ AGENT  │
  │DISCOVER│ │EXTRACT │ │TRANS-│ │QUALITY │ │MODEL │ │MONITOR │
  │  Y     │ │  ION   │ │FORM  │ │ GATE   │ │ GOLD │ │  ING   │
  └────────┘ └────────┘ └──────┘ └────────┘ └──────┘ └────────┘
  Inventaire  Pipeline   Bronze   Validation  Silver   Observa-
  des tables  ODBC/CSV   →Silver  & contrôle  →Gold    bilité
  Harry2      →Bronze    nettoy.  qualité     modèle   & alertes
```

### 9.3 Rôles et responsabilités des agents

#### Agent 0 : Orchestrateur (Workflow Agent — Microsoft Foundry)

| Attribut | Valeur |
|----------|--------|
| **Type** | Workflow Agent (Foundry Agent Service) |
| **Pattern** | Sequential → Handoff → Human-in-the-loop |
| **Rôle** | Coordonne l'enchaînement des agents, gère les erreurs, déclenche les validations humaines |
| **Outils** | Fabric REST API, Notifications Teams, État de pipeline |

**Instructions de l'orchestrateur :**
```yaml
# workflow-orchestrator.yaml (Foundry Agent Service)
name: harry2-migration-orchestrator
description: >
  Orchestre la migration complète Harry2 → Fabric Lakehouse.
  Séquence : Discovery → Extraction → Transform → Quality → Gold → Monitor
  
steps:
  - id: discovery
    agent: agent-discovery
    description: "Inventaire automatique des tables Harry2"
    on_success: extraction
    on_failure: notify_human

  - id: extraction
    agent: agent-extraction
    description: "Extraction des données Harry2 → Bronze"
    parallel: true  # extraction multi-tables en parallèle
    on_success: transform
    on_failure: retry_then_notify

  - id: transform
    agent: agent-transform
    description: "Nettoyage Bronze → Silver"
    on_success: quality_gate
    on_failure: notify_human

  - id: quality_gate
    agent: agent-quality
    description: "Validation qualité des données"
    human_in_the_loop: true  # validation humaine requise
    on_success: gold_model
    on_failure: transform  # retour au nettoyage si échec qualité

  - id: gold_model
    agent: agent-gold
    description: "Modélisation dimensionnelle Gold + V-Order"
    on_success: monitoring
    on_failure: notify_human

  - id: monitoring
    agent: agent-monitoring
    description: "Mise en place du monitoring continu"
    on_success: complete
```

---

#### Agent 1 : Discovery (Inventaire Harry2)

| Attribut | Valeur |
|----------|--------|
| **Type** | Prompt Agent (Foundry) + Function Calling |
| **Rôle** | Catalogue automatique des tables Harry2 : structure, types, volumes, dépendances |
| **Outils** | ODBC Query (DB2 catalog), Fabric REST API |
| **Output** | Manifeste JSON de toutes les tables à migrer |

```python
# Agent Discovery — Notebook Fabric
# Interroge le catalogue système DB2 pour inventorier les tables Harry2

def discover_harry2_tables(odbc_connection_string: str) -> dict:
    """
    Agent Discovery : catalogue automatique des tables Harry2.
    Interroge SYSCOLUMNS, SYSTABLES, SYSINDEXES dans DB2.
    """
    import pyodbc
    
    conn = pyodbc.connect(odbc_connection_string)
    
    # 1. Lister toutes les tables du schéma Harry2
    tables_query = """
        SELECT TABLE_NAME, TABLE_TYPE, 
               ROW_COUNT_ESTIMATE, DATA_SIZE,
               CASE WHEN ORGANIZE_BY = 'C' THEN 'COLUMN (BLU)' 
                    ELSE 'ROW' END AS STORAGE_TYPE
        FROM QSYS2.SYSTABLES 
        WHERE TABLE_SCHEMA = 'HARRY2'
        ORDER BY ROW_COUNT_ESTIMATE DESC
    """
    tables = conn.execute(tables_query).fetchall()
    
    # 2. Pour chaque table, inventorier les colonnes
    manifest = {"schema": "HARRY2", "tables": []}
    for table in tables:
        cols_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, LENGTH, NUMERIC_SCALE,
                   IS_NULLABLE, COLUMN_DEFAULT, CCSID
            FROM QSYS2.SYSCOLUMNS 
            WHERE TABLE_SCHEMA = 'HARRY2' AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        columns = conn.execute(cols_query, table.TABLE_NAME).fetchall()
        
        manifest["tables"].append({
            "name": table.TABLE_NAME,
            "type": table.TABLE_TYPE,
            "storage": table.STORAGE_TYPE,
            "estimated_rows": table.ROW_COUNT_ESTIMATE,
            "size_bytes": table.DATA_SIZE,
            "columns": [
                {
                    "name": c.COLUMN_NAME,
                    "type": c.DATA_TYPE,
                    "length": c.LENGTH,
                    "nullable": c.IS_NULLABLE,
                    "ccsid": c.CCSID,
                    "spark_type": map_db2_to_spark(c.DATA_TYPE, c.LENGTH, c.NUMERIC_SCALE)
                }
                for c in columns
            ]
        })
    
    conn.close()
    return manifest

def map_db2_to_spark(db2_type: str, length: int, scale: int) -> str:
    """Mapping des types DB2 → Spark SQL."""
    mapping = {
        "CHAR": "STRING",
        "VARCHAR": "STRING",
        "CLOB": "STRING",
        "SMALLINT": "SMALLINT",
        "INTEGER": "INT",
        "BIGINT": "BIGINT",
        "DECIMAL": f"DECIMAL({length},{scale})",
        "NUMERIC": f"DECIMAL({length},{scale})",
        "REAL": "FLOAT",
        "DOUBLE": "DOUBLE",
        "DATE": "DATE",
        "TIME": "STRING",
        "TIMESTAMP": "TIMESTAMP",
        "BLOB": "BINARY",
        "GRAPHIC": "STRING",
        "VARGRAPHIC": "STRING",
    }
    return mapping.get(db2_type, "STRING")
```

**Output JSON (manifeste) :**
```json
{
  "schema": "HARRY2",
  "tables": [
    {
      "name": "FACT_VENTES",
      "type": "TABLE",
      "storage": "COLUMN (BLU)",
      "estimated_rows": 25000000,
      "size_bytes": 4800000000,
      "columns": [
        {"name": "ID_VENTE", "type": "BIGINT", "spark_type": "BIGINT", "ccsid": 65535},
        {"name": "ISBN13", "type": "CHAR", "spark_type": "STRING", "ccsid": 297},
        {"name": "DATE_VENTE", "type": "DATE", "spark_type": "DATE", "ccsid": 0}
      ]
    },
    {
      "name": "DIM_OUVRAGES",
      "type": "TABLE",
      "storage": "ROW",
      "estimated_rows": 850000,
      "columns": [ "..." ]
    }
  ]
}
```

---

#### Agent 2 : Extraction (Harry2 → Bronze)

| Attribut | Valeur |
|----------|--------|
| **Type** | Hosted Agent (Container) — orchestration parallèle |
| **Rôle** | Extraire chaque table Harry2 selon sa taille et son type de stockage, router vers la bonne méthode d'ingestion |
| **Outils** | Fabric Pipeline API, ODBC, azcopy, Spark |
| **Input** | Manifeste JSON de l'Agent Discovery |
| **Output** | Tables Delta Bronze dans le Lakehouse |

```python
# Agent Extraction — Routage intelligent selon taille et type
import json

def plan_extraction(manifest: dict) -> list:
    """
    Agent Extraction : décide de la stratégie d'extraction pour chaque table.
    - Petites tables (< 1M rows) → ODBC direct via Fabric Pipeline
    - Moyennes tables (1M-10M rows) → Export CSV + upload ADLS
    - Grandes tables (> 10M rows) → Export Parquet partitionné
    - Tables BLU → même extraction, mais tag V-Order pour la couche Gold
    """
    extraction_plan = []
    
    for table in manifest["tables"]:
        rows = table["estimated_rows"]
        is_blu = "COLUMN" in table.get("storage", "ROW")
        
        if rows < 1_000_000:
            method = "ODBC_DIRECT"
            parallelism = 1
        elif rows < 10_000_000:
            method = "CSV_EXPORT"
            parallelism = 4
        else:
            method = "PARQUET_PARTITIONED"
            parallelism = 8
        
        extraction_plan.append({
            "table": table["name"],
            "method": method,
            "parallelism": parallelism,
            "is_blu_source": is_blu,
            "target_vorder": is_blu,
            "bronze_table": f"bronze_{table['name'].lower()}",
            "columns": table["columns"],
            "estimated_rows": rows
        })
    
    return extraction_plan

def execute_extraction(plan_item: dict):
    """Exécute l'extraction d'une table selon le plan."""
    
    if plan_item["method"] == "ODBC_DIRECT":
        df = (spark.read
            .format("jdbc")
            .option("url", "jdbc:as400://HARRY2_HOST/HARRY2")
            .option("dbtable", f"HARRY2.{plan_item['table']}")
            .option("driver", "com.ibm.as400.access.AS400JDBCDriver")
            .load()
        )
        df.write.format("delta").mode("overwrite") \
            .saveAsTable(plan_item["bronze_table"])
    
    elif plan_item["method"] == "CSV_EXPORT":
        df = (spark.read
            .option("header", "true").option("delimiter", "|")
            .option("encoding", "UTF-8")
            .csv(f"Files/harry2-landing/{plan_item['table'].lower()}.csv")
        )
        df.write.format("delta").mode("overwrite") \
            .saveAsTable(plan_item["bronze_table"])
    
    elif plan_item["method"] == "PARQUET_PARTITIONED":
        df = spark.read.parquet(
            f"Files/harry2-landing/{plan_item['table'].lower()}/"
        )
        df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(plan_item["bronze_table"])
    
    return {"table": plan_item["table"], "status": "OK", 
            "rows": spark.table(plan_item["bronze_table"]).count()}
```

---

#### Agent 3 : Transform (Bronze → Silver)

| Attribut | Valeur |
|----------|--------|
| **Type** | Prompt Agent avec Function Calling |
| **Rôle** | Nettoyer, typer, dédupliquer les données Bronze. Gère l'encodage EBCDIC, les dates IBM, les packed decimal |
| **Outils** | Spark SQL, PySpark, manifeste de colonnes |
| **Input** | Tables Bronze + Manifeste JSON |
| **Output** | Tables Silver nettoyées |

```python
# Agent Transform — Nettoyage intelligent par colonne
from pyspark.sql.functions import col, trim, to_date, when, regexp_replace

def transform_table(bronze_table: str, columns_meta: list) -> str:
    """
    Agent Transform : applique le nettoyage adapté au type de chaque colonne.
    Gère automatiquement : EBCDIC, dates IBM CYYMMDD, packed decimal, accents.
    """
    silver_table = bronze_table.replace("bronze_", "silver_")
    df = spark.read.format("delta").table(bronze_table)
    
    for col_meta in columns_meta:
        cname = col_meta["name"].lower()
        ctype = col_meta["type"]
        ccsid = col_meta.get("ccsid", 0)
        
        # Trim sur tous les CHAR (longueur fixe IBM)
        if ctype in ("CHAR", "GRAPHIC"):
            df = df.withColumn(cname, trim(col(cname)))
        
        # Conversion dates IBM CYYMMDD (C=siècle: 0=19xx, 1=20xx)
        if ctype == "DATE" and "CYYMMDD" in col_meta.get("format", ""):
            df = df.withColumn(cname,
                to_date(
                    when(col(cname).substr(1,1) == "0", "19")
                    .otherwise("20")
                    .concat(col(cname).substr(2,6)),
                    "yyyyMMdd"
                )
            )
        
        # Nettoyage accents pour CCSID EBCDIC (297 = France)
        if ccsid == 297:
            df = df.withColumn(cname,
                regexp_replace(col(cname), r'[\x00-\x1f]', '')
            )
        
        # Cast vers le type Spark cible
        spark_type = col_meta["spark_type"]
        if spark_type != "STRING":
            df = df.withColumn(cname, col(cname).cast(spark_type))
    
    # Déduplication si clé primaire identifiée
    pk_cols = [c["name"].lower() for c in columns_meta if c.get("is_pk")]
    if pk_cols:
        df = df.dropDuplicates(pk_cols)
    
    df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
    return silver_table
```

---

#### Agent 4 : Quality Gate (Validation qualité)

| Attribut | Valeur |
|----------|--------|
| **Type** | Prompt Agent + Human-in-the-loop |
| **Rôle** | Valide la qualité des données Silver avant passage en Gold. Bloque si des seuils sont dépassés. Notifie l'humain pour décision. |
| **Outils** | Spark SQL, Great Expectations, Teams Notification |
| **Input** | Tables Silver |
| **Output** | Rapport qualité + GO/NO-GO |

```python
# Agent Quality Gate — Contrôle qualité automatique + décision humaine
def quality_check(silver_table: str, bronze_table: str) -> dict:
    """
    Agent Quality : vérifie intégrité, complétude, cohérence.
    Si un seuil échoue → demande validation humaine.
    """
    df_silver = spark.table(silver_table)
    df_bronze = spark.table(bronze_table)
    
    report = {
        "table": silver_table,
        "checks": [],
        "status": "PASS"
    }
    
    # Check 1 : Perte de lignes (tolérance 0.1%)
    bronze_count = df_bronze.count()
    silver_count = df_silver.count()
    loss_pct = (1 - silver_count / bronze_count) * 100 if bronze_count > 0 else 0
    report["checks"].append({
        "name": "row_loss",
        "bronze_rows": bronze_count,
        "silver_rows": silver_count,
        "loss_pct": round(loss_pct, 2),
        "threshold": 0.1,
        "status": "PASS" if loss_pct <= 0.1 else "FAIL"
    })
    
    # Check 2 : Colonnes nulles inattendues
    for column in df_silver.columns:
        null_count = df_silver.filter(col(column).isNull()).count()
        null_pct = (null_count / silver_count) * 100 if silver_count > 0 else 0
        if null_pct > 5:
            report["checks"].append({
                "name": f"null_check_{column}",
                "null_pct": round(null_pct, 2),
                "threshold": 5.0,
                "status": "WARN" if null_pct <= 20 else "FAIL"
            })
    
    # Check 3 : Validité ISBN-13 (checksum)
    if "isbn13" in df_silver.columns:
        invalid_isbn = df_silver.filter(
            ~col("isbn13").rlike(r"^97[89]\d{10}$")
        ).count()
        isbn_error_pct = (invalid_isbn / silver_count) * 100
        report["checks"].append({
            "name": "isbn13_validity",
            "invalid_count": invalid_isbn,
            "error_pct": round(isbn_error_pct, 2),
            "status": "PASS" if isbn_error_pct <= 1 else "FAIL"
        })
    
    # Décision globale
    failed = [c for c in report["checks"] if c["status"] == "FAIL"]
    if failed:
        report["status"] = "FAIL"
        report["action"] = "HUMAN_REVIEW_REQUIRED"
    
    return report
```

**Notification Teams (via l'orchestrateur) :**
```json
{
  "type": "AdaptiveCard",
  "body": [
    {"type": "TextBlock", "text": "Quality Gate — Migration Harry2", "weight": "Bolder"},
    {"type": "TextBlock", "text": "Table: silver_fact_ventes"},
    {"type": "FactSet", "facts": [
      {"title": "Lignes Bronze", "value": "25,000,000"},
      {"title": "Lignes Silver", "value": "24,987,523"},
      {"title": "Perte", "value": "0.05%"},
      {"title": "ISBN invalides", "value": "1.3% > seuil 1%"}
    ]},
    {"type": "TextBlock", "text": "Action requise : ISBN invalides au-dessus du seuil"}
  ],
  "actions": [
    {"type": "Action.Submit", "title": "Approuver", "data": {"decision": "APPROVE"}},
    {"type": "Action.Submit", "title": "Rejeter", "data": {"decision": "REJECT"}}
  ]
}
```

---

#### Agent 5 : Gold Modeler (Silver → Gold + V-Order)

| Attribut | Valeur |
|----------|--------|
| **Type** | Prompt Agent + Function Calling |
| **Rôle** | Construit le modèle dimensionnel Gold. Active V-Order sur les tables de faits (ex-BLU). Crée les relations. |
| **Outils** | Spark SQL, Delta Lake API, Fabric Semantic Model API |
| **Input** | Tables Silver + manifeste (flag is_blu_source) |
| **Output** | Tables Gold optimisées + Semantic Model Power BI |

```python
# Agent Gold Modeler — Modélisation dimensionnelle + V-Order automatique
def build_gold_table(silver_table: str, extraction_plan_item: dict):
    """
    Agent Gold : construit la table Gold avec les optimisations appropriées.
    Si la table source était BLU → active V-Order + Z-Order automatiquement.
    """
    gold_table = silver_table.replace("silver_", "gold_")
    df = spark.table(silver_table)
    
    use_vorder = extraction_plan_item.get("target_vorder", False)
    
    writer = df.write.format("delta").mode("overwrite")
    if use_vorder:
        writer = writer.option("parquet.vorder.enabled", "true")
    writer.saveAsTable(gold_table)
    
    if use_vorder:
        zorder_cols = identify_zorder_columns(extraction_plan_item)
        if zorder_cols:
            zorder_str = ", ".join(zorder_cols)
            spark.sql(f"OPTIMIZE {gold_table} ZORDER BY ({zorder_str}) VORDER")
    
    return {"table": gold_table, "vorder": use_vorder, "rows": df.count()}

def identify_zorder_columns(plan_item: dict) -> list:
    """Identifie les meilleures colonnes Z-Order selon le type de table."""
    table_name = plan_item["table"].upper()
    zorder_map = {
        "FACT_VENTES":    ["date_vente", "isbn13"],
        "FACT_STOCKS":    ["date_stock", "code_entrepot"],
        "FACT_COMMANDES": ["date_commande", "code_editeur"],
        "FACT_DROITS":    ["periode", "code_auteur"],
    }
    return zorder_map.get(table_name, [])
```

---

#### Agent 6 : Monitoring (Observabilité continue)

| Attribut | Valeur |
|----------|--------|
| **Type** | Prompt Agent — always-on |
| **Rôle** | Surveille la santé des pipelines, la freshness des données Gold, les anomalies de volume |
| **Outils** | Fabric REST API, Delta Lake history, Teams Notifications |
| **Output** | Dashboard de santé + alertes proactives |

```python
# Agent Monitoring — Surveillance continue post-migration
def check_data_freshness() -> dict:
    """
    Agent Monitoring : vérifie que les données Gold sont à jour.
    Alerte si le dernier chargement date de plus de 24h.
    """
    from datetime import datetime, timedelta
    
    gold_tables = ["gold_fact_ventes", "gold_fact_stocks", 
                   "gold_fact_commandes", "gold_fact_droits"]
    
    health_report = {"timestamp": datetime.now().isoformat(), "tables": []}
    
    for table in gold_tables:
        history = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()
        last_update = history[0]["timestamp"] if history else None
        
        is_stale = False
        if last_update:
            age_hours = (datetime.now() - last_update).total_seconds() / 3600
            is_stale = age_hours > 24
        
        current_count = spark.table(table).count()
        
        health_report["tables"].append({
            "table": table,
            "last_update": str(last_update),
            "age_hours": round(age_hours, 1) if last_update else None,
            "is_stale": is_stale,
            "row_count": current_count,
            "status": "HEALTHY" if not is_stale else "STALE"
        })
    
    return health_report
```

### 9.4 Flux d'orchestration séquentiel complet

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│ AGENT 1  │     │ AGENT 2  │     │ AGENT 3  │     │ AGENT 4  │     │ AGENT 5  │     │ AGENT 6  │
│ DISCOVERY│────▶│EXTRACTION│────▶│TRANSFORM │────▶│ QUALITY  │────▶│  GOLD    │────▶│ MONITOR  │
│          │     │          │     │          │     │  GATE    │     │ MODELER  │     │          │
│ Inventaire│     │ Harry2 → │     │ Bronze → │     │ Contrôle │     │ Silver → │     │ Santé    │
│ tables   │     │ Bronze   │     │ Silver   │     │ qualité  │     │ Gold +   │     │ données  │
│          │     │(parallel)│     │ nettoyage│     │          │     │ V-Order  │     │ continu  │
└──────────┘     └──────────┘     └──────────┘     └─────┬────┘     └──────────┘     └──────────┘
                                                         │
                                                    ┌────▼─────┐
                                                    │ HUMAIN   │
                                                    │ Validation│
                                                    │ via Teams │
                                                    └──────────┘
```

### 9.5 Parallélisation entre agents

L'**Agent Extraction** est le seul qui opère en parallèle massif (extraction multi-tables). Les autres fonctionnent en séquence avec handoff :

| Phase | Pattern | Parallélisme | Commentaire |
|-------|---------|-------------|-------------|
| Discovery → Extraction | Sequential | 1 agent | Le manifeste doit être complet avant extraction |
| Extraction (intra) | **Concurrent** | **N agents** | Chaque table est extraite en parallèle |
| Extraction → Transform | Handoff | Par table | Dès qu'une table Bronze est prête → Transform |
| Transform → Quality | Sequential | 1 agent/table | Chaque table passe le quality gate |
| Quality → Gold | **Handoff conditionnel** | 1 agent | Handoff seulement si PASS ou APPROVE humain |
| Gold → Monitor | Always-on | 1 agent | Monitoring continu post-migration |

### 9.6 Stack technologique multi-agent

| Composant | Technologie | Rôle |
|-----------|-------------|------|
| **Orchestrateur** | Microsoft Foundry Agent Service (Workflow Agent) | Coordination, séquencement, gestion d'erreurs |
| **Agents spécialisés** | Foundry Prompt Agents + Function Calling | Logique métier de chaque étape |
| **Agent Extraction** | Hosted Agent (Container) ou Fabric Pipeline | Extraction parallèle lourde |
| **LLM** | GPT-4o (via Foundry Model Catalog) | Raisonnement, décision de routage, mapping types |
| **Pipelines data** | Fabric Data Factory Pipelines | Exécution ODBC, Copy Activity |
| **Compute** | Fabric Spark (Notebooks / SJD) | Transformations PySpark |
| **Stockage** | Fabric Lakehouse (OneLake / Delta) | Bronze → Silver → Gold |
| **Notification** | Microsoft Teams (Adaptive Cards) | Human-in-the-loop validations |
| **Observabilité** | Application Insights + Fabric Monitor | Tracing agent, métriques pipeline |
| **Semantic Kernel** | SDK Python / .NET | Plugins, function calling, orchestration code |

### 9.7 Déploiement accéléré (8 semaines)

En parallélisant le développement des agents avec les phases data, on passe de 15 à **8 semaines** :

```
Semaine       1       2       3       4       5       6       7       8
              ├───────┤
Sprint 1      ███████ Discovery + Extraction (PoC 3 tables)
              ├───────┤
  (parallèle) ███████ Scaffolding Transform + Quality agents
                      ├───────────┤
Sprint 2              █████████████ Transform + Quality Gate (toutes tables)
                      ├───────┤
  (parallèle)         ███████ Développement Agent Gold + Monitoring
                                    ├───────────┤
Sprint 3                            █████████████ Gold + V-Order + Power BI
                                    ├───────┤
  (parallèle)                       ███████ Orchestrateur Foundry complet
                                                  ├───────┤
Sprint 4                                          ███████ Industrialisation + go-live
```

| Sprint | Durée | Track Data | Track Agent (parallèle) |
|--------|-------|-----------|------------------------|
| **Sprint 1** | 2 sem. | Discovery + PoC Extraction (3 tables) | Scaffolding agents Transform + Quality |
| **Sprint 2** | 2 sem. | Transform + Quality Gate (toutes tables) | Développement Agent Gold + Monitoring |
| **Sprint 3** | 2 sem. | Gold + V-Order + Power BI | Orchestrateur Foundry + intégration |
| **Sprint 4** | 2 sem. | Tests end-to-end + go-live | Monitoring continu + alertes Teams |

**Gain de temps : 15 → 8 semaines** grâce à :
- **2 tracks parallèles** : Track Data (pipeline) + Track Agent (développement)
- **Flux continu table par table** : pas d'attente batch entre phases
- **Quality Gate auto-approve** par défaut (notification humaine seulement sur FAIL)
- **Agents réutilisables** : le code des agents Sprint N sert de base au Sprint N+1

---

## 📚 Annexes

### 💜 A. Ressources Microsoft Fabric

- 📖 [Lakehouse Overview](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview)
- 📥 [Load data into Lakehouse](https://learn.microsoft.com/en-us/fabric/data-engineering/load-data-lakehouse)
- 🔌 [ODBC Connector](https://learn.microsoft.com/en-us/fabric/data-factory/connector-odbc)
- ⚙️ [Data Factory Pipelines](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-landing-page)
- 🔗 [OneLake Shortcuts](https://learn.microsoft.com/en-us/fabric/onelake/create-adls-shortcut)
- ⚡ [V-Order Optimization](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order)

### 💻 B. Ressources IBM i

- 📘 IBM i Access ODBC Driver documentation
- 📝 IBM i Journal-based CDC
- 📚 DB2 for i SQL Reference

### 🤖 C. Ressources Multi-Agent

- 🧠 [Microsoft Foundry Agent Service](https://learn.microsoft.com/en-us/azure/foundry/agents/overview)
- 🏛️ [Semantic Kernel Agent Architecture](https://learn.microsoft.com/en-us/semantic-kernel/frameworks/agent/agent-architecture)
- 🔄 [Agent Orchestration Patterns](https://learn.microsoft.com/en-us/semantic-kernel/frameworks/agent/agent-orchestration/)
- 📡 [Workflow Agents](https://learn.microsoft.com/en-us/azure/foundry/agents/concepts/workflow)

---

<div align="center">

📦 **Migration IBM Harry2 → Microsoft Fabric Lakehouse** | v2.0 | Mars 2026

![IBM](https://img.shields.io/badge/IBM%20i-AS%2F400-054ADA?style=flat-square&logo=ibm&logoColor=white)
![Arrow](https://img.shields.io/badge/→-migration-lightgrey?style=flat-square)
![Fabric](https://img.shields.io/badge/Microsoft%20Fabric-Lakehouse-742774?style=flat-square&logo=microsoftazure&logoColor=white)
![Delta](https://img.shields.io/badge/Delta%20Lake-V--Order-00ADD8?style=flat-square)
![Copilot](https://img.shields.io/badge/Copilot-Accelerated-24292e?style=flat-square&logo=github&logoColor=white)

*Document généré avec GitHub Copilot — Équipe Data & Analytics*

</div>
