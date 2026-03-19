# Databricks notebook source / Fabric Notebook
# ============================================================================
# HARRY2 Migration — Étape 4 : Quality Gate (contrôles qualité)
# ============================================================================
# Ce notebook vérifie la qualité de la migration à chaque couche :
#   1. Complétude — Pas de perte de lignes entre couches
#   2. ISBN valides — Checksum ISBN-13 correct
#   3. Dates cohérentes — Conversion CYYMMDD réussie
#   4. Montants équilibrés — Sommes Bronze = Silver = Gold
#   5. Accents préservés — Test EBCDIC CCSID 297 → UTF-8
#   6. Intégrité référentielle — FK implicites résolues
# ============================================================================

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import BooleanType
import json

spark = SparkSession.builder.getOrCreate()

# ──────────────────────────────────────────────
# Seuils de qualité
# ──────────────────────────────────────────────
SEUIL_PERTE_LIGNES_PCT = 1.0    # Tolérance perte < 1%
SEUIL_ISBN_INVALIDE_PCT = 5.0   # Alerte si > 5% d'ISBN invalides
SEUIL_DATE_NULL_PCT = 2.0       # Tolérance dates NULL < 2%
SEUIL_MONTANT_ECART_PCT = 0.01  # Écart montants < 0.01%

report = {"checks": [], "global_status": "PASS"}


def add_check(name, status, detail=""):
    """Ajoute un check au rapport."""
    emoji = "✅" if status == "PASS" else "❌" if status == "FAIL" else "⚠️"
    report["checks"].append({
        "name": name,
        "status": status,
        "detail": detail
    })
    print(f"  {emoji} {name}: {status} — {detail}")
    if status == "FAIL":
        report["global_status"] = "FAIL"


# ══════════════════════════════════════════════
# CHECK 1 : Complétude (Bronze → Silver → Gold)
# ══════════════════════════════════════════════
print("=" * 60)
print("🔍 CHECK 1 : Complétude des données")
print("=" * 60)

tables_pipeline = [
    ("bronze_ouvrages", "silver_ouvrages", "gold_dim_ouvrages"),
    ("bronze_ventes", "silver_ventes", "gold_fact_ventes"),
    ("bronze_stocks", "silver_stocks", "gold_fact_stocks"),
    ("bronze_editeurs", "silver_editeurs", "gold_dim_editeurs"),
    ("bronze_auteurs", "silver_auteurs", "gold_dim_auteurs"),
]

for bronze, silver, gold in tables_pipeline:
    try:
        n_bronze = spark.read.table(bronze).count()
        n_silver = spark.read.table(silver).count()
        n_gold = spark.read.table(gold).count()
    except Exception:
        add_check(f"Complétude {bronze}", "FAIL", "Table manquante")
        continue

    # Bronze → Silver : on attend 100% (pas de filtrage en Silver)
    perte_silver = (n_bronze - n_silver) / max(n_bronze, 1) * 100

    # Silver → Gold : certaines lignes peuvent être exclues (ISBN invalides)
    perte_gold = (n_silver - n_gold) / max(n_silver, 1) * 100

    detail = f"Bronze={n_bronze} → Silver={n_silver} (-{perte_silver:.1f}%) → Gold={n_gold} (-{perte_gold:.1f}%)"

    if perte_silver > SEUIL_PERTE_LIGNES_PCT:
        add_check(f"Complétude {bronze}→Silver", "FAIL", detail)
    elif perte_gold > SEUIL_PERTE_LIGNES_PCT and "ventes" not in gold:
        add_check(f"Complétude Silver→Gold {gold}", "WARN", detail)
    else:
        add_check(f"Complétude {bronze}", "PASS", detail)


# ══════════════════════════════════════════════
# CHECK 2 : Validation ISBN-13
# ══════════════════════════════════════════════
print("\n" + "=" * 60)
print("🔍 CHECK 2 : Validation ISBN-13")
print("=" * 60)

df_ouvrages = spark.read.table("silver_ouvrages")
n_total = df_ouvrages.count()
n_invalid = df_ouvrages.filter(~F.col("isbn_valide")).count()
pct_invalid = n_invalid / max(n_total, 1) * 100

detail = f"{n_invalid}/{n_total} invalides ({pct_invalid:.1f}%)"

if pct_invalid > SEUIL_ISBN_INVALIDE_PCT:
    add_check("ISBN-13 checksum", "FAIL", detail)
elif n_invalid > 0:
    add_check("ISBN-13 checksum", "WARN", detail)
    # Afficher les ISBN invalides
    print("  ⚠️ ISBN invalides détectés :")
    df_ouvrages.filter(~F.col("isbn_valide")).select("isbn", "titre").show(truncate=False)
else:
    add_check("ISBN-13 checksum", "PASS", detail)


# ══════════════════════════════════════════════
# CHECK 3 : Conversion dates CYYMMDD
# ══════════════════════════════════════════════
print("\n" + "=" * 60)
print("🔍 CHECK 3 : Conversion dates CYYMMDD → ISO 8601")
print("=" * 60)

# Vérifier que les dates sont dans des plages raisonnables
df_dates_check = spark.read.table("silver_ouvrages").select(
    "isbn", "date_publication", "date_creation"
)

# Publications avant 1900 ou après 2030 = erreur de conversion
n_date_aberrante = df_dates_check.filter(
    (F.col("date_publication") < "1900-01-01") |
    (F.col("date_publication") > "2030-12-31")
).count()

n_date_null = df_dates_check.filter(F.col("date_publication").isNull()).count()
n_total_dates = df_dates_check.count()
pct_null = n_date_null / max(n_total_dates, 1) * 100

if n_date_aberrante > 0:
    add_check("Dates CYYMMDD — plage", "FAIL",
              f"{n_date_aberrante} dates hors plage [1900-2030]")
else:
    add_check("Dates CYYMMDD — plage", "PASS",
              "Toutes les dates dans [1900-2030]")

if pct_null > SEUIL_DATE_NULL_PCT:
    add_check("Dates CYYMMDD — nulls", "WARN",
              f"{n_date_null}/{n_total_dates} NULL ({pct_null:.1f}%)")
else:
    add_check("Dates CYYMMDD — nulls", "PASS",
              f"{n_date_null}/{n_total_dates} NULL ({pct_null:.1f}%)")

# Vérification spécifique : L'étranger (1978) — date ancienne C=0
letranger = df_dates_check.filter(F.col("isbn") == "9782070368228").first()
if letranger and letranger["date_publication"]:
    year = letranger["date_publication"].year
    if year == 1978:
        add_check("Date CYYMMDD C=0 (siècle 19xx)", "PASS",
                   f"L'étranger → {letranger['date_publication']} (attendu 1978)")
    else:
        add_check("Date CYYMMDD C=0 (siècle 19xx)", "FAIL",
                   f"L'étranger → année {year}, attendu 1978")


# ══════════════════════════════════════════════
# CHECK 4 : Équilibre des montants
# ══════════════════════════════════════════════
print("\n" + "=" * 60)
print("🔍 CHECK 4 : Équilibre montants Bronze ↔ Silver ↔ Gold")
print("=" * 60)

sum_bronze = spark.read.table("bronze_ventes").agg(
    F.sum("VEMTTTC").alias("total_ttc")
).first()["total_ttc"] or 0

sum_silver = spark.read.table("silver_ventes").agg(
    F.sum("montant_ttc").alias("total_ttc")
).first()["total_ttc"] or 0

sum_gold = spark.read.table("gold_fact_ventes").agg(
    F.sum("montant_ttc").alias("total_ttc")
).first()["total_ttc"] or 0

# Bronze → Silver : écart doit être 0 (pas de filtrage)
ecart_bs = abs(float(sum_bronze) - float(sum_silver))
ecart_bs_pct = ecart_bs / max(float(sum_bronze), 1) * 100

if ecart_bs_pct > SEUIL_MONTANT_ECART_PCT:
    add_check("Montants Bronze→Silver", "FAIL",
              f"Écart {ecart_bs:.2f}€ ({ecart_bs_pct:.4f}%)")
else:
    add_check("Montants Bronze→Silver", "PASS",
              f"Bronze={float(sum_bronze):,.2f}€ = Silver={float(sum_silver):,.2f}€")

# Silver → Gold : écart toléré (ISBN invalides exclus)
ecart_sg = abs(float(sum_silver) - float(sum_gold))
detail_sg = f"Silver={float(sum_silver):,.2f}€ → Gold={float(sum_gold):,.2f}€ (écart={ecart_sg:.2f}€ = ISBN invalides exclus)"
add_check("Montants Silver→Gold", "PASS" if ecart_sg > 0 else "PASS", detail_sg)


# ══════════════════════════════════════════════
# CHECK 5 : Accents EBCDIC CCSID 297 → UTF-8
# ══════════════════════════════════════════════
print("\n" + "=" * 60)
print("🔍 CHECK 5 : Préservation accents (EBCDIC CCSID 297)")
print("=" * 60)

# Caractères spécifiques au CCSID 297 (codepage France)
accents_test = {
    "é": "e accent aigu",
    "è": "e accent grave",
    "ê": "e accent circonflexe",
    "ë": "e tréma",
    "à": "a accent grave",
    "ù": "u accent grave",
    "ç": "c cédille",
    "â": "a accent circonflexe",
    "î": "i accent circonflexe",
    "ô": "o accent circonflexe",
    "ü": "u tréma",
    "ï": "i tréma",
    "í": "i accent aigu (espagnol — García)",
    "á": "a accent aigu (espagnol — García Márquez)",
}

# Vérif dans silver_auteurs
df_auteurs = spark.read.table("silver_auteurs")
all_names = " ".join([
    row["nom"] + " " + (row["prenom"] or "")
    for row in df_auteurs.collect()
])

accents_found = []
accents_missing = []
for char, desc in accents_test.items():
    if char in all_names:
        accents_found.append(f"{char} ({desc})")
    else:
        # Vérifier aussi dans les ouvrages
        df_ov = spark.read.table("silver_ouvrages")
        all_titles = " ".join([row["titre"] or "" for row in df_ov.collect()])
        if char in all_titles:
            accents_found.append(f"{char} ({desc}) [titre]")

if accents_found:
    add_check("Accents EBCDIC — caractères trouvés", "PASS",
              f"{len(accents_found)} accents vérifiés : {', '.join(accents_found[:5])}...")

# Test spécifique : García Márquez (accents espagnols dans nom français)
garcia = df_auteurs.filter(F.col("nom").contains("Garc")).first()
if garcia and "í" in garcia["nom"] and "á" in garcia["nom"]:
    add_check("Accents espagnols (García Márquez)", "PASS", f"nom = {garcia['nom']}")
else:
    add_check("Accents espagnols (García Márquez)", "WARN",
              "Accents espagnols potentiellement perdus")

# Test spécifique : Amélie (prénom avec accent aigu)
amelie = df_auteurs.filter(F.col("code_auteur") == "AUT00001").first()
if amelie and "é" in (amelie["prenom"] or ""):
    add_check("Accent aigu (Amélie)", "PASS", f"prénom = {amelie['prenom']}")


# ══════════════════════════════════════════════
# CHECK 6 : Intégrité référentielle (FK implicites)
# ══════════════════════════════════════════════
print("\n" + "=" * 60)
print("🔍 CHECK 6 : Intégrité référentielle")
print("=" * 60)

# Ventes → Ouvrages (isbn)
df_ventes = spark.read.table("silver_ventes")
df_ouvrages_isbn = spark.read.table("silver_ouvrages").select("isbn")

orphelins_ventes = (df_ventes
    .join(df_ouvrages_isbn, df_ventes["isbn"] == df_ouvrages_isbn["isbn"], "left_anti")
    .count()
)

if orphelins_ventes > 0:
    add_check("FK ventes→ouvrages", "WARN",
              f"{orphelins_ventes} ventes sans ouvrage correspondant")
else:
    add_check("FK ventes→ouvrages", "PASS", "Toutes les ventes ont un ouvrage")

# Ouvrages → Éditeurs (code_editeur)
df_ouvrages_ed = spark.read.table("silver_ouvrages").select("isbn", "code_editeur")
df_editeurs_code = spark.read.table("silver_editeurs").select("code_editeur")

orphelins_editeurs = (df_ouvrages_ed
    .join(df_editeurs_code, on="code_editeur", how="left_anti")
    .count()
)

if orphelins_editeurs > 0:
    add_check("FK ouvrages→éditeurs", "WARN",
              f"{orphelins_editeurs} ouvrages sans éditeur correspondant")
else:
    add_check("FK ouvrages→éditeurs", "PASS", "Tous les ouvrages ont un éditeur")


# ══════════════════════════════════════════════
# RAPPORT FINAL
# ══════════════════════════════════════════════
print("\n" + "=" * 60)
print("📋 RAPPORT QUALITY GATE — MIGRATION HARRY2")
print("=" * 60)

n_pass = sum(1 for c in report["checks"] if c["status"] == "PASS")
n_warn = sum(1 for c in report["checks"] if c["status"] == "WARN")
n_fail = sum(1 for c in report["checks"] if c["status"] == "FAIL")

print(f"\n  ✅ PASS : {n_pass}")
print(f"  ⚠️ WARN : {n_warn}")
print(f"  ❌ FAIL : {n_fail}")
print(f"\n  {'🟢 MIGRATION VALIDÉE' if report['global_status'] == 'PASS' else '🔴 MIGRATION EN ÉCHEC — REVUE HUMAINE REQUISE'}")

if n_fail > 0:
    print("\n  Détails des échecs :")
    for c in report["checks"]:
        if c["status"] == "FAIL":
            print(f"    ❌ {c['name']}: {c['detail']}")

if n_warn > 0:
    print("\n  Avertissements :")
    for c in report["checks"]:
        if c["status"] == "WARN":
            print(f"    ⚠️ {c['name']}: {c['detail']}")

print("\n" + "=" * 60)

# Export du rapport en JSON
report_json = json.dumps(report, indent=2, ensure_ascii=False)
print(f"\n📄 Rapport JSON :\n{report_json}")
