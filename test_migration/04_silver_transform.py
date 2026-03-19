# Databricks notebook source / Fabric Notebook
# ============================================================================
# HARRY2 Migration — Étape 2 : Transformation Bronze → Silver
# ============================================================================
# Ce notebook applique toutes les transformations spécifiques IBM i :
#   1. Conversion dates CYYMMDD → ISO 8601 (YYYY-MM-DD)
#   2. Trim des champs CHAR à longueur fixe
#   3. Renommage colonnes IBM i → snake_case lisible
#   4. Typage explicite (DECIMAL → proper types)
#   5. Validation ISBN-13 (checksum)
#   6. Déduplication par clés naturelles
#
# En production, la conversion EBCDIC CCSID 297 → UTF-8 est gérée
# par le driver ODBC IBM i Access. Ici les données sont déjà en UTF-8.
# ============================================================================

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, DateType, BooleanType

spark = SparkSession.builder.getOrCreate()

# ──────────────────────────────────────────────
# Fonctions utilitaires de conversion IBM i
# ──────────────────────────────────────────────

def cyymmdd_to_date(col_name):
    """
    Convertit une date IBM i au format CYYMMDD (DECIMAL(7,0)) en DateType.
    C = siècle : 0 = 19xx, 1 = 20xx
    Exemples : 1250315 → 2025-03-15, 0780301 → 1978-03-01
    """
    return F.to_date(
        F.concat(
            # Année = (C + 19) * 100 + AA
            (F.floor(F.col(col_name) / 10000) + 19).cast("int").cast("string"),
            F.lpad((F.floor(F.col(col_name) % 10000 / 100)).cast("int").cast("string"), 2, "0"),
            F.lpad((F.col(col_name) % 100).cast("int").cast("string"), 2, "0")
        ),
        "yyyyMMdd"
    )


@F.udf(returnType=BooleanType())
def isbn13_is_valid(isbn):
    """
    Valide un ISBN-13 en vérifiant le checksum.
    Algorithme : somme pondérée (1,3,1,3,...) modulo 10 doit être 0.
    """
    if isbn is None or len(isbn.strip()) != 13:
        return False
    isbn = isbn.strip()
    if not isbn.isdigit():
        return False
    total = sum(int(d) * (1 if i % 2 == 0 else 3) for i, d in enumerate(isbn))
    return total % 10 == 0


# ══════════════════════════════════════════════
# Silver : Éditeurs
# ══════════════════════════════════════════════
print("🔄 Transformation bronze_editeurs → silver_editeurs...")

df_ed = spark.read.table("bronze_editeurs")
df_silver_editeurs = (df_ed
    .withColumn("code_editeur", F.trim(F.col("EDCODE")))
    .withColumn("nom_editeur", F.trim(F.col("EDNOM")))
    .withColumn("imprint", F.trim(F.col("EDIMPR")))
    .withColumn("groupe", F.trim(F.col("EDGRPE")))
    .withColumn("pays", F.trim(F.col("EDPAYS")))
    .withColumn("statut", F.trim(F.col("EDSTAT")))
    .select("code_editeur", "nom_editeur", "imprint", "groupe", "pays", "statut")
    .dropDuplicates(["code_editeur"])
)

df_silver_editeurs.write.format("delta").mode("overwrite").saveAsTable("silver_editeurs")
print(f"✅ silver_editeurs : {df_silver_editeurs.count()} lignes")


# ══════════════════════════════════════════════
# Silver : Auteurs
# ══════════════════════════════════════════════
print("\n🔄 Transformation bronze_auteurs → silver_auteurs...")

df_au = spark.read.table("bronze_auteurs")
df_silver_auteurs = (df_au
    .withColumn("code_auteur", F.trim(F.col("AUCODE")))
    .withColumn("nom", F.trim(F.col("AUNOM")))
    .withColumn("prenom", F.trim(F.col("AUPRN")))
    .withColumn("pseudonyme", F.trim(F.col("AUPSEUD")))
    .withColumn("nationalite", F.trim(F.col("AUNATI")))
    .withColumn("date_naissance", cyymmdd_to_date("AUDTNS"))
    .withColumn("date_deces", cyymmdd_to_date("AUDTDC"))
    .withColumn("date_creation_fiche", cyymmdd_to_date("AUDTCR"))
    .withColumn("statut", F.trim(F.col("AUSTAT")))
    .select(
        "code_auteur", "nom", "prenom", "pseudonyme",
        "nationalite", "date_naissance", "date_deces",
        "date_creation_fiche", "statut"
    )
    .dropDuplicates(["code_auteur"])
)

df_silver_auteurs.write.format("delta").mode("overwrite").saveAsTable("silver_auteurs")
print(f"✅ silver_auteurs : {df_silver_auteurs.count()} lignes")

# Vérification accents (test EBCDIC)
print("   🔍 Test accents EBCDIC CCSID 297 :")
df_silver_auteurs.filter(
    F.col("nom").contains("é") |
    F.col("prenom").contains("é") |
    F.col("nom").contains("í") |
    F.col("nom").contains("á")
).select("code_auteur", "nom", "prenom").show(truncate=False)


# ══════════════════════════════════════════════
# Silver : Ouvrages
# ══════════════════════════════════════════════
print("\n🔄 Transformation bronze_ouvrages → silver_ouvrages...")

df_ov = spark.read.table("bronze_ouvrages")
df_silver_ouvrages = (df_ov
    .withColumn("isbn", F.trim(F.col("OVISBN")))
    .withColumn("titre", F.trim(F.col("OVTITR")))
    .withColumn("sous_titre", F.trim(F.col("OVSTIT")))
    .withColumn("code_editeur", F.trim(F.col("OVEDIT")))
    .withColumn("code_collection", F.trim(F.col("OVCOLL")))
    .withColumn("format", F.trim(F.col("OVFMT")))
    .withColumn("langue", F.trim(F.col("OVLANG")))
    .withColumn("nb_pages", F.col("OVNBPG"))
    .withColumn("prix_ht", F.col("OVPXHT").cast("decimal(9,2)"))
    .withColumn("prix_ttc", F.col("OVPXTTC").cast("decimal(9,2)"))
    .withColumn("taux_tva", F.col("OVTVA").cast("decimal(5,2)"))
    .withColumn("date_publication", cyymmdd_to_date("OVDTPB"))
    .withColumn("date_creation", cyymmdd_to_date("OVDTCR"))
    .withColumn("date_maj", cyymmdd_to_date("OVDTMJ"))
    .withColumn("statut", F.trim(F.col("OVSTAT")))
    .withColumn("tirage_initial", F.col("OVTIRG"))
    .withColumn("poids_grammes", F.col("OVPOID"))
    .withColumn("ean", F.trim(F.col("OVEAN")))
    .withColumn("commentaire", F.trim(F.col("OVCOMM")))
    # Validation ISBN
    .withColumn("isbn_valide", isbn13_is_valid(F.col("isbn")))
    .select(
        "isbn", "titre", "sous_titre", "code_editeur", "code_collection",
        "format", "langue", "nb_pages", "prix_ht", "prix_ttc", "taux_tva",
        "date_publication", "date_creation", "date_maj", "statut",
        "tirage_initial", "poids_grammes", "ean", "commentaire", "isbn_valide"
    )
    .dropDuplicates(["isbn"])
)

df_silver_ouvrages.write.format("delta").mode("overwrite").saveAsTable("silver_ouvrages")
print(f"✅ silver_ouvrages : {df_silver_ouvrages.count()} lignes")

# Rapport ISBN
isbn_invalid = df_silver_ouvrages.filter(~F.col("isbn_valide")).count()
isbn_total = df_silver_ouvrages.count()
print(f"   ⚠️ ISBN invalides : {isbn_invalid}/{isbn_total} ({isbn_invalid/isbn_total*100:.1f}%)")
df_silver_ouvrages.filter(~F.col("isbn_valide")).select("isbn", "titre").show(truncate=False)


# ══════════════════════════════════════════════
# Silver : Ventes
# ══════════════════════════════════════════════
print("\n🔄 Transformation bronze_ventes → silver_ventes...")

df_ve = spark.read.table("bronze_ventes")
df_silver_ventes = (df_ve
    .withColumn("num_mouvement", F.col("VENUMR"))
    .withColumn("isbn", F.trim(F.col("VEISBN")))
    .withColumn("date_vente", cyymmdd_to_date("VEDTVT"))
    .withColumn("quantite", F.col("VEQTE"))
    .withColumn("montant_ht", F.col("VEMTHT").cast("decimal(11,2)"))
    .withColumn("montant_ttc", F.col("VEMTTTC").cast("decimal(11,2)"))
    .withColumn("canal", F.trim(F.col("VECANL")))
    .withColumn("code_client", F.trim(F.col("VECLIE")))
    .withColumn("code_entrepot", F.trim(F.col("VEENTR")))
    .withColumn("date_reglement", cyymmdd_to_date("VEDTRG"))
    .withColumn("type_mouvement",
        F.when(F.col("VESTAT") == "V", "Vente")
         .when(F.col("VESTAT") == "R", "Retour")
         .when(F.col("VESTAT") == "A", "Avoir")
         .otherwise("Inconnu"))
    .withColumn("isbn_valide", isbn13_is_valid(F.trim(F.col("VEISBN"))))
    .select(
        "num_mouvement", "isbn", "date_vente", "quantite",
        "montant_ht", "montant_ttc", "canal", "code_client",
        "code_entrepot", "date_reglement", "type_mouvement", "isbn_valide"
    )
)

df_silver_ventes.write.format("delta").mode("overwrite").saveAsTable("silver_ventes")
print(f"✅ silver_ventes : {df_silver_ventes.count()} lignes")


# ══════════════════════════════════════════════
# Silver : Stocks
# ══════════════════════════════════════════════
print("\n🔄 Transformation bronze_stocks → silver_stocks...")

df_sk = spark.read.table("bronze_stocks")
df_silver_stocks = (df_sk
    .withColumn("isbn", F.trim(F.col("SKISBN")))
    .withColumn("code_entrepot", F.trim(F.col("SKENTR")))
    .withColumn("date_position", cyymmdd_to_date("SKDTPS"))
    .withColumn("qte_disponible", F.col("SKQTDS"))
    .withColumn("qte_reservee", F.col("SKQTRS"))
    .withColumn("qte_transit", F.col("SKQTTR"))
    .withColumn("qte_pilonnee", F.col("SKQTPI"))
    .withColumn("qte_totale", F.col("SKQTDS") + F.col("SKQTRS") + F.col("SKQTTR"))
    .select(
        "isbn", "code_entrepot", "date_position",
        "qte_disponible", "qte_reservee", "qte_transit",
        "qte_pilonnee", "qte_totale"
    )
)

df_silver_stocks.write.format("delta").mode("overwrite").saveAsTable("silver_stocks")
print(f"✅ silver_stocks : {df_silver_stocks.count()} lignes")


# ══════════════════════════════════════════════
# Résumé de la transformation Silver
# ══════════════════════════════════════════════
print("\n" + "=" * 60)
print("📊 RÉSUMÉ TRANSFORMATION BRONZE → SILVER")
print("=" * 60)
for table in ["silver_editeurs", "silver_auteurs", "silver_ouvrages", "silver_ventes", "silver_stocks"]:
    count = spark.read.table(table).count()
    print(f"   {table:25s} : {count:>8,} lignes")
print("=" * 60)
