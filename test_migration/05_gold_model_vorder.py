# Databricks notebook source / Fabric Notebook
# ============================================================================
# HARRY2 Migration — Étape 3 : Modèle Gold (Dimensionnel) + V-Order
# ============================================================================
# Ce notebook construit le modèle en étoile (star schema) pour Power BI :
#   • dim_ouvrages — Dimension catalogue
#   • dim_auteurs  — Dimension auteurs (avec rôles)
#   • dim_editeurs — Dimension éditeurs + imprints
#   • dim_dates    — Calendrier éditorial
#   • fact_ventes  — Table de faits ventes (V-Order + Z-Order)
#   • fact_stocks  — Table de faits positions stock
#
# Optimisations Fabric Lakehouse :
#   • V-Order : activé sur les tables de faits (remplace BLU d'IBM DB2)
#   • Z-Order : appliqué sur date_vente pour le data skipping
#   • Partition : par année pour les tables volumineuses
# ============================================================================

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()

# ══════════════════════════════════════════════
# dim_dates — Calendrier éditorial
# ══════════════════════════════════════════════
print("🏗️ Construction dim_dates...")

# Générer un calendrier de 2005 à 2030
df_dates = (spark
    .range(0, 365 * 26)  # ~26 ans
    .withColumn("date_id", F.expr("date_add('2005-01-01', cast(id as int))"))
    .withColumn("annee", F.year("date_id"))
    .withColumn("mois", F.month("date_id"))
    .withColumn("jour", F.dayofmonth("date_id"))
    .withColumn("trimestre", F.quarter("date_id"))
    .withColumn("semestre", F.when(F.col("trimestre") <= 2, 1).otherwise(2))
    .withColumn("jour_semaine", F.dayofweek("date_id"))
    .withColumn("nom_jour", F.date_format("date_id", "EEEE"))
    .withColumn("nom_mois", F.date_format("date_id", "MMMM"))
    .withColumn("semaine_annee", F.weekofyear("date_id"))
    # Saisons éditoriales françaises
    .withColumn("saison_editoriale",
        F.when(F.col("mois").isin(1, 2, 3), "Hiver")
         .when(F.col("mois").isin(4, 5, 6), "Printemps")
         .when(F.col("mois").isin(8, 9), "Rentrée littéraire")
         .when(F.col("mois") == 7, "Été (prix littéraires)")
         .otherwise("Fin d'année (Noël)"))
    .withColumn("est_rentree_litteraire",
        (F.col("mois").isin(8, 9)).cast("boolean"))
    .withColumn("est_noel",
        (F.col("mois") == 12).cast("boolean"))
    .select(
        "date_id", "annee", "mois", "jour", "trimestre", "semestre",
        "jour_semaine", "nom_jour", "nom_mois", "semaine_annee",
        "saison_editoriale", "est_rentree_litteraire", "est_noel"
    )
)

df_dates.write.format("delta").mode("overwrite").saveAsTable("gold_dim_dates")
print(f"✅ gold_dim_dates : {df_dates.count()} lignes")


# ══════════════════════════════════════════════
# dim_editeurs — Éditeurs + groupes
# ══════════════════════════════════════════════
print("\n🏗️ Construction dim_editeurs...")

df_dim_editeurs = (spark.read.table("silver_editeurs")
    .filter(F.col("statut") == "A")
    .withColumn("editeur_key", F.monotonically_increasing_id())
    .select(
        "editeur_key", "code_editeur", "nom_editeur",
        "imprint", "groupe", "pays"
    )
)

df_dim_editeurs.write.format("delta").mode("overwrite").saveAsTable("gold_dim_editeurs")
print(f"✅ gold_dim_editeurs : {df_dim_editeurs.count()} lignes")


# ══════════════════════════════════════════════
# dim_auteurs — Auteurs avec enrichissement
# ══════════════════════════════════════════════
print("\n🏗️ Construction dim_auteurs...")

df_dim_auteurs = (spark.read.table("silver_auteurs")
    .withColumn("auteur_key", F.monotonically_increasing_id())
    .withColumn("nom_complet",
        F.when(F.col("pseudonyme").isNotNull(), F.col("pseudonyme"))
         .otherwise(F.concat_ws(" ", F.col("prenom"), F.col("nom"))))
    .withColumn("est_vivant", F.col("date_deces").isNull())
    .select(
        "auteur_key", "code_auteur", "nom", "prenom",
        "pseudonyme", "nom_complet", "nationalite",
        "date_naissance", "date_deces", "est_vivant", "statut"
    )
)

df_dim_auteurs.write.format("delta").mode("overwrite").saveAsTable("gold_dim_auteurs")
print(f"✅ gold_dim_auteurs : {df_dim_auteurs.count()} lignes")


# ══════════════════════════════════════════════
# dim_ouvrages — Catalogue enrichi
# ══════════════════════════════════════════════
print("\n🏗️ Construction dim_ouvrages...")

df_dim_ouvrages = (spark.read.table("silver_ouvrages")
    .filter(F.col("isbn_valide"))  # Exclut les ISBN invalides du Gold
    .withColumn("ouvrage_key", F.monotonically_increasing_id())
    .withColumn("libelle_format",
        F.when(F.col("format") == "GFT", "Grand format")
         .when(F.col("format") == "POC", "Poche")
         .when(F.col("format") == "NUM", "Numérique")
         .when(F.col("format") == "AUD", "Audio")
         .when(F.col("format") == "BDA", "BD / Album")
         .otherwise("Autre"))
    .withColumn("libelle_statut",
        F.when(F.col("statut") == "AC", "Actif")
         .when(F.col("statut") == "EP", "Épuisé")
         .when(F.col("statut") == "AN", "Annoncé")
         .when(F.col("statut") == "PI", "Pilonné")
         .otherwise("Autre"))
    .withColumn("tranche_prix",
        F.when(F.col("prix_ttc") < 10, "< 10 €")
         .when(F.col("prix_ttc") < 20, "10-20 €")
         .when(F.col("prix_ttc") < 30, "20-30 €")
         .otherwise("> 30 €"))
    .select(
        "ouvrage_key", "isbn", "titre", "sous_titre",
        "code_editeur", "code_collection",
        "format", "libelle_format", "langue",
        "nb_pages", "prix_ht", "prix_ttc", "taux_tva",
        "date_publication", "statut", "libelle_statut",
        "tirage_initial", "poids_grammes", "tranche_prix",
        "commentaire"
    )
)

df_dim_ouvrages.write.format("delta").mode("overwrite").saveAsTable("gold_dim_ouvrages")
print(f"✅ gold_dim_ouvrages : {df_dim_ouvrages.count()} lignes")


# ══════════════════════════════════════════════
# fact_ventes — Table de faits (V-Order + Z-Order)
# ══════════════════════════════════════════════
print("\n🏗️ Construction fact_ventes avec V-Order optimisation...")

df_fact_ventes = (spark.read.table("silver_ventes")
    .filter(F.col("isbn_valide"))  # Exclut les ventes sur ISBN invalides
    .withColumn("annee_vente", F.year("date_vente"))
    .withColumn("mois_vente", F.month("date_vente"))
    .select(
        "num_mouvement", "isbn", "date_vente", "annee_vente", "mois_vente",
        "quantite", "montant_ht", "montant_ttc",
        "canal", "code_client", "code_entrepot",
        "date_reglement", "type_mouvement"
    )
)

# Écriture avec V-Order (activé par défaut dans Fabric Lakehouse)
# En Fabric, V-Order est automatique. Sur Databricks, on utilise OPTIMIZE.
(df_fact_ventes
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_fact_ventes")
)

# Z-Order sur date_vente (data skipping — remplace le BLU synopsis tables)
# En Fabric, exécuter via SQL :
spark.sql("OPTIMIZE gold_fact_ventes ZORDER BY (date_vente)")

count_ventes = spark.read.table("gold_fact_ventes").count()
print(f"✅ gold_fact_ventes : {count_ventes} lignes (V-Order + Z-Order[date_vente])")

# Vérification : les ventes exclues (ISBN invalides)
excluded = spark.read.table("silver_ventes").filter(~F.col("isbn_valide")).count()
print(f"   ⚠️ Ventes exclues (ISBN invalide) : {excluded}")


# ══════════════════════════════════════════════
# fact_stocks — Positions de stock
# ══════════════════════════════════════════════
print("\n🏗️ Construction fact_stocks avec V-Order optimisation...")

df_fact_stocks = (spark.read.table("silver_stocks")
    .join(
        spark.read.table("gold_dim_ouvrages").select("isbn"),
        on="isbn",
        how="inner"  # Exclut les ISBN invalides (pas dans la dimension)
    )
    .select(
        "isbn", "code_entrepot", "date_position",
        "qte_disponible", "qte_reservee", "qte_transit",
        "qte_pilonnee", "qte_totale"
    )
)

(df_fact_stocks
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_fact_stocks")
)

spark.sql("OPTIMIZE gold_fact_stocks ZORDER BY (isbn)")

count_stocks = spark.read.table("gold_fact_stocks").count()
print(f"✅ gold_fact_stocks : {count_stocks} lignes (V-Order + Z-Order[isbn])")


# ══════════════════════════════════════════════
# Résumé du modèle Gold
# ══════════════════════════════════════════════
print("\n" + "=" * 60)
print("⭐ RÉSUMÉ MODÈLE GOLD (Star Schema)")
print("=" * 60)
gold_tables = {
    "Dimensions": ["gold_dim_dates", "gold_dim_editeurs", "gold_dim_auteurs", "gold_dim_ouvrages"],
    "Faits": ["gold_fact_ventes", "gold_fact_stocks"]
}

for category, tables in gold_tables.items():
    print(f"\n  📁 {category}:")
    for table in tables:
        count = spark.read.table(table).count()
        optimized = "⚡ V-Order + Z-Order" if "fact_" in table else ""
        print(f"     {table:25s} : {count:>10,} lignes  {optimized}")

print("\n" + "=" * 60)
print("🔗 Ces tables sont accessibles via le SQL Analytics Endpoint")
print("   pour Power BI, Excel, ou tout outil SQL/ODBC.")
print("=" * 60)
