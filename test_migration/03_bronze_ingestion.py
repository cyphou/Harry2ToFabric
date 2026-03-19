# Databricks notebook source / Fabric Notebook
# ============================================================================
# HARRY2 Migration — Étape 1 : Ingestion Bronze
# ============================================================================
# Ce notebook simule l'ingestion des tables Harry2 dans la couche Bronze
# du Lakehouse Fabric. En production, les données viendraient via ODBC
# (On-Premises Data Gateway → DB2 for i).
#
# Pour le test, on crée les DataFrames depuis des données en mémoire
# qui reproduisent fidèlement les spécificités IBM i :
#   • Noms de colonnes courts (10 car.)
#   • Dates CYYMMDD en DECIMAL(7,0)
#   • Montants en DECIMAL (packed decimal simulé)
#   • Champs CHAR paddés d'espaces
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, IntegerType
)
from datetime import datetime
import json

spark = SparkSession.builder.getOrCreate()

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
LAKEHOUSE_BRONZE_PATH = "Tables"  # En Fabric Lakehouse, les tables managées sont sous "Tables/"
EXTRACTION_TIMESTAMP = datetime.utcnow().isoformat()

# ──────────────────────────────────────────────
# Table : bronze_editeurs (EDITRP)
# ──────────────────────────────────────────────
editeurs_data = [
    ("GRASSET  ", "Éditions Grasset                                                                ", "Grasset                                                                         ", "GRPEDI01  ", "FR", "A"),
    ("FAYARD   ", "Librairie Arthème Fayard                                                        ", "Fayard                                                                          ", "GRPEDI01  ", "FR", "A"),
    ("STOCK    ", "Éditions Stock                                                                  ", "Stock                                                                           ", "GRPEDI01  ", "FR", "A"),
    ("CALMLVY  ", "Calmann-Lévy                                                                    ", "Calmann-Lévy                                                                    ", "GRPEDI01  ", "FR", "A"),
    ("LGENPOC  ", "Le Livre de Poche                                                               ", "LGF                                                                             ", "GRPEDI01  ", "FR", "A"),
    ("LAFFONT  ", "Éditions Robert Laffont                                                         ", "Robert Laffont                                                                  ", "EDITIS    ", "FR", "A"),
    ("GALLMRD  ", "Éditions Gallimard                                                              ", "Gallimard                                                                       ", "GALLMRD   ", "FR", "A"),
    ("FLAMMRN  ", "Flammarion                                                                      ", "Flammarion                                                                      ", "MADRIGALL ", "FR", "A"),
]

editeurs_schema = StructType([
    StructField("EDCODE", StringType(), False),
    StructField("EDNOM", StringType(), False),
    StructField("EDIMPR", StringType(), True),
    StructField("EDGRPE", StringType(), True),
    StructField("EDPAYS", StringType(), True),
    StructField("EDSTAT", StringType(), True),
])

df_editeurs = spark.createDataFrame(editeurs_data, editeurs_schema)
df_editeurs.write.format("delta").mode("overwrite").saveAsTable("bronze_editeurs")
print(f"✅ bronze_editeurs : {df_editeurs.count()} lignes")

# ──────────────────────────────────────────────
# Table : bronze_auteurs (AUTURP)
# ──────────────────────────────────────────────
auteurs_data = [
    ("AUT00001  ", "Nothomb                                                     ", "Amélie                                                      ", None, "BE", 660709, None, None, 1200101, "A"),
    ("AUT00002  ", "Houellebecq                                                 ", "Michel                                                      ", None, "FR", 580226, None, None, 1200101, "A"),
    ("AUT00003  ", "Musso                                                       ", "Guillaume                                                   ", None, "FR", 740628, None, None, 1200301, "A"),
    ("AUT00004  ", "Lévy                                                        ", "Marc                                                        ", None, "FR", 611016, None, None, 1200101, "A"),
    ("AUT00005  ", "Gavalda                                                     ", "Anna                                                        ", None, "FR", 701209, None, None, 1200601, "A"),
    ("AUT00006  ", "Modiano                                                     ", "Patrick                                                     ", None, "FR", 450730, None, None, 1200101, "A"),
    ("AUT00007  ", "Ernaux                                                      ", "Annie                                                       ", None, "FR", 400901, None, None, 1200101, "A"),
    ("AUT00008  ", "Despentes                                                   ", "Virginie                                                    ", None, "FR", 690613, None, None, 1200501, "A"),
    ("AUT00009  ", "García Márquez                                              ", "Gabriel                                                     ", "Gabo                                                                            ", "CO", 270306, 1140417, None, 1200101, "I"),
    ("AUT00010  ", "Durand                                                      ", "Claude                                                      ", None, "FR", 550115, None, None, 1200101, "A"),
]

auteurs_schema = StructType([
    StructField("AUCODE", StringType(), False),
    StructField("AUNOM", StringType(), False),
    StructField("AUPRN", StringType(), True),
    StructField("AUPSEUD", StringType(), True),
    StructField("AUNATI", StringType(), True),
    StructField("AUDTNS", IntegerType(), True),
    StructField("AUDTDC", IntegerType(), True),
    StructField("AUEMAIL", StringType(), True),
    StructField("AUDTCR", IntegerType(), True),
    StructField("AUSTAT", StringType(), True),
])

df_auteurs = spark.createDataFrame(auteurs_data, auteurs_schema)
df_auteurs.write.format("delta").mode("overwrite").saveAsTable("bronze_auteurs")
print(f"✅ bronze_auteurs : {df_auteurs.count()} lignes")

# ──────────────────────────────────────────────
# Table : bronze_ouvrages (OUVRGP)
# ──────────────────────────────────────────────
ouvrages_data = [
    ("9782246831471", "Premier sang", None, "GRASSET", "LITT_FR", "GFT", "FRE", 192, 18.00, 18.99, 5.50, 1211006, 1210901, 1250101, "AC", 80000, 280, "9782246831471", "Prix Renaudot 2021"),
    ("9782080287410", "Anéantir", None, "FLAMMRN", "LITT_FR", "GFT", "FRE", 736, 26.00, 27.43, 5.50, 1220107, 1211201, 1250101, "AC", 300000, 890, "9782080287410", "Roman de Houellebecq"),
    ("9782702183694", "L'inconnue de la Seine", None, "CALMLVY", "POL_CML", "GFT", "FRE", 480, 21.90, 23.10, 5.50, 1220413, 1220301, 1250201, "AC", 150000, 520, "9782702183694", "Thriller de Musso"),
    ("9782221267639", "C'est arrivé la nuit", None, "LAFFONT", "LITT_FR", "GFT", "FRE", 528, 22.00, 23.21, 5.50, 1211021, 1210801, 1250201, "AC", 200000, 600, "9782221267639", "Marc Lévy saga 9"),
    ("9782253237341", "Anéantir", None, "LGENPOC", "LDP_LIT", "POC", "FRE", 736, 9.40, 9.92, 5.50, 1230209, 1221201, 1250101, "AC", 500000, 380, "9782253237341", "Édition poche"),
    ("9782070368228", "L'étranger", None, "GALLMRD", "FOLIO", "POC", "FRE", 186, 7.50, 7.91, 5.50, 780301, 780101, 1250101, "AC", 10000000, 120, "9782070368228", "Camus 1942 — réédition Folio"),
    ("9782246834519", "Psychopompe", None, "GRASSET", "LITT_FR", "GFT", "FRE", 160, 18.00, 18.99, 5.50, 1230816, 1230601, 1250301, "AC", 100000, 220, "9782246834519", "Nothomb rentrée 2023"),
    ("9782070000000", "Titre fictif test QG", None, "GALLMRD", "BLNC_GL", "GFT", "FRE", 300, 20.00, 21.10, 5.50, 1240515, 1240401, 1250301, "AC", 5000, 350, "9782070000000", "ISBN INVALIDE pour test"),
    ("9782246819523", "Cent ans de solitude", None, "GRASSET", "LITT_ET", "GFT", "FRE", 470, 24.00, 25.32, 5.50, 1070301, 1060101, 1250101, "AC", 2000000, 510, "9782246819523", "García Márquez trad. française"),
]

ouvrages_schema = StructType([
    StructField("OVISBN", StringType(), False),
    StructField("OVTITR", StringType(), False),
    StructField("OVSTIT", StringType(), True),
    StructField("OVEDIT", StringType(), False),
    StructField("OVCOLL", StringType(), True),
    StructField("OVFMT", StringType(), False),
    StructField("OVLANG", StringType(), True),
    StructField("OVNBPG", IntegerType(), True),
    StructField("OVPXHT", DecimalType(9, 2), False),
    StructField("OVPXTTC", DecimalType(9, 2), False),
    StructField("OVTVA", DecimalType(5, 2), True),
    StructField("OVDTPB", IntegerType(), True),
    StructField("OVDTCR", IntegerType(), True),
    StructField("OVDTMJ", IntegerType(), True),
    StructField("OVSTAT", StringType(), True),
    StructField("OVTIRG", IntegerType(), True),
    StructField("OVPOID", IntegerType(), True),
    StructField("OVEAN", StringType(), True),
    StructField("OVCOMM", StringType(), True),
])

df_ouvrages = spark.createDataFrame(ouvrages_data, ouvrages_schema)
df_ouvrages.write.format("delta").mode("overwrite").saveAsTable("bronze_ouvrages")
print(f"✅ bronze_ouvrages : {df_ouvrages.count()} lignes")

# ──────────────────────────────────────────────
# Table : bronze_ventes (VENTEP) — table BLU simulée
# ──────────────────────────────────────────────
ventes_data = [
    (100001, "9782246831471", 1250110, 250, 4500.00, 4747.50, "LIB", "FNAC_PAR", "ENT01", 1250210, "V"),
    (100002, "9782246831471", 1250110, 100, 1800.00, 1899.00, "GMS", "LECLERC1", "ENT01", 1250210, "V"),
    (100003, "9782246831471", 1250111, 50, 900.00, 949.50, "WEB", "AMAZON_F", "ENT02", 1250211, "V"),
    (100004, "9782246831471", 1250115, -20, -360.00, -379.80, "LIB", "FNAC_PAR", "ENT01", None, "R"),
    (100005, "9782080287410", 1250112, 800, 20800.00, 21944.00, "LIB", "FNAC_PAR", "ENT01", 1250212, "V"),
    (100006, "9782080287410", 1250112, 500, 13000.00, 13715.00, "GMS", "CULTURA1", "ENT01", 1250212, "V"),
    (100007, "9782080287410", 1250113, 300, 7800.00, 8229.00, "WEB", "AMAZON_F", "ENT02", 1250213, "V"),
    (100008, "9782080287410", 1250120, -50, -1300.00, -1371.50, "GMS", "CULTURA1", "ENT01", None, "R"),
    (100009, "9782702183694", 1250115, 400, 8760.00, 9240.00, "LIB", "FNAC_LYO", "ENT01", 1250215, "V"),
    (100010, "9782702183694", 1250116, 200, 4380.00, 4620.00, "WEB", "FNAC_WEB", "ENT02", 1250216, "V"),
    (100011, "9782221267639", 1250117, 350, 7700.00, 8123.50, "LIB", "GIBERT_P", "ENT01", 1250217, "V"),
    (100012, "9782221267639", 1250118, 150, 3300.00, 3481.50, "GMS", "LECLERC1", "ENT01", 1250218, "V"),
    (100013, "9782253237341", 1250120, 1500, 14100.00, 14880.00, "GMS", "LECLERC1", "ENT01", 1250220, "V"),
    (100014, "9782253237341", 1250121, 2000, 18800.00, 19840.00, "WEB", "AMAZON_F", "ENT02", 1250221, "V"),
    (100015, "9782253237341", 1250122, 800, 7520.00, 7936.00, "LIB", "FNAC_PAR", "ENT01", 1250222, "V"),
    (100016, "9782253237341", 1250125, -100, -940.00, -992.00, "GMS", "LECLERC1", "ENT01", None, "R"),
    (100017, "9782070368228", 1250101, 3000, 22500.00, 23730.00, "LIB", "GIBERT_P", "ENT01", 1250201, "V"),
    (100018, "9782070368228", 1250103, 1500, 11250.00, 11865.00, "WEB", "AMAZON_F", "ENT02", 1250203, "V"),
    (100019, "9782070368228", 1250105, 2500, 18750.00, 19775.00, "GMS", "CULTURA1", "ENT01", 1250205, "V"),
    (100020, "9782246834519", 1250201, 600, 10800.00, 11394.00, "LIB", "FNAC_PAR", "ENT01", 1250301, "V"),
    (100021, "9782246834519", 1250203, 300, 5400.00, 5697.00, "WEB", "FNAC_WEB", "ENT02", 1250303, "V"),
    (100022, "9782246834519", 1250205, 400, 7200.00, 7596.00, "GMS", "LECLERC1", "ENT01", 1250305, "V"),
    (100023, "9782246819523", 1100615, 100, 2400.00, 2532.00, "LIB", "FNAC_PAR", "ENT01", 1100715, "V"),
    (100024, "9782246819523", 1100620, 50, 1200.00, 1266.00, "EXP", "EXPORT01", "ENT03", 1100720, "V"),
    (100025, "9782070000000", 1250301, 10, 200.00, 211.00, "LIB", "FNAC_PAR", "ENT01", None, "V"),
]

ventes_schema = StructType([
    StructField("VENUMR", IntegerType(), False),
    StructField("VEISBN", StringType(), False),
    StructField("VEDTVT", IntegerType(), False),
    StructField("VEQTE", IntegerType(), False),
    StructField("VEMTHT", DecimalType(11, 2), False),
    StructField("VEMTTTC", DecimalType(11, 2), False),
    StructField("VECANL", StringType(), False),
    StructField("VECLIE", StringType(), True),
    StructField("VEENTR", StringType(), True),
    StructField("VEDTRG", IntegerType(), True),
    StructField("VESTAT", StringType(), True),
])

df_ventes = spark.createDataFrame(ventes_data, ventes_schema)
df_ventes.write.format("delta").mode("overwrite").saveAsTable("bronze_ventes")
print(f"✅ bronze_ventes : {df_ventes.count()} lignes")

# ──────────────────────────────────────────────
# Table : bronze_stocks (STCKP)
# ──────────────────────────────────────────────
stocks_data = [
    ("9782246831471", "ENT01", 1250315, 12500, 500, 200, 300),
    ("9782080287410", "ENT01", 1250315, 45000, 2000, 1500, 0),
    ("9782080287410", "ENT02", 1250315, 8000, 500, 300, 0),
    ("9782702183694", "ENT01", 1250315, 22000, 600, 0, 500),
    ("9782253237341", "ENT01", 1250315, 180000, 5000, 3000, 0),
    ("9782253237341", "ENT02", 1250315, 60000, 2000, 1000, 0),
    ("9782070368228", "ENT01", 1250315, 500000, 0, 0, 10000),
    ("9782070368228", "ENT02", 1250315, 150000, 0, 0, 5000),
    ("9782246834519", "ENT01", 1250315, 35000, 1000, 800, 0),
    ("9782246819523", "ENT01", 1250315, 75000, 100, 0, 2000),
    ("9782070000000", "ENT01", 1250315, 5000, 0, 0, 0),
]

stocks_schema = StructType([
    StructField("SKISBN", StringType(), False),
    StructField("SKENTR", StringType(), False),
    StructField("SKDTPS", IntegerType(), False),
    StructField("SKQTDS", IntegerType(), True),
    StructField("SKQTRS", IntegerType(), True),
    StructField("SKQTTR", IntegerType(), True),
    StructField("SKQTPI", IntegerType(), True),
])

df_stocks = spark.createDataFrame(stocks_data, stocks_schema)
df_stocks.write.format("delta").mode("overwrite").saveAsTable("bronze_stocks")
print(f"✅ bronze_stocks : {df_stocks.count()} lignes")

# ──────────────────────────────────────────────
# Manifeste d'extraction Bronze
# ──────────────────────────────────────────────
manifeste = {
    "extraction_timestamp": EXTRACTION_TIMESTAMP,
    "source_system": "Harry2 / DB2 for i (simulation)",
    "tables": [
        {"name": "bronze_editeurs", "source": "EDITRP", "rows": df_editeurs.count(), "is_blu_source": False},
        {"name": "bronze_auteurs", "source": "AUTURP", "rows": df_auteurs.count(), "is_blu_source": False},
        {"name": "bronze_ouvrages", "source": "OUVRGP", "rows": df_ouvrages.count(), "is_blu_source": False},
        {"name": "bronze_ventes", "source": "VENTEP", "rows": df_ventes.count(), "is_blu_source": True},
        {"name": "bronze_stocks", "source": "STCKP", "rows": df_stocks.count(), "is_blu_source": False},
    ]
}
print("\n📋 Manifeste Bronze :")
print(json.dumps(manifeste, indent=2, default=str))
