-- ============================================================================
-- HARRY2 — Requêtes d'extraction DB2 for i (production)
-- ============================================================================
-- Ces requêtes sont prévues pour être exécutées sur le vrai système IBM i
-- via ODBC (IBM i Access ODBC Driver) à travers le On-Premises Data Gateway.
--
-- Elles peuvent aussi servir de base pour les Copy Activities de
-- Fabric Data Factory (source = ODBC, requête = SQL ci-dessous).
-- ============================================================================

-- ──────────────────────────────────────────────
-- 0. INVENTAIRE : Découverte automatique des tables Harry2
-- ──────────────────────────────────────────────
-- Lister toutes les tables de la bibliothèque HARRY2LIB
SELECT
    TABLE_NAME,
    TABLE_TYPE,
    COLUMN_COUNT,
    ROW_COUNT_ESTIMATE
FROM QSYS2.SYSTABLES
WHERE TABLE_SCHEMA = 'HARRY2LIB'
ORDER BY TABLE_NAME;

-- Lister les colonnes d'une table
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    LENGTH,
    NUMERIC_SCALE,
    NUMERIC_PRECISION,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    CCSID           -- Important ! CCSID 297 = France EBCDIC
FROM QSYS2.SYSCOLUMNS
WHERE TABLE_SCHEMA = 'HARRY2LIB'
  AND TABLE_NAME = 'OUVRGP'
ORDER BY ORDINAL_POSITION;

-- Détecter les tables BLU (column-organized)
SELECT
    TABLE_NAME,
    ORGANIZE_BY       -- 'COLUMN' = BLU, 'ROW' = classique
FROM QSYS2.SYSTABLES
WHERE TABLE_SCHEMA = 'HARRY2LIB'
  AND ORGANIZE_BY = 'COLUMN';


-- ──────────────────────────────────────────────
-- 1. EXTRACTION : Ouvrages (OUVRGP)
-- ──────────────────────────────────────────────
-- Extraction complète avec conversion CCSID explicite
SELECT
    OVISBN,
    CAST(OVTITR AS VARCHAR(200) CCSID 1208) AS OVTITR,    -- CCSID 1208 = UTF-8
    CAST(OVSTIT AS VARCHAR(200) CCSID 1208) AS OVSTIT,
    OVEDIT,
    OVCOLL,
    OVFMT,
    OVLANG,
    OVNBPG,
    OVPXHT,
    OVPXTTC,
    OVTVA,
    OVDTPB,
    OVDTCR,
    OVDTMJ,
    OVSTAT,
    OVTIRG,
    OVPOID,
    OVEAN,
    CAST(OVCOMM AS VARCHAR(500) CCSID 1208) AS OVCOMM
FROM HARRY2LIB.OUVRGP;


-- ──────────────────────────────────────────────
-- 2. EXTRACTION : Auteurs (AUTURP)
-- ──────────────────────────────────────────────
SELECT
    AUCODE,
    CAST(AUNOM  AS VARCHAR(60) CCSID 1208) AS AUNOM,
    CAST(AUPRN  AS VARCHAR(60) CCSID 1208) AS AUPRN,
    CAST(AUPSEUD AS VARCHAR(80) CCSID 1208) AS AUPSEUD,
    AUNATI,
    AUDTNS,
    AUDTDC,
    CAST(AUEMAIL AS VARCHAR(100) CCSID 1208) AS AUEMAIL,
    AUDTCR,
    AUSTAT
FROM HARRY2LIB.AUTURP;


-- ──────────────────────────────────────────────
-- 3. EXTRACTION : Éditeurs (EDITRP)
-- ──────────────────────────────────────────────
SELECT
    EDCODE,
    CAST(EDNOM  AS VARCHAR(80) CCSID 1208) AS EDNOM,
    CAST(EDIMPR AS VARCHAR(80) CCSID 1208) AS EDIMPR,
    EDGRPE,
    EDPAYS,
    EDSTAT
FROM HARRY2LIB.EDITRP;


-- ──────────────────────────────────────────────
-- 4. EXTRACTION : Ventes (VENTEP) — table BLU
-- ──────────────────────────────────────────────
-- NOTE: Même si VENTEP est ORGANIZE BY COLUMN (BLU),
-- l'extraction SQL retourne des lignes normalement.
-- Le driver ODBC gère la conversion de manière transparente.

-- Extraction incrémentale par date (recommandée pour le volume)
SELECT
    VENUMR,
    VEISBN,
    VEDTVT,
    VEQTE,
    VEMTHT,
    VEMTTTC,
    VECANL,
    VECLIE,
    VEENTR,
    VEDTRG,
    VESTAT
FROM HARRY2LIB.VENTEP
WHERE VEDTVT >= ?  -- Paramètre : dernière date extraite (CYYMMDD)
ORDER BY VEDTVT, VENUMR;

-- Extraction complète (initial load)
SELECT * FROM HARRY2LIB.VENTEP ORDER BY VENUMR;


-- ──────────────────────────────────────────────
-- 5. EXTRACTION : Commandes (CMDEP)
-- ──────────────────────────────────────────────
SELECT
    CMNUMR,
    CMISBN,
    CMDTCM,
    CMDTLV,
    CMQTE,
    CMQTLV,
    CMMTHT,
    CMCLIE,
    CMSTAT
FROM HARRY2LIB.CMDEP
ORDER BY CMNUMR;


-- ──────────────────────────────────────────────
-- 6. EXTRACTION : Stocks (STCKP)
-- ──────────────────────────────────────────────
-- Extraction de la position la plus récente uniquement
SELECT
    SKISBN,
    SKENTR,
    SKDTPS,
    SKQTDS,
    SKQTRS,
    SKQTTR,
    SKQTPI
FROM HARRY2LIB.STCKP s
WHERE SKDTPS = (
    SELECT MAX(s2.SKDTPS)
    FROM HARRY2LIB.STCKP s2
    WHERE s2.SKISBN = s.SKISBN
      AND s2.SKENTR = s.SKENTR
);


-- ──────────────────────────────────────────────
-- 7. EXTRACTION : Contrats (CNTRTP)
-- ──────────────────────────────────────────────
SELECT
    CTNUM,
    CTAUTE,
    CTISBN,
    CTTYPE,
    CTPCTD,
    CTAVNC,
    CTDTDB,
    CTDTFN,
    CTSTAT
FROM HARRY2LIB.CNTRTP
ORDER BY CTNUM;


-- ──────────────────────────────────────────────
-- 8. VÉRIFICATION : Comptages pour le Quality Gate
-- ──────────────────────────────────────────────
-- Exécuter côté source pour comparer avec les comptages côté Fabric

SELECT 'OUVRGP'  AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM HARRY2LIB.OUVRGP
UNION ALL
SELECT 'AUTURP',  COUNT(*) FROM HARRY2LIB.AUTURP
UNION ALL
SELECT 'EDITRP',  COUNT(*) FROM HARRY2LIB.EDITRP
UNION ALL
SELECT 'VENTEP',  COUNT(*) FROM HARRY2LIB.VENTEP
UNION ALL
SELECT 'CMDEP',   COUNT(*) FROM HARRY2LIB.CMDEP
UNION ALL
SELECT 'STCKP',   COUNT(*) FROM HARRY2LIB.STCKP
UNION ALL
SELECT 'CNTRTP',  COUNT(*) FROM HARRY2LIB.CNTRTP;


-- ──────────────────────────────────────────────
-- 9. VÉRIFICATION : Sommes de contrôle pour les montants
-- ──────────────────────────────────────────────
SELECT
    'VENTEP' AS TABLE_NAME,
    COUNT(*)            AS NB_LIGNES,
    SUM(VEMTHT)        AS TOTAL_HT,
    SUM(VEMTTTC)       AS TOTAL_TTC,
    SUM(VEQTE)         AS TOTAL_QTE,
    MIN(VEDTVT)         AS DATE_MIN,
    MAX(VEDTVT)         AS DATE_MAX
FROM HARRY2LIB.VENTEP;


-- ──────────────────────────────────────────────
-- 10. TEST EBCDIC : Vérification des accents
-- ──────────────────────────────────────────────
-- Chercher des caractères accentués dans les noms d'auteurs
-- Si le résultat affiche correctement é, è, ê, ç, à, ù → la conversion fonctionne
SELECT AUCODE, AUNOM, AUPRN
FROM HARRY2LIB.AUTURP
WHERE AUNOM LIKE '%é%'
   OR AUNOM LIKE '%è%'
   OR AUNOM LIKE '%ê%'
   OR AUNOM LIKE '%ç%'
   OR AUNOM LIKE '%à%'
   OR AUNOM LIKE '%ù%'
   OR AUPRN LIKE '%é%';
