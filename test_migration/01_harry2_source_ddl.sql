-- ============================================================================
-- HARRY2 — Simulation du schéma source DB2 for i (AS/400)
-- ============================================================================
-- Ce script simule les tables Harry2 avec les conventions IBM i :
--   • Noms courts (10 car. max / style RPG)
--   • Dates au format CYYMMDD (C = siècle : 0=19xx, 1=20xx)
--   • Packed decimal (DECIMAL) pour les montants
--   • Champs CHAR à longueur fixe (padding espaces)
--   • Pas de clés étrangères déclarées (implicites dans RPG)
--
-- Peut être exécuté sur : PostgreSQL, SQL Server, ou tout moteur SQL standard
-- pour simuler la source avant migration vers Fabric Lakehouse.
-- ============================================================================

-- ──────────────────────────────────────────────
-- BIBLIOTHÈQUE : HARRY2LIB (schéma)
-- ──────────────────────────────────────────────
-- En DB2 for i, un schéma s'appelle une "bibliothèque" (library).
-- CREATE SCHEMA HARRY2LIB;

-- ──────────────────────────────────────────────
-- Table : OUVRGP — Fichier physique Ouvrages
-- ──────────────────────────────────────────────
CREATE TABLE OUVRGP (
    OVISBN      CHAR(13)        NOT NULL,   -- ISBN-13 (clé naturelle)
    OVTITR      CHAR(200)       NOT NULL,   -- Titre de l'ouvrage (fixe 200, padded)
    OVSTIT      CHAR(200),                  -- Sous-titre
    OVEDIT      CHAR(10)        NOT NULL,   -- Code éditeur (FK implicite → EDITRP)
    OVCOLL      CHAR(10),                   -- Code collection (FK implicite → COLLCP)
    OVFMT       CHAR(3)         NOT NULL,   -- Format : POC=poche, GFT=grand format, NUM=numérique
    OVLANG      CHAR(3)         DEFAULT 'FRE', -- Code langue ISO 639-2 (FRE, ENG, SPA...)
    OVNBPG      DECIMAL(5,0)    DEFAULT 0,  -- Nombre de pages
    OVPXHT      DECIMAL(9,2)    NOT NULL,   -- Prix HT (packed decimal)
    OVPXTTC     DECIMAL(9,2)    NOT NULL,   -- Prix TTC
    OVTVA       DECIMAL(5,2)    DEFAULT 5.50, -- Taux TVA (5.50% livre en France)
    OVDTPB      DECIMAL(7,0),               -- Date publication CYYMMDD
    OVDTCR      DECIMAL(7,0),               -- Date création fiche CYYMMDD
    OVDTMJ      DECIMAL(7,0),               -- Date dernière MAJ CYYMMDD
    OVSTAT      CHAR(2)         DEFAULT 'AC', -- Statut : AC=actif, EP=épuisé, AN=annoncé, PI=pilon
    OVTIRG      DECIMAL(9,0)    DEFAULT 0,  -- Tirage initial
    OVPOID      DECIMAL(7,0)    DEFAULT 0,  -- Poids en grammes
    OVEAN       CHAR(13),                   -- Code EAN (souvent = ISBN)
    OVCOMM      CHAR(500),                  -- Commentaire éditorial
    PRIMARY KEY (OVISBN)
);

-- ──────────────────────────────────────────────
-- Table : AUTURP — Fichier physique Auteurs
-- ──────────────────────────────────────────────
CREATE TABLE AUTURP (
    AUCODE      CHAR(10)        NOT NULL,   -- Code auteur interne Harry2
    AUNOM       CHAR(60)        NOT NULL,   -- Nom famille
    AUPRN       CHAR(60),                   -- Prénom
    AUPSEUD     CHAR(80),                   -- Pseudonyme (si applicable)
    AUNATI      CHAR(3)         DEFAULT 'FR', -- Nationalité ISO 3166-1 alpha-2
    AUDTNS      DECIMAL(7,0),               -- Date naissance CYYMMDD
    AUDTDC      DECIMAL(7,0),               -- Date décès CYYMMDD (NULLS = vivant)
    AUEMAIL     CHAR(100),                  -- Email
    AUDTCR      DECIMAL(7,0),               -- Date création CYYMMDD
    AUSTAT      CHAR(1)         DEFAULT 'A', -- A=actif, I=inactif
    PRIMARY KEY (AUCODE)
);

-- ──────────────────────────────────────────────
-- Table : OVAUJP — Jointure Ouvrage-Auteur (N:N)
-- ──────────────────────────────────────────────
CREATE TABLE OVAUJP (
    JAISBN      CHAR(13)        NOT NULL,   -- ISBN ouvrage
    JAAUTE      CHAR(10)        NOT NULL,   -- Code auteur
    JAROLE      CHAR(3)         DEFAULT 'AUT', -- AUT=auteur, TRA=traducteur, ILL=illustrateur, PRE=préfacier
    JAORDR      DECIMAL(3,0)    DEFAULT 1,  -- Ordre d'apparition
    PRIMARY KEY (JAISBN, JAAUTE, JAROLE)
);

-- ──────────────────────────────────────────────
-- Table : EDITRP — Fichier physique Éditeurs
-- ──────────────────────────────────────────────
CREATE TABLE EDITRP (
    EDCODE      CHAR(10)        NOT NULL,   -- Code éditeur interne
    EDNOM       CHAR(80)        NOT NULL,   -- Raison sociale
    EDIMPR      CHAR(80),                   -- Nom d'imprint (marque éditoriale)
    EDGRPE      CHAR(10),                   -- Code groupe (ex: GRPEDI01)
    EDPAYS      CHAR(2)         DEFAULT 'FR',
    EDSTAT      CHAR(1)         DEFAULT 'A',
    PRIMARY KEY (EDCODE)
);

-- ──────────────────────────────────────────────
-- Table : COLLCP — Fichier physique Collections
-- ──────────────────────────────────────────────
CREATE TABLE COLLCP (
    CLCODE      CHAR(10)        NOT NULL,   -- Code collection
    CLNOM       CHAR(80)        NOT NULL,   -- Nom collection
    CLEDIT      CHAR(10)        NOT NULL,   -- Code éditeur propriétaire
    CLDTCR      DECIMAL(7,0),               -- Date création CYYMMDD
    CLSTAT      CHAR(1)         DEFAULT 'A',
    PRIMARY KEY (CLCODE)
);

-- ──────────────────────────────────────────────
-- Table : VENTEP — Fichier physique Ventes
-- ──────────────────────────────────────────────
-- Table BLU (ORGANIZE BY COLUMN) dans le vrai Harry2 DB2
CREATE TABLE VENTEP (
    VENUMR      DECIMAL(15,0)   NOT NULL,   -- Numéro de mouvement (séquentiel)
    VEISBN      CHAR(13)        NOT NULL,   -- ISBN ouvrage vendu
    VEDTVT      DECIMAL(7,0)    NOT NULL,   -- Date vente CYYMMDD
    VEQTE       DECIMAL(9,0)    NOT NULL,   -- Quantité vendue
    VEMTHT      DECIMAL(11,2)   NOT NULL,   -- Montant HT
    VEMTTTC     DECIMAL(11,2)   NOT NULL,   -- Montant TTC
    VECANL      CHAR(3)         NOT NULL,   -- Canal : LIB=librairie, GMS=grande surface, WEB=vente en ligne, EXP=export
    VECLIE      CHAR(10),                   -- Code client / point de vente
    VEENTR      CHAR(5)         DEFAULT 'ENT01', -- Code entrepôt expéditeur
    VEDTRG      DECIMAL(7,0),               -- Date de règlement CYYMMDD
    VESTAT      CHAR(1)         DEFAULT 'V', -- V=vendu, R=retour, A=avoir
    PRIMARY KEY (VENUMR)
);

-- ──────────────────────────────────────────────
-- Table : CMDEP  — Fichier physique Commandes
-- ──────────────────────────────────────────────
CREATE TABLE CMDEP (
    CMNUMR      DECIMAL(15,0)   NOT NULL,   -- Numéro commande
    CMISBN      CHAR(13)        NOT NULL,   -- ISBN commandé
    CMDTCM      DECIMAL(7,0)    NOT NULL,   -- Date commande CYYMMDD
    CMDTLV      DECIMAL(7,0),               -- Date livraison CYYMMDD
    CMQTE       DECIMAL(9,0)    NOT NULL,   -- Quantité commandée
    CMQTLV      DECIMAL(9,0)    DEFAULT 0,  -- Quantité livrée
    CMMTHT      DECIMAL(11,2),              -- Montant HT
    CMCLIE      CHAR(10)        NOT NULL,   -- Code client
    CMSTAT      CHAR(2)         DEFAULT 'EN', -- EN=en cours, LV=livrée, AN=annulée, PA=partielle
    PRIMARY KEY (CMNUMR)
);

-- ──────────────────────────────────────────────
-- Table : STCKP  — Fichier physique Stocks
-- ──────────────────────────────────────────────
CREATE TABLE STCKP (
    SKISBN      CHAR(13)        NOT NULL,   -- ISBN
    SKENTR      CHAR(5)         NOT NULL,   -- Code entrepôt
    SKDTPS      DECIMAL(7,0)    NOT NULL,   -- Date position CYYMMDD
    SKQTDS      DECIMAL(9,0)    DEFAULT 0,  -- Quantité disponible
    SKQTRS      DECIMAL(9,0)    DEFAULT 0,  -- Quantité réservée
    SKQTTR      DECIMAL(9,0)    DEFAULT 0,  -- Quantité en transit
    SKQTPI      DECIMAL(9,0)    DEFAULT 0,  -- Quantité pilonnée (détruite)
    PRIMARY KEY (SKISBN, SKENTR, SKDTPS)
);

-- ──────────────────────────────────────────────
-- Table : CNTRTP — Fichier physique Contrats/Droits
-- ──────────────────────────────────────────────
CREATE TABLE CNTRTP (
    CTNUM       DECIMAL(10,0)   NOT NULL,   -- Numéro contrat
    CTAUTE      CHAR(10)        NOT NULL,   -- Code auteur
    CTISBN      CHAR(13),                   -- ISBN (NULL si contrat global)
    CTTYPE      CHAR(3)         NOT NULL,   -- CES=cession, COE=co-édition, TRA=traduction
    CTPCTD      DECIMAL(5,2)    NOT NULL,   -- Pourcentage droits auteur
    CTAVNC      DECIMAL(11,2)   DEFAULT 0,  -- Avance (à-valoir)
    CTDTDB      DECIMAL(7,0)    NOT NULL,   -- Date début contrat CYYMMDD
    CTDTFN      DECIMAL(7,0),               -- Date fin contrat CYYMMDD
    CTSTAT      CHAR(1)         DEFAULT 'A', -- A=actif, T=terminé, S=suspendu
    PRIMARY KEY (CTNUM)
);

-- ──────────────────────────────────────────────
-- Tables de référence / lookup
-- ──────────────────────────────────────────────
CREATE TABLE RFCANL (   -- Référentiel canaux de vente
    RCCODE      CHAR(3)         NOT NULL,
    RCLIB       CHAR(40)        NOT NULL,
    PRIMARY KEY (RCCODE)
);

CREATE TABLE RFFMT (    -- Référentiel formats
    RFCODE      CHAR(3)         NOT NULL,
    RFLIB       CHAR(40)        NOT NULL,
    PRIMARY KEY (RFCODE)
);

CREATE TABLE RFSTAT (   -- Référentiel statuts ouvrage
    RSCODE      CHAR(2)         NOT NULL,
    RSLIB       CHAR(40)        NOT NULL,
    PRIMARY KEY (RSCODE)
);

CREATE TABLE RFENTR (   -- Référentiel entrepôts
    RECODE      CHAR(5)         NOT NULL,
    RENOM       CHAR(60)        NOT NULL,
    REVILL      CHAR(40),
    PRIMARY KEY (RECODE)
);
