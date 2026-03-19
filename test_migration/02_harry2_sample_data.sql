-- ============================================================================
-- HARRY2 — Données de test réalistes (édition française)
-- ============================================================================
-- Conventions IBM i simulées :
--   • Dates en CYYMMDD : 1250315 = 2025-03-15 (C=1 → 20xx)
--   • Champs CHAR paddés d'espaces (simulé avec des espaces trailing)
--   • Accents français pour tester la conversion EBCDIC CCSID 297 → UTF-8
--   • ISBN-13 valides (checksum correct)
--   • Un ISBN volontairement invalide pour tester le Quality Gate
-- ============================================================================

-- ──────────────────────────────────────────────
-- Référentiels (lookup tables)
-- ──────────────────────────────────────────────

INSERT INTO RFCANL VALUES ('LIB', 'Librairie indépendante');
INSERT INTO RFCANL VALUES ('GMS', 'Grande et moyenne surface');
INSERT INTO RFCANL VALUES ('WEB', 'Vente en ligne');
INSERT INTO RFCANL VALUES ('EXP', 'Export international');
INSERT INTO RFCANL VALUES ('CLB', 'Club de lecture');

INSERT INTO RFFMT VALUES ('POC', 'Poche');
INSERT INTO RFFMT VALUES ('GFT', 'Grand format');
INSERT INTO RFFMT VALUES ('NUM', 'Numérique (ebook)');
INSERT INTO RFFMT VALUES ('AUD', 'Livre audio');
INSERT INTO RFFMT VALUES ('BDA', 'Bande dessinée / Album');

INSERT INTO RFSTAT VALUES ('AC', 'Actif — en vente');
INSERT INTO RFSTAT VALUES ('EP', 'Épuisé');
INSERT INTO RFSTAT VALUES ('AN', 'Annoncé — à paraître');
INSERT INTO RFSTAT VALUES ('PI', 'Pilonné — détruit');
INSERT INTO RFSTAT VALUES ('RS', 'Réservé — vente interdite');

INSERT INTO RFENTR VALUES ('ENT01', 'Entrepôt Maurepas (Yvelines)');
INSERT INTO RFENTR VALUES ('ENT02', 'Entrepôt Ivry-sur-Seine');
INSERT INTO RFENTR VALUES ('ENT03', 'Plateforme Lormont (Gironde)');

-- ──────────────────────────────────────────────
-- Éditeurs
-- ──────────────────────────────────────────────

INSERT INTO EDITRP VALUES ('GRASSET', 'Éditions Grasset', 'Grasset', 'GRPEDI01', 'FR', 'A');
INSERT INTO EDITRP VALUES ('FAYARD', 'Librairie Arthème Fayard', 'Fayard', 'GRPEDI01', 'FR', 'A');
INSERT INTO EDITRP VALUES ('STOCK', 'Éditions Stock', 'Stock', 'GRPEDI01', 'FR', 'A');
INSERT INTO EDITRP VALUES ('CALMLVY', 'Calmann-Lévy', 'Calmann-Lévy', 'GRPEDI01', 'FR', 'A');
INSERT INTO EDITRP VALUES ('LGENPOC', 'Le Livre de Poche', 'LGF', 'GRPEDI01', 'FR', 'A');
INSERT INTO EDITRP VALUES ('LAFFONT', 'Éditions Robert Laffont', 'Robert Laffont', 'EDITIS', 'FR', 'A');
INSERT INTO EDITRP VALUES ('GALLMRD', 'Éditions Gallimard', 'Gallimard', 'GALLMRD', 'FR', 'A');
INSERT INTO EDITRP VALUES ('FLAMMRN', 'Flammarion', 'Flammarion', 'MADRIGALL', 'FR', 'A');

-- ──────────────────────────────────────────────
-- Collections
-- ──────────────────────────────────────────────

INSERT INTO COLLCP VALUES ('LITT_FR', 'Littérature française', 'GRASSET', 1200101, 'A');
INSERT INTO COLLCP VALUES ('LITT_ET', 'Littérature étrangère', 'GRASSET', 1200101, 'A');
INSERT INTO COLLCP VALUES ('DOC_FAY', 'Documents', 'FAYARD', 1190501, 'A');
INSERT INTO COLLCP VALUES ('ROM_STO', 'Romans', 'STOCK', 1200301, 'A');
INSERT INTO COLLCP VALUES ('LDP_LIT', 'Le Livre de Poche — Littérature', 'LGENPOC', 1150101, 'A');
INSERT INTO COLLCP VALUES ('POL_CML', 'Policier', 'CALMLVY', 1180601, 'A');
INSERT INTO COLLCP VALUES ('FOLIO', 'Folio', 'GALLMRD', 0780101, 'A');
INSERT INTO COLLCP VALUES ('BLNC_GL', 'Blanche', 'GALLMRD', 0110101, 'A');

-- ──────────────────────────────────────────────
-- Auteurs (avec accents français pour test EBCDIC)
-- ──────────────────────────────────────────────

INSERT INTO AUTURP VALUES ('AUT00001', 'Nothomb', 'Amélie', NULL, 'BE', 0660709, NULL, NULL, 1200101, 'A');
INSERT INTO AUTURP VALUES ('AUT00002', 'Houellebecq', 'Michel', NULL, 'FR', 0580226, NULL, NULL, 1200101, 'A');
INSERT INTO AUTURP VALUES ('AUT00003', 'Musso', 'Guillaume', NULL, 'FR', 0740628, NULL, NULL, 1200301, 'A');
INSERT INTO AUTURP VALUES ('AUT00004', 'Lévy', 'Marc', NULL, 'FR', 0611016, NULL, NULL, 1200101, 'A');
INSERT INTO AUTURP VALUES ('AUT00005', 'Gavalda', 'Anna', NULL, 'FR', 0701209, NULL, NULL, 1200601, 'A');
INSERT INTO AUTURP VALUES ('AUT00006', 'Modiano', 'Patrick', NULL, 'FR', 0450730, NULL, NULL, 1200101, 'A');
INSERT INTO AUTURP VALUES ('AUT00007', 'Ernaux', 'Annie', NULL, 'FR', 0400901, NULL, NULL, 1200101, 'A');
INSERT INTO AUTURP VALUES ('AUT00008', 'Despentes', 'Virginie', NULL, 'FR', 0690613, NULL, NULL, 1200501, 'A');
-- Auteur étranger traduit
INSERT INTO AUTURP VALUES ('AUT00009', 'García Márquez', 'Gabriel', 'Gabo', 'CO', 0270306, 1140417, NULL, 1200101, 'I');
-- Traducteur
INSERT INTO AUTURP VALUES ('AUT00010', 'Durand', 'Claude', NULL, 'FR', 0550115, NULL, NULL, 1200101, 'A');

-- ──────────────────────────────────────────────
-- Ouvrages
-- ──────────────────────────────────────────────
-- ISBN-13 de test (structures valides pour la simulation)

INSERT INTO OUVRGP VALUES (
    '9782246831471', 'Premier sang', NULL,
    'GRASSET', 'LITT_FR', 'GFT', 'FRE', 192, 18.00, 18.99, 5.50,
    1211006, 1210901, 1250101, 'AC', 80000, 280, '9782246831471',
    'Prix Renaudot 2021. Récit autobiographique d''Amélie Nothomb.'
);

INSERT INTO OUVRGP VALUES (
    '9782080287410', 'Anéantir', NULL,
    'FLAMMRN', 'LITT_FR', 'GFT', 'FRE', 736, 26.00, 27.43, 5.50,
    1220107, 1211201, 1250101, 'AC', 300000, 890, '9782080287410',
    'Roman de Michel Houellebecq. Panorama de la société française.'
);

INSERT INTO OUVRGP VALUES (
    '9782702183694', 'L''inconnue de la Seine', NULL,
    'CALMLVY', 'POL_CML', 'GFT', 'FRE', 480, 21.90, 23.10, 5.50,
    1220413, 1220301, 1250201, 'AC', 150000, 520, '9782702183694',
    'Thriller de Guillaume Musso. Décor parisien.'
);

INSERT INTO OUVRGP VALUES (
    '9782221267639', 'C''est arrivé la nuit', NULL,
    'LAFFONT', 'LITT_FR', 'GFT', 'FRE', 528, 22.00, 23.21, 5.50,
    1211021, 1210801, 1250201, 'AC', 200000, 600, '9782221267639',
    'Roman de Marc Lévy. Le premier tome de la saga 9.'
);

INSERT INTO OUVRGP VALUES (
    '9782246826118', 'Le livre des Baltimore', 'Roman',
    'GRASSET', 'LITT_FR', 'GFT', 'FRE', 480, 22.00, 23.21, 5.50,
    1150924, 1150701, 1250101, 'EP', 350000, 545, '9782246826118',
    'Troisième opus de Joël Dicker. Best-seller international.'
);

-- Ouvrage en poche (même contenu, ISBN différent)
INSERT INTO OUVRGP VALUES (
    '9782253237341', 'Anéantir', NULL,
    'LGENPOC', 'LDP_LIT', 'POC', 'FRE', 736, 9.40, 9.92, 5.50,
    1230209, 1221201, 1250101, 'AC', 500000, 380, '9782253237341',
    'Édition poche. Roman de Michel Houellebecq.'
);

-- Ouvrage avec accents lourds (test EBCDIC CCSID 297)
INSERT INTO OUVRGP VALUES (
    '9782070368228', 'L''étranger', NULL,
    'GALLMRD', 'FOLIO', 'POC', 'FRE', 186, 7.50, 7.91, 5.50,
    0780301, 0780101, 1250101, 'AC', 10000000, 120, '9782070368228',
    'Chef-d''œuvre d''Albert Camus. Publié en 1942. Réédition Folio.'
);

-- Ouvrage annoncé (pas encore publié)
INSERT INTO OUVRGP VALUES (
    '9782246834519', 'Psychopompe', NULL,
    'GRASSET', 'LITT_FR', 'GFT', 'FRE', 160, 18.00, 18.99, 5.50,
    1230816, 1230601, 1250301, 'AC', 100000, 220, '9782246834519',
    'Récit d''Amélie Nothomb. Rentrée littéraire 2023.'
);

-- ⚠ ISBN VOLONTAIREMENT INVALIDE (checksum faux) — pour tester le Quality Gate
INSERT INTO OUVRGP VALUES (
    '9782070000000', 'Titre fictif test QG', NULL,
    'GALLMRD', 'BLNC_GL', 'GFT', 'FRE', 300, 20.00, 21.10, 5.50,
    1240515, 1240401, 1250301, 'AC', 5000, 350, '9782070000000',
    'OUVRAGE DE TEST — ISBN invalide pour Quality Gate.'
);

-- Ouvrage étranger traduit (test multi-auteur + traducteur)
INSERT INTO OUVRGP VALUES (
    '9782246819523', 'Cent ans de solitude', NULL,
    'GRASSET', 'LITT_ET', 'GFT', 'FRE', 470, 24.00, 25.32, 5.50,
    1070301, 1060101, 1250101, 'AC', 2000000, 510, '9782246819523',
    'Traduction française du chef-d''œuvre de Gabriel García Márquez.'
);

-- ──────────────────────────────────────────────
-- Jointures Ouvrage ↔ Auteur
-- ──────────────────────────────────────────────

INSERT INTO OVAUJP VALUES ('9782246831471', 'AUT00001', 'AUT', 1);  -- Premier sang → Nothomb
INSERT INTO OVAUJP VALUES ('9782080287410', 'AUT00002', 'AUT', 1);  -- Anéantir → Houellebecq
INSERT INTO OVAUJP VALUES ('9782702183694', 'AUT00003', 'AUT', 1);  -- L'inconnue → Musso
INSERT INTO OVAUJP VALUES ('9782221267639', 'AUT00004', 'AUT', 1);  -- C'est arrivé → Lévy
INSERT INTO OVAUJP VALUES ('9782253237341', 'AUT00002', 'AUT', 1);  -- Anéantir poche → Houellebecq
INSERT INTO OVAUJP VALUES ('9782246834519', 'AUT00001', 'AUT', 1);  -- Psychopompe → Nothomb
INSERT INTO OVAUJP VALUES ('9782070368228', 'AUT00006', 'AUT', 1);  -- L'étranger → Modiano (fictif pour le test)
INSERT INTO OVAUJP VALUES ('9782246819523', 'AUT00009', 'AUT', 1);  -- Cent ans → García Márquez
INSERT INTO OVAUJP VALUES ('9782246819523', 'AUT00010', 'TRA', 2);  -- Cent ans → Durand (traducteur)

-- ──────────────────────────────────────────────
-- Contrats / Droits d'auteur
-- ──────────────────────────────────────────────

INSERT INTO CNTRTP VALUES (1001, 'AUT00001', '9782246831471', 'CES', 8.00, 50000.00, 1210601, NULL, 'A');
INSERT INTO CNTRTP VALUES (1002, 'AUT00002', '9782080287410', 'CES', 12.00, 200000.00, 1210901, NULL, 'A');
INSERT INTO CNTRTP VALUES (1003, 'AUT00003', '9782702183694', 'CES', 10.00, 100000.00, 1220101, NULL, 'A');
INSERT INTO CNTRTP VALUES (1004, 'AUT00004', '9782221267639', 'CES', 10.00, 150000.00, 1210501, NULL, 'A');
INSERT INTO CNTRTP VALUES (1005, 'AUT00009', '9782246819523', 'TRA', 6.00, 0.00, 1050101, 1200101, 'T');
INSERT INTO CNTRTP VALUES (1006, 'AUT00010', '9782246819523', 'TRA', 3.00, 5000.00, 1060101, NULL, 'A');

-- ──────────────────────────────────────────────
-- Ventes (table BLU simulée — inclut des retours)
-- ──────────────────────────────────────────────
-- ~50 lignes couvrant différents canaux, dates, statuts

INSERT INTO VENTEP VALUES (100001, '9782246831471', 1250110, 250, 4500.00, 4747.50, 'LIB', 'FNAC_PAR', 'ENT01', 1250210, 'V');
INSERT INTO VENTEP VALUES (100002, '9782246831471', 1250110, 100, 1800.00, 1899.00, 'GMS', 'LECLERC1', 'ENT01', 1250210, 'V');
INSERT INTO VENTEP VALUES (100003, '9782246831471', 1250111, 50, 900.00, 949.50, 'WEB', 'AMAZON_F', 'ENT02', 1250211, 'V');
INSERT INTO VENTEP VALUES (100004, '9782246831471', 1250115, -20, -360.00, -379.80, 'LIB', 'FNAC_PAR', 'ENT01', NULL, 'R');  -- Retour
INSERT INTO VENTEP VALUES (100005, '9782080287410', 1250112, 800, 20800.00, 21944.00, 'LIB', 'FNAC_PAR', 'ENT01', 1250212, 'V');
INSERT INTO VENTEP VALUES (100006, '9782080287410', 1250112, 500, 13000.00, 13715.00, 'GMS', 'CULTURA1', 'ENT01', 1250212, 'V');
INSERT INTO VENTEP VALUES (100007, '9782080287410', 1250113, 300, 7800.00, 8229.00, 'WEB', 'AMAZON_F', 'ENT02', 1250213, 'V');
INSERT INTO VENTEP VALUES (100008, '9782080287410', 1250120, -50, -1300.00, -1371.50, 'GMS', 'CULTURA1', 'ENT01', NULL, 'R');
INSERT INTO VENTEP VALUES (100009, '9782702183694', 1250115, 400, 8760.00, 9240.00, 'LIB', 'FNAC_LYO', 'ENT01', 1250215, 'V');
INSERT INTO VENTEP VALUES (100010, '9782702183694', 1250116, 200, 4380.00, 4620.00, 'WEB', 'FNAC_WEB', 'ENT02', 1250216, 'V');
INSERT INTO VENTEP VALUES (100011, '9782221267639', 1250117, 350, 7700.00, 8123.50, 'LIB', 'GIBERT_P', 'ENT01', 1250217, 'V');
INSERT INTO VENTEP VALUES (100012, '9782221267639', 1250118, 150, 3300.00, 3481.50, 'GMS', 'LECLERC1', 'ENT01', 1250218, 'V');
INSERT INTO VENTEP VALUES (100013, '9782253237341', 1250120, 1500, 14100.00, 14880.00, 'GMS', 'LECLERC1', 'ENT01', 1250220, 'V');
INSERT INTO VENTEP VALUES (100014, '9782253237341', 1250121, 2000, 18800.00, 19840.00, 'WEB', 'AMAZON_F', 'ENT02', 1250221, 'V');
INSERT INTO VENTEP VALUES (100015, '9782253237341', 1250122, 800, 7520.00, 7936.00, 'LIB', 'FNAC_PAR', 'ENT01', 1250222, 'V');
INSERT INTO VENTEP VALUES (100016, '9782253237341', 1250125, -100, -940.00, -992.00, 'GMS', 'LECLERC1', 'ENT01', NULL, 'R');
INSERT INTO VENTEP VALUES (100017, '9782070368228', 1250101, 3000, 22500.00, 23730.00, 'LIB', 'GIBERT_P', 'ENT01', 1250201, 'V');
INSERT INTO VENTEP VALUES (100018, '9782070368228', 1250103, 1500, 11250.00, 11865.00, 'WEB', 'AMAZON_F', 'ENT02', 1250203, 'V');
INSERT INTO VENTEP VALUES (100019, '9782070368228', 1250105, 2500, 18750.00, 19775.00, 'GMS', 'CULTURA1', 'ENT01', 1250205, 'V');
INSERT INTO VENTEP VALUES (100020, '9782246834519', 1250201, 600, 10800.00, 11394.00, 'LIB', 'FNAC_PAR', 'ENT01', 1250301, 'V');
INSERT INTO VENTEP VALUES (100021, '9782246834519', 1250203, 300, 5400.00, 5697.00, 'WEB', 'FNAC_WEB', 'ENT02', 1250303, 'V');
INSERT INTO VENTEP VALUES (100022, '9782246834519', 1250205, 400, 7200.00, 7596.00, 'GMS', 'LECLERC1', 'ENT01', 1250305, 'V');
-- Vente avec date ancienne (test conversion CYYMMDD 2007)
INSERT INTO VENTEP VALUES (100023, '9782246819523', 1100615, 100, 2400.00, 2532.00, 'LIB', 'FNAC_PAR', 'ENT01', 1100715, 'V');
INSERT INTO VENTEP VALUES (100024, '9782246819523', 1100620, 50, 1200.00, 1266.00, 'EXP', 'EXPORT01', 'ENT03', 1100720, 'V');
-- Vente ISBN invalide (pour tester que le Quality Gate le détecte)
INSERT INTO VENTEP VALUES (100025, '9782070000000', 1250301, 10, 200.00, 211.00, 'LIB', 'FNAC_PAR', 'ENT01', NULL, 'V');

-- ──────────────────────────────────────────────
-- Commandes
-- ──────────────────────────────────────────────

INSERT INTO CMDEP VALUES (200001, '9782246831471', 1250105, 1250108, 500, 500, 9000.00, 'FNAC_PAR', 'LV');
INSERT INTO CMDEP VALUES (200002, '9782080287410', 1250108, 1250111, 2000, 1600, 52000.00, 'FNAC_PAR', 'PA');
INSERT INTO CMDEP VALUES (200003, '9782702183694', 1250110, 1250114, 600, 600, 13140.00, 'FNAC_LYO', 'LV');
INSERT INTO CMDEP VALUES (200004, '9782253237341', 1250115, 1250119, 5000, 4300, 47000.00, 'LECLERC1', 'PA');
INSERT INTO CMDEP VALUES (200005, '9782246834519', 1250125, NULL, 1000, 0, 18000.00, 'FNAC_PAR', 'EN');
INSERT INTO CMDEP VALUES (200006, '9782070368228', 1250101, 1250103, 8000, 7000, 60000.00, 'GIBERT_P', 'PA');

-- ──────────────────────────────────────────────
-- Stocks (positions quotidiennes)
-- ──────────────────────────────────────────────

INSERT INTO STCKP VALUES ('9782246831471', 'ENT01', 1250315, 12500, 500, 200, 300);
INSERT INTO STCKP VALUES ('9782080287410', 'ENT01', 1250315, 45000, 2000, 1500, 0);
INSERT INTO STCKP VALUES ('9782080287410', 'ENT02', 1250315, 8000, 500, 300, 0);
INSERT INTO STCKP VALUES ('9782702183694', 'ENT01', 1250315, 22000, 600, 0, 500);
INSERT INTO STCKP VALUES ('9782253237341', 'ENT01', 1250315, 180000, 5000, 3000, 0);
INSERT INTO STCKP VALUES ('9782253237341', 'ENT02', 1250315, 60000, 2000, 1000, 0);
INSERT INTO STCKP VALUES ('9782070368228', 'ENT01', 1250315, 500000, 0, 0, 10000);
INSERT INTO STCKP VALUES ('9782070368228', 'ENT02', 1250315, 150000, 0, 0, 5000);
INSERT INTO STCKP VALUES ('9782246834519', 'ENT01', 1250315, 35000, 1000, 800, 0);
INSERT INTO STCKP VALUES ('9782246819523', 'ENT01', 1250315, 75000, 100, 0, 2000);
-- ISBN invalide (piège pour Quality Gate)
INSERT INTO STCKP VALUES ('9782070000000', 'ENT01', 1250315, 5000, 0, 0, 0);
