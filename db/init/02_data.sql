-- =========================================================
-- ORIONIS - 02_data.sql (seed/demo data)
-- Compatible with 01_schema.sql
-- =========================================================

BEGIN;

-- ---------------------------------------------------------
-- Context (for audit_event_trigger)
-- ---------------------------------------------------------
SELECT set_config('app.request_id', 'seed-20260120', true);
SELECT set_config('app.actor_id', 'system:seed', true);
SELECT set_config('app.role', 'System', true);
SELECT set_config('app.entreprise_id', '', true);
SELECT set_config('app.projet_id', '', true);
SELECT set_config('app.disable_audit', '1', true);

-- ---------------------------------------------------------
-- Fix sequences (audit_event)
-- ---------------------------------------------------------
SELECT setval(
  pg_get_serial_sequence('public.audit_event','id'),
  (SELECT COALESCE(MAX(id),0) FROM public.audit_event) + 1,
  false
);

-- ---------------------------------------------------------
-- Entreprises
-- ---------------------------------------------------------
INSERT INTO public.entreprise (id, nom, pays, devise_principale)
VALUES
  (1, 'Orionis France',   'France',               'EUR'),
  (2, 'Orionis Émirats',  'Émirats Arabes Unis',  'AED'),
  (3, 'Orionis Algérie',  'Algérie',              'DZD'),
  (4, 'Orionis Syrie',    'Syrie',                'SYP')
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------
-- Bureaux (schema: id, entreprise_id, nom, ville)
-- ---------------------------------------------------------
INSERT INTO public.bureau (id, entreprise_id, nom, ville)
VALUES
  (1, 1, 'Siège Orionis France',             'Paris'),
  (2, 1, 'Bureau régional Orionis France',   'Bordeaux'),
  (3, 2, 'Siège Orionis Émirats',            'Dubaï'),
  (4, 2, 'Bureau régional Orionis Émirats',  'Sharjah'),
  (5, 3, 'Siège Orionis Algérie',            'Alger'),
  (6, 3, 'Bureau régional Orionis Algérie',  'Constantine'),
  (7, 4, 'Siège Orionis Syrie',              'Alep'),
  (8, 4, 'Bureau régional Orionis Syrie',    'Damas')
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------
-- Rôles
-- ---------------------------------------------------------
INSERT INTO public.role (id, nom)
VALUES
  (1, 'ReadOnly'),
  (2, 'FinanceAdmin'),
  (3, 'Manager')
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------
-- Utilisateurs (exemples)
-- ---------------------------------------------------------
INSERT INTO public.utilisateur (id, nom, numero_whatsapp, role_id)
VALUES
  (1, 'Sarah Harrouche', 'whatsapp:+33749986718', 2),
  (2, 'Demo ReadOnly',   'whatsapp:+33600000001', 1),
  (3, 'Demo Manager',    'whatsapp:+33600000002', 3)
ON CONFLICT (id) DO NOTHING;

-- Accès multi-entité (utilisateur_entreprise)
INSERT INTO public.utilisateur_entreprise (id, utilisateur_id, entreprise_id, role_id)
VALUES
  (1, 1, 1, 2),
  (2, 1, 2, 2),
  (3, 2, 1, 1),
  (4, 3, 1, 3),
  (5, 3, 3, 3)
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------
-- Clients (multi-entité : entreprise_id obligatoire)
-- (Distribution simple : 1..6 FR, 7..12 UAE, 13..16 DZ, 17..20 SY)
-- ---------------------------------------------------------
INSERT INTO public.client (id, entreprise_id, nom, email)
VALUES
  (1,  1, 'Client Alpha',   'alpha@example.com'),
  (2,  1, 'Client Beta',    'beta@example.com'),
  (3,  1, 'Client Gamma',   'gamma@example.com'),
  (4,  1, 'Client Delta',   'delta@example.com'),
  (5,  1, 'Client Epsilon', 'epsilon@example.com'),
  (6,  1, 'Client Zeta',    'zeta@example.com'),
  (7,  2, 'Client Eta',     'eta@example.com'),
  (8,  2, 'Client Theta',   'theta@example.com'),
  (9,  2, 'Client Iota',    'iota@example.com'),
  (10, 2, 'Client Kappa',   'kappa@example.com'),
  (11, 2, 'Client Lambda',  'lambda@example.com'),
  (12, 2, 'Client Mu',      'mu@example.com'),
  (13, 3, 'Client Nu',      'nu@example.com'),
  (14, 3, 'Client Xi',      'xi@example.com'),
  (15, 3, 'Client Omicron', 'omicron@example.com'),
  (16, 3, 'Client Pi',      'pi@example.com'),
  (17, 4, 'Client Rho',     'rho@example.com'),
  (18, 4, 'Client Sigma',   'sigma@example.com'),
  (19, 4, 'Client Tau',     'tau@example.com'),
  (20, 4, 'Client Upsilon', 'upsilon@example.com')
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------
-- Comptes financiers (schema: id, entreprise_id, nom, devise, solde)
-- Notes: le schema ne contient plus type_compte/date_creation.
-- ---------------------------------------------------------
INSERT INTO public.compte_financier (id, entreprise_id, nom, devise, solde)
VALUES
  (1,  1, 'Paiements clients - Orionis France (banque)', 'EUR', 1036091.00),
  (2,  1, 'Compte principal - Orionis France (banque)',  'EUR', 1497089.34),
  (3,  1, 'Caisse Paris (caisse)',                       'EUR', 50000.00),

  (4,  2, 'Paiements clients - Orionis Émirats (banque)', 'AED', 1535000.00),
  (5,  2, 'Compte principal - Orionis Émirats (banque)',  'AED', 1751953.75),
  (6,  2, 'Caisse Dubaï (caisse)',                        'AED', 65000.00),

  (7,  3, 'Paiements clients - Orionis Algérie (banque)', 'DZD', 2110000.00),
  (8,  3, 'Compte principal - Orionis Algérie (banque)',  'DZD', 1000000.00),
  (9,  3, 'Caisse Alger (caisse)',                        'DZD', 67000.00),

  (10, 4, 'Paiements clients - Orionis Syrie (banque)',   'SYP', 2120000.00),
  (11, 4, 'Compte principal - Orionis Syrie (banque)',    'SYP', 1500080.00),
  (12, 4, 'Caisse Alep (caisse)',                         'SYP', 918000.00)
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------
-- Projets (un portefeuille par entreprise)
-- ---------------------------------------------------------
INSERT INTO public.projet (id, entreprise_id, nom, budget_total, date_debut, date_fin)
VALUES
  (1, 1, 'Migration SI Finance (FR)',    250000.00, '2025-01-15', '2025-10-31'),
  (2, 1, 'Chatbot WhatsApp (FR)',        120000.00, '2025-02-01', '2025-06-30'),
  (3, 2, 'Déploiement Cloud (UAE)',      420000.00, '2025-03-01', '2025-12-15'),
  (4, 2, 'ERP Facturation (UAE)',        300000.00, '2025-04-01', '2025-11-30'),
  (5, 3, 'Data Warehouse (DZ)',         3800000.00,'2025-01-10', '2025-12-31'),
  (6, 3, 'Sécurité Réseau (DZ)',        900000.00, '2025-05-01', '2025-09-30'),
  (7, 4, 'Support & Exploitation (SY)', 15000000.00,'2025-01-01','2025-12-31'),
  (8, 4, 'Modernisation Infra (SY)',    22000000.00,'2025-03-15','2025-12-20')
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------
-- Factures (cohérence imposée par trigger: client.entreprise_id == projet.entreprise_id)
-- ---------------------------------------------------------
INSERT INTO public.facture (id, projet_id, client_id, montant, devise, statut, date_emission, date_paiement)
VALUES
  (1, 1, 1,  45000.00, 'EUR', 'PAYEE', '2025-02-10', '2025-02-25'),
  (2, 2, 3,  18000.00, 'EUR', 'EMISE', '2025-03-05', NULL),
  (3, 3, 7,  92000.00, 'AED', 'PAYEE', '2025-05-12', '2025-05-20'),
  (4, 4, 10, 60000.00, 'AED', 'EMISE', '2025-06-01', NULL),
  (5, 5, 13, 850000.00,'DZD', 'PAYEE', '2025-04-18', '2025-04-30'),
  (6, 6, 15, 210000.00,'DZD', 'EMISE', '2025-07-07', NULL),
  (7, 7, 17, 1200000.00,'SYP','PAYEE','2025-02-02', '2025-02-10'),
  (8, 8, 19, 1750000.00,'SYP','EMISE','2025-08-15', NULL)
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------
-- Dépenses
-- Triggers imposent:
--  - depense.devise == compte_financier.devise
--  - compte.entreprise_id == projet.entreprise_id
--  - mise à jour solde + contrôle budget (INSERT/UPDATE/DELETE)
-- ---------------------------------------------------------
INSERT INTO public.depense (id, projet_id, compte_id, type_depense, montant, devise, description, date_depense)
VALUES
  -- FR (EUR)
  (1, 1, 2, 'Prestataire', 15000.00, 'EUR', 'Assistance migration - lot 1', '2025-02-15'),
  (2, 1, 3, 'Matériel',     3200.00, 'EUR', 'Équipements réseau',          '2025-02-20'),
  (3, 2, 2, 'Logiciel',     4800.00, 'EUR', 'Licence NLP / tests',         '2025-03-10'),

  -- UAE (AED)
  (4, 3, 5, 'Cloud',       25000.00, 'AED', 'Instances + stockage',        '2025-05-15'),
  (5, 4, 6, 'Déplacement',  1800.00, 'AED', 'Déplacement Sharjah',         '2025-06-05'),

  -- DZ (DZD)
  (6, 5, 8, 'Consulting',  300000.00,'DZD', 'Modélisation DWH',            '2025-04-20'),
  (7, 6, 9, 'Matériel',     45000.00,'DZD', 'Switches + câblage',          '2025-05-10'),

  -- SY (SYP)
  (8, 7, 11, 'Support',    500000.00,'SYP', 'Support mensuel',             '2025-02-05'),
  (9, 8, 12, 'Matériel',   220000.00,'SYP', 'Remplacement serveurs',       '2025-03-20')
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------
-- Transferts internes
-- Triggers imposent:
--  - même entreprise (source/destination)
--  - même devise (et = devise en paramètre)
--  - update soldes + contrôle solde
-- ---------------------------------------------------------
INSERT INTO public.transfert_interne (id, compte_source_id, compte_destination_id, montant, devise, date_transfert)
VALUES
  (1, 2, 3,  5000.00, 'EUR', '2025-02-01'),
  (2, 5, 6,  7500.00, 'AED', '2025-05-01'),
  (3, 8, 9, 25000.00, 'DZD', '2025-04-01'),
  (4, 11,12,60000.00, 'SYP', '2025-01-20')
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------
-- Sequence alignment
-- ---------------------------------------------------------
SELECT setval('public.entreprise_id_seq',            (SELECT COALESCE(MAX(id), 1) FROM public.entreprise));
SELECT setval('public.role_id_seq',                 (SELECT COALESCE(MAX(id), 1) FROM public.role));
SELECT setval('public.utilisateur_id_seq',          (SELECT COALESCE(MAX(id), 1) FROM public.utilisateur));
SELECT setval('public.utilisateur_entreprise_id_seq',(SELECT COALESCE(MAX(id), 1) FROM public.utilisateur_entreprise));
SELECT setval('public.bureau_id_seq',               (SELECT COALESCE(MAX(id), 1) FROM public.bureau));
SELECT setval('public.client_id_seq',               (SELECT COALESCE(MAX(id), 1) FROM public.client));
SELECT setval('public.compte_financier_id_seq',     (SELECT COALESCE(MAX(id), 1) FROM public.compte_financier));
SELECT setval('public.projet_id_seq',               (SELECT COALESCE(MAX(id), 1) FROM public.projet));
SELECT setval('public.facture_id_seq',              (SELECT COALESCE(MAX(id), 1) FROM public.facture));
SELECT setval('public.depense_id_seq',              (SELECT COALESCE(MAX(id), 1) FROM public.depense));
SELECT setval('public.transfert_interne_id_seq',    (SELECT COALESCE(MAX(id), 1) FROM public.transfert_interne));
SELECT set_config('app.disable_audit', '0', true);

COMMIT;
