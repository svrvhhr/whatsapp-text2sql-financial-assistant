--
-- PostgreSQL database dump
--

\restrict vgIDoYedc1envmdbaz4MOy14wvfwGbXHaC7Ajw7bHuWI1AVIPBNFnjKlRP3mYnf

-- Dumped from database version 18.1
-- Dumped by pg_dump version 18.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
-- SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

ALTER TABLE IF EXISTS ONLY public.utilisateur DROP CONSTRAINT IF EXISTS utilisateur_role_id_fkey;
ALTER TABLE IF EXISTS ONLY public.transfert_interne DROP CONSTRAINT IF EXISTS transfert_interne_compte_source_id_fkey;
ALTER TABLE IF EXISTS ONLY public.transfert_interne DROP CONSTRAINT IF EXISTS transfert_interne_compte_destination_id_fkey;
ALTER TABLE IF EXISTS ONLY public.projet DROP CONSTRAINT IF EXISTS projet_entreprise_id_fkey;
ALTER TABLE IF EXISTS ONLY public.facture DROP CONSTRAINT IF EXISTS facture_projet_id_fkey;
ALTER TABLE IF EXISTS ONLY public.facture DROP CONSTRAINT IF EXISTS facture_client_id_fkey;
ALTER TABLE IF EXISTS ONLY public.depense DROP CONSTRAINT IF EXISTS depense_projet_id_fkey;
ALTER TABLE IF EXISTS ONLY public.depense DROP CONSTRAINT IF EXISTS depense_compte_id_fkey;
ALTER TABLE IF EXISTS ONLY public.compte_financier DROP CONSTRAINT IF EXISTS compte_financier_entreprise_id_fkey;
ALTER TABLE IF EXISTS ONLY public.bureau DROP CONSTRAINT IF EXISTS bureau_entreprise_id_fkey;

DROP TRIGGER IF EXISTS trg_update_solde ON public.depense;
DROP TRIGGER IF EXISTS trg_check_solde ON public.depense;
DROP TRIGGER IF EXISTS trg_check_budget ON public.depense;
DROP TRIGGER IF EXISTS trg_audit_depense ON public.depense;

ALTER TABLE IF EXISTS ONLY public.utilisateurs DROP CONSTRAINT IF EXISTS utilisateurs_whatsapp_number_key;
ALTER TABLE IF EXISTS ONLY public.utilisateurs DROP CONSTRAINT IF EXISTS utilisateurs_pkey;
ALTER TABLE IF EXISTS ONLY public.utilisateur DROP CONSTRAINT IF EXISTS utilisateur_pkey;
ALTER TABLE IF EXISTS ONLY public.utilisateur DROP CONSTRAINT IF EXISTS utilisateur_numero_whatsapp_key;
ALTER TABLE IF EXISTS ONLY public.transfert_interne DROP CONSTRAINT IF EXISTS transfert_interne_pkey;
ALTER TABLE IF EXISTS ONLY public.role DROP CONSTRAINT IF EXISTS role_pkey;
ALTER TABLE IF EXISTS ONLY public.role DROP CONSTRAINT IF EXISTS role_nom_key;
ALTER TABLE IF EXISTS ONLY public.projet DROP CONSTRAINT IF EXISTS projet_pkey;
ALTER TABLE IF EXISTS ONLY public.facture DROP CONSTRAINT IF EXISTS facture_pkey;
ALTER TABLE IF EXISTS ONLY public.entreprise DROP CONSTRAINT IF EXISTS entreprise_pkey;
ALTER TABLE IF EXISTS ONLY public.depense DROP CONSTRAINT IF EXISTS depense_pkey;
ALTER TABLE IF EXISTS ONLY public.compte_financier DROP CONSTRAINT IF EXISTS compte_financier_pkey;
ALTER TABLE IF EXISTS ONLY public.client DROP CONSTRAINT IF EXISTS client_pkey;
ALTER TABLE IF EXISTS ONLY public.bureau DROP CONSTRAINT IF EXISTS bureau_pkey;
ALTER TABLE IF EXISTS ONLY public.audit_event DROP CONSTRAINT IF EXISTS audit_event_pkey;

ALTER TABLE IF EXISTS public.utilisateurs ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.utilisateur ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.transfert_interne ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.role ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.projet ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.facture ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.entreprise ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.depense ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.compte_financier ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.client ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.bureau ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.audit_event ALTER COLUMN id DROP DEFAULT;

DROP VIEW IF EXISTS public.vue_soldes_comptes;
DROP VIEW IF EXISTS public.vue_factures_statut;
DROP VIEW IF EXISTS public.vue_depenses_par_projet;

DROP SEQUENCE IF EXISTS public.utilisateurs_id_seq;
DROP TABLE IF EXISTS public.utilisateurs;

DROP SEQUENCE IF EXISTS public.utilisateur_id_seq;
DROP TABLE IF EXISTS public.utilisateur;

DROP SEQUENCE IF EXISTS public.transfert_interne_id_seq;
DROP TABLE IF EXISTS public.transfert_interne;

DROP SEQUENCE IF EXISTS public.role_id_seq;
DROP TABLE IF EXISTS public.role;

DROP SEQUENCE IF EXISTS public.projet_id_seq;
DROP TABLE IF EXISTS public.projet;

DROP SEQUENCE IF EXISTS public.facture_id_seq;
DROP TABLE IF EXISTS public.facture;

DROP SEQUENCE IF EXISTS public.entreprise_id_seq;
DROP TABLE IF EXISTS public.entreprise;

DROP SEQUENCE IF EXISTS public.depense_id_seq;
DROP TABLE IF EXISTS public.depense;

DROP SEQUENCE IF EXISTS public.compte_financier_id_seq;
DROP TABLE IF EXISTS public.compte_financier;

DROP SEQUENCE IF EXISTS public.client_id_seq;
DROP TABLE IF EXISTS public.client;

DROP SEQUENCE IF EXISTS public.bureau_id_seq;
DROP TABLE IF EXISTS public.bureau;

DROP SEQUENCE IF EXISTS public.audit_event_id_seq;
DROP TABLE IF EXISTS public.audit_event;

DROP FUNCTION IF EXISTS public.update_solde_compte();
DROP FUNCTION IF EXISTS public.payer_facture(p_facture_id integer);
DROP FUNCTION IF EXISTS public.effectuer_transfert(p_source_id integer, p_destination_id integer, p_montant numeric, p_devise character varying);
DROP FUNCTION IF EXISTS public.check_solde_compte();
DROP FUNCTION IF EXISTS public.check_budget_projet();
DROP FUNCTION IF EXISTS public.audit_event_trigger();


-- =========================================================
-- FUNCTIONS
-- =========================================================

-- 1) Audit trigger => writes into audit_event (official audit)
CREATE FUNCTION public.audit_event_trigger() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_request_id TEXT;
  v_actor_id TEXT;
  v_role TEXT;
  v_entreprise_id INT;
  v_projet_id INT;
  v_entity_id TEXT;
BEGIN
  v_request_id := current_setting('app.request_id', true);
  v_actor_id := current_setting('app.actor_id', true);
  v_role := current_setting('app.role', true);

  v_entreprise_id := NULLIF(current_setting('app.entreprise_id', true), '')::int;
  v_projet_id := NULLIF(current_setting('app.projet_id', true), '')::int;

  -- best-effort entity_id for tables that have "id"
  BEGIN
    IF (TG_OP = 'INSERT') THEN
      v_entity_id := (NEW.id)::text;
    ELSIF (TG_OP = 'UPDATE') THEN
      v_entity_id := (NEW.id)::text;
    ELSIF (TG_OP = 'DELETE') THEN
      v_entity_id := (OLD.id)::text;
    END IF;
  EXCEPTION WHEN others THEN
    v_entity_id := NULL;
  END;

  INSERT INTO audit_event(
    request_id, actor_id, role, entreprise_id, projet_id,
    operation, sql, params, status, reasons, duration_ms, row_count, affected_rows,
    entity, entity_id
  )
  VALUES (
    v_request_id, v_actor_id, v_role, v_entreprise_id, v_projet_id,
    TG_OP,
    'TRIGGER ' || TG_OP || ' ON ' || TG_TABLE_NAME,
    NULL,
    'db_trigger',
    NULL,
    NULL, NULL, NULL,
    TG_TABLE_NAME, v_entity_id
  );

  IF (TG_OP = 'DELETE') THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END;
$$;

ALTER FUNCTION public.audit_event_trigger() OWNER TO orionis;


-- 2) Budget check (already existed in your dump, now kept)
CREATE FUNCTION public.check_budget_projet() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    total_depenses NUMERIC;
    budget NUMERIC;
BEGIN
    SELECT COALESCE(SUM(montant), 0)
    INTO total_depenses
    FROM depense
    WHERE projet_id = NEW.projet_id;

    SELECT budget_total
    INTO budget
    FROM projet
    WHERE id = NEW.projet_id;

    IF total_depenses + NEW.montant > budget THEN
        RAISE EXCEPTION 'Depassement du budget du projet %', NEW.projet_id;
    END IF;

    RETURN NEW;
END;
$$;

ALTER FUNCTION public.check_budget_projet() OWNER TO orionis;


-- 3) Balance check (already existed)
CREATE FUNCTION public.check_solde_compte() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    solde_actuel NUMERIC;
BEGIN
    SELECT solde
    INTO solde_actuel
    FROM compte_financier
    WHERE id = NEW.compte_id;

    IF solde_actuel - NEW.montant < 0 THEN
        RAISE EXCEPTION 'Solde insuffisant sur le compte %', NEW.compte_id;
    END IF;

    RETURN NEW;
END;
$$;

ALTER FUNCTION public.check_solde_compte() OWNER TO orionis;


-- 4) Transfer function (already existed)
CREATE FUNCTION public.effectuer_transfert(
  p_source_id integer,
  p_destination_id integer,
  p_montant numeric,
  p_devise character varying
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    solde_source NUMERIC;
BEGIN
    IF p_source_id = p_destination_id THEN
        RAISE EXCEPTION 'Les comptes source et destination doivent etre differents';
    END IF;

    SELECT solde
    INTO solde_source
    FROM compte_financier
    WHERE id = p_source_id;

    IF solde_source < p_montant THEN
        RAISE EXCEPTION 'Solde insuffisant pour le transfert';
    END IF;

    UPDATE compte_financier
    SET solde = solde - p_montant
    WHERE id = p_source_id;

    UPDATE compte_financier
    SET solde = solde + p_montant
    WHERE id = p_destination_id;

    INSERT INTO transfert_interne (
        compte_source_id,
        compte_destination_id,
        montant,
        devise,
        date_transfert
    )
    VALUES (
        p_source_id,
        p_destination_id,
        p_montant,
        p_devise,
        CURRENT_DATE
    );
END;
$$;

ALTER FUNCTION public.effectuer_transfert(p_source_id integer, p_destination_id integer, p_montant numeric, p_devise character varying) OWNER TO orionis;


-- 5) Pay invoice function (already existed)
CREATE FUNCTION public.payer_facture(p_facture_id integer) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE facture
    SET statut = 'PAYEE',
        date_paiement = CURRENT_DATE
    WHERE id = p_facture_id
      AND statut = 'EMISE';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Facture % introuvable ou deja payee', p_facture_id;
    END IF;
END;
$$;

ALTER FUNCTION public.payer_facture(p_facture_id integer) OWNER TO orionis;


-- 6) Update balance AFTER insert expense (already existed)
CREATE FUNCTION public.update_solde_compte() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE compte_financier
    SET solde = solde - NEW.montant
    WHERE id = NEW.compte_id;

    RETURN NEW;
END;
$$;

ALTER FUNCTION public.update_solde_compte() OWNER TO orionis;


-- =========================================================
-- TABLES + SEQUENCES
-- =========================================================

-- Official audit table
CREATE TABLE public.audit_event (
    id integer NOT NULL,
    request_id text,
    created_at timestamp with time zone DEFAULT now(),
    actor_id text,
    role text,
    entreprise_id integer,
    projet_id integer,
    operation text,
    sql text,
    params jsonb,
    status text,
    reasons text,
    duration_ms integer,
    row_count integer,
    affected_rows integer,
    entity text,
    entity_id text
);

ALTER TABLE public.audit_event OWNER TO orionis;

CREATE SEQUENCE public.audit_event_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.audit_event_id_seq OWNER TO orionis;
ALTER SEQUENCE public.audit_event_id_seq OWNED BY public.audit_event.id;

-- Bureau
CREATE TABLE public.bureau (
    id integer NOT NULL,
    entreprise_id integer NOT NULL,
    nom character varying(100),
    ville character varying(50)
);
ALTER TABLE public.bureau OWNER TO orionis;

CREATE SEQUENCE public.bureau_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.bureau_id_seq OWNER TO orionis;
ALTER SEQUENCE public.bureau_id_seq OWNED BY public.bureau.id;

-- Client
CREATE TABLE public.client (
    id integer NOT NULL,
    nom character varying(100) NOT NULL,
    email character varying(100)
);
ALTER TABLE public.client OWNER TO orionis;

CREATE SEQUENCE public.client_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.client_id_seq OWNER TO orionis;
ALTER SEQUENCE public.client_id_seq OWNED BY public.client.id;

-- Compte financier
CREATE TABLE public.compte_financier (
    id integer NOT NULL,
    entreprise_id integer NOT NULL,
    nom character varying(100) NOT NULL,
    devise character varying(10) NOT NULL,
    solde numeric(14,2) DEFAULT 0,
    CONSTRAINT compte_financier_solde_check CHECK ((solde >= (0)::numeric))
);
ALTER TABLE public.compte_financier OWNER TO orionis;

CREATE SEQUENCE public.compte_financier_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.compte_financier_id_seq OWNER TO orionis;
ALTER SEQUENCE public.compte_financier_id_seq OWNED BY public.compte_financier.id;

-- Depense
CREATE TABLE public.depense (
    id integer NOT NULL,
    projet_id integer NOT NULL,
    compte_id integer NOT NULL,
    type_depense character varying(50) NOT NULL,
    montant numeric(14,2) NOT NULL,
    devise character varying(10) NOT NULL,
    description text,
    date_depense date NOT NULL,
    CONSTRAINT depense_montant_check CHECK ((montant > (0)::numeric))
);
ALTER TABLE public.depense OWNER TO orionis;

CREATE SEQUENCE public.depense_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.depense_id_seq OWNER TO orionis;
ALTER SEQUENCE public.depense_id_seq OWNED BY public.depense.id;

-- Entreprise
CREATE TABLE public.entreprise (
    id integer NOT NULL,
    nom character varying(100) NOT NULL,
    pays character varying(50),
    devise_principale character varying(10) NOT NULL
);
ALTER TABLE public.entreprise OWNER TO orionis;

CREATE SEQUENCE public.entreprise_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.entreprise_id_seq OWNER TO orionis;
ALTER SEQUENCE public.entreprise_id_seq OWNED BY public.entreprise.id;

-- Facture
CREATE TABLE public.facture (
    id integer NOT NULL,
    projet_id integer NOT NULL,
    client_id integer NOT NULL,
    montant numeric(14,2) NOT NULL,
    devise character varying(10) NOT NULL,
    statut character varying(20) NOT NULL,
    date_emission date NOT NULL,
    date_paiement date,
    CONSTRAINT facture_statut_check CHECK (((statut)::text = ANY ((ARRAY['EMISE'::character varying, 'PAYEE'::character varying])::text[])))
);
ALTER TABLE public.facture OWNER TO orionis;

CREATE SEQUENCE public.facture_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.facture_id_seq OWNER TO orionis;
ALTER SEQUENCE public.facture_id_seq OWNED BY public.facture.id;

-- Projet
CREATE TABLE public.projet (
    id integer NOT NULL,
    entreprise_id integer NOT NULL,
    nom character varying(100) NOT NULL,
    budget_total numeric(14,2) NOT NULL,
    date_debut date,
    date_fin date
);
ALTER TABLE public.projet OWNER TO orionis;

CREATE SEQUENCE public.projet_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.projet_id_seq OWNER TO orionis;
ALTER SEQUENCE public.projet_id_seq OWNED BY public.projet.id;

-- Role
CREATE TABLE public.role (
    id integer NOT NULL,
    nom character varying(50) NOT NULL
);
ALTER TABLE public.role OWNER TO orionis;

CREATE SEQUENCE public.role_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.role_id_seq OWNER TO orionis;
ALTER SEQUENCE public.role_id_seq OWNED BY public.role.id;

-- Transfert interne
CREATE TABLE public.transfert_interne (
    id integer NOT NULL,
    compte_source_id integer NOT NULL,
    compte_destination_id integer NOT NULL,
    montant numeric(14,2) NOT NULL,
    devise character varying(10) NOT NULL,
    date_transfert date NOT NULL,
    CONSTRAINT transfert_interne_check CHECK ((compte_source_id <> compte_destination_id)),
    CONSTRAINT transfert_interne_montant_check CHECK ((montant > (0)::numeric))
);
ALTER TABLE public.transfert_interne OWNER TO orionis;

CREATE SEQUENCE public.transfert_interne_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.transfert_interne_id_seq OWNER TO orionis;
ALTER SEQUENCE public.transfert_interne_id_seq OWNED BY public.transfert_interne.id;

-- Utilisateur (table "utilisateur" avec role_id)
CREATE TABLE public.utilisateur (
    id integer NOT NULL,
    nom character varying(100),
    numero_whatsapp character varying(20) NOT NULL,
    role_id integer
);
ALTER TABLE public.utilisateur OWNER TO orionis;

CREATE SEQUENCE public.utilisateur_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.utilisateur_id_seq OWNER TO orionis;
ALTER SEQUENCE public.utilisateur_id_seq OWNED BY public.utilisateur.id;

-- Utilisateurs (table legacy alternative)
CREATE TABLE public.utilisateurs (
    id integer NOT NULL,
    nom character varying(100) NOT NULL,
    whatsapp_number character varying(20) NOT NULL,
    role character varying(50) NOT NULL,
    CONSTRAINT utilisateurs_role_check CHECK (((role)::text = ANY ((ARRAY['admin_financier'::character varying, 'responsable_projet'::character varying, 'lecture_seule'::character varying])::text[])))
);
ALTER TABLE public.utilisateurs OWNER TO orionis;

CREATE SEQUENCE public.utilisateurs_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.utilisateurs_id_seq OWNER TO orionis;
ALTER SEQUENCE public.utilisateurs_id_seq OWNED BY public.utilisateurs.id;


-- =========================================================
-- VIEWS
-- =========================================================
CREATE VIEW public.vue_depenses_par_projet AS
SELECT
  p.id AS projet_id,
  p.nom AS projet,
  SUM(d.montant) AS total_depenses
FROM public.projet p
LEFT JOIN public.depense d ON (p.id = d.projet_id)
GROUP BY p.id, p.nom;

ALTER VIEW public.vue_depenses_par_projet OWNER TO orionis;

CREATE VIEW public.vue_factures_statut AS
SELECT
  statut,
  COUNT(*) AS nombre_factures,
  SUM(montant) AS montant_total
FROM public.facture
GROUP BY statut;

ALTER VIEW public.vue_factures_statut OWNER TO orionis;

CREATE VIEW public.vue_soldes_comptes AS
SELECT
  id AS compte_id,
  nom,
  devise,
  solde
FROM public.compte_financier;

ALTER VIEW public.vue_soldes_comptes OWNER TO orionis;


-- =========================================================
-- DEFAULTS (serial)
-- =========================================================
ALTER TABLE ONLY public.audit_event ALTER COLUMN id SET DEFAULT nextval('public.audit_event_id_seq'::regclass);
ALTER TABLE ONLY public.bureau ALTER COLUMN id SET DEFAULT nextval('public.bureau_id_seq'::regclass);
ALTER TABLE ONLY public.client ALTER COLUMN id SET DEFAULT nextval('public.client_id_seq'::regclass);
ALTER TABLE ONLY public.compte_financier ALTER COLUMN id SET DEFAULT nextval('public.compte_financier_id_seq'::regclass);
ALTER TABLE ONLY public.depense ALTER COLUMN id SET DEFAULT nextval('public.depense_id_seq'::regclass);
ALTER TABLE ONLY public.entreprise ALTER COLUMN id SET DEFAULT nextval('public.entreprise_id_seq'::regclass);
ALTER TABLE ONLY public.facture ALTER COLUMN id SET DEFAULT nextval('public.facture_id_seq'::regclass);
ALTER TABLE ONLY public.projet ALTER COLUMN id SET DEFAULT nextval('public.projet_id_seq'::regclass);
ALTER TABLE ONLY public.role ALTER COLUMN id SET DEFAULT nextval('public.role_id_seq'::regclass);
ALTER TABLE ONLY public.transfert_interne ALTER COLUMN id SET DEFAULT nextval('public.transfert_interne_id_seq'::regclass);
ALTER TABLE ONLY public.utilisateur ALTER COLUMN id SET DEFAULT nextval('public.utilisateur_id_seq'::regclass);
ALTER TABLE ONLY public.utilisateurs ALTER COLUMN id SET DEFAULT nextval('public.utilisateurs_id_seq'::regclass);


-- =========================================================
-- DATA (seed)
-- =========================================================

-- Audit init event (replaces old audit_log seed)
COPY public.audit_event (id, request_id, created_at, actor_id, role, entreprise_id, projet_id, operation, sql, params, status, reasons, duration_ms, row_count, affected_rows, entity, entity_id) FROM stdin;
1	\N	2026-01-11 21:06:12.892437+00	\N	\N	\N	\N	INIT	Initialisation de la base de donnees	\N	executed	\N	\N	\N	\N	system	\N
\.

COPY public.bureau (id, entreprise_id, nom, ville) FROM stdin;
1	1	Siege Paris	Paris
2	1	Agence Lyon	Lyon
3	2	Bureau Berlin	Berlin
\.

COPY public.client (id, nom, email) FROM stdin;
1	Client Alpha	contact@alpha.com
2	Client Beta	finance@beta.com
3	Client Gamma	admin@gamma.com
4	Client Alpha	contact@alpha.com
5	Client Beta	finance@beta.com
6	Client Gamma	admin@gamma.com
\.

COPY public.compte_financier (id, entreprise_id, nom, devise, solde) FROM stdin;
3	2	Compte International	EUR	40000.00
1	1	Compte Principal EUR	EUR	20000.00
2	1	Compte Projets	EUR	17000.00
\.

COPY public.entreprise (id, nom, pays, devise_principale) FROM stdin;
1	Orionis Group	France	EUR
2	Orionis International	Germany	EUR
\.

COPY public.projet (id, entreprise_id, nom, budget_total, date_debut, date_fin) FROM stdin;
1	1	Projet IA Finance	20000.00	2025-01-01	2025-12-31
2	1	Migration Cloud	15000.00	2025-02-01	2025-10-31
3	2	Analyse Data Europe	30000.00	2025-03-01	2025-11-30
\.

COPY public.role (id, nom) FROM stdin;
1	ADMIN
2	FINANCE
3	CONSULTATION
\.

COPY public.utilisateur (id, nom, numero_whatsapp, role_id) FROM stdin;
1	Sarah Harrouche	+33600000001	1
2	Lina Chetti	+33600000002	2
3	Jennifer Said	+33600000003	3
\.

COPY public.utilisateurs (id, nom, whatsapp_number, role) FROM stdin;
1	Alice Admin	+33123456789	admin_financier
2	Bob ProjetX	+33987654321	responsable_projet
3	Charlie Audit	+33611223344	lecture_seule
4	Diane ProjetY	+33799887766	responsable_projet
5	Eve Admin2	+33555443322	admin_financier
\.

COPY public.transfert_interne (id, compte_source_id, compte_destination_id, montant, devise, date_transfert) FROM stdin;
1	1	2	5000.00	EUR	2026-01-11
\.

COPY public.facture (id, projet_id, client_id, montant, devise, statut, date_emission, date_paiement) FROM stdin;
1	1	1	5000.00	EUR	EMISE	2025-03-10	\N
2	1	2	7200.00	EUR	EMISE	2025-03-15	\N
3	2	3	6500.00	EUR	EMISE	2025-04-01	\N
\.

COPY public.depense (id, projet_id, compte_id, type_depense, montant, devise, description, date_depense) FROM stdin;
1	1	1	cloud	3000.00	EUR	Serveurs cloud AWS	2025-03-20
2	1	1	freelance	2500.00	EUR	Consultant IA	2025-03-25
3	2	2	logiciel	4000.00	EUR	Licences logicielles	2025-04-05
\.


-- Sequence values
SELECT pg_catalog.setval('public.audit_event_id_seq', 1, true);
SELECT pg_catalog.setval('public.bureau_id_seq', 3, true);
SELECT pg_catalog.setval('public.client_id_seq', 6, true);
SELECT pg_catalog.setval('public.compte_financier_id_seq', 3, true);
SELECT pg_catalog.setval('public.depense_id_seq', 4, true);
SELECT pg_catalog.setval('public.entreprise_id_seq', 2, true);
SELECT pg_catalog.setval('public.facture_id_seq', 3, true);
SELECT pg_catalog.setval('public.projet_id_seq', 3, true);
SELECT pg_catalog.setval('public.role_id_seq', 3, true);
SELECT pg_catalog.setval('public.transfert_interne_id_seq', 1, true);
SELECT pg_catalog.setval('public.utilisateur_id_seq', 3, true);
SELECT pg_catalog.setval('public.utilisateurs_id_seq', 5, true);


-- =========================================================
-- CONSTRAINTS
-- =========================================================

ALTER TABLE ONLY public.audit_event
    ADD CONSTRAINT audit_event_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.bureau
    ADD CONSTRAINT bureau_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.client
    ADD CONSTRAINT client_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.compte_financier
    ADD CONSTRAINT compte_financier_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.depense
    ADD CONSTRAINT depense_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.entreprise
    ADD CONSTRAINT entreprise_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.facture
    ADD CONSTRAINT facture_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.projet
    ADD CONSTRAINT projet_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.role
    ADD CONSTRAINT role_nom_key UNIQUE (nom);

ALTER TABLE ONLY public.role
    ADD CONSTRAINT role_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.transfert_interne
    ADD CONSTRAINT transfert_interne_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.utilisateur
    ADD CONSTRAINT utilisateur_numero_whatsapp_key UNIQUE (numero_whatsapp);

ALTER TABLE ONLY public.utilisateur
    ADD CONSTRAINT utilisateur_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.utilisateurs
    ADD CONSTRAINT utilisateurs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.utilisateurs
    ADD CONSTRAINT utilisateurs_whatsapp_number_key UNIQUE (whatsapp_number);


-- =========================================================
-- TRIGGERS (IMPORTANT)
-- =========================================================

-- audit on depense => into audit_event
CREATE TRIGGER trg_audit_depense
AFTER INSERT OR UPDATE OR DELETE ON public.depense
FOR EACH ROW EXECUTE FUNCTION public.audit_event_trigger();

-- enforce sufficient balance
CREATE TRIGGER trg_check_solde
BEFORE INSERT ON public.depense
FOR EACH ROW EXECUTE FUNCTION public.check_solde_compte();

-- enforce project budget (THIS WAS MISSING IN YOUR ORIGINAL INIT)
CREATE TRIGGER trg_check_budget
BEFORE INSERT ON public.depense
FOR EACH ROW EXECUTE FUNCTION public.check_budget_projet();

-- update balance after insert
CREATE TRIGGER trg_update_solde
AFTER INSERT ON public.depense
FOR EACH ROW EXECUTE FUNCTION public.update_solde_compte();


-- =========================================================
-- FOREIGN KEYS
-- =========================================================

ALTER TABLE ONLY public.bureau
    ADD CONSTRAINT bureau_entreprise_id_fkey FOREIGN KEY (entreprise_id) REFERENCES public.entreprise(id);

ALTER TABLE ONLY public.compte_financier
    ADD CONSTRAINT compte_financier_entreprise_id_fkey FOREIGN KEY (entreprise_id) REFERENCES public.entreprise(id);

ALTER TABLE ONLY public.depense
    ADD CONSTRAINT depense_compte_id_fkey FOREIGN KEY (compte_id) REFERENCES public.compte_financier(id);

ALTER TABLE ONLY public.depense
    ADD CONSTRAINT depense_projet_id_fkey FOREIGN KEY (projet_id) REFERENCES public.projet(id);

ALTER TABLE ONLY public.facture
    ADD CONSTRAINT facture_client_id_fkey FOREIGN KEY (client_id) REFERENCES public.client(id);

ALTER TABLE ONLY public.facture
    ADD CONSTRAINT facture_projet_id_fkey FOREIGN KEY (projet_id) REFERENCES public.projet(id);

ALTER TABLE ONLY public.projet
    ADD CONSTRAINT projet_entreprise_id_fkey FOREIGN KEY (entreprise_id) REFERENCES public.entreprise(id);

ALTER TABLE ONLY public.transfert_interne
    ADD CONSTRAINT transfert_interne_compte_destination_id_fkey FOREIGN KEY (compte_destination_id) REFERENCES public.compte_financier(id);

ALTER TABLE ONLY public.transfert_interne
    ADD CONSTRAINT transfert_interne_compte_source_id_fkey FOREIGN KEY (compte_source_id) REFERENCES public.compte_financier(id);

ALTER TABLE ONLY public.utilisateur
    ADD CONSTRAINT utilisateur_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.role(id);


-- =========================================================
-- INDEXES (audit_event)
-- =========================================================
CREATE INDEX IF NOT EXISTS idx_audit_event_created_at ON public.audit_event(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_event_request_id ON public.audit_event(request_id);
CREATE INDEX IF NOT EXISTS idx_audit_event_status ON public.audit_event(status);
CREATE INDEX IF NOT EXISTS idx_audit_event_entity ON public.audit_event(entity);


-- Done
