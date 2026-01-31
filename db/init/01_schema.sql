--
-- ORIONIS - init.sql (improved for multi-entite, audit, securite, coherences metier)
-- Generated: 2026-01-20
--

-- =========================================================
-- SAFE RESET (idempotent)
-- =========================================================

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', 'public', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

-- Drop triggers first
DROP TRIGGER IF EXISTS trg_set_updated_at_bureau ON public.bureau;
DROP TRIGGER IF EXISTS trg_set_updated_at_client ON public.client;
DROP TRIGGER IF EXISTS trg_set_updated_at_compte_financier ON public.compte_financier;
DROP TRIGGER IF EXISTS trg_set_updated_at_depense ON public.depense;
DROP TRIGGER IF EXISTS trg_set_updated_at_entreprise ON public.entreprise;
DROP TRIGGER IF EXISTS trg_set_updated_at_facture ON public.facture;
DROP TRIGGER IF EXISTS trg_set_updated_at_projet ON public.projet;
DROP TRIGGER IF EXISTS trg_set_updated_at_role ON public.role;
DROP TRIGGER IF EXISTS trg_set_updated_at_transfert_interne ON public.transfert_interne;
DROP TRIGGER IF EXISTS trg_set_updated_at_utilisateur ON public.utilisateur;
DROP TRIGGER IF EXISTS trg_set_updated_at_utilisateur_entreprise ON public.utilisateur_entreprise;

DROP TRIGGER IF EXISTS trg_audit_bureau ON public.bureau;
DROP TRIGGER IF EXISTS trg_audit_client ON public.client;
DROP TRIGGER IF EXISTS trg_audit_compte_financier ON public.compte_financier;
DROP TRIGGER IF EXISTS trg_audit_depense ON public.depense;
DROP TRIGGER IF EXISTS trg_audit_entreprise ON public.entreprise;
DROP TRIGGER IF EXISTS trg_audit_facture ON public.facture;
DROP TRIGGER IF EXISTS trg_audit_projet ON public.projet;
DROP TRIGGER IF EXISTS trg_audit_role ON public.role;
DROP TRIGGER IF EXISTS trg_audit_transfert_interne ON public.transfert_interne;
DROP TRIGGER IF EXISTS trg_audit_utilisateur ON public.utilisateur;
DROP TRIGGER IF EXISTS trg_audit_utilisateur_entreprise ON public.utilisateur_entreprise;

DROP TRIGGER IF EXISTS trg_check_solde ON public.depense;
DROP TRIGGER IF EXISTS trg_check_budget ON public.depense;
DROP TRIGGER IF EXISTS trg_validate_depense ON public.depense;
DROP TRIGGER IF EXISTS trg_apply_solde_depense ON public.depense;

DROP TRIGGER IF EXISTS trg_validate_transfert ON public.transfert_interne;
DROP TRIGGER IF EXISTS trg_validate_facture ON public.facture;

-- Drop views
DROP VIEW IF EXISTS public.vue_soldes_comptes;
DROP VIEW IF EXISTS public.vue_factures_statut;
DROP VIEW IF EXISTS public.vue_depenses_par_projet;

-- Drop legacy table if exists
DROP TABLE IF EXISTS public.utilisateurs;
DROP SEQUENCE IF EXISTS public.utilisateurs_id_seq;

-- Drop tables (reverse dependency order)
DROP TABLE IF EXISTS public.utilisateur_entreprise;
DROP TABLE IF EXISTS public.utilisateur;
DROP TABLE IF EXISTS public.transfert_interne;
DROP TABLE IF EXISTS public.facture;
DROP TABLE IF EXISTS public.depense;
DROP TABLE IF EXISTS public.compte_financier;
DROP TABLE IF EXISTS public.client;
DROP TABLE IF EXISTS public.projet;
DROP TABLE IF EXISTS public.bureau;
DROP TABLE IF EXISTS public.role;
DROP TABLE IF EXISTS public.entreprise;
DROP TABLE IF EXISTS public.audit_event;

-- Drop sequences
DROP SEQUENCE IF EXISTS public.utilisateur_entreprise_id_seq;
DROP SEQUENCE IF EXISTS public.utilisateur_id_seq;
DROP SEQUENCE IF EXISTS public.transfert_interne_id_seq;
DROP SEQUENCE IF EXISTS public.facture_id_seq;
DROP SEQUENCE IF EXISTS public.depense_id_seq;
DROP SEQUENCE IF EXISTS public.compte_financier_id_seq;
DROP SEQUENCE IF EXISTS public.client_id_seq;
DROP SEQUENCE IF EXISTS public.projet_id_seq;
DROP SEQUENCE IF EXISTS public.bureau_id_seq;
DROP SEQUENCE IF EXISTS public.role_id_seq;
DROP SEQUENCE IF EXISTS public.entreprise_id_seq;
DROP SEQUENCE IF EXISTS public.audit_event_id_seq;

-- Drop functions
DROP FUNCTION IF EXISTS public.audit_event_trigger();
DROP FUNCTION IF EXISTS public.set_updated_at();
DROP FUNCTION IF EXISTS public.validate_depense_integrity();
DROP FUNCTION IF EXISTS public.check_solde_compte();
DROP FUNCTION IF EXISTS public.check_budget_projet();
DROP FUNCTION IF EXISTS public.apply_solde_depense();
DROP FUNCTION IF EXISTS public.validate_transfert_integrity();
DROP FUNCTION IF EXISTS public.validate_facture_integrity();
DROP FUNCTION IF EXISTS public.effectuer_transfert(integer, integer, numeric, character varying);
DROP FUNCTION IF EXISTS public.payer_facture(integer);


-- =========================================================
-- FUNCTIONS
-- =========================================================

-- 1) Generic updated_at trigger
CREATE FUNCTION public.set_updated_at() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;


-- 2) Audit trigger => writes into audit_event
-- Uses optional app.* settings for traceability (set by API layer)
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
-- Disable audit when explicitly requested (init/seed/migrations)
  IF current_setting('app.disable_audit', true) = '1' THEN
    RETURN NEW;
  END IF;

-- Also disable audit for seed actor
  IF current_setting('app.actor_id', true) = 'system:seed' THEN
    RETURN NEW;
  END IF;
  v_request_id := current_setting('app.request_id', true);
  v_actor_id := current_setting('app.actor_id', true);
  v_role := current_setting('app.role', true);

  v_entreprise_id := NULLIF(current_setting('app.entreprise_id', true), '')::int;
  v_projet_id := NULLIF(current_setting('app.projet_id', true), '')::int;

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


-- 3) Depense integrity (multi-entite + devise)
-- Enforce:
--  - compte_financier.entreprise_id == projet.entreprise_id
--  - depense.devise == compte_financier.devise
CREATE FUNCTION public.validate_depense_integrity() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_proj_ent INT;
  v_comp_ent INT;
  v_comp_dev TEXT;
BEGIN
  SELECT entreprise_id INTO v_proj_ent FROM projet WHERE id = NEW.projet_id;
  IF v_proj_ent IS NULL THEN
    RAISE EXCEPTION 'Projet % introuvable', NEW.projet_id;
  END IF;

  SELECT entreprise_id, devise INTO v_comp_ent, v_comp_dev FROM compte_financier WHERE id = NEW.compte_id;
  IF v_comp_ent IS NULL THEN
    RAISE EXCEPTION 'Compte % introuvable', NEW.compte_id;
  END IF;

  IF v_proj_ent <> v_comp_ent THEN
    RAISE EXCEPTION 'Incoherence multi-entite: projet(entreprise_id=%) != compte(entreprise_id=%)', v_proj_ent, v_comp_ent;
  END IF;

  IF NEW.devise <> v_comp_dev THEN
    RAISE EXCEPTION 'Incoherence devise: depense.devise=% != compte.devise=%', NEW.devise, v_comp_dev;
  END IF;

  RETURN NEW;
END;
$$;


-- 4) Balance check (INSERT/UPDATE)
-- Ensures the affected account(s) will not go negative.
CREATE FUNCTION public.check_solde_compte() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  solde_actuel NUMERIC;
  delta NUMERIC;
BEGIN
  IF TG_OP = 'INSERT' THEN
    delta := NEW.montant;

    SELECT solde INTO solde_actuel FROM compte_financier WHERE id = NEW.compte_id;
    IF solde_actuel IS NULL THEN
      RAISE EXCEPTION 'Compte % introuvable', NEW.compte_id;
    END IF;

    IF solde_actuel - delta < 0 THEN
      RAISE EXCEPTION 'Solde insuffisant sur le compte %', NEW.compte_id;
    END IF;

    RETURN NEW;

  ELSIF TG_OP = 'UPDATE' THEN
    -- If account changes, we must ensure the NEW account can afford NEW.montant
    -- (old account will be credited back later).
    IF NEW.compte_id <> OLD.compte_id THEN
      SELECT solde INTO solde_actuel FROM compte_financier WHERE id = NEW.compte_id;
      IF solde_actuel IS NULL THEN
        RAISE EXCEPTION 'Compte % introuvable', NEW.compte_id;
      END IF;
      IF solde_actuel - NEW.montant < 0 THEN
        RAISE EXCEPTION 'Solde insuffisant sur le compte %', NEW.compte_id;
      END IF;
      RETURN NEW;
    END IF;

    -- Same account: ensure it can afford the increment only
    delta := NEW.montant - OLD.montant;
    IF delta <= 0 THEN
      RETURN NEW;
    END IF;

    SELECT solde INTO solde_actuel FROM compte_financier WHERE id = NEW.compte_id;
    IF solde_actuel IS NULL THEN
      RAISE EXCEPTION 'Compte % introuvable', NEW.compte_id;
    END IF;

    IF solde_actuel - delta < 0 THEN
      RAISE EXCEPTION 'Solde insuffisant sur le compte %', NEW.compte_id;
    END IF;

    RETURN NEW;
  END IF;

  RETURN NEW;
END;
$$;


-- 5) Budget check (INSERT/UPDATE)
CREATE FUNCTION public.check_budget_projet() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  total_depenses NUMERIC;
  budget NUMERIC;
BEGIN
  IF TG_OP = 'INSERT' THEN
    SELECT COALESCE(SUM(montant), 0) INTO total_depenses
    FROM depense
    WHERE projet_id = NEW.projet_id;

    SELECT budget_total INTO budget
    FROM projet
    WHERE id = NEW.projet_id;

    IF budget IS NULL THEN
      RAISE EXCEPTION 'Projet % introuvable', NEW.projet_id;
    END IF;

    IF total_depenses + NEW.montant > budget THEN
      RAISE EXCEPTION 'Depassement du budget du projet %', NEW.projet_id;
    END IF;

    RETURN NEW;

  ELSIF TG_OP = 'UPDATE' THEN
    -- Recompute sum excluding current row
    SELECT COALESCE(SUM(montant), 0) INTO total_depenses
    FROM depense
    WHERE projet_id = NEW.projet_id
      AND id <> OLD.id;

    SELECT budget_total INTO budget
    FROM projet
    WHERE id = NEW.projet_id;

    IF budget IS NULL THEN
      RAISE EXCEPTION 'Projet % introuvable', NEW.projet_id;
    END IF;

    IF total_depenses + NEW.montant > budget THEN
      RAISE EXCEPTION 'Depassement du budget du projet %', NEW.projet_id;
    END IF;

    RETURN NEW;
  END IF;

  RETURN NEW;
END;
$$;


-- 6) Apply account balance changes for depense (INSERT/UPDATE/DELETE)
CREATE FUNCTION public.apply_solde_depense() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    UPDATE compte_financier
      SET solde = solde - NEW.montant
    WHERE id = NEW.compte_id;
    RETURN NEW;

  ELSIF TG_OP = 'UPDATE' THEN
    IF NEW.compte_id = OLD.compte_id THEN
      -- Same account: apply delta
      UPDATE compte_financier
        SET solde = solde - (NEW.montant - OLD.montant)
      WHERE id = NEW.compte_id;
    ELSE
      -- Account changed: credit old, debit new
      UPDATE compte_financier
        SET solde = solde + OLD.montant
      WHERE id = OLD.compte_id;

      UPDATE compte_financier
        SET solde = solde - NEW.montant
      WHERE id = NEW.compte_id;
    END IF;
    RETURN NEW;

  ELSIF TG_OP = 'DELETE' THEN
    UPDATE compte_financier
      SET solde = solde + OLD.montant
    WHERE id = OLD.compte_id;
    RETURN OLD;
  END IF;

  RETURN NEW;
END;
$$;


-- 7) Validate transfert integrity (same entreprise + same devise)
CREATE FUNCTION public.validate_transfert_integrity() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  src_ent INT;
  dst_ent INT;
  src_dev TEXT;
  dst_dev TEXT;
BEGIN
  SELECT entreprise_id, devise INTO src_ent, src_dev FROM compte_financier WHERE id = NEW.compte_source_id;
  IF src_ent IS NULL THEN
    RAISE EXCEPTION 'Compte source % introuvable', NEW.compte_source_id;
  END IF;

  SELECT entreprise_id, devise INTO dst_ent, dst_dev FROM compte_financier WHERE id = NEW.compte_destination_id;
  IF dst_ent IS NULL THEN
    RAISE EXCEPTION 'Compte destination % introuvable', NEW.compte_destination_id;
  END IF;

  IF src_ent <> dst_ent THEN
    RAISE EXCEPTION 'Transfert interdit: comptes de differentes entreprises (% vs %)', src_ent, dst_ent;
  END IF;

  IF NEW.devise <> src_dev OR NEW.devise <> dst_dev THEN
    RAISE EXCEPTION 'Transfert devise incoherente: transfert.devise=% source=% destination=%', NEW.devise, src_dev, dst_dev;
  END IF;

  RETURN NEW;
END;
$$;


-- 8) Validate facture integrity (client.entreprise_id must match projet.entreprise_id)
CREATE FUNCTION public.validate_facture_integrity() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  proj_ent INT;
  client_ent INT;
BEGIN
  SELECT entreprise_id INTO proj_ent FROM projet WHERE id = NEW.projet_id;
  IF proj_ent IS NULL THEN
    RAISE EXCEPTION 'Projet % introuvable', NEW.projet_id;
  END IF;

  SELECT entreprise_id INTO client_ent FROM client WHERE id = NEW.client_id;
  IF client_ent IS NULL THEN
    RAISE EXCEPTION 'Client % introuvable', NEW.client_id;
  END IF;

  IF proj_ent <> client_ent THEN
    RAISE EXCEPTION 'Facture interdite: client(entreprise_id=%) != projet(entreprise_id=%)', client_ent, proj_ent;
  END IF;

  RETURN NEW;
END;
$$;


-- 9) Transfer function (with integrity + balance checks)
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
  src_ent INT;
  dst_ent INT;
  src_dev TEXT;
  dst_dev TEXT;
BEGIN
  IF p_source_id = p_destination_id THEN
    RAISE EXCEPTION 'Les comptes source et destination doivent etre differents';
  END IF;

  SELECT solde, entreprise_id, devise INTO solde_source, src_ent, src_dev
  FROM compte_financier
  WHERE id = p_source_id;

  IF src_ent IS NULL THEN
    RAISE EXCEPTION 'Compte source % introuvable', p_source_id;
  END IF;

  SELECT entreprise_id, devise INTO dst_ent, dst_dev
  FROM compte_financier
  WHERE id = p_destination_id;

  IF dst_ent IS NULL THEN
    RAISE EXCEPTION 'Compte destination % introuvable', p_destination_id;
  END IF;

  IF src_ent <> dst_ent THEN
    RAISE EXCEPTION 'Transfert interdit: comptes de differentes entreprises (% vs %)', src_ent, dst_ent;
  END IF;

  IF p_devise <> src_dev OR p_devise <> dst_dev THEN
    RAISE EXCEPTION 'Transfert devise incoherente: p_devise=% source=% destination=%', p_devise, src_dev, dst_dev;
  END IF;

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


-- 10) Pay invoice function (unchanged)
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


-- Apply account balance changes for transfert_interne
CREATE OR REPLACE FUNCTION public.apply_solde_transfert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    -- Débit du compte source
    UPDATE compte_financier
      SET solde = solde - NEW.montant
    WHERE id = NEW.compte_source_id;

    -- Crédit du compte destination
    UPDATE compte_financier
      SET solde = solde + NEW.montant
    WHERE id = NEW.compte_destination_id;

    RETURN NEW;

  ELSIF TG_OP = 'DELETE' THEN
    -- Annulation du transfert
    UPDATE compte_financier
      SET solde = solde + OLD.montant
    WHERE id = OLD.compte_source_id;

    UPDATE compte_financier
      SET solde = solde - OLD.montant
    WHERE id = OLD.compte_destination_id;

    RETURN OLD;
  END IF;

  RETURN NEW;
END;
$$;

-- Check source account balance before transfert
CREATE OR REPLACE FUNCTION public.check_solde_transfert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  solde_source NUMERIC;
BEGIN
  SELECT solde INTO solde_source
  FROM compte_financier
  WHERE id = NEW.compte_source_id;

  IF solde_source IS NULL THEN
    RAISE EXCEPTION 'Compte source % introuvable', NEW.compte_source_id;
  END IF;

  IF solde_source - NEW.montant < 0 THEN
    RAISE EXCEPTION 'Solde insuffisant sur le compte source %', NEW.compte_source_id;
  END IF;

  RETURN NEW;
END;
$$;


-- =========================================================
-- TABLES + SEQUENCES
-- =========================================================

-- Audit table
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

CREATE SEQUENCE public.audit_event_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.audit_event_id_seq OWNED BY public.audit_event.id;


-- Entreprise
CREATE TABLE public.entreprise (
  id integer NOT NULL,
  nom character varying(100) NOT NULL,
  pays character varying(50),
  devise_principale character varying(10) NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now()
);

CREATE SEQUENCE public.entreprise_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.entreprise_id_seq OWNED BY public.entreprise.id;


-- Role
CREATE TABLE public.role (
  id integer NOT NULL,
  nom character varying(50) NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now()
);

CREATE SEQUENCE public.role_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.role_id_seq OWNED BY public.role.id;


-- Utilisateur
CREATE TABLE public.utilisateur (
  id integer NOT NULL,
  nom character varying(100),
  numero_whatsapp character varying(100) NOT NULL,
  role_id integer,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now()
);

CREATE SEQUENCE public.utilisateur_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.utilisateur_id_seq OWNED BY public.utilisateur.id;


-- Utilisateur <-> Entreprise (RBAC multi-entite)
CREATE TABLE public.utilisateur_entreprise (
  id integer NOT NULL,
  utilisateur_id integer NOT NULL,
  entreprise_id integer NOT NULL,
  role_id integer NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  CONSTRAINT utilisateur_entreprise_unique UNIQUE (utilisateur_id, entreprise_id)
);

CREATE SEQUENCE public.utilisateur_entreprise_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.utilisateur_entreprise_id_seq OWNED BY public.utilisateur_entreprise.id;


-- Bureau
CREATE TABLE public.bureau (
  id integer NOT NULL,
  entreprise_id integer NOT NULL,
  nom character varying(100),
  ville character varying(50),
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now()
);

CREATE SEQUENCE public.bureau_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.bureau_id_seq OWNED BY public.bureau.id;


-- Client (now multi-entite)
CREATE TABLE public.client (
  id integer NOT NULL,
  entreprise_id integer NOT NULL,
  nom character varying(100) NOT NULL,
  email character varying(100),
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now()
);

CREATE SEQUENCE public.client_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.client_id_seq OWNED BY public.client.id;


-- Compte financier
CREATE TABLE public.compte_financier (
  id integer NOT NULL,
  entreprise_id integer NOT NULL,
  nom character varying(100) NOT NULL,
  devise character varying(10) NOT NULL,
  solde numeric(14,2) DEFAULT 0,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  CONSTRAINT compte_financier_solde_check CHECK ((solde >= (0)::numeric))
);

CREATE SEQUENCE public.compte_financier_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.compte_financier_id_seq OWNED BY public.compte_financier.id;


-- Projet
CREATE TABLE public.projet (
  id integer NOT NULL,
  entreprise_id integer NOT NULL,
  nom character varying(100) NOT NULL,
  budget_total numeric(14,2) NOT NULL,
  date_debut date,
  date_fin date,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now()
);

CREATE SEQUENCE public.projet_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.projet_id_seq OWNED BY public.projet.id;


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
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  CONSTRAINT facture_statut_check CHECK (((statut)::text = ANY ((ARRAY['EMISE'::character varying, 'PAYEE'::character varying])::text[])))
);

CREATE SEQUENCE public.facture_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.facture_id_seq OWNED BY public.facture.id;


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
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  CONSTRAINT depense_montant_check CHECK ((montant > (0)::numeric))
);

CREATE SEQUENCE public.depense_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.depense_id_seq OWNED BY public.depense.id;


-- Transfert interne
CREATE TABLE public.transfert_interne (
  id integer NOT NULL,
  compte_source_id integer NOT NULL,
  compte_destination_id integer NOT NULL,
  montant numeric(14,2) NOT NULL,
  devise character varying(10) NOT NULL,
  date_transfert date NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  CONSTRAINT transfert_interne_check CHECK ((compte_source_id <> compte_destination_id)),
  CONSTRAINT transfert_interne_montant_check CHECK ((montant > (0)::numeric))
);

CREATE SEQUENCE public.transfert_interne_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.transfert_interne_id_seq OWNED BY public.transfert_interne.id;

CREATE TABLE IF NOT EXISTS taux_change (
  id          BIGSERIAL PRIMARY KEY,
  date_rate   DATE NOT NULL,
  from_devise VARCHAR(10) NOT NULL,
  to_devise   VARCHAR(10) NOT NULL,
  rate        NUMERIC(18,8) NOT NULL,
  source      TEXT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT ck_taux_change_rate_positive CHECK (rate > 0),
  CONSTRAINT ck_taux_change_pair_not_equal CHECK (from_devise <> to_devise),
  CONSTRAINT uq_taux_change_pair_day UNIQUE (date_rate, from_devise, to_devise)
);

CREATE INDEX IF NOT EXISTS ix_taux_change_lookup
ON taux_change (from_devise, to_devise, date_rate DESC);


-- =========================================================
-- DEFAULTS (serial)
-- =========================================================
ALTER TABLE ONLY public.audit_event ALTER COLUMN id SET DEFAULT nextval('public.audit_event_id_seq'::regclass);
ALTER TABLE ONLY public.entreprise ALTER COLUMN id SET DEFAULT nextval('public.entreprise_id_seq'::regclass);
ALTER TABLE ONLY public.role ALTER COLUMN id SET DEFAULT nextval('public.role_id_seq'::regclass);
ALTER TABLE ONLY public.utilisateur ALTER COLUMN id SET DEFAULT nextval('public.utilisateur_id_seq'::regclass);
ALTER TABLE ONLY public.utilisateur_entreprise ALTER COLUMN id SET DEFAULT nextval('public.utilisateur_entreprise_id_seq'::regclass);
ALTER TABLE ONLY public.bureau ALTER COLUMN id SET DEFAULT nextval('public.bureau_id_seq'::regclass);
ALTER TABLE ONLY public.client ALTER COLUMN id SET DEFAULT nextval('public.client_id_seq'::regclass);
ALTER TABLE ONLY public.compte_financier ALTER COLUMN id SET DEFAULT nextval('public.compte_financier_id_seq'::regclass);
ALTER TABLE ONLY public.projet ALTER COLUMN id SET DEFAULT nextval('public.projet_id_seq'::regclass);
ALTER TABLE ONLY public.facture ALTER COLUMN id SET DEFAULT nextval('public.facture_id_seq'::regclass);
ALTER TABLE ONLY public.depense ALTER COLUMN id SET DEFAULT nextval('public.depense_id_seq'::regclass);
ALTER TABLE ONLY public.transfert_interne ALTER COLUMN id SET DEFAULT nextval('public.transfert_interne_id_seq'::regclass);


-- =========================================================
-- CONSTRAINTS (PK / UNIQUE)
-- =========================================================
ALTER TABLE ONLY public.audit_event ADD CONSTRAINT audit_event_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.entreprise ADD CONSTRAINT entreprise_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.role ADD CONSTRAINT role_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.role ADD CONSTRAINT role_nom_key UNIQUE (nom);
ALTER TABLE ONLY public.utilisateur ADD CONSTRAINT utilisateur_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.utilisateur ADD CONSTRAINT utilisateur_numero_whatsapp_key UNIQUE (numero_whatsapp);
ALTER TABLE ONLY public.utilisateur_entreprise ADD CONSTRAINT utilisateur_entreprise_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.bureau ADD CONSTRAINT bureau_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.client ADD CONSTRAINT client_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.compte_financier ADD CONSTRAINT compte_financier_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.projet ADD CONSTRAINT projet_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.facture ADD CONSTRAINT facture_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.depense ADD CONSTRAINT depense_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.transfert_interne ADD CONSTRAINT transfert_interne_pkey PRIMARY KEY (id);

-- Uniques "pro" pour multi-entite
ALTER TABLE ONLY public.compte_financier ADD CONSTRAINT compte_financier_unique_nom UNIQUE (entreprise_id, nom);
ALTER TABLE ONLY public.projet ADD CONSTRAINT projet_unique_nom UNIQUE (entreprise_id, nom);


-- =========================================================
-- FOREIGN KEYS
-- =========================================================
ALTER TABLE ONLY public.bureau
  ADD CONSTRAINT bureau_entreprise_id_fkey FOREIGN KEY (entreprise_id) REFERENCES public.entreprise(id);

ALTER TABLE ONLY public.client
  ADD CONSTRAINT client_entreprise_id_fkey FOREIGN KEY (entreprise_id) REFERENCES public.entreprise(id);

ALTER TABLE ONLY public.compte_financier
  ADD CONSTRAINT compte_financier_entreprise_id_fkey FOREIGN KEY (entreprise_id) REFERENCES public.entreprise(id);

ALTER TABLE ONLY public.projet
  ADD CONSTRAINT projet_entreprise_id_fkey FOREIGN KEY (entreprise_id) REFERENCES public.entreprise(id);

ALTER TABLE ONLY public.facture
  ADD CONSTRAINT facture_projet_id_fkey FOREIGN KEY (projet_id) REFERENCES public.projet(id);

ALTER TABLE ONLY public.facture
  ADD CONSTRAINT facture_client_id_fkey FOREIGN KEY (client_id) REFERENCES public.client(id);

ALTER TABLE ONLY public.depense
  ADD CONSTRAINT depense_projet_id_fkey FOREIGN KEY (projet_id) REFERENCES public.projet(id);

ALTER TABLE ONLY public.depense
  ADD CONSTRAINT depense_compte_id_fkey FOREIGN KEY (compte_id) REFERENCES public.compte_financier(id);

ALTER TABLE ONLY public.transfert_interne
  ADD CONSTRAINT transfert_interne_compte_source_id_fkey FOREIGN KEY (compte_source_id) REFERENCES public.compte_financier(id);

ALTER TABLE ONLY public.transfert_interne
  ADD CONSTRAINT transfert_interne_compte_destination_id_fkey FOREIGN KEY (compte_destination_id) REFERENCES public.compte_financier(id);

ALTER TABLE ONLY public.utilisateur
  ADD CONSTRAINT utilisateur_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.role(id);

ALTER TABLE ONLY public.utilisateur_entreprise
  ADD CONSTRAINT utilisateur_entreprise_utilisateur_id_fkey FOREIGN KEY (utilisateur_id) REFERENCES public.utilisateur(id);

ALTER TABLE ONLY public.utilisateur_entreprise
  ADD CONSTRAINT utilisateur_entreprise_entreprise_id_fkey FOREIGN KEY (entreprise_id) REFERENCES public.entreprise(id);

ALTER TABLE ONLY public.utilisateur_entreprise
  ADD CONSTRAINT utilisateur_entreprise_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.role(id);


-- =========================================================
-- INDEXES (perf + serieux)
-- =========================================================
-- audit
CREATE INDEX IF NOT EXISTS idx_audit_event_created_at ON public.audit_event(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_event_request_id ON public.audit_event(request_id);
CREATE INDEX IF NOT EXISTS idx_audit_event_status ON public.audit_event(status);
CREATE INDEX IF NOT EXISTS idx_audit_event_entity ON public.audit_event(entity);

-- fks
CREATE INDEX IF NOT EXISTS idx_bureau_entreprise_id ON public.bureau(entreprise_id);
CREATE INDEX IF NOT EXISTS idx_client_entreprise_id ON public.client(entreprise_id);
CREATE INDEX IF NOT EXISTS idx_compte_entreprise_id ON public.compte_financier(entreprise_id);
CREATE INDEX IF NOT EXISTS idx_projet_entreprise_id ON public.projet(entreprise_id);
CREATE INDEX IF NOT EXISTS idx_facture_projet_id ON public.facture(projet_id);
CREATE INDEX IF NOT EXISTS idx_facture_client_id ON public.facture(client_id);
CREATE INDEX IF NOT EXISTS idx_depense_projet_id ON public.depense(projet_id);
CREATE INDEX IF NOT EXISTS idx_depense_compte_id ON public.depense(compte_id);
CREATE INDEX IF NOT EXISTS idx_transfert_source_id ON public.transfert_interne(compte_source_id);
CREATE INDEX IF NOT EXISTS idx_transfert_destination_id ON public.transfert_interne(compte_destination_id);
CREATE INDEX IF NOT EXISTS idx_utilisateur_entreprise_utilisateur_id ON public.utilisateur_entreprise(utilisateur_id);
CREATE INDEX IF NOT EXISTS idx_utilisateur_entreprise_entreprise_id ON public.utilisateur_entreprise(entreprise_id);


-- =========================================================
-- TRIGGERS
-- =========================================================

-- updated_at everywhere

CREATE TRIGGER trg_set_updated_at_entreprise BEFORE UPDATE ON public.entreprise FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER trg_set_updated_at_role BEFORE UPDATE ON public.role FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER trg_set_updated_at_utilisateur BEFORE UPDATE ON public.utilisateur FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER trg_set_updated_at_utilisateur_entreprise BEFORE UPDATE ON public.utilisateur_entreprise FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER trg_set_updated_at_bureau BEFORE UPDATE ON public.bureau FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER trg_set_updated_at_client BEFORE UPDATE ON public.client FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER trg_set_updated_at_compte_financier BEFORE UPDATE ON public.compte_financier FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER trg_set_updated_at_projet BEFORE UPDATE ON public.projet FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER trg_set_updated_at_facture BEFORE UPDATE ON public.facture FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER trg_set_updated_at_depense BEFORE UPDATE ON public.depense FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER trg_set_updated_at_transfert_interne BEFORE UPDATE ON public.transfert_interne FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- audit on key tables (not only depense)
CREATE TRIGGER trg_audit_entreprise AFTER INSERT OR UPDATE OR DELETE ON public.entreprise FOR EACH ROW EXECUTE FUNCTION public.audit_event_trigger();
CREATE TRIGGER trg_audit_role AFTER INSERT OR UPDATE OR DELETE ON public.role FOR EACH ROW EXECUTE FUNCTION public.audit_event_trigger();
CREATE TRIGGER trg_audit_utilisateur AFTER INSERT OR UPDATE OR DELETE ON public.utilisateur FOR EACH ROW EXECUTE FUNCTION public.audit_event_trigger();
CREATE TRIGGER trg_audit_utilisateur_entreprise AFTER INSERT OR UPDATE OR DELETE ON public.utilisateur_entreprise FOR EACH ROW EXECUTE FUNCTION public.audit_event_trigger();
CREATE TRIGGER trg_audit_bureau AFTER INSERT OR UPDATE OR DELETE ON public.bureau FOR EACH ROW EXECUTE FUNCTION public.audit_event_trigger();
CREATE TRIGGER trg_audit_client AFTER INSERT OR UPDATE OR DELETE ON public.client FOR EACH ROW EXECUTE FUNCTION public.audit_event_trigger();
CREATE TRIGGER trg_audit_compte_financier AFTER INSERT OR UPDATE OR DELETE ON public.compte_financier FOR EACH ROW EXECUTE FUNCTION public.audit_event_trigger();
CREATE TRIGGER trg_audit_projet AFTER INSERT OR UPDATE OR DELETE ON public.projet FOR EACH ROW EXECUTE FUNCTION public.audit_event_trigger();
CREATE TRIGGER trg_audit_facture AFTER INSERT OR UPDATE OR DELETE ON public.facture FOR EACH ROW EXECUTE FUNCTION public.audit_event_trigger();
CREATE TRIGGER trg_audit_depense AFTER INSERT OR UPDATE OR DELETE ON public.depense FOR EACH ROW EXECUTE FUNCTION public.audit_event_trigger();
CREATE TRIGGER trg_audit_transfert_interne AFTER INSERT OR UPDATE OR DELETE ON public.transfert_interne FOR EACH ROW EXECUTE FUNCTION public.audit_event_trigger();

-- facture integrity (client entreprise == projet entreprise)
CREATE TRIGGER trg_validate_facture BEFORE INSERT OR UPDATE ON public.facture
FOR EACH ROW EXECUTE FUNCTION public.validate_facture_integrity();

-- depense pipeline: validate -> check balance -> check budget
CREATE TRIGGER trg_validate_depense BEFORE INSERT OR UPDATE ON public.depense
FOR EACH ROW EXECUTE FUNCTION public.validate_depense_integrity();

CREATE TRIGGER trg_check_solde BEFORE INSERT OR UPDATE ON public.depense
FOR EACH ROW EXECUTE FUNCTION public.check_solde_compte();

CREATE TRIGGER trg_check_budget BEFORE INSERT OR UPDATE ON public.depense
FOR EACH ROW EXECUTE FUNCTION public.check_budget_projet();

-- apply balance changes (after successful write)
CREATE TRIGGER trg_apply_solde_depense AFTER INSERT OR UPDATE OR DELETE ON public.depense
FOR EACH ROW EXECUTE FUNCTION public.apply_solde_depense();

-- transfert integrity
CREATE TRIGGER trg_validate_transfert BEFORE INSERT OR UPDATE ON public.transfert_interne
FOR EACH ROW EXECUTE FUNCTION public.validate_transfert_integrity();

DROP TRIGGER IF EXISTS trg_check_solde_transfert ON public.transfert_interne;

CREATE TRIGGER trg_check_solde_transfert
BEFORE INSERT OR UPDATE ON public.transfert_interne
FOR EACH ROW
EXECUTE FUNCTION public.check_solde_transfert();

DROP TRIGGER IF EXISTS trg_apply_solde_transfert ON public.transfert_interne;

CREATE TRIGGER trg_apply_solde_transfert
AFTER INSERT OR DELETE ON public.transfert_interne
FOR EACH ROW
EXECUTE FUNCTION public.apply_solde_transfert();


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

CREATE VIEW public.vue_factures_statut AS
SELECT
  statut,
  COUNT(*) AS nombre_factures,
  SUM(montant) AS montant_total
FROM public.facture
GROUP BY statut;

CREATE VIEW public.vue_soldes_comptes AS
SELECT
  id AS compte_id,
  nom,
  devise,
  solde
FROM public.compte_financier;


-- =========================================================
-- FIX SEQUENCES
-- =========================================================
CREATE EXTENSION IF NOT EXISTS pg_trgm;
SELECT pg_catalog.setval('public.audit_event_id_seq', 1, true);
SELECT pg_catalog.setval('public.entreprise_id_seq', 2, true);
SELECT pg_catalog.setval('public.role_id_seq', 3, true);
SELECT pg_catalog.setval('public.utilisateur_id_seq', 3, true);
SELECT pg_catalog.setval('public.utilisateur_entreprise_id_seq', 4, true);
SELECT pg_catalog.setval('public.bureau_id_seq', 3, true);
SELECT pg_catalog.setval('public.client_id_seq', 6, true);
SELECT pg_catalog.setval('public.compte_financier_id_seq', 3, true);
SELECT pg_catalog.setval('public.projet_id_seq', 3, true);
SELECT pg_catalog.setval('public.facture_id_seq', 3, true);
SELECT pg_catalog.setval('public.depense_id_seq', 3, true);
SELECT pg_catalog.setval('public.transfert_interne_id_seq', 1, true);

-- Done
