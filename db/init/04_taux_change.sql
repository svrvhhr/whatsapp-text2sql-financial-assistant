-- =========================================================
-- ORIONIS - 03_taux_change.sql
-- Seed des taux de change (FX)
-- =========================================================

BEGIN;

-- ---------------------------------------------------------
-- Désactiver l'audit pendant le seed
-- ---------------------------------------------------------
SELECT set_config('app.disable_audit', '1', true);

-- ---------------------------------------------------------
-- Nettoyage optionnel (si tu veux rejouer proprement)
-- ---------------------------------------------------------
-- DELETE FROM taux_change;

-- ---------------------------------------------------------
-- Taux de change de référence (date fixe)
-- Base : EUR
-- ---------------------------------------------------------
INSERT INTO taux_change (date_rate, from_devise, to_devise, rate, source)
VALUES
  -- EUR → devises locales
  ('2026-01-27', 'EUR', 'DZD', 146.32000000, 'manual-demo'),
  ('2026-01-27', 'EUR', 'AED',   3.99000000, 'manual-demo'),
  ('2026-01-27', 'EUR', 'SYP',14000.00000000,'manual-demo'),

  -- Devises locales → EUR (inverse)
  ('2026-01-27', 'DZD', 'EUR', 1 / 146.32000000, 'manual-demo'),
  ('2026-01-27', 'AED', 'EUR', 1 /   3.99000000, 'manual-demo'),
  ('2026-01-27', 'SYP', 'EUR', 1 / 14000.00000000,'manual-demo')

ON CONFLICT (date_rate, from_devise, to_devise)
DO UPDATE SET
  rate   = EXCLUDED.rate,
  source = EXCLUDED.source;

-- ---------------------------------------------------------
-- Réactiver l'audit
-- ---------------------------------------------------------
SELECT set_config('app.disable_audit', '0', true);

COMMIT;
