ALTER TABLE public.audit_service_event
  ADD COLUMN IF NOT EXISTS entreprise_id INT,
  ADD COLUMN IF NOT EXISTS projet_id INT,
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_audit_service_event_created_at
  ON public.audit_service_event (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_service_event_request_id
  ON public.audit_service_event (request_id);

CREATE INDEX IF NOT EXISTS idx_audit_service_event_status
  ON public.audit_service_event (status);

CREATE INDEX IF NOT EXISTS idx_audit_service_event_operation
  ON public.audit_service_event (operation);

CREATE INDEX IF NOT EXISTS idx_audit_service_event_actor_id
  ON public.audit_service_event (actor_id);
