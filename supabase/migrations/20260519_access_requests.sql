-- Access requests table for invite-only signup flow
CREATE TABLE IF NOT EXISTS access_requests (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  reason TEXT,
  status TEXT NOT NULL DEFAULT 'pending', -- pending | approved | denied
  reviewed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_access_requests_status ON access_requests(status);
CREATE INDEX IF NOT EXISTS idx_access_requests_email ON access_requests(email);

-- Anyone can insert (apply), nobody can read their own row (admin only via service_role)
ALTER TABLE access_requests ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Anyone can apply"
  ON access_requests FOR INSERT
  WITH CHECK (true);

-- Only service_role (admin edge function) can read/update
