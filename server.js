import express from 'express'
import pg from 'pg'
import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { readFileSync } from 'fs'
import { fileURLToPath } from 'url'
import { dirname, join, extname } from 'path'
import { randomUUID } from 'crypto'

const __dirname = dirname(fileURLToPath(import.meta.url))
const app = express()
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } })

// ── Allowed file types ────────────────────────────────────────────────────────

const ALLOWED_MIME_TYPES = new Set([
  // Documents
  'application/pdf',
  'application/msword',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'application/vnd.ms-excel',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'application/vnd.ms-powerpoint',
  'application/vnd.openxmlformats-officedocument.presentationml.presentation',
  'text/plain',
  'text/csv',
  // Images
  'image/jpeg',
  'image/png',
  'image/gif',
  'image/webp',
  'image/svg+xml',
  'image/tiff',
  // Video
  'video/mp4',
  'video/quicktime',
  'video/x-msvideo',
  'video/x-matroska',
  'video/webm',
  // Audio
  'audio/mpeg',
  'audio/wav',
  'audio/aac',
  'audio/ogg',
  'audio/x-m4a',
  // Archives
  'application/zip',
  'application/x-zip-compressed',
])

const BLOCKED_EXTENSIONS = new Set([
  '.exe', '.bat', '.cmd', '.sh', '.ps1', '.msi', '.dll', '.com',
  '.scr', '.vbs', '.js', '.jar', '.php', '.py', '.rb', '.pl',
])

// ── Rate limiting (in-memory, per token) ─────────────────────────────────────

const uploadCounts = new Map()
const RATE_WINDOW_MS = 60 * 1000  // 1 minute
const RATE_MAX       = 20         // max uploads per window per token

function isRateLimited(token) {
  const now = Date.now()
  const entry = uploadCounts.get(token) ?? { count: 0, windowStart: now }
  if (now - entry.windowStart > RATE_WINDOW_MS) {
    entry.count = 0
    entry.windowStart = now
  }
  entry.count++
  uploadCounts.set(token, entry)
  return entry.count > RATE_MAX
}

// ── Upload size ceiling ─────────────────────────────────────────────────────
//
// Clients upload straight to R2 via a presigned PUT, so there is no multer
// middleware to enforce a size limit. The cap is checked client-side before a
// URL is issued and re-verified server-side (HEAD) at confirm time. 5 GiB is the
// ceiling for a single S3/R2 PUT; larger would need multipart.

const MAX_UPLOAD_BYTES = 5 * 1024 * 1024 * 1024  // 5 GiB
function formatMaxSize() { return `${MAX_UPLOAD_BYTES / (1024 ** 3)} GB` }

const s3 = new S3Client({
  region: 'auto',
  endpoint: process.env.R2_ENDPOINT,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
})

app.use(express.static(join(__dirname, 'public')))
app.use(express.urlencoded({ extended: true }))
app.use(express.json())

// ── Delivery table init ───────────────────────────────────────────────────────

async function initDeliveryTables() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS delivery_links (
      token                 TEXT PRIMARY KEY,
      project_name          TEXT NOT NULL,
      client_name           TEXT,
      label                 TEXT,
      expires_at            TIMESTAMPTZ NOT NULL,
      created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      revoked_at            TIMESTAMPTZ,
      created_by_user_email TEXT,
      project_id            TEXT
    )
  `)
  // Idempotent ALTERs for existing deployments — Postgres supports IF NOT EXISTS
  // on ADD COLUMN, so re-running these on a fresh DB is a no-op.
  await db.query(`ALTER TABLE delivery_links ADD COLUMN IF NOT EXISTS created_by_user_email TEXT`)
  await db.query(`ALTER TABLE delivery_links ADD COLUMN IF NOT EXISTS project_id            TEXT`)
  await db.query(`
    CREATE TABLE IF NOT EXISTS delivery_link_assets (
      id              SERIAL PRIMARY KEY,
      token           TEXT NOT NULL REFERENCES delivery_links(token) ON DELETE CASCADE,
      r2_key          TEXT NOT NULL,
      filename        TEXT NOT NULL,
      file_size       BIGINT NOT NULL,
      mime_type       TEXT NOT NULL,
      thumbnail_url   TEXT,
      thumbnail_r2_key TEXT
    )
  `)
  // Migrate existing tables — safe to run repeatedly
  await db.query(`ALTER TABLE delivery_link_assets ADD COLUMN IF NOT EXISTS thumbnail_url    TEXT`)
  await db.query(`ALTER TABLE delivery_link_assets ADD COLUMN IF NOT EXISTS thumbnail_r2_key TEXT`)
  await db.query(`ALTER TABLE delivery_link_assets ADD COLUMN IF NOT EXISTS proxy_r2_key     TEXT`)
  await db.query(`ALTER TABLE delivery_link_assets ADD COLUMN IF NOT EXISTS proxy_file_size  BIGINT`)
  await db.query(`
    CREATE TABLE IF NOT EXISTS delivery_link_transcripts (
      id           SERIAL PRIMARY KEY,
      token        TEXT NOT NULL REFERENCES delivery_links(token) ON DELETE CASCADE,
      asset_r2_key TEXT NOT NULL,
      r2_key       TEXT NOT NULL,
      filename     TEXT NOT NULL,
      file_size    BIGINT NOT NULL,
      kind         TEXT NOT NULL
    )
  `)
  await db.query(`
    CREATE TABLE IF NOT EXISTS delivery_link_access (
      id          SERIAL PRIMARY KEY,
      token       TEXT NOT NULL REFERENCES delivery_links(token) ON DELETE CASCADE,
      accessed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      ip          TEXT,
      user_agent  TEXT,
      file_key    TEXT
    )
  `)
}

// ── Internal API auth ─────────────────────────────────────────────────────────

function requireApiKey(req, res, next) {
  const key = req.headers['x-api-key']
  if (!key || key !== process.env.INGEST_API_KEY) return res.status(401).json({ error: 'Unauthorized' })
  next()
}

// ── Delivery API CORS ─────────────────────────────────────────────────────────

app.use('/api/delivery', (req, res, next) => {
  const origin = process.env.DASHBOARD_ORIGIN || '*'
  res.setHeader('Access-Control-Allow-Origin', origin)
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Api-Key')
  if (req.method === 'OPTIONS') return res.sendStatus(204)
  next()
})

// ── Health / root ─────────────────────────────────────────────────────────────

app.get('/', (_req, res) => res.status(200).send('OK'))

// Temporary key diagnostic — remove after confirming Railway env vars
app.get('/api/key-debug', (_req, res) => {
  const k = process.env.INGEST_API_KEY
  res.json({
    set: !!k,
    length: k?.length ?? 0,
    first4: k?.slice(0, 4) ?? '',
    last4:  k?.slice(-4) ?? '',
  })
})

// ── Client upload page ────────────────────────────────────────────────────────

app.get('/c/:token', async (req, res) => {
  const result = await db.query(
    'SELECT id, first_name FROM ingest_clients WHERE token = $1 AND active = true',
    [req.params.token]
  )
  if (!result.rows.length) return res.status(404).send('Not found')

  const client = result.rows[0]
  const html = readFileSync(join(__dirname, 'views/client.html'), 'utf8')
    .replace('{{CLIENT_NAME}}', escapeHtml(client.first_name))
    .replace('{{TOKEN}}', escapeHtml(req.params.token))
  res.send(html)
})

// ── File upload (presigned direct-to-R2 handshake) ─────────────────────────────
//
// File bytes never transit this server. The browser:
//   1. POST /c/:token/request-upload → we validate + return a presigned PUT URL
//   2. PUTs the file straight to R2 (requires bucket CORS allowing PUT)
//   3. POST /c/:token/confirm-upload → we HEAD-verify and record the submission

app.post('/c/:token/request-upload', async (req, res) => {
  if (isRateLimited(req.params.token)) {
    return res.status(429).json({ error: 'Too many uploads — please wait a moment' })
  }

  const result = await db.query(
    'SELECT id FROM ingest_clients WHERE token = $1 AND active = true',
    [req.params.token]
  )
  if (!result.rows.length) return res.status(404).json({ error: 'Not found' })

  const { fileName, size, contentType } = req.body ?? {}
  if (!fileName || !contentType) {
    return res.status(400).json({ error: 'fileName and contentType are required' })
  }

  const ext = extname(fileName).toLowerCase()
  if (BLOCKED_EXTENSIONS.has(ext)) {
    return res.status(400).json({ error: `File type ${ext} is not allowed` })
  }
  if (!ALLOWED_MIME_TYPES.has(contentType)) {
    return res.status(400).json({ error: `File type ${contentType} is not allowed` })
  }
  if (typeof size === 'number' && size > MAX_UPLOAD_BYTES) {
    return res.status(400).json({ error: `File is too large (max ${formatMaxSize()})` })
  }

  const safeName = sanitizeFilename(fileName)
  const fileKey  = `${req.params.token}/${Date.now()}-${safeName}`

  // Sign ContentType too, so the stored object gets the right type and the
  // browser must PUT with a matching Content-Type header.
  const uploadUrl = await getSignedUrl(s3, new PutObjectCommand({
    Bucket: process.env.R2_BUCKET,
    Key: fileKey,
    ContentType: contentType,
  }), { expiresIn: 6 * 60 * 60 })  // 6h — room for large uploads on slow links

  res.json({ uploadUrl, fileKey })
})

app.post('/c/:token/confirm-upload', async (req, res) => {
  const result = await db.query(
    'SELECT id FROM ingest_clients WHERE token = $1 AND active = true',
    [req.params.token]
  )
  if (!result.rows.length) return res.status(404).json({ error: 'Not found' })

  const clientId = result.rows[0].id
  const { fileKey } = req.body ?? {}
  if (!fileKey) return res.status(400).json({ error: 'fileKey is required' })

  // The key must live under this token's prefix — stops a client claiming
  // another client's object.
  if (!fileKey.startsWith(`${req.params.token}/`)) {
    return res.status(403).json({ error: 'Forbidden' })
  }

  // Confirm the object actually landed in R2 and read its real size/type.
  let head
  try {
    head = await s3.send(new HeadObjectCommand({
      Bucket: process.env.R2_BUCKET,
      Key: fileKey,
    }))
  } catch {
    return res.status(400).json({ error: 'Upload not found in storage' })
  }

  const fileSize = Number(head.ContentLength ?? 0)
  const mimeType = head.ContentType || 'application/octet-stream'

  // Re-enforce the size ceiling — a presigned PUT can't cap size itself.
  if (fileSize > MAX_UPLOAD_BYTES) {
    await s3.send(new DeleteObjectCommand({ Bucket: process.env.R2_BUCKET, Key: fileKey }))
      .catch(() => {})
    return res.status(400).json({ error: `File is too large (max ${formatMaxSize()})` })
  }

  // Idempotent — a retried confirm shouldn't insert a duplicate row.
  const existing = await db.query(
    'SELECT id FROM ingest_submissions WHERE file_key = $1',
    [fileKey]
  )
  if (existing.rows.length) return res.json({ ok: true })

  // Recover the original (already-sanitized) name embedded in the key.
  const fileName = fileKey.split('/').pop().replace(/^\d+-/, '')

  await db.query(
    `INSERT INTO ingest_submissions (client_id, file_key, file_name, file_size, mime_type)
     VALUES ($1, $2, $3, $4, $5)`,
    [clientId, fileKey, fileName, fileSize, mimeType]
  )

  res.json({ ok: true })
})

// ── File list for a client ────────────────────────────────────────────────────

app.get('/c/:token/files', async (req, res) => {
  const result = await db.query(
    'SELECT id FROM ingest_clients WHERE token = $1 AND active = true',
    [req.params.token]
  )
  if (!result.rows.length) return res.status(404).json({ error: 'Not found' })

  const files = await db.query(
    `SELECT file_name, file_size, mime_type, file_key, created_at
     FROM ingest_submissions
     WHERE client_id = $1
     ORDER BY created_at DESC`,
    [result.rows[0].id]
  )

  res.json(files.rows)
})

// ── Presigned download URL ────────────────────────────────────────────────────

app.get('/c/:token/download', async (req, res) => {
  const { key } = req.query
  if (!key) return res.status(400).json({ error: 'No key' })

  // Verify this key belongs to this client's token
  const result = await db.query(
    `SELECT s.id FROM ingest_submissions s
     JOIN ingest_clients c ON c.id = s.client_id
     WHERE c.token = $1 AND s.file_key = $2`,
    [req.params.token, key]
  )
  if (!result.rows.length) return res.status(403).json({ error: 'Forbidden' })

  const url = await getSignedUrl(s3, new GetObjectCommand({
    Bucket: process.env.R2_BUCKET,
    Key: key,
  }), { expiresIn: 300 })

  res.redirect(url)
})

// ── Delivery API ──────────────────────────────────────────────────────────────
//
// Files are uploaded directly to R2 by the LPOS dashboard before calling these
// endpoints. The ingest server only registers/manages the link metadata and
// serves the public delivery page.

// Create a delivery link
app.post('/api/delivery', requireApiKey, async (req, res) => {
  const {
    token: requestToken,
    project_name, client_name, label, expires_at, assets,
    created_by_user_email, project_id,
  } = req.body
  if (!project_name || !expires_at || !Array.isArray(assets) || !assets.length) {
    return res.status(400).json({ error: 'project_name, expires_at, and assets are required' })
  }
  for (const a of assets) {
    if (!a.r2_key || !a.filename || !a.file_size || !a.mime_type) {
      return res.status(400).json({ error: 'Each asset requires r2_key, filename, file_size, mime_type' })
    }
  }

  // Use the dashboard-supplied token (so R2 keys already uploaded under that token match).
  // Fall back to generating one for older callers that don't send a token.
  const token = requestToken || randomUUID()

  await db.query(
    `INSERT INTO delivery_links
       (token, project_name, client_name, label, expires_at, created_by_user_email, project_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [
      token, project_name, client_name || null, label || null, expires_at,
      created_by_user_email || null, project_id || null,
    ]
  )

  for (const a of assets) {
    await db.query(
      `INSERT INTO delivery_link_assets (token, r2_key, filename, file_size, mime_type, thumbnail_url, thumbnail_r2_key)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [token, a.r2_key, a.filename, a.file_size, a.mime_type, a.thumbnail_url || null, a.thumbnail_r2_key || null]
    )
  }

  const baseUrl = process.env.INGEST_BASE_URL || `http://localhost:${process.env.PORT || 3000}`
  res.json({ token, url: `${baseUrl}/d/${token}` })
})

// List delivery links — optionally filter by project_name
app.get('/api/delivery', requireApiKey, async (req, res) => {
  const { project_name } = req.query
  const params = []
  let where = 'WHERE dl.revoked_at IS NULL'
  if (project_name) {
    params.push(project_name)
    where += ` AND dl.project_name = $${params.length}`
  }

  const result = await db.query(
    `SELECT
       dl.token, dl.project_name, dl.client_name, dl.label,
       dl.expires_at, dl.created_at,
       COUNT(DISTINCT a.id)::int        AS asset_count,
       COUNT(DISTINCT ac.id)::int       AS access_count,
       MAX(ac.accessed_at)              AS last_accessed_at
     FROM delivery_links dl
     LEFT JOIN delivery_link_assets  a  ON a.token  = dl.token
     LEFT JOIN delivery_link_access  ac ON ac.token = dl.token
     ${where}
     GROUP BY dl.token
     ORDER BY dl.created_at DESC`,
    params
  )

  const baseUrl = process.env.INGEST_BASE_URL || `http://localhost:${process.env.PORT || 3000}`
  res.json(result.rows.map(r => ({ ...r, url: `${baseUrl}/d/${r.token}` })))
})

// Update label or expiry on a delivery link
app.patch('/api/delivery/:token', requireApiKey, async (req, res) => {
  const { label, expires_at } = req.body
  const sets = []
  const params = [req.params.token]

  if (label !== undefined) { params.push(label); sets.push(`label = $${params.length}`) }
  if (expires_at)          { params.push(expires_at); sets.push(`expires_at = $${params.length}`) }

  if (!sets.length) return res.status(400).json({ error: 'Nothing to update' })

  const result = await db.query(
    `UPDATE delivery_links SET ${sets.join(', ')} WHERE token = $1 AND revoked_at IS NULL RETURNING token`,
    params
  )
  if (!result.rows.length) return res.status(404).json({ error: 'Not found or already revoked' })
  res.json({ ok: true })
})

// Mark proxy ready for a specific asset — called by dashboard after each proxy upload
app.patch('/api/delivery/:token/assets/proxy', requireApiKey, async (req, res) => {
  const { r2_key, proxy_r2_key, proxy_file_size } = req.body
  if (!r2_key || !proxy_r2_key || !proxy_file_size) {
    return res.status(400).json({ error: 'r2_key, proxy_r2_key, and proxy_file_size are required' })
  }
  const result = await db.query(
    `UPDATE delivery_link_assets SET proxy_r2_key = $1, proxy_file_size = $2
     WHERE token = $3 AND r2_key = $4 RETURNING id`,
    [proxy_r2_key, proxy_file_size, req.params.token, r2_key]
  )
  if (!result.rows.length) return res.status(404).json({ error: 'Asset not found' })
  res.json({ ok: true })
})

// Register transcripts for an asset — called by dashboard after transcript upload
app.post('/api/delivery/:token/transcripts', requireApiKey, async (req, res) => {
  const { asset_r2_key, transcripts } = req.body
  if (!asset_r2_key || !Array.isArray(transcripts) || !transcripts.length) {
    return res.status(400).json({ error: 'asset_r2_key and transcripts array are required' })
  }
  for (const t of transcripts) {
    if (!t.r2_key || !t.filename || !t.file_size || !t.kind) {
      return res.status(400).json({ error: 'Each transcript requires r2_key, filename, file_size, kind' })
    }
  }
  // Verify asset belongs to this token
  const asset = await db.query(
    'SELECT id FROM delivery_link_assets WHERE token = $1 AND r2_key = $2',
    [req.params.token, asset_r2_key]
  )
  if (!asset.rows.length) return res.status(404).json({ error: 'Asset not found' })

  for (const t of transcripts) {
    await db.query(
      `INSERT INTO delivery_link_transcripts (token, asset_r2_key, r2_key, filename, file_size, kind)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT DO NOTHING`,
      [req.params.token, asset_r2_key, t.r2_key, t.filename, t.file_size, t.kind]
    )
  }
  res.json({ ok: true })
})

// Revoke a delivery link and delete its R2 objects
app.delete('/api/delivery/:token', requireApiKey, async (req, res) => {
  const link = await db.query(
    'SELECT token FROM delivery_links WHERE token = $1 AND revoked_at IS NULL',
    [req.params.token]
  )
  if (!link.rows.length) return res.status(404).json({ error: 'Not found or already revoked' })

  const assets = await db.query(
    'SELECT r2_key, thumbnail_r2_key, proxy_r2_key FROM delivery_link_assets WHERE token = $1',
    [req.params.token]
  )
  const transcripts = await db.query(
    'SELECT r2_key FROM delivery_link_transcripts WHERE token = $1',
    [req.params.token]
  )

  const keysToDelete = [
    ...assets.rows.flatMap(a => [a.r2_key, a.thumbnail_r2_key, a.proxy_r2_key].filter(Boolean)),
    ...transcripts.rows.map(t => t.r2_key),
  ]
  await Promise.all(keysToDelete.map(k =>
    s3.send(new DeleteObjectCommand({ Bucket: process.env.R2_BUCKET, Key: k })).catch(() => {})
  ))

  await db.query(
    'UPDATE delivery_links SET revoked_at = NOW() WHERE token = $1',
    [req.params.token]
  )

  res.json({ ok: true })
})

// ── Delivery public page ──────────────────────────────────────────────────────

app.get('/d/:token', async (req, res) => {
  const result = await db.query(
    `SELECT project_name, client_name, label, expires_at, revoked_at
     FROM delivery_links WHERE token = $1`,
    [req.params.token]
  )
  if (!result.rows.length) return res.status(404).send('Not found')
  const link = result.rows[0]
  if (link.revoked_at || new Date(link.expires_at) < new Date()) {
    return res.status(410).send('This delivery link has expired.')
  }

  const expiryStr = new Date(link.expires_at).toLocaleDateString(undefined, {
    month: 'long', day: 'numeric', year: 'numeric',
  })

  const clientNameTitle = link.client_name ? `${escapeHtml(link.client_name)} — ` : ''

  const html = readFileSync(join(__dirname, 'views/delivery.html'), 'utf8')
    .replace('{{TOKEN}}',             escapeHtml(req.params.token))
    .replace('{{PROJECT_NAME}}',      escapeHtml(link.project_name))
    .replace('{{CLIENT_NAME}}',       escapeHtml(link.client_name || ''))
    .replace('{{CLIENT_NAME_TITLE}}', clientNameTitle)
    .replace('{{LABEL}}',             escapeHtml(link.label || ''))
    .replace('{{EXPIRES}}',           escapeHtml(expiryStr))

  res.send(html)
})

// File list for a delivery token
app.get('/d/:token/files', async (req, res) => {
  const link = await db.query(
    'SELECT expires_at, revoked_at FROM delivery_links WHERE token = $1',
    [req.params.token]
  )
  if (!link.rows.length) return res.status(404).json({ error: 'Not found' })
  const { expires_at, revoked_at } = link.rows[0]
  if (revoked_at || new Date(expires_at) < new Date()) {
    return res.status(410).json({ error: 'Expired' })
  }

  const assets = await db.query(
    `SELECT r2_key, filename, file_size, mime_type, thumbnail_url, thumbnail_r2_key,
            proxy_r2_key, proxy_file_size
     FROM delivery_link_assets WHERE token = $1 ORDER BY id`,
    [req.params.token]
  )

  const transcriptRows = await db.query(
    'SELECT asset_r2_key, r2_key, filename, file_size, kind FROM delivery_link_transcripts WHERE token = $1',
    [req.params.token]
  )
  // Group transcripts by asset r2_key for O(1) lookup below
  const transcriptsByAsset = new Map()
  for (const t of transcriptRows.rows) {
    if (!transcriptsByAsset.has(t.asset_r2_key)) transcriptsByAsset.set(t.asset_r2_key, [])
    transcriptsByAsset.get(t.asset_r2_key).push({ r2_key: t.r2_key, filename: t.filename, file_size: t.file_size, kind: t.kind })
  }

  const rows = await Promise.all(assets.rows.map(async (a) => {
    let thumbnailUrl = a.thumbnail_url || null
    if (!thumbnailUrl && a.thumbnail_r2_key) {
      thumbnailUrl = await getSignedUrl(s3, new GetObjectCommand({
        Bucket: process.env.R2_BUCKET,
        Key: a.thumbnail_r2_key,
      }), { expiresIn: 86400 }).catch(() => null)
    }
    return {
      r2_key:          a.r2_key,
      filename:        a.filename,
      file_size:       a.file_size,
      mime_type:       a.mime_type,
      thumbnail_url:   thumbnailUrl,
      proxy_r2_key:    a.proxy_r2_key    || null,
      proxy_file_size: a.proxy_file_size || null,
      transcripts:     transcriptsByAsset.get(a.r2_key) || [],
    }
  }))

  res.json(rows)
})

// Range-aware file download proxy — logs access per file
// Returns a short-lived pre-signed R2 URL so the browser downloads directly
// from Cloudflare instead of piping through this server. All validation and
// access logging still happens here; only the bytes bypass Railway.
app.get('/d/:token/download', async (req, res) => {
  const { key } = req.query
  if (!key) return res.status(400).json({ error: 'Missing key' })

  const link = await db.query(
    'SELECT expires_at, revoked_at FROM delivery_links WHERE token = $1',
    [req.params.token]
  )
  if (!link.rows.length) return res.status(404).json({ error: 'Not found' })
  const { expires_at, revoked_at } = link.rows[0]
  if (revoked_at || new Date(expires_at) < new Date()) return res.status(410).json({ error: 'Expired' })

  // Verify this key belongs to this token — check original, proxy, and transcript keys
  const asset = await db.query(
    `SELECT filename, mime_type, file_size, r2_key AS orig_key, proxy_r2_key, proxy_file_size
     FROM delivery_link_assets
     WHERE token = $1 AND (r2_key = $2 OR proxy_r2_key = $2)`,
    [req.params.token, key]
  )
  let filename, mime_type, file_size
  if (asset.rows.length) {
    const a = asset.rows[0]
    const isProxy = a.proxy_r2_key === key && a.orig_key !== key
    filename  = a.filename
    mime_type = isProxy ? 'video/mp4' : (a.mime_type || 'application/octet-stream')
    file_size = isProxy ? a.proxy_file_size : a.file_size
  } else {
    const transcript = await db.query(
      'SELECT filename, file_size FROM delivery_link_transcripts WHERE token = $1 AND r2_key = $2',
      [req.params.token, key]
    )
    if (!transcript.rows.length) return res.status(403).json({ error: 'Forbidden' })
    filename  = transcript.rows[0].filename
    mime_type = 'application/octet-stream'
    file_size = transcript.rows[0].file_size
  }

  // Pre-sign for 4 hours — enough for slow connections downloading large files.
  const url = await getSignedUrl(
    s3,
    new GetObjectCommand({
      Bucket: process.env.R2_BUCKET,
      Key:    key,
      ResponseContentDisposition: `attachment; filename="${sanitizeFilename(filename)}"`,
      ResponseContentType: mime_type,
    }),
    { expiresIn: 4 * 60 * 60 }
  )

  // Log once per file (not per chunk — chunks now go direct to R2).
  db.query(
    'INSERT INTO delivery_link_access (token, ip, user_agent, file_key) VALUES ($1, $2, $3, $4)',
    [req.params.token, req.ip, req.headers['user-agent'] || null, key]
  ).catch(() => {})

  res.json({ url, filename, mime_type, file_size })
})

// ── Delivery trouble report ───────────────────────────────────────────────────
//
// Public POST endpoint hit by the delivery page when a recipient submits the
// "Having trouble?" form. The ingest server looks up the delivery's owner,
// then forwards the report to the dashboard's internal endpoint, which
// handles in-app notification + Slack DM fan-out.
//
// Rate limit: max 2 reports per token per 5-minute window, enforced in-memory.
// More than enough for legitimate use; cheap and obvious spam guard.

const TROUBLE_RATE_LIMIT = { max: 2, windowMs: 5 * 60_000 }
const troubleReportTimes = new Map() // token → [unix_ms, unix_ms]

function checkTroubleRateLimit(token) {
  const now = Date.now()
  const cutoff = now - TROUBLE_RATE_LIMIT.windowMs
  const recent = (troubleReportTimes.get(token) ?? []).filter((t) => t > cutoff)
  if (recent.length >= TROUBLE_RATE_LIMIT.max) return false
  recent.push(now)
  troubleReportTimes.set(token, recent)
  return true
}

// Sweep stale rate-limit entries hourly so the map can't grow unbounded
setInterval(() => {
  const cutoff = Date.now() - TROUBLE_RATE_LIMIT.windowMs
  for (const [token, times] of troubleReportTimes) {
    const fresh = times.filter((t) => t > cutoff)
    if (!fresh.length) troubleReportTimes.delete(token)
    else troubleReportTimes.set(token, fresh)
  }
}, 60 * 60 * 1000)

app.post('/d/:token/report-trouble', async (req, res) => {
  const { token } = req.params
  const { description, queueSummary } = req.body ?? {}

  if (!checkTroubleRateLimit(token)) {
    return res.status(429).json({ error: 'Too many reports — please wait a few minutes' })
  }

  // Look up the delivery — must be live (not revoked or expired) to accept reports
  const link = await db.query(
    `SELECT project_name, client_name, label, expires_at, revoked_at,
            created_by_user_email, project_id
     FROM delivery_links WHERE token = $1`,
    [token]
  )
  if (!link.rows.length) return res.status(404).json({ error: 'Not found' })
  const l = link.rows[0]
  if (l.revoked_at || new Date(l.expires_at) < new Date()) {
    return res.status(410).json({ error: 'Expired' })
  }

  const dashboardOrigin = (process.env.DASHBOARD_ORIGIN || '').replace(/\/$/, '')
  const apiKey = process.env.INGEST_API_KEY
  if (!dashboardOrigin || !apiKey) {
    // Misconfigured — log loudly but still 200 so the recipient gets a clean
    // confirmation. The recipient shouldn't see our infra issues.
    console.error('[delivery-trouble] DASHBOARD_ORIGIN or INGEST_API_KEY missing — alert dropped')
    return res.json({ ok: true })
  }

  const payload = {
    deliveryToken:      token,
    projectName:        l.project_name,
    clientName:         l.client_name,
    label:              l.label,
    description:        typeof description === 'string' ? description.slice(0, 2000) : null,
    queueSummary:       typeof queueSummary === 'string' ? queueSummary.slice(0, 200) : null,
    userAgent:          req.headers['user-agent'] || null,
    createdByUserEmail: l.created_by_user_email,
    projectId:          l.project_id,
  }

  // Fire-and-forget — never block the recipient's request on dashboard latency.
  // Failures are logged but the recipient still gets 200; their ack is what matters.
  fetch(`${dashboardOrigin}/api/internal/delivery-trouble`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey },
    body:    JSON.stringify(payload),
  })
    .then(async (r) => {
      if (!r.ok) {
        const text = await r.text().catch(() => '(unreadable)')
        console.error(`[delivery-trouble] dashboard returned ${r.status}: ${text}`)
      }
    })
    .catch((err) => {
      console.error('[delivery-trouble] forward to dashboard failed:', err)
    })

  res.json({ ok: true })
})

// ── Delivery sweep — purge expired links and their R2 objects ─────────────────

async function notifyDeliveryExpired({ token, project_name, client_name, label, created_by_user_email, project_id }) {
  const dashboardOrigin = (process.env.DASHBOARD_ORIGIN || '').replace(/\/$/, '')
  const apiKey = process.env.INGEST_API_KEY
  if (!dashboardOrigin || !apiKey) return
  try {
    const r = await fetch(`${dashboardOrigin}/api/internal/delivery-expired`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey },
      body: JSON.stringify({
        deliveryToken:      token,
        projectName:        project_name,
        clientName:         client_name,
        label,
        createdByUserEmail: created_by_user_email,
        projectId:          project_id,
      }),
    })
    if (!r.ok) console.error(`[delivery-expired] dashboard returned ${r.status}`)
  } catch (err) {
    console.error('[delivery-expired] forward to dashboard failed:', err)
  }
}

async function sweepExpiredDeliveries() {
  const expired = await db.query(
    `SELECT token, project_name, client_name, label, created_by_user_email, project_id
     FROM delivery_links
     WHERE revoked_at IS NULL AND expires_at < NOW()`
  )
  for (const link of expired.rows) {
    const { token, project_name, client_name, label, created_by_user_email, project_id } = link
    const assets = await db.query(
      'SELECT r2_key, thumbnail_r2_key, proxy_r2_key FROM delivery_link_assets WHERE token = $1',
      [token]
    )
    const transcripts = await db.query(
      'SELECT r2_key FROM delivery_link_transcripts WHERE token = $1',
      [token]
    )
    const keysToDelete = [
      ...assets.rows.flatMap(a => [a.r2_key, a.thumbnail_r2_key, a.proxy_r2_key].filter(Boolean)),
      ...transcripts.rows.map(t => t.r2_key),
    ]
    await Promise.all(keysToDelete.map(k =>
      s3.send(new DeleteObjectCommand({ Bucket: process.env.R2_BUCKET, Key: k })).catch(() => {})
    ))
    await db.query('UPDATE delivery_links SET revoked_at = NOW() WHERE token = $1', [token])
    notifyDeliveryExpired({ token, project_name, client_name, label, created_by_user_email, project_id }).catch(() => {})
  }
}

setInterval(sweepExpiredDeliveries, 60 * 60 * 1000) // hourly

// ── Helpers ───────────────────────────────────────────────────────────────────

function sanitizeFilename(name) {
  return name
    .replace(/[^a-zA-Z0-9._\-() ]/g, '_')  // replace unsafe chars
    .replace(/\.{2,}/g, '.')                 // collapse multiple dots (path traversal)
    .replace(/^[\s.]+|[\s.]+$/g, '')         // trim leading/trailing dots and spaces
    .slice(0, 200)                           // cap length
    || 'upload'
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

// Start listening immediately so Railway's health check passes, then init the
// DB in the background with retries. The DB is almost always ready by the time
// real traffic arrives; if not, individual requests will fail gracefully.
app.listen(process.env.PORT || 3000, () => {
  console.log(`lpos-ingest running on :${process.env.PORT || 3000}`)
})

async function initWithRetry(attempts = 0) {
  try {
    await initDeliveryTables()
    console.log('DB tables ready')
  } catch (err) {
    const delay = Math.min(2 ** attempts * 1000, 30_000)
    console.error(`DB init failed (attempt ${attempts + 1}), retrying in ${delay / 1000}s:`, err.message)
    setTimeout(() => initWithRetry(attempts + 1), delay)
  }
}
initWithRetry()
