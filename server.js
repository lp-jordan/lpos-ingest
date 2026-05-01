import express from 'express'
import multer from 'multer'
import pg from 'pg'
import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3'
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

// ── Multer ────────────────────────────────────────────────────────────────────

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 500 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const ext = extname(file.originalname).toLowerCase()
    if (BLOCKED_EXTENSIONS.has(ext)) {
      return cb(new Error(`File type ${ext} is not allowed`))
    }
    if (!ALLOWED_MIME_TYPES.has(file.mimetype)) {
      return cb(new Error(`MIME type ${file.mimetype} is not allowed`))
    }
    cb(null, true)
  },
})

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
      token        TEXT PRIMARY KEY,
      project_name TEXT NOT NULL,
      client_name  TEXT,
      label        TEXT,
      expires_at   TIMESTAMPTZ NOT NULL,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      revoked_at   TIMESTAMPTZ
    )
  `)
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
  await db.query(`ALTER TABLE delivery_link_assets ADD COLUMN IF NOT EXISTS thumbnail_url TEXT`)
  await db.query(`ALTER TABLE delivery_link_assets ADD COLUMN IF NOT EXISTS thumbnail_r2_key TEXT`)
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

// ── File upload ───────────────────────────────────────────────────────────────

app.post('/c/:token/upload', (req, res, next) => {
  // Run multer, catch fileFilter rejections
  upload.single('file')(req, res, (err) => {
    if (err) return res.status(400).json({ error: err.message })
    next()
  })
}, async (req, res) => {
  if (isRateLimited(req.params.token)) {
    return res.status(429).json({ error: 'Too many uploads — please wait a moment' })
  }

  const result = await db.query(
    'SELECT id FROM ingest_clients WHERE token = $1 AND active = true',
    [req.params.token]
  )
  if (!result.rows.length) return res.status(404).json({ error: 'Not found' })

  if (!req.file) return res.status(400).json({ error: 'No file' })

  const clientId = result.rows[0].id
  const safeName = sanitizeFilename(req.file.originalname)
  const fileKey  = `${req.params.token}/${Date.now()}-${safeName}`

  await s3.send(new PutObjectCommand({
    Bucket: process.env.R2_BUCKET,
    Key: fileKey,
    Body: req.file.buffer,
    ContentType: req.file.mimetype,
  }))

  await db.query(
    `INSERT INTO ingest_submissions (client_id, file_key, file_name, file_size, mime_type)
     VALUES ($1, $2, $3, $4, $5)`,
    [clientId, fileKey, safeName, req.file.size, req.file.mimetype]
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
  const { project_name, client_name, label, expires_at, assets } = req.body
  if (!project_name || !expires_at || !Array.isArray(assets) || !assets.length) {
    return res.status(400).json({ error: 'project_name, expires_at, and assets are required' })
  }
  for (const a of assets) {
    if (!a.r2_key || !a.filename || !a.file_size || !a.mime_type) {
      return res.status(400).json({ error: 'Each asset requires r2_key, filename, file_size, mime_type' })
    }
  }

  const token = randomUUID()

  await db.query(
    `INSERT INTO delivery_links (token, project_name, client_name, label, expires_at)
     VALUES ($1, $2, $3, $4, $5)`,
    [token, project_name, client_name || null, label || null, expires_at]
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

// Revoke a delivery link and delete its R2 objects
app.delete('/api/delivery/:token', requireApiKey, async (req, res) => {
  const link = await db.query(
    'SELECT token FROM delivery_links WHERE token = $1 AND revoked_at IS NULL',
    [req.params.token]
  )
  if (!link.rows.length) return res.status(404).json({ error: 'Not found or already revoked' })

  const assets = await db.query(
    'SELECT r2_key, thumbnail_r2_key FROM delivery_link_assets WHERE token = $1',
    [req.params.token]
  )

  await Promise.all(assets.rows.flatMap(a => [
    s3.send(new DeleteObjectCommand({ Bucket: process.env.R2_BUCKET, Key: a.r2_key })).catch(() => {}),
    a.thumbnail_r2_key
      ? s3.send(new DeleteObjectCommand({ Bucket: process.env.R2_BUCKET, Key: a.thumbnail_r2_key })).catch(() => {})
      : null,
  ].filter(Boolean)))

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
    'SELECT r2_key, filename, file_size, mime_type, thumbnail_url, thumbnail_r2_key FROM delivery_link_assets WHERE token = $1 ORDER BY id',
    [req.params.token]
  )

  const rows = await Promise.all(assets.rows.map(async (a) => {
    let thumbnailUrl = a.thumbnail_url || null
    if (!thumbnailUrl && a.thumbnail_r2_key) {
      thumbnailUrl = await getSignedUrl(s3, new GetObjectCommand({
        Bucket: process.env.R2_BUCKET,
        Key: a.thumbnail_r2_key,
      }), { expiresIn: 86400 }).catch(() => null) // 24h, silent on error
    }
    return { r2_key: a.r2_key, filename: a.filename, file_size: a.file_size, mime_type: a.mime_type, thumbnail_url: thumbnailUrl }
  }))

  res.json(rows)
})

// Range-aware file download proxy — logs access per file
app.get('/d/:token/download', async (req, res) => {
  const { key } = req.query
  if (!key) return res.status(400).send('Missing key')

  const link = await db.query(
    'SELECT expires_at, revoked_at FROM delivery_links WHERE token = $1',
    [req.params.token]
  )
  if (!link.rows.length) return res.status(404).send('Not found')
  const { expires_at, revoked_at } = link.rows[0]
  if (revoked_at || new Date(expires_at) < new Date()) return res.status(410).send('Expired')

  // Verify this key belongs to this token
  const asset = await db.query(
    'SELECT filename, mime_type, file_size FROM delivery_link_assets WHERE token = $1 AND r2_key = $2',
    [req.params.token, key]
  )
  if (!asset.rows.length) return res.status(403).send('Forbidden')

  const { filename, mime_type, file_size } = asset.rows[0]
  const rangeHeader = req.headers['range']

  const s3Params = { Bucket: process.env.R2_BUCKET, Key: key }
  if (rangeHeader) s3Params.Range = rangeHeader

  let s3Res
  try {
    s3Res = await s3.send(new GetObjectCommand(s3Params))
  } catch (err) {
    if (err.name === 'NoSuchKey') return res.status(404).send('File not found')
    throw err
  }

  res.setHeader('Accept-Ranges', 'bytes')
  res.setHeader('Content-Type', mime_type || 'application/octet-stream')
  res.setHeader('Content-Disposition', `attachment; filename="${sanitizeFilename(filename)}"`)

  if (s3Res.ContentLength) res.setHeader('Content-Length', s3Res.ContentLength)
  if (s3Res.ContentRange)  res.setHeader('Content-Range', s3Res.ContentRange)

  res.status(rangeHeader ? 206 : 200)
  s3Res.Body.pipe(res)

  // Log asynchronously — don't await so the download isn't delayed
  db.query(
    'INSERT INTO delivery_link_access (token, ip, user_agent, file_key) VALUES ($1, $2, $3, $4)',
    [req.params.token, req.ip, req.headers['user-agent'] || null, key]
  ).catch(() => {})
})

// ── Delivery sweep — purge expired links and their R2 objects ─────────────────

async function sweepExpiredDeliveries() {
  const expired = await db.query(
    `SELECT token FROM delivery_links
     WHERE revoked_at IS NULL AND expires_at < NOW()`
  )
  for (const { token } of expired.rows) {
    const assets = await db.query('SELECT r2_key, thumbnail_r2_key FROM delivery_link_assets WHERE token = $1', [token])
    await Promise.all(assets.rows.flatMap(a => [
      s3.send(new DeleteObjectCommand({ Bucket: process.env.R2_BUCKET, Key: a.r2_key })).catch(() => {}),
      a.thumbnail_r2_key
        ? s3.send(new DeleteObjectCommand({ Bucket: process.env.R2_BUCKET, Key: a.thumbnail_r2_key })).catch(() => {})
        : null,
    ].filter(Boolean)))
    await db.query('UPDATE delivery_links SET revoked_at = NOW() WHERE token = $1', [token])
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

initDeliveryTables()
  .then(() => app.listen(process.env.PORT || 3000, () => {
    console.log(`lpos-ingest running on :${process.env.PORT || 3000}`)
  }))
  .catch(err => { console.error('Failed to init delivery tables:', err); process.exit(1) })
