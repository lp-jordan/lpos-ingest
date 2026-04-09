import express from 'express'
import multer from 'multer'
import pg from 'pg'
import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { readFileSync } from 'fs'
import { fileURLToPath } from 'url'
import { dirname, join, extname } from 'path'

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

// ── Health / root ─────────────────────────────────────────────────────────────

app.get('/', (_req, res) => res.status(200).send('OK'))

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

app.listen(process.env.PORT || 3000, () => {
  console.log(`lpos-ingest running on :${process.env.PORT || 3000}`)
})
