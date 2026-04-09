import express from 'express'
import multer from 'multer'
import pg from 'pg'
import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { readFileSync } from 'fs'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'

const __dirname = dirname(fileURLToPath(import.meta.url))
const app = express()
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 500 * 1024 * 1024 } })
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } })

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

app.post('/c/:token/upload', upload.single('file'), async (req, res) => {
  const result = await db.query(
    'SELECT id FROM ingest_clients WHERE token = $1 AND active = true',
    [req.params.token]
  )
  if (!result.rows.length) return res.status(404).json({ error: 'Not found' })

  const clientId = result.rows[0].id

  if (!req.file) return res.status(400).json({ error: 'No file' })

  const fileKey = `${req.params.token}/${Date.now()}-${req.file.originalname}`

  await s3.send(new PutObjectCommand({
    Bucket: process.env.R2_BUCKET,
    Key: fileKey,
    Body: req.file.buffer,
    ContentType: req.file.mimetype,
  }))

  await db.query(
    `INSERT INTO ingest_submissions (client_id, file_key, file_name, file_size, mime_type)
     VALUES ($1, $2, $3, $4, $5)`,
    [clientId, fileKey, req.file.originalname, req.file.size, req.file.mimetype]
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
