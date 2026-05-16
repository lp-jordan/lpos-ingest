/**
 * Delivery page client
 *
 * Replaces the old per-file <a download> trigger with a smart queue that:
 *   • Downloads files sequentially through StreamSaver (true streaming-to-disk).
 *   • Splits each file into 25 MB Range-requested chunks. A failed chunk is
 *     retried in place (3 attempts, exponential backoff) without re-downloading
 *     completed bytes.
 *   • Tracks per-file completion in localStorage keyed by delivery token, so
 *     a recipient who closes and reopens the page sees what they've already
 *     downloaded.
 *   • Surfaces failures clearly with a per-file Retry and an overall
 *     "Retry failed (N)" action.
 *   • Offers a "Having trouble?" link that posts to /d/<token>/report-trouble
 *     so the LPOS user who created the delivery (and admins as fallback) get
 *     pinged in-app and via Slack DM.
 */

const { token, project: projectName, client: clientName, label } = document.documentElement.dataset

const list                 = document.getElementById('delivery-list')
const dlBtn                = document.getElementById('download-all')
const retryBtn             = document.getElementById('retry-failed')
const overallProgressWrap  = document.getElementById('overall-progress')
const overallBar           = document.getElementById('overall-progress-bar')
const overallLabel         = document.getElementById('overall-progress-label')
const headingEl            = document.getElementById('delivery-heading')
const subtextEl            = document.getElementById('delivery-subtext')
const helpLink             = document.getElementById('trouble-link')
const helpModal            = document.getElementById('trouble-modal')
const helpForm             = document.getElementById('trouble-form')
const helpTextarea         = document.getElementById('trouble-message')
const helpStateLine        = document.getElementById('trouble-state-line')
const helpCancel           = document.getElementById('trouble-cancel')
const helpSubmit           = document.getElementById('trouble-submit')
const toast                = document.getElementById('toast')

const CHUNK_SIZE         = 25 * 1024 * 1024    // 25 MB per Range request
const CHUNK_RETRIES      = 3
const CHUNK_BACKOFF_MS   = [1000, 2000, 4000]   // attempt 1, 2, 3
const LOCAL_STORAGE_KEY  = `lpos-delivery-${token}`

let toastTimer = null
let files = []
/** key → 'pending' | 'downloading' | 'complete' | 'failed' */
const state = new Map()
/** key → last error message (only set when state === 'failed') */
const errors = new Map()
/** key → percent 0-100 (only meaningful while state === 'downloading') */
const progress = new Map()
/** Is the queue currently consuming items? */
let queueRunning = false

// ── Header copy ────────────────────────────────────────────────────────────────

if (clientName) headingEl.textContent = `Hi, ${clientName}!`
subtextEl.textContent = label || ''

// ── Persistence ────────────────────────────────────────────────────────────────
//
// localStorage shape: { completed: ["r2_key", ...], lastUpdated: ISO }
// Stored per delivery token so two different deliveries opened in the same
// browser don't bleed state. Cleared if the server returns 410 (link expired
// or revoked) so a stale "downloaded" check doesn't mislead future recipients.

function loadCompletedFromStorage() {
  try {
    const raw = localStorage.getItem(LOCAL_STORAGE_KEY)
    if (!raw) return new Set()
    const parsed = JSON.parse(raw)
    return new Set(Array.isArray(parsed.completed) ? parsed.completed : [])
  } catch {
    return new Set()
  }
}

function persistCompleted() {
  try {
    const completed = []
    for (const [key, s] of state) if (s === 'complete') completed.push(key)
    localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify({
      completed,
      lastUpdated: new Date().toISOString(),
    }))
  } catch {
    // Storage full or disabled — silently degrade, recipient just won't see
    // remembered state on reopen.
  }
}

function clearStorage() {
  try { localStorage.removeItem(LOCAL_STORAGE_KEY) } catch { /* ignore */ }
}

// ── Toast ──────────────────────────────────────────────────────────────────────

function showToast(msg, type = '') {
  toast.textContent = msg
  toast.className = `toast toast--visible${type ? ` toast--${type}` : ''}`
  clearTimeout(toastTimer)
  toastTimer = setTimeout(() => { toast.className = 'toast' }, 3200)
}

// ── State queries ──────────────────────────────────────────────────────────────

function countByState() {
  const counts = { pending: 0, downloading: 0, complete: 0, failed: 0 }
  for (const s of state.values()) counts[s]++
  return counts
}

function queueSummary() {
  const c = countByState()
  const total = files.length
  const bits = [`${c.complete} of ${total} complete`]
  if (c.failed)      bits.push(`${c.failed} failed`)
  if (c.downloading) bits.push('1 in progress')
  if (c.pending)     bits.push(`${c.pending} pending`)
  return bits.join(' · ')
}

function renderOverallProgress() {
  const c = countByState()
  const total = files.length
  if (!total) {
    overallProgressWrap.hidden = true
    return
  }
  const anyActive = c.downloading > 0 || c.complete > 0 || c.failed > 0
  if (!anyActive) {
    overallProgressWrap.hidden = true
    return
  }
  overallProgressWrap.hidden = false
  // Each completed file contributes 100; in-progress contributes its percent.
  let pctSum = c.complete * 100
  for (const [key, s] of state) {
    if (s === 'downloading') pctSum += progress.get(key) || 0
  }
  const overall = Math.round(pctSum / total)
  overallBar.style.width = `${overall}%`
  overallLabel.textContent = queueSummary()

  // Retry button visibility — show only when there are completed-with-failures
  // and no active downloads (otherwise the user might retry mid-flight).
  retryBtn.hidden = !(c.failed > 0 && c.downloading === 0)
  retryBtn.textContent = c.failed > 1 ? `Retry ${c.failed} failed` : 'Retry failed'
}

// ── Download button ────────────────────────────────────────────────────────────

function updateDownloadAllBtn() {
  const c = countByState()
  const remaining = c.pending
  dlBtn.disabled = remaining === 0 || queueRunning
  dlBtn.textContent = queueRunning
    ? 'Downloading…'
    : remaining === 0
      ? 'All files downloaded'
      : remaining < files.length
        ? `Resume (${remaining} left)`
        : `Download all ${files.length} files`
}

dlBtn.addEventListener('click', () => { runQueue() })
retryBtn.addEventListener('click', () => {
  // Reset failed files to pending so the queue picks them up
  for (const [key, s] of state) {
    if (s === 'failed') {
      state.set(key, 'pending')
      errors.delete(key)
    }
  }
  renderRows()
  runQueue()
})

// ── Trouble report modal ───────────────────────────────────────────────────────

helpLink.addEventListener('click', (e) => {
  e.preventDefault()
  helpStateLine.textContent = queueSummary() || 'No downloads started yet'
  helpTextarea.value = ''
  helpModal.hidden = false
  helpTextarea.focus()
})

helpCancel.addEventListener('click', () => { helpModal.hidden = true })

helpModal.addEventListener('click', (e) => {
  if (e.target === helpModal) helpModal.hidden = true
})

helpForm.addEventListener('submit', async (e) => {
  e.preventDefault()
  helpSubmit.disabled = true
  helpSubmit.textContent = 'Sending…'
  try {
    const res = await fetch(`/d/${token}/report-trouble`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({
        description:  helpTextarea.value.trim() || null,
        queueSummary: queueSummary() || null,
      }),
    })
    if (res.status === 429) {
      throw new Error('Already sent recently — please wait a few minutes before trying again.')
    }
    if (!res.ok) throw new Error('Network error — please try again.')
    helpModal.hidden = true
    showToast('Thanks — someone will reach out shortly.', 'success')
  } catch (err) {
    showToast(err.message || 'Could not send the report.', 'error')
  } finally {
    helpSubmit.disabled = false
    helpSubmit.textContent = 'Send'
  }
})

// ── File list rendering ────────────────────────────────────────────────────────

function renderRows() {
  if (!files.length) {
    list.innerHTML = '<p class="files-empty">No files in this delivery.</p>'
    return
  }

  list.innerHTML = files.map((f) => {
    const s    = state.get(f.r2_key) || 'pending'
    const pct  = Math.round(progress.get(f.r2_key) || 0)
    const err  = errors.get(f.r2_key)
    const stateClass = `dlv-row dlv-row--${s}`
    return `
      <div class="${stateClass}" data-key="${escHtml(f.r2_key)}">
        <div class="dlv-row-icon">
          ${f.thumbnail_url
            ? `<img src="${escHtml(f.thumbnail_url)}" alt="" class="dlv-row-thumb" />`
            : iconForMime(f.mime_type)}
        </div>
        <div class="dlv-row-info">
          <div class="dlv-row-name" title="${escHtml(f.filename)}">${escHtml(f.filename)}</div>
          <div class="dlv-row-meta">
            <span>${formatSize(f.file_size)} · ${extLabel(f.filename)}</span>
            ${statusBadge(s, pct, err)}
          </div>
          ${s === 'downloading' ? `<div class="dlv-row-bar"><div class="dlv-row-bar-fill" style="width:${pct}%"></div></div>` : ''}
        </div>
        ${renderRowAction(f, s)}
      </div>
    `
  }).join('')

  list.querySelectorAll('.dlv-row-action[data-action="retry"]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const key = btn.dataset.key
      state.set(key, 'pending')
      errors.delete(key)
      renderRows()
      runQueue()
    })
  })

  list.querySelectorAll('.dlv-row-action[data-action="download"]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const key = btn.dataset.key
      // Bypass the queue — download this one file ad hoc. Use the same chunked
      // path so the retry/streaming behavior is consistent.
      const file = files.find((f) => f.r2_key === key)
      if (file) downloadOne(file)
    })
  })

  updateDownloadAllBtn()
  renderOverallProgress()
}

function statusBadge(s, pct, err) {
  if (s === 'pending')     return ''
  if (s === 'downloading') return `<span class="dlv-row-badge dlv-row-badge--active">${pct}%</span>`
  if (s === 'complete')    return `<span class="dlv-row-badge dlv-row-badge--done">Downloaded</span>`
  if (s === 'failed')      return `<span class="dlv-row-badge dlv-row-badge--failed" title="${escHtml(err || '')}">Failed</span>`
  return ''
}

function renderRowAction(f, s) {
  if (s === 'complete') {
    return '<span class="dlv-row-check-icon" aria-hidden="true">✓</span>'
  }
  if (s === 'failed') {
    return `<button class="dlv-row-action dlv-row-action--retry" data-action="retry" data-key="${escHtml(f.r2_key)}">Retry</button>`
  }
  if (s === 'pending') {
    return `<button class="dlv-row-action" data-action="download" data-key="${escHtml(f.r2_key)}" title="Download this file now">Download</button>`
  }
  return ''
}

// ── Download mechanics ─────────────────────────────────────────────────────────

async function runQueue() {
  if (queueRunning) return
  queueRunning = true
  updateDownloadAllBtn()
  renderOverallProgress()

  try {
    for (const f of files) {
      if (state.get(f.r2_key) !== 'pending') continue
      await downloadOne(f)
    }
  } finally {
    queueRunning = false
    updateDownloadAllBtn()
    renderOverallProgress()

    const c = countByState()
    if (c.failed > 0) {
      showToast(`${c.failed} file${c.failed === 1 ? '' : 's'} failed — Retry available`, 'error')
    } else if (c.complete === files.length && files.length > 0) {
      showToast('All files downloaded', 'success')
    }
  }
}

async function downloadOne(file) {
  if (state.get(file.r2_key) === 'complete') return
  state.set(file.r2_key, 'downloading')
  progress.set(file.r2_key, 0)
  errors.delete(file.r2_key)
  renderRows()

  let writer
  try {
    const stream = await window.streamSaver.createWriteStream(file.filename, {
      size:     file.file_size,
      mimeType: file.mime_type,
    })
    writer = stream.getWriter()
  } catch (err) {
    state.set(file.r2_key, 'failed')
    errors.set(file.r2_key, `Couldn't start download: ${err.message || err}`)
    renderRows()
    return
  }

  const total = file.file_size
  let written = 0
  try {
    for (let start = 0; start < total; start += CHUNK_SIZE) {
      const end = Math.min(start + CHUNK_SIZE - 1, total - 1)
      const buf = await fetchChunkWithRetry(file.r2_key, start, end)
      await writer.write(new Uint8Array(buf))
      written += buf.byteLength
      progress.set(file.r2_key, Math.round((written / total) * 100))
      renderRows()
    }
    await writer.close()
    state.set(file.r2_key, 'complete')
    progress.delete(file.r2_key)
    persistCompleted()
  } catch (err) {
    try { await writer.abort() } catch { /* ignore */ }
    state.set(file.r2_key, 'failed')
    errors.set(file.r2_key, err.message || String(err))
  }
  renderRows()
}

async function fetchChunkWithRetry(key, start, end) {
  const url = `/d/${token}/download?key=${encodeURIComponent(key)}`
  let lastErr
  for (let attempt = 0; attempt < CHUNK_RETRIES; attempt++) {
    try {
      const res = await fetch(url, {
        headers: { Range: `bytes=${start}-${end}` },
      })
      if (res.status === 410) {
        // Link expired or revoked — no point retrying; also wipe persisted state.
        clearStorage()
        throw new Error('This delivery link has expired.')
      }
      if (!(res.status === 206 || res.status === 200)) {
        throw new Error(`HTTP ${res.status}`)
      }
      return await res.arrayBuffer()
    } catch (err) {
      lastErr = err
      if (attempt < CHUNK_RETRIES - 1) {
        await sleep(CHUNK_BACKOFF_MS[attempt])
      }
    }
  }
  throw lastErr || new Error('chunk fetch failed')
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

// ── Load files ─────────────────────────────────────────────────────────────────

async function loadFiles() {
  const res = await fetch(`/d/${token}/files`)
  if (res.status === 410) {
    clearStorage()
    list.innerHTML = '<p class="files-empty">This delivery link has expired.</p>'
    return
  }
  if (!res.ok) {
    list.innerHTML = '<p class="files-empty">Could not load files.</p>'
    return
  }
  files = await res.json()

  // Seed per-file state — completed entries from localStorage start as 'complete',
  // everything else starts as 'pending'.
  const completed = loadCompletedFromStorage()
  for (const f of files) {
    state.set(f.r2_key, completed.has(f.r2_key) ? 'complete' : 'pending')
  }

  // StreamSaver-unsupported browser fallback — degrade with a clear message.
  // The recipient can still hit "Having trouble?" to ping us.
  if (!window.streamSaver || !window.streamSaver.isSupported()) {
    list.innerHTML = '<p class="files-empty">Your browser doesn\'t support streamed downloads. Try Chrome or Firefox on desktop, or use "Having trouble?" below to reach us.</p>'
    dlBtn.disabled = true
    return
  }

  renderRows()
}

// ── Helpers ────────────────────────────────────────────────────────────────────

function extLabel(name) {
  const parts = name.split('.')
  return parts.length > 1 ? parts.pop().toUpperCase() : '—'
}

function formatSize(bytes) {
  if (!bytes) return '—'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')
}

function iconForMime(mime) {
  if (!mime) return fileIcon()
  if (mime.startsWith('video/')) return `
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
      <rect x="2" y="2" width="20" height="20" rx="2.18" ry="2.18"/><line x1="7" y1="2" x2="7" y2="22"/><line x1="17" y1="2" x2="17" y2="22"/><line x1="2" y1="12" x2="22" y2="12"/><line x1="2" y1="7" x2="7" y2="7"/><line x1="2" y1="17" x2="7" y2="17"/><line x1="17" y1="17" x2="22" y2="17"/><line x1="17" y1="7" x2="22" y2="7"/>
    </svg>`
  if (mime.startsWith('audio/')) return `
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
      <path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/>
    </svg>`
  if (mime.startsWith('image/')) return `
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
      <rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/>
    </svg>`
  if (mime === 'application/pdf') return `
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/>
    </svg>`
  return fileIcon()
}

function fileIcon() {
  return `
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
      <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><polyline points="13 2 13 9 20 9"/>
    </svg>`
}

// ── Init ───────────────────────────────────────────────────────────────────────

loadFiles()
