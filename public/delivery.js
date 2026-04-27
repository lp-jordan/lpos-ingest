const { token, project: projectName, client: clientName, label } = document.documentElement.dataset
const list        = document.getElementById('delivery-list')
const selectAll   = document.getElementById('select-all')
const dlBtn       = document.getElementById('download-selected')
const headingEl   = document.getElementById('delivery-heading')
const subtextEl   = document.getElementById('delivery-subtext')
const toast       = document.getElementById('toast')

let toastTimer = null
let files = []
const selected = new Set()

// ── Header copy ────────────────────────────────────────────────────────────────

if (clientName) headingEl.textContent = `Hi, ${clientName}!`

const parts = [projectName, label].filter(Boolean)
subtextEl.textContent = parts.join(' · ')

// ── Toast ──────────────────────────────────────────────────────────────────────

function showToast(msg, type = '') {
  toast.textContent = msg
  toast.className = `toast toast--visible${type ? ` toast--${type}` : ''}`
  clearTimeout(toastTimer)
  toastTimer = setTimeout(() => { toast.className = 'toast' }, 3200)
}

// ── Selection state ────────────────────────────────────────────────────────────

function updateSelectionState() {
  const count = selected.size
  dlBtn.disabled = count === 0
  dlBtn.textContent = count > 1
    ? `Download ${count} files`
    : count === 1 ? 'Download file' : 'Download selected'

  const icon = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>
  </svg>`
  dlBtn.innerHTML = `${icon} ${dlBtn.textContent}`

  selectAll.indeterminate = count > 0 && count < files.length
  selectAll.checked = files.length > 0 && count === files.length

  document.querySelectorAll('.dlv-row-check').forEach(cb => {
    cb.checked = selected.has(cb.dataset.key)
  })
}

selectAll.addEventListener('change', () => {
  if (selectAll.checked) {
    files.forEach(f => selected.add(f.r2_key))
  } else {
    selected.clear()
  }
  updateSelectionState()
})

// ── Download ───────────────────────────────────────────────────────────────────

function triggerDownload(key, filename) {
  const a = document.createElement('a')
  a.href = `/d/${token}/download?key=${encodeURIComponent(key)}`
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
}

dlBtn.addEventListener('click', () => {
  const keys = [...selected]
  if (!keys.length) return
  keys.forEach(key => {
    const file = files.find(f => f.r2_key === key)
    if (file) triggerDownload(key, file.filename)
  })
  showToast(
    keys.length === 1 ? 'Download started' : `${keys.length} downloads started`,
    'success'
  )
})

// ── File list ──────────────────────────────────────────────────────────────────

function renderFiles() {
  if (!files.length) {
    list.innerHTML = '<p class="files-empty">No files in this delivery.</p>'
    return
  }

  list.innerHTML = files.map(f => `
    <div class="dlv-row" data-key="${escHtml(f.r2_key)}">
      <label class="dlv-row-check-wrap">
        <input type="checkbox" class="dlv-row-check" data-key="${escHtml(f.r2_key)}" />
      </label>
      <div class="dlv-row-icon">${iconForMime(f.mime_type)}</div>
      <div class="dlv-row-info">
        <div class="dlv-row-name" title="${escHtml(f.filename)}">${escHtml(f.filename)}</div>
        <div class="dlv-row-meta">${formatSize(f.file_size)} &middot; ${extLabel(f.filename)}</div>
      </div>
      <button class="dlv-row-dl" title="Download ${escHtml(f.filename)}" data-key="${escHtml(f.r2_key)}" data-name="${escHtml(f.filename)}">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>
        </svg>
      </button>
    </div>
  `).join('')

  list.querySelectorAll('.dlv-row-check').forEach(cb => {
    cb.addEventListener('change', () => {
      if (cb.checked) selected.add(cb.dataset.key)
      else selected.delete(cb.dataset.key)
      updateSelectionState()
    })
  })

  list.querySelectorAll('.dlv-row-dl').forEach(btn => {
    btn.addEventListener('click', () => {
      triggerDownload(btn.dataset.key, btn.dataset.name)
      showToast('Download started', 'success')
    })
  })
}

async function loadFiles() {
  const res = await fetch(`/d/${token}/files`)
  if (!res.ok) {
    list.innerHTML = '<p class="files-empty">Could not load files.</p>'
    return
  }
  files = await res.json()
  renderFiles()
  updateSelectionState()
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
