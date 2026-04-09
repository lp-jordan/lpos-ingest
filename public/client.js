const token = document.documentElement.dataset.token
const zone = document.getElementById('upload-zone')
const fileInput = document.getElementById('file-input')
const barFill = document.getElementById('upload-bar-fill')
const zoneLabel = document.getElementById('zone-label')
const zoneIcon = document.getElementById('zone-icon')
const fileGrid = document.getElementById('file-grid')
const toast = document.getElementById('toast')

let toastTimer = null

// ── Toast ──────────────────────────────────────────────────────────────────────

function showToast(msg, type = '') {
  toast.textContent = msg
  toast.className = `toast toast--visible${type ? ` toast--${type}` : ''}`
  clearTimeout(toastTimer)
  toastTimer = setTimeout(() => { toast.className = 'toast' }, 3200)
}

// ── Drop zone interactions ─────────────────────────────────────────────────────

zone.addEventListener('click', () => fileInput.click())

zone.addEventListener('keydown', e => {
  if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); fileInput.click() }
})

zone.addEventListener('dragover', e => {
  e.preventDefault()
  zone.classList.add('upload-zone--active')
})

zone.addEventListener('dragleave', e => {
  if (!zone.contains(e.relatedTarget)) zone.classList.remove('upload-zone--active')
})

zone.addEventListener('drop', e => {
  e.preventDefault()
  zone.classList.remove('upload-zone--active')
  const files = Array.from(e.dataTransfer.files)
  if (files.length) uploadFiles(files)
})

fileInput.addEventListener('change', () => {
  const files = Array.from(fileInput.files)
  if (files.length) uploadFiles(files)
  fileInput.value = ''
})

// ── Upload ─────────────────────────────────────────────────────────────────────

async function uploadFiles(files) {
  for (const file of files) {
    await uploadOne(file)
  }
  loadFiles()
}

function uploadOne(file) {
  return new Promise(resolve => {
    const xhr = new XMLHttpRequest()
    const fd = new FormData()
    fd.append('file', file)

    setZoneBusy(true)

    xhr.upload.addEventListener('progress', e => {
      if (e.lengthComputable) {
        barFill.style.width = `${Math.round((e.loaded / e.total) * 100)}%`
      }
    })

    xhr.addEventListener('load', () => {
      barFill.style.width = '100%'
      setTimeout(() => {
        barFill.style.width = '0%'
        setZoneBusy(false)
        if (xhr.status === 200) {
          showToast(`${file.name} uploaded`, 'success')
        } else {
          showToast(`Failed to upload ${file.name}`, 'error')
        }
        resolve()
      }, 400)
    })

    xhr.addEventListener('error', () => {
      setZoneBusy(false)
      barFill.style.width = '0%'
      showToast(`Upload failed`, 'error')
      resolve()
    })

    xhr.open('POST', `/c/${token}/upload`)
    xhr.send(fd)
  })
}

function setZoneBusy(busy) {
  zone.classList.toggle('upload-zone--active', busy)
  zoneLabel.innerHTML = busy
    ? 'Uploading…'
    : '<strong>Click to upload</strong> or drag and drop'
}

// ── File browser ───────────────────────────────────────────────────────────────

async function loadFiles() {
  const res = await fetch(`/c/${token}/files`)
  if (!res.ok) return
  const files = await res.json()

  if (!files.length) {
    fileGrid.innerHTML = '<p class="files-empty">No files uploaded yet.</p>'
    return
  }

  fileGrid.innerHTML = files.map(f => `
    <div class="file-card">
      <div class="file-card-thumb">
        ${thumbHtml(f)}
        <span class="file-card-ext">${ext(f.file_name)}</span>
      </div>
      <div class="file-card-body">
        <div class="file-card-name" title="${escHtml(f.file_name)}">${escHtml(f.file_name)}</div>
        <div class="file-card-meta">${formatSize(f.file_size)} &middot; ${formatDate(f.created_at)}</div>
      </div>
      <button class="file-card-download" title="Download" onclick="downloadFile('${escHtml(f.file_key)}')">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>
        </svg>
      </button>
    </div>
  `).join('')
}

function thumbHtml(f) {
  if (f.mime_type && f.mime_type.startsWith('image/')) {
    return `<img src="/c/${token}/download?key=${encodeURIComponent(f.file_key)}" alt="${escHtml(f.file_name)}" loading="lazy">`
  }
  return iconForMime(f.mime_type)
}

function iconForMime(mime) {
  if (!mime) return fileIcon()
  if (mime.startsWith('video/')) return `
    <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
      <rect x="2" y="2" width="20" height="20" rx="2.18" ry="2.18"/><line x1="7" y1="2" x2="7" y2="22"/><line x1="17" y1="2" x2="17" y2="22"/><line x1="2" y1="12" x2="22" y2="12"/><line x1="2" y1="7" x2="7" y2="7"/><line x1="2" y1="17" x2="7" y2="17"/><line x1="17" y1="17" x2="22" y2="17"/><line x1="17" y1="7" x2="22" y2="7"/>
    </svg>`
  if (mime.startsWith('audio/')) return `
    <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
      <path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/>
    </svg>`
  if (mime === 'application/pdf') return `
    <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/>
    </svg>`
  return fileIcon()
}

function fileIcon() {
  return `
    <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
      <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><polyline points="13 2 13 9 20 9"/>
    </svg>`
}

window.downloadFile = function(key) {
  window.location.href = `/c/${token}/download?key=${encodeURIComponent(key)}`
}

// ── Helpers ────────────────────────────────────────────────────────────────────

function ext(name) {
  const parts = name.split('.')
  return parts.length > 1 ? parts.pop() : '—'
}

function formatSize(bytes) {
  if (!bytes) return '—'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

function formatDate(iso) {
  if (!iso) return ''
  return new Date(iso).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })
}

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')
}

// ── Init ───────────────────────────────────────────────────────────────────────

loadFiles()
