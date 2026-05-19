/**
 * Delivery page client
 *
 * Downloads files sequentially through StreamSaver (true streaming-to-disk).
 * Splits each file into 25 MB Range-requested chunks with retry + backoff.
 * Tracks per-file completion in localStorage so re-opening shows prior state.
 *
 * Download UX:
 *   Batch: "Download All" (nothing selected) or "Download Selected (N)" (1+ selected).
 *         Clicking either shows a quality picker if any video is in the selection —
 *         "Full Quality" (originals) or "Web Quality" (proxies). Audio-only skips the picker.
 *   Individual: download icon per file. Videos get a popout with Full/Web options
 *               (Web grayed while proxy is still processing). Audio downloads directly.
 *   Transcripts: small transcript button on video rows that have transcripts — opens a
 *                popout listing downloadable formats (SRT, VTT, TXT).
 *   Progress bar: hidden until a download actually begins.
 */

const { token, project: projectName, client: clientName, label } = document.documentElement.dataset

const list                = document.getElementById('delivery-list')
const dlBtn               = document.getElementById('download-all')
const retryBtn            = document.getElementById('retry-failed')
const overallProgressWrap = document.getElementById('overall-progress')
const overallBar          = document.getElementById('overall-progress-bar')
const overallLabel        = document.getElementById('overall-progress-label')
const headingEl           = document.getElementById('delivery-heading')
const subtextEl           = document.getElementById('delivery-subtext')
const helpLink            = document.getElementById('trouble-link')
const helpModal           = document.getElementById('trouble-modal')
const helpForm            = document.getElementById('trouble-form')
const helpTextarea        = document.getElementById('trouble-message')
const helpStateLine       = document.getElementById('trouble-state-line')
const helpCancel          = document.getElementById('trouble-cancel')
const helpSubmit          = document.getElementById('trouble-submit')
const toast               = document.getElementById('toast')

const CHUNK_SIZE        = 25 * 1024 * 1024
const CHUNK_RETRIES     = 3
const CHUNK_BACKOFF_MS  = [1000, 2000, 4000]
const LOCAL_STORAGE_KEY = `lpos-delivery-${token}`

let toastTimer = null
let files = []

/** key → 'pending' | 'downloading' | 'complete' | 'failed' */
const state  = new Map()
/** key → last error message */
const errors = new Map()
/** key → percent 0-100 */
const progress = new Map()
/** Set of selected file r2_keys */
const selected = new Set()

let queueRunning = false
let queuePaused  = false

/** Active download descriptors — may be originals or proxies */
let downloadQueue = []

/** Cached DOM row elements (key → row div) */
const rowCache = new Map()
let pendingRender = false

/** Batch quality picker state */
let qualityPickerVisible = false

/** Individual file popout state */
let activePopoutKey     = null
let activeTranscriptKey = null

// ── Header copy ────────────────────────────────────────────────────────────────

if (clientName) headingEl.textContent = `Hi, ${clientName}!`
subtextEl.textContent = label || ''

// ── Persistence ────────────────────────────────────────────────────────────────

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
    localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify({ completed, lastUpdated: new Date().toISOString() }))
  } catch { /* ignore */ }
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

// ── File helpers ───────────────────────────────────────────────────────────────

function isVideoFile(f) {
  if (!f.mime_type) return false
  return f.mime_type.startsWith('video/')
}

function isAudioFile(f) {
  if (!f.mime_type) return false
  return f.mime_type.startsWith('audio/')
}

function hasProxy(f) {
  return !!f.proxy_r2_key
}

function hasTranscripts(f) {
  return Array.isArray(f.transcripts) && f.transcripts.length > 0
}

function proxyFilename(filename) {
  const dot = filename.lastIndexOf('.')
  if (dot < 0) return `${filename} (Web Quality)`
  return `${filename.slice(0, dot)} (Web Quality)${filename.slice(dot)}`
}

// ── State queries ──────────────────────────────────────────────────────────────

function getEffectiveState(f) {
  const orig  = state.get(f.r2_key)        || 'pending'
  const proxy = f.proxy_r2_key ? (state.get(f.proxy_r2_key) || 'pending') : 'pending'
  if (orig  === 'downloading') return { s: 'downloading', pct: progress.get(f.r2_key) || 0 }
  if (proxy === 'downloading') return { s: 'downloading', pct: progress.get(f.proxy_r2_key) || 0 }
  if (orig  === 'failed')      return { s: 'failed',      err: errors.get(f.r2_key) }
  if (proxy === 'failed')      return { s: 'failed',      err: errors.get(f.proxy_r2_key) }
  if (orig  === 'complete' || proxy === 'complete') return { s: 'complete' }
  return { s: 'pending' }
}

function countByEffectiveState() {
  const counts = { pending: 0, downloading: 0, complete: 0, failed: 0 }
  for (const f of files) counts[getEffectiveState(f).s]++
  return counts
}

function queueSummary() {
  const c = countByEffectiveState()
  const total = files.length
  const bits = [`${c.complete} of ${total} complete`]
  if (c.failed)      bits.push(`${c.failed} failed`)
  if (c.downloading) bits.push('1 in progress')
  if (c.pending)     bits.push(`${c.pending} pending`)
  return bits.join(' · ')
}

// ── Overall progress bar ───────────────────────────────────────────────────────

function renderOverallProgress() {
  const c = countByEffectiveState()
  const total = files.length
  const anyStarted = c.downloading > 0 || c.complete > 0 || c.failed > 0
  if (!total || !anyStarted) {
    overallProgressWrap.hidden = true
    return
  }
  overallProgressWrap.hidden = false

  let pctSum = c.complete * 100
  for (const f of files) {
    const { s, pct } = getEffectiveState(f)
    if (s === 'downloading') pctSum += pct || 0
  }
  const overall = Math.round(pctSum / total)
  overallBar.style.width = `${overall}%`
  overallLabel.textContent = queueSummary()

  retryBtn.hidden = !(c.failed > 0 && c.downloading === 0)
  retryBtn.textContent = c.failed > 1 ? `Retry ${c.failed} failed` : 'Retry failed'
}

// ── Batch download button ──────────────────────────────────────────────────────

function getSelectedFiles() {
  return files.filter((f) => selected.has(f.r2_key))
}

function getTargetFiles() {
  return selected.size > 0 ? getSelectedFiles() : files
}

function selectionHasVideo() {
  return getTargetFiles().some(isVideoFile)
}

function updateBatchBtn() {
  const actionsEl = document.querySelector('.delivery-actions')
  if (!actionsEl) return

  if (qualityPickerVisible) {
    renderQualityPicker(actionsEl)
    return
  }

  // Restore/update the normal button state
  ensureNormalBatchBtn(actionsEl)

  const c = countByEffectiveState()
  const nSelected = selected.size
  const targetCount = nSelected > 0 ? nSelected : files.length
  const targetFiles = getTargetFiles()
  const anyPending = targetFiles.some((f) => getEffectiveState(f).s === 'pending')

  dlBtn.disabled = !anyPending || queueRunning

  if (queueRunning && !queuePaused) {
    dlBtn.textContent = 'Downloading…'
  } else if (queueRunning && queuePaused) {
    dlBtn.textContent = 'Paused…'
  } else if (!anyPending && c.complete === files.length && files.length > 0) {
    dlBtn.textContent = 'All files downloaded'
  } else if (nSelected > 0) {
    dlBtn.textContent = `Download Selected (${nSelected})`
  } else {
    dlBtn.textContent = `Download all ${files.length} files`
  }
}

function ensureNormalBatchBtn(actionsEl) {
  // Remove quality picker if present, restore the original download button
  const picker = actionsEl.querySelector('.delivery-quality-picker')
  if (picker) picker.remove()
  dlBtn.style.display = ''
}

function renderQualityPicker(actionsEl) {
  dlBtn.style.display = 'none'
  if (actionsEl.querySelector('.delivery-quality-picker')) return // already there

  const picker = document.createElement('div')
  picker.className = 'delivery-quality-picker'
  picker.innerHTML = `
    <button class="delivery-quality-btn delivery-quality-btn--full" id="dl-full-quality">
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>
      </svg>
      Full Quality
    </button>
    <button class="delivery-quality-btn delivery-quality-btn--web" id="dl-web-quality">
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>
      </svg>
      Web Quality
    </button>
    <button class="delivery-quality-close" id="dl-quality-close" aria-label="Close">✕</button>
  `
  // Insert before the retry button (or at end of actions)
  const retryEl = actionsEl.querySelector('#retry-failed')
  actionsEl.insertBefore(picker, retryEl || null)

  document.getElementById('dl-full-quality').addEventListener('click', () => startBatchDownload('full'))
  document.getElementById('dl-web-quality').addEventListener('click',  () => startBatchDownload('web'))
  document.getElementById('dl-quality-close').addEventListener('click', closeQualityPicker)
}

function openQualityPicker() {
  qualityPickerVisible = true
  updateBatchBtn()
}

function closeQualityPicker() {
  qualityPickerVisible = false
  updateBatchBtn()
}

dlBtn.addEventListener('click', () => {
  if (queueRunning && !queuePaused) {
    queuePaused = true
    updateBatchBtn()
    return
  }
  if (queueRunning && queuePaused) {
    queuePaused = false
    updateBatchBtn()
    runQueue()
    return
  }

  const targetFiles = getTargetFiles()
  const hasVideo = targetFiles.some(isVideoFile)
  if (hasVideo) {
    openQualityPicker()
  } else {
    // Audio-only or non-video — download directly at full quality
    startBatchDownload('full')
  }
})

retryBtn.addEventListener('click', () => {
  for (const [key, s] of state) {
    if (s === 'failed') { state.set(key, 'pending'); errors.delete(key) }
  }
  // Re-queue failed files in their respective download queue entries
  const failedEntries = downloadQueue.filter((d) => state.get(d.r2_key) === 'pending')
  if (!failedEntries.length) {
    downloadQueue = files.map((f) => ({ r2_key: f.r2_key, filename: f.filename, file_size: f.file_size, mime_type: f.mime_type }))
    for (const d of downloadQueue) if (!state.has(d.r2_key) || state.get(d.r2_key) === 'failed') state.set(d.r2_key, 'pending')
  }
  scheduleRender()
  runQueue()
})

function startBatchDownload(quality) {
  closeQualityPicker()

  const targetFiles = getTargetFiles()
  downloadQueue = targetFiles.map((f) => {
    if (quality === 'web' && hasProxy(f) && isVideoFile(f)) {
      return { r2_key: f.proxy_r2_key, filename: proxyFilename(f.filename), file_size: f.proxy_file_size, mime_type: 'video/mp4' }
    }
    return { r2_key: f.r2_key, filename: f.filename, file_size: f.file_size, mime_type: f.mime_type }
  })

  for (const d of downloadQueue) {
    if (!state.has(d.r2_key)) state.set(d.r2_key, 'pending')
  }

  selected.clear()
  updateCheckboxesFromSelection()
  queuePaused = false
  scheduleRender()
  runQueue()
}

// ── Trouble report modal ───────────────────────────────────────────────────────

helpLink.addEventListener('click', (e) => {
  e.preventDefault()
  helpStateLine.textContent = queueSummary() || 'No downloads started yet'
  helpTextarea.value = ''
  helpModal.hidden = false
  helpTextarea.focus()
})

helpCancel.addEventListener('click', () => { helpModal.hidden = true })
helpModal.addEventListener('click', (e) => { if (e.target === helpModal) helpModal.hidden = true })

helpForm.addEventListener('submit', async (e) => {
  e.preventDefault()
  helpSubmit.disabled = true
  helpSubmit.textContent = 'Sending…'
  try {
    const res = await fetch(`/d/${token}/report-trouble`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ description: helpTextarea.value.trim() || null, queueSummary: queueSummary() || null }),
    })
    if (res.status === 429) throw new Error('Already sent recently — please wait a few minutes.')
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

// ── Popout management ──────────────────────────────────────────────────────────

function openPopout(key) {
  activePopoutKey     = key
  activeTranscriptKey = null
  scheduleRender()
}

function closePopout() {
  activePopoutKey     = null
  activeTranscriptKey = null
  scheduleRender()
}

function openTranscript(key) {
  activeTranscriptKey = key
  activePopoutKey     = null
  scheduleRender()
}

// Close popouts when clicking outside a row's action area
document.addEventListener('click', (e) => {
  if (!e.target.closest('.dlv-row-actions')) {
    if (activePopoutKey !== null || activeTranscriptKey !== null) closePopout()
  }
  // Close quality picker when clicking outside the actions row
  if (qualityPickerVisible && !e.target.closest('.delivery-actions')) {
    closeQualityPicker()
  }
})

// ── File list rendering ────────────────────────────────────────────────────────

function scheduleRender() {
  if (pendingRender) return
  pendingRender = true
  requestAnimationFrame(() => { pendingRender = false; doRender() })
}

function initRenderRows() {
  if (!files.length) {
    list.innerHTML = '<p class="files-empty">No files in this delivery.</p>'
    return
  }

  list.innerHTML = ''
  for (const f of files) {
    const row = document.createElement('div')
    row.className = 'dlv-row'
    row.dataset.key = f.r2_key
    row.innerHTML = `
      <label class="dlv-row-check-wrap">
        <input type="checkbox" class="dlv-row-check" data-key="${escHtml(f.r2_key)}" />
      </label>
      <div class="dlv-row-icon">
        ${f.thumbnail_url
          ? `<img src="${escHtml(f.thumbnail_url)}" alt="" class="dlv-row-thumb" />`
          : iconForMime(f.mime_type)}
      </div>
      <div class="dlv-row-info">
        <div class="dlv-row-name" title="${escHtml(f.filename)}">${escHtml(f.filename)}</div>
        <div class="dlv-row-meta">
          <span>${formatSize(f.file_size)} · ${extLabel(f.filename)}</span>
          <span class="dlv-row-status-placeholder"></span>
        </div>
        <div class="dlv-row-bar" style="display: none;">
          <div class="dlv-row-bar-fill" style="width: 0%"></div>
        </div>
      </div>
      <div class="dlv-row-actions"></div>
    `
    list.appendChild(row)
    rowCache.set(f.r2_key, row)

    const checkbox = row.querySelector('.dlv-row-check')
    checkbox?.addEventListener('change', () => {
      if (checkbox.checked) selected.add(f.r2_key)
      else selected.delete(f.r2_key)
      if (qualityPickerVisible) closeQualityPicker()
      doRender()
    })
    row.addEventListener('click', (e) => {
      if (e.target.closest('.dlv-row-actions, .dlv-row-check-wrap')) return
      e.preventDefault()
      handleCheckboxClick(f.r2_key, { shiftKey: e.shiftKey })
    })
  }

  doRender()

  const selectAllBtn = document.querySelector('#select-all-files')
  if (selectAllBtn) {
    selectAllBtn.addEventListener('change', (e) => {
      if (e.target.checked) { selected.clear(); for (const f of files) selected.add(f.r2_key) }
      else selected.clear()
      if (qualityPickerVisible) closeQualityPicker()
      updateCheckboxesFromSelection()
      doRender()
    })
  }
}

let lastSelectedKey = null

function handleCheckboxClick(key, e) {
  if (e.shiftKey && lastSelectedKey) {
    const keys = files.map((f) => f.r2_key)
    const idx  = keys.indexOf(key)
    const last = keys.indexOf(lastSelectedKey)
    if (idx >= 0 && last >= 0) {
      const [start, end] = [Math.min(idx, last), Math.max(idx, last)]
      for (let i = start; i <= end; i++) selected.add(keys[i])
    }
  } else {
    if (selected.has(key)) selected.delete(key)
    else selected.add(key)
    lastSelectedKey = key
  }
  if (qualityPickerVisible) closeQualityPicker()
  updateCheckboxesFromSelection()
  doRender()
}

function updateCheckboxesFromSelection() {
  for (const [key, row] of rowCache) {
    const checkbox = row.querySelector('.dlv-row-check')
    if (checkbox) checkbox.checked = selected.has(key)
  }
}

function doRender() {
  if (!files.length) return

  for (const f of files) {
    const row = rowCache.get(f.r2_key)
    if (!row) continue

    const { s, pct, err } = getEffectiveState(f)
    row.className = `dlv-row dlv-row--${s}${selected.has(f.r2_key) ? ' dlv-row--selected' : ''}`

    const statusEl = row.querySelector('.dlv-row-status-placeholder')
    if (statusEl) statusEl.innerHTML = getStatusHtml(s, pct || 0, err)

    const bar     = row.querySelector('.dlv-row-bar')
    const barFill = row.querySelector('.dlv-row-bar-fill')
    if (s === 'downloading') {
      bar.style.display = ''
      if (barFill) barFill.style.width = `${pct || 0}%`
    } else {
      bar.style.display = 'none'
    }

    const actionDiv = row.querySelector('.dlv-row-actions')
    if (actionDiv) {
      actionDiv.innerHTML = getActionHtml(f, s)
      attachActionHandlers(actionDiv, f, s)
    }
  }

  updateBatchBtn()
  renderOverallProgress()
}

function attachActionHandlers(actionDiv, f, s) {
  // Individual download icon
  const dlIcon = actionDiv.querySelector('[data-action="download"]')
  if (dlIcon) {
    dlIcon.addEventListener('click', (e) => {
      e.stopPropagation()
      if (isAudioFile(f) || (!isVideoFile(f))) {
        downloadOne({ r2_key: f.r2_key, filename: f.filename, file_size: f.file_size, mime_type: f.mime_type })
        return
      }
      if (activePopoutKey === f.r2_key) { closePopout(); return }
      openPopout(f.r2_key)
    })
  }

  // Quality popout buttons
  const btnFull = actionDiv.querySelector('[data-action="dl-full"]')
  if (btnFull) {
    btnFull.addEventListener('click', (e) => {
      e.stopPropagation()
      closePopout()
      downloadOne({ r2_key: f.r2_key, filename: f.filename, file_size: f.file_size, mime_type: f.mime_type })
    })
  }

  const btnWeb = actionDiv.querySelector('[data-action="dl-web"]')
  if (btnWeb && hasProxy(f)) {
    btnWeb.addEventListener('click', (e) => {
      e.stopPropagation()
      closePopout()
      downloadOne({ r2_key: f.proxy_r2_key, filename: proxyFilename(f.filename), file_size: f.proxy_file_size, mime_type: 'video/mp4' })
    })
  }

  // Transcript button
  const txBtn = actionDiv.querySelector('[data-action="transcript"]')
  if (txBtn) {
    txBtn.addEventListener('click', (e) => {
      e.stopPropagation()
      if (activeTranscriptKey === f.r2_key) { closePopout(); return }
      openTranscript(f.r2_key)
    })
  }

  // Transcript download links inside popout
  actionDiv.querySelectorAll('[data-action="dl-transcript"]').forEach((link) => {
    link.addEventListener('click', (e) => {
      e.stopPropagation()
      closePopout()
    })
  })

  // Retry button
  const retryBtnEl = actionDiv.querySelector('[data-action="retry"]')
  if (retryBtnEl) {
    retryBtnEl.addEventListener('click', (e) => {
      e.stopPropagation()
      state.set(f.r2_key, 'pending')
      if (f.proxy_r2_key && state.get(f.proxy_r2_key) === 'failed') state.set(f.proxy_r2_key, 'pending')
      errors.delete(f.r2_key)
      errors.delete(f.proxy_r2_key)
      selected.delete(f.r2_key)
      if (!downloadQueue.find((d) => d.r2_key === f.r2_key)) {
        downloadQueue.push({ r2_key: f.r2_key, filename: f.filename, file_size: f.file_size, mime_type: f.mime_type })
      }
      scheduleRender()
      runQueue()
    })
  }
}

function getStatusHtml(s, pct, err) {
  if (s === 'pending')     return ''
  if (s === 'downloading') return `<span class="dlv-row-badge dlv-row-badge--active">${pct}%</span>`
  if (s === 'complete')    return `<span class="dlv-row-badge dlv-row-badge--done">Downloaded</span>`
  if (s === 'failed')      return `<span class="dlv-row-badge dlv-row-badge--failed" title="${escHtml(err || '')}">Failed</span>`
  return ''
}

function getActionHtml(f, s) {
  if (s === 'complete') return '<span class="dlv-row-check-icon" aria-hidden="true">✓</span>'
  if (s === 'failed')   return `<button class="dlv-row-action dlv-row-action--retry" data-action="retry" data-key="${escHtml(f.r2_key)}">Retry</button>`

  const showPopout      = isVideoFile(f) && activePopoutKey === f.r2_key
  const showTranscript  = activeTranscriptKey === f.r2_key && hasTranscripts(f)
  const isVideo         = isVideoFile(f)

  const dlIconHtml = `
    <button class="dlv-row-dl${showPopout ? ' dlv-row-dl--active' : ''}" data-action="download" data-key="${escHtml(f.r2_key)}"
      title="${isVideo ? 'Download options' : 'Download this file'}"
      aria-label="${isVideo ? 'Download options' : 'Download this file'}">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>
      </svg>
    </button>`

  let transcriptBtnHtml = ''
  if (isVideo && hasTranscripts(f)) {
    transcriptBtnHtml = `
      <button class="dlv-row-tx${showTranscript ? ' dlv-row-tx--active' : ''}" data-action="transcript" data-key="${escHtml(f.r2_key)}"
        title="Download transcript" aria-label="Download transcript">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/>
          <line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/>
        </svg>
      </button>`
  }

  let popoutHtml = ''
  if (showPopout && isVideo) {
    const webSize = f.proxy_file_size ? ` · ${formatSize(f.proxy_file_size)}` : ''
    const origSize = f.file_size ? ` · ${formatSize(f.file_size)}` : ''
    const webReady = hasProxy(f)
    popoutHtml = `
      <div class="dlv-popout">
        <button class="dlv-popout-btn" data-action="dl-full">Full Quality${origSize}</button>
        <button class="dlv-popout-btn${webReady ? '' : ' dlv-popout-btn--disabled'}" data-action="dl-web" ${webReady ? '' : 'disabled'}>
          Web Quality${webReady ? webSize : ' · Processing…'}
        </button>
      </div>`
  }

  let transcriptPopoutHtml = ''
  if (showTranscript && hasTranscripts(f)) {
    const kindLabel = { srt: 'SRT Subtitles', vtt: 'VTT Subtitles', txt: 'Plain Text', json: 'JSON' }
    transcriptPopoutHtml = `
      <div class="dlv-popout dlv-popout--transcript">
        ${f.transcripts.map((t) => `
          <a class="dlv-popout-btn" data-action="dl-transcript"
            href="/d/${escHtml(token)}/download?key=${encodeURIComponent(t.r2_key)}"
            download="${escHtml(t.filename)}">
            ${escHtml(kindLabel[t.kind] || t.kind.toUpperCase())}
          </a>`).join('')}
      </div>`
  }

  return `<div class="dlv-row-action-group">${dlIconHtml}${transcriptBtnHtml}${popoutHtml}${transcriptPopoutHtml}</div>`
}

// Legacy alias
function renderRows() { scheduleRender() }

// ── Download mechanics ─────────────────────────────────────────────────────────

async function runQueue() {
  if (queueRunning) return
  queueRunning = true
  queuePaused  = false
  updateBatchBtn()
  renderOverallProgress()

  try {
    for (const d of downloadQueue) {
      if (state.get(d.r2_key) !== 'pending') continue
      if (queuePaused) break
      await downloadOne(d)
    }
  } finally {
    queueRunning = false
    updateBatchBtn()
    renderOverallProgress()

    const c = countByEffectiveState()
    if (!queuePaused) {
      if (c.failed > 0) {
        showToast(`${c.failed} file${c.failed === 1 ? '' : 's'} failed — Retry available`, 'error')
      } else if (c.complete === files.length && files.length > 0) {
        showToast('All files downloaded', 'success')
      }
    }
  }
}

async function downloadOne(fileDesc) {
  const key = fileDesc.r2_key
  if (state.get(key) === 'complete') return
  state.set(key, 'downloading')
  progress.set(key, 0)
  errors.delete(key)
  scheduleRender()

  let writer
  try {
    const stream = await window.streamSaver.createWriteStream(fileDesc.filename, {
      size:     fileDesc.file_size,
      mimeType: fileDesc.mime_type,
    })
    writer = stream.getWriter()
  } catch (err) {
    state.set(key, 'failed')
    errors.set(key, `Couldn't start download: ${err.message || err}`)
    scheduleRender()
    return
  }

  const total = fileDesc.file_size
  let written = 0
  try {
    for (let start = 0; start < total; start += CHUNK_SIZE) {
      if (queuePaused) {
        while (queuePaused && state.get(key) === 'downloading') await sleep(100)
        if (state.get(key) !== 'downloading') break
      }
      const end = Math.min(start + CHUNK_SIZE - 1, total - 1)
      const buf = await fetchChunkWithRetry(key, start, end)
      await writer.write(new Uint8Array(buf))
      written += buf.byteLength
      progress.set(key, Math.round((written / total) * 100))
      scheduleRender()
    }
    await writer.close()
    state.set(key, 'complete')
    progress.delete(key)
    persistCompleted()
  } catch (err) {
    try { await writer.abort() } catch { /* ignore */ }
    state.set(key, 'failed')
    errors.set(key, err.message || String(err))
  }
  scheduleRender()
}

async function fetchChunkWithRetry(key, start, end) {
  const url = `/d/${token}/download?key=${encodeURIComponent(key)}`
  let lastErr
  for (let attempt = 0; attempt < CHUNK_RETRIES; attempt++) {
    try {
      const res = await fetch(url, { headers: { Range: `bytes=${start}-${end}` } })
      if (res.status === 410) { clearStorage(); throw new Error('This delivery link has expired.') }
      if (!(res.status === 206 || res.status === 200)) throw new Error(`HTTP ${res.status}`)
      return await res.arrayBuffer()
    } catch (err) {
      lastErr = err
      if (attempt < CHUNK_RETRIES - 1) await sleep(CHUNK_BACKOFF_MS[attempt])
    }
  }
  throw lastErr || new Error('chunk fetch failed')
}

function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)) }

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

  const completed = loadCompletedFromStorage()
  for (const f of files) {
    state.set(f.r2_key, completed.has(f.r2_key) ? 'complete' : 'pending')
    if (f.proxy_r2_key) {
      state.set(f.proxy_r2_key, completed.has(f.proxy_r2_key) ? 'complete' : 'pending')
    }
  }

  if (!window.streamSaver || !window.streamSaver.isSupported()) {
    list.innerHTML = '<p class="files-empty">Your browser doesn\'t support streamed downloads. Try Chrome or Firefox on desktop, or use "Having trouble?" below to reach us.</p>'
    dlBtn.disabled = true
    return
  }

  initRenderRows()
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
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')
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
