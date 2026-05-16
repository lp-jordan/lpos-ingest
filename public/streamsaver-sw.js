/**
 * StreamSaver service worker (minimal in-house version)
 *
 * Pattern (same as the open-source StreamSaver.js library, simplified for our
 * single-origin case): the page registers a virtual "stream" with this worker
 * via postMessage, gets back a MessagePort, then triggers a download by
 * navigating an invisible iframe to /streamsaver-dl/<id>/<filename>.
 *
 * This worker intercepts that fetch, builds a streaming Response whose body
 * pulls chunks from the MessagePort, and sets Content-Disposition: attachment
 * so the browser writes the bytes straight to disk as they arrive.
 *
 * Why this exists: the browser's <a download> mechanism doesn't expose any
 * success/failure signal to JS, and fetch()+Blob requires buffering the whole
 * file in RAM. This combo lets us do retryable chunked range fetches AND get
 * proper streaming-to-disk in every modern browser.
 */

self.addEventListener('install', () => self.skipWaiting())
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()))

// streamId → { filename, mimeType, size, port, controller }
const pending = new Map()

self.addEventListener('message', (event) => {
  const data = event.data ?? {}

  if (data.type === 'register-stream') {
    const port = event.ports[0]
    if (!port) return
    pending.set(data.id, {
      filename: data.filename,
      mimeType: data.mimeType || 'application/octet-stream',
      size:     data.size,
      port,
      controller: null,   // set when the fetch handler builds the ReadableStream
    })

    port.onmessage = (e) => handlePortMessage(data.id, e.data)
    port.start()

    // Ack so the page knows the SW is ready to receive chunks
    port.postMessage({ type: 'registered' })
    return
  }
})

function handlePortMessage(streamId, msg) {
  const entry = pending.get(streamId)
  if (!entry || !entry.controller) {
    // Either the entry is gone (fetch already consumed it and the page is
    // racing the abort), or the fetch hasn't fired yet — chunks before the
    // fetch are dropped. The page is expected to only start writing after
    // the iframe load has triggered the fetch handler below, which is
    // arranged by waitForReady() on the client side.
    return
  }

  if (msg.type === 'chunk') {
    try {
      entry.controller.enqueue(new Uint8Array(msg.chunk))
      entry.port.postMessage({ type: 'chunk-ack' })
    } catch (err) {
      entry.port.postMessage({ type: 'chunk-error', message: String(err) })
    }
  } else if (msg.type === 'close') {
    try { entry.controller.close() } catch { /* already closed */ }
    pending.delete(streamId)
  } else if (msg.type === 'abort') {
    try { entry.controller.error(new Error('aborted')) } catch { /* already errored */ }
    pending.delete(streamId)
  }
}

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url)
  const match = url.pathname.match(/^\/streamsaver-dl\/([^/]+)/)
  if (!match) return

  const streamId = match[1]
  const entry = pending.get(streamId)
  if (!entry) {
    event.respondWith(new Response('Stream not registered', { status: 404 }))
    return
  }

  const stream = new ReadableStream({
    start(controller) {
      entry.controller = controller
      // Signal the page that the download navigation has landed and writes
      // will now be relayed to the browser's download manager.
      entry.port.postMessage({ type: 'ready' })
    },
    cancel() {
      // Recipient closed the tab or the browser cancelled the download
      try { entry.port.postMessage({ type: 'consumer-cancel' }) } catch { /* port gone */ }
      pending.delete(streamId)
    },
  })

  const headers = new Headers({
    'Content-Type':        entry.mimeType,
    'Content-Disposition': `attachment; filename="${sanitizeFilename(entry.filename)}"`,
    // Prevent intermediaries from buffering — we want bytes to flow.
    'Cache-Control':       'no-store',
    'X-Content-Type-Options': 'nosniff',
  })
  if (typeof entry.size === 'number' && Number.isFinite(entry.size)) {
    headers.set('Content-Length', String(entry.size))
  }

  event.respondWith(new Response(stream, { headers }))
})

function sanitizeFilename(name) {
  return String(name || 'download')
    .replace(/[^a-zA-Z0-9._\-() ]/g, '_')
    .replace(/\.{2,}/g, '.')
    .replace(/^[\s.]+|[\s.]+$/g, '')
    .slice(0, 200) || 'download'
}
