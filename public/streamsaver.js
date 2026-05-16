/**
 * StreamSaver client library (minimal in-house version)
 *
 * Pairs with /streamsaver-sw.js. Exposes window.streamSaver.createWriteStream(filename, opts)
 * which returns a WritableStream that streams bytes directly to the browser's
 * download manager via a service-worker-intercepted fetch.
 *
 * Cross-browser: Chrome, Edge, Firefox, Safari (desktop). iOS Safari throttles
 * service workers aggressively and may fail above ~500 MB — degrade message
 * is surfaced by the delivery page.
 *
 * Usage:
 *   const writer = await streamSaver.createWriteStream('big.mp4', { size, mimeType }).getWriter()
 *   await writer.write(uint8Array)
 *   await writer.close()
 */

(function () {
  const SW_PATH = '/streamsaver-sw.js'
  const SW_SCOPE = '/streamsaver-dl/'
  let swRegistrationPromise = null

  async function ensureWorker() {
    if (!('serviceWorker' in navigator)) {
      throw new Error('Service workers are not supported in this browser')
    }
    if (swRegistrationPromise) return swRegistrationPromise

    swRegistrationPromise = (async () => {
      const reg = await navigator.serviceWorker.register(SW_PATH, { scope: SW_SCOPE })
      // Wait until the SW is in "activated" state — needed before the first
      // navigation will be intercepted by the fetch handler.
      if (reg.active) return reg
      await new Promise((resolve) => {
        const target = reg.installing || reg.waiting
        if (!target) return resolve()
        target.addEventListener('statechange', () => {
          if (target.state === 'activated') resolve()
        })
      })
      return reg
    })()
    return swRegistrationPromise
  }

  function isSupported() {
    return typeof window !== 'undefined'
      && 'serviceWorker' in navigator
      && typeof WritableStream !== 'undefined'
      && typeof MessageChannel !== 'undefined'
  }

  /**
   * Create a WritableStream that, when written to, surfaces a download in the
   * browser's downloads bar. The download's bytes are exactly what you write.
   * Returns a WritableStream (call .getWriter() to write).
   */
  async function createWriteStream(filename, { size, mimeType } = {}) {
    const reg = await ensureWorker()
    const sw = reg.active || navigator.serviceWorker.controller
    if (!sw) throw new Error('Service worker is not active yet')

    const id = (crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`)
    const channel = new MessageChannel()

    // Hand the port and metadata to the SW
    sw.postMessage(
      { type: 'register-stream', id, filename, size, mimeType },
      [channel.port2],
    )

    // Wait for the SW to ack registration before we trigger the download fetch
    await new Promise((resolve) => {
      const ack = (e) => {
        if (e.data && e.data.type === 'registered') {
          channel.port1.removeEventListener('message', ack)
          resolve()
        }
      }
      channel.port1.addEventListener('message', ack)
      channel.port1.start()
    })

    // Trigger the download — invisible iframe so the page isn't navigated away
    const iframe = document.createElement('iframe')
    iframe.hidden = true
    iframe.src = `${SW_SCOPE}${id}/${encodeURIComponent(filename)}`
    document.body.appendChild(iframe)

    // Wait for the SW to confirm the fetch handler has hooked up its controller
    await new Promise((resolve, reject) => {
      const handler = (e) => {
        if (!e.data) return
        if (e.data.type === 'ready') {
          channel.port1.removeEventListener('message', handler)
          resolve()
        } else if (e.data.type === 'consumer-cancel') {
          channel.port1.removeEventListener('message', handler)
          reject(new Error('Download cancelled by browser'))
        }
      }
      channel.port1.addEventListener('message', handler)
    })

    // Backpressure: only one chunk in flight at a time. Resolves when SW acks.
    function writeChunk(chunk) {
      return new Promise((resolve, reject) => {
        const ack = (e) => {
          if (!e.data) return
          if (e.data.type === 'chunk-ack') {
            channel.port1.removeEventListener('message', ack)
            resolve()
          } else if (e.data.type === 'chunk-error') {
            channel.port1.removeEventListener('message', ack)
            reject(new Error(e.data.message || 'chunk write failed'))
          } else if (e.data.type === 'consumer-cancel') {
            channel.port1.removeEventListener('message', ack)
            reject(new Error('Download cancelled by browser'))
          }
        }
        channel.port1.addEventListener('message', ack)
        // Transfer the chunk's underlying buffer to the SW so we don't pay a copy
        const buf = chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength)
        channel.port1.postMessage({ type: 'chunk', chunk: buf }, [buf])
      })
    }

    return new WritableStream({
      write(chunk) {
        if (!(chunk instanceof Uint8Array)) chunk = new Uint8Array(chunk)
        return writeChunk(chunk)
      },
      close() {
        channel.port1.postMessage({ type: 'close' })
        try { iframe.remove() } catch { /* ignore */ }
      },
      abort() {
        channel.port1.postMessage({ type: 'abort' })
        try { iframe.remove() } catch { /* ignore */ }
      },
    })
  }

  window.streamSaver = { createWriteStream, isSupported }
})()
