// LLM 上游（nginx）对 HTTP/1.1 的聊天补全响应只回 200 响应头、体一个字节
// 都不来（chunked + br），Node 内置 fetch（undici）读不到体，AbortSignal
// 也救不了卡住的体流；同一条上游走 HTTP/2 时体 2 秒内完整回来。
// 所以 LLM 请求走内置的 node:http2，接口保持 fetch 的样子，llm.ts 里
// 的超时、重试、中止逻辑原样复用；本地 http 地址仍走全局 fetch。
import http2 from "node:http2"
import { brotliDecompressSync, gunzipSync, inflateSync } from "node:zlib"

function decodeBody(body: Buffer, encoding: string | null): string {
  if (encoding === "br") {
    try {
      return brotliDecompressSync(body).toString("utf8")
    } catch {
      return body.toString("utf8")
    }
  }
  if (encoding === "gzip") {
    try {
      return gunzipSync(body).toString("utf8")
    } catch {
      return body.toString("utf8")
    }
  }
  if (encoding === "deflate") {
    try {
      return inflateSync(body).toString("utf8")
    } catch {
      return body.toString("utf8")
    }
  }
  return body.toString("utf8")
}

type HeaderMap = Map<string, string>

function makeResponse(status: number, headers: HeaderMap, body: Buffer): Response {
  let textCache: string | undefined
  const text = async (): Promise<string> => {
    if (textCache === undefined) textCache = decodeBody(body, headers.get("content-encoding") ?? null)
    return textCache
  }
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (name: string) => headers.get(name.toLowerCase()) ?? null },
    text,
    json: async () => JSON.parse(await text()) as unknown,
  } as unknown as Response
}

function abortError(signal: AbortSignal): Error {
  const reason = signal.reason
  return reason instanceof Error ? reason : new Error("aborted")
}

export function createHttp2Fetch(options: { http2ForHttp?: boolean } = {}): typeof fetch {
  let session: http2.ClientHttp2Session | undefined
  let sessionOrigin = ""
  const dropSession = () => {
    const current = session
    session = undefined
    sessionOrigin = ""
    if (current && !current.destroyed) current.destroy()
  }
  const getSession = (url: URL): http2.ClientHttp2Session => {
    if (session && !session.destroyed && !session.closed && sessionOrigin === url.origin) return session
    dropSession()
    const next = http2.connect(url.origin)
    session = next
    sessionOrigin = url.origin
    const drop = () => {
      if (session === next) {
        session = undefined
        sessionOrigin = ""
      }
    }
    next.on("error", drop)
    next.on("close", drop)
    next.on("goaway", drop)
    return next
  }
  const http2Fetch = async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url)
    if (url.protocol !== "https:" && options.http2ForHttp !== true) return fetch(input, init)
    const signal = init?.signal
    const reqHeaders: Record<string, string> = {}
    const rawHeaders = init?.headers
    if (rawHeaders) {
      if (rawHeaders instanceof Headers) {
        rawHeaders.forEach((value, key) => { reqHeaders[key.toLowerCase()] = value })
      } else if (Array.isArray(rawHeaders)) {
        for (const [key, value] of rawHeaders) reqHeaders[key.toLowerCase()] = value
      } else {
        for (const [key, value] of Object.entries(rawHeaders)) reqHeaders[key.toLowerCase()] = value
      }
    }
    const body = init?.body
    return new Promise<Response>((resolve, reject) => {
      let stream: http2.ClientHttp2Stream | undefined
      let settled = false
      const finish = (fn: () => void) => {
        if (settled) return
        settled = true
        fn()
      }
      const onAbort = () => {
        stream?.close()
        finish(() => reject(signal ? abortError(signal) : new Error("aborted")))
      }
      if (signal) {
        if (signal.aborted) {
          onAbort()
          return
        }
        signal.addEventListener("abort", onAbort, { once: true })
      }
      let sess: http2.ClientHttp2Session
      try {
        sess = getSession(url)
      } catch (err) {
        finish(() => reject(err instanceof Error ? err : new Error(String(err))))
        return
      }
      const chunks: Buffer[] = []
      let status = 0
      let ended = false
      const headers = new Map<string, string>()
      try {
        stream = sess.request({
          ":method": init?.method ?? "GET",
          ":path": url.pathname + url.search,
          ...reqHeaders,
        })
      } catch (err) {
        finish(() => reject(err instanceof Error ? err : new Error(String(err))))
        return
      }
      stream.on("response", (responseHeaders) => {
        status = Number((responseHeaders as Record<string, unknown>)[":status"] ?? 0)
        for (const [key, value] of Object.entries(responseHeaders)) {
          if (key.startsWith(":")) continue
          if (typeof value === "string") headers.set(key.toLowerCase(), value)
          else if (Array.isArray(value)) headers.set(key.toLowerCase(), value.join(", "))
        }
      })
      stream.on("data", (chunk: Buffer) => chunks.push(Buffer.from(chunk)))
      stream.on("end", () => {
        ended = true
        if (signal) signal.removeEventListener("abort", onAbort)
        finish(() => resolve(makeResponse(status, headers, Buffer.concat(chunks))))
      })
      stream.on("error", (err) => {
        if (signal) signal.removeEventListener("abort", onAbort)
        finish(() => reject(err))
      })
      stream.on("close", () => {
        if (ended) return
        if (signal) signal.removeEventListener("abort", onAbort)
        finish(() => reject(new Error("LLM API: http2 stream closed before the response body finished")))
      })
      if (typeof body === "string") stream.end(body)
      else if (body instanceof Uint8Array) stream.end(body)
      else stream.end()
    })
  }
  return http2Fetch as unknown as typeof fetch
}
