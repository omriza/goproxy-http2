# goproxy HTTP/2 MITM buffers streamed responses (breaks gRPC/Connect streaming)

Self-contained reproduction of a response-streaming bug in goproxy's HTTP/2 MITM support
(the `http2-mitm` branch, commit
[`8007ee3`](https://github.com/elazarl/goproxy/commit/8007ee385c9f)).

This is a follow-up to the trailer-drop fix that already landed on the branch (see the repo
root). Same file, same handler, next gap.

## TL;DR

When goproxy MITMs an HTTP/2 connection (`ProxyHttpServer.AllowHTTP2 = true`), it forwards the
upstream response body to the client with a plain `io.Copy` and **never flushes**. Go's HTTP/2
server coalesces writes and only flushes when its buffer fills or the handler returns, so
mid-stream DATA frames are stuck until the stream closes. Effects:

- **Server-streaming**: every message is batched and delivered at the end (high latency).
- **Bidirectional streaming**: structurally broken. A bidi client must receive server message N
  before sending N+1, but the replies sit buffered and never arrive mid-stream, so the client
  deadlocks.

The same affects Connect streaming and Server-Sent Events (SSE).

## Prerequisites

- Go 1.25 or newer (`go version`).

## Run the reproduction

```
go run .
```

Everything runs in-process: a TLS (h2) gRPC `TestService` server, a plain goproxy MITM
(`AllowHTTP2 = true`, `AlwaysMitm`), and a gRPC client that tunnels through the proxy via HTTP
CONNECT and runs a full-duplex `FullDuplexCall` ping-pong (send message N, wait for reply N,
then send N+1).

### Expected output (bug present, against `elazarl/goproxy@8007ee3`)

```
=== goproxy HTTP/2 MITM — gRPC bidirectional streaming reproduction ===
gRPC server: 127.0.0.1:xxxxx
goproxy MITM: 127.0.0.1:xxxxx

RPC FAILED on Recv 0: rpc error: code = DeadlineExceeded desc = context deadline exceeded

BUG REPRODUCED: the bidirectional stream deadlocked. The client sent
message 0 and waited for the server's reply, but goproxy's HTTP/2 MITM
buffered the response body (plain io.Copy, no flush), so the reply never
reached the client mid-stream and the context deadline fired.
```

The client's very first `Recv` blocks until the context deadline (~15s), because the server's
reply is buffered inside the proxy. Exit code: `1` = bug reproduced, `0` = streaming works
(fixed).

## Root cause

`http2.go`, `handleH2MitmStream`: the response body is forwarded with a plain `io.Copy` and no
flush.

```go
w.WriteHeader(resp.StatusCode)

if resp.Body != nil {
    if _, err := io.Copy(w, resp.Body); err != nil { // never flushes
        ctx.Warnf("HTTP/2 MITM: error writing response body: %v", err)
    }
}
```

Go's `http2.Server` `ResponseWriter` coalesces writes and only emits a DATA frame when its
buffer fills or the handler returns. For streaming bodies (small, infrequent messages) nothing
reaches the client until the whole stream ends. The request side is fine: `http2.Transport`
streams the request body concurrently, so the origin sees client messages promptly.

## The fix

See [`suggested-fix.patch`](./suggested-fix.patch). It mirrors
`net/http/httputil.ReverseProxy.flushInterval`: flush after each chunk **only for streaming
responses** (base `Content-Type == text/event-stream`, or an unknown `Content-Length` of `-1`,
which covers gRPC, Connect, and chunked). Fixed-length responses keep the batched `io.Copy`, so
there is no throughput regression on regular downloads.

```go
if resp.Body != nil {
    if shouldFlushStreaming(resp) {
        // Streaming (gRPC/Connect/SSE/chunked): flush each chunk so it reaches the
        // client as it arrives. Mirrors net/http/httputil.ReverseProxy.flushInterval.
        rc := http.NewResponseController(w)
        buf := make([]byte, 32*1024)
        for {
            nr, er := resp.Body.Read(buf)
            if nr > 0 {
                if _, ew := w.Write(buf[:nr]); ew != nil {
                    ctx.Warnf("HTTP/2 MITM: error writing response body: %v", ew)
                    break
                }
                _ = rc.Flush()
            }
            if er != nil {
                if er != io.EOF {
                    ctx.Warnf("HTTP/2 MITM: error reading response body: %v", er)
                }
                break
            }
        }
    } else {
        // Fixed-length response: let the h2 server batch writes for throughput.
        if _, err := io.Copy(w, resp.Body); err != nil {
            ctx.Warnf("HTTP/2 MITM: error writing response body: %v", err)
        }
    }
}

// shouldFlushStreaming mirrors httputil.ReverseProxy.flushInterval: Server-Sent Events,
// or any response with an unknown (-1) Content-Length (gRPC, Connect, chunked).
func shouldFlushStreaming(resp *http.Response) bool {
    if baseCT, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); baseCT == "text/event-stream" {
        return true
    }
    return resp.ContentLength == -1
}
```

Adds a `mime` import to `http2.go`. It sits directly before the trailer-forwarding block from the
earlier fix (which does its own final flush), so the two compose without conflict.

### Verify the fix

1. Clone goproxy, check out the branch, and apply the patch:
   ```
   git clone https://github.com/elazarl/goproxy
   cd goproxy && git checkout http2-mitm && git apply /path/to/suggested-fix.patch && cd ..
   ```
2. Point this module at your patched checkout and re-run:
   ```
   go mod edit -replace github.com/elazarl/goproxy=../goproxy
   go mod tidy
   go run .
   ```
3. It now prints five `ping-pong N OK` lines and `RPC OK: full-duplex ping-pong completed`, and
   exits `0`.

## Alternatives considered

- **Always flush per chunk** (no heuristic): simplest and correct, but forces a DATA frame plus
  flush per ~32KB even on large fixed-length downloads, defeating the h2 server's write
  coalescing. The heuristic in `suggested-fix.patch` avoids that cost; the always-flush variant
  is a fine fallback if you'd prefer no `Content-Type`/`Content-Length` branch.

## Prior art

- Go's own `net/http/httputil.ReverseProxy` uses exactly this heuristic (`flushInterval`:
  `text/event-stream` or `ContentLength == -1` flush immediately). Same runtime, same
  constraints. Motivated by [golang/go#41642](https://github.com/golang/go/issues/41642) and
  refined by [golang/go#47359](https://github.com/golang/go/issues/47359).
- nginx `grpc_pass` forwards the response synchronously as it is received (does not buffer to
  completion). Envoy streams by default; buffering is opt-in.

## Notes

- This reproduction covers **response** streaming (server to client), which is what breaks bidi
  and server-streaming. Request streaming (client to server) already works: `http2.Transport`
  streams the request body concurrently.
- The bidi `FullDuplexCall` case is the decisive one because it deadlocks deterministically. A
  server-streaming call would still "work" but deliver everything in one late batch.
