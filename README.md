# goproxy HTTP/2 MITM reproductions

This repo hosts self-contained reproductions of HTTP/2 MITM bugs on goproxy's `http2-mitm`
branch, each with a suggested fix:

| Repro | Issue | Status |
|-------|-------|--------|
| root (this file) | h2 MITM drops response **trailers** (breaks gRPC status) | fixed and merged on the branch |
| [`streaming/`](./streaming) | h2 MITM **buffers streamed responses** (breaks gRPC/Connect bidi + server-streaming) | proposed fix, see `streaming/suggested-fix.patch` |

---

# goproxy HTTP/2 MITM drops response trailers (breaks gRPC)

Self-contained reproduction of an HTTP/2 **trailer-propagation** bug in goproxy's HTTP/2 MITM
support (the `http2-mitm` branch, commit
[`2ac13d1`](https://github.com/elazarl/goproxy/commit/2ac13d10e306b54cc0aeb9ecb5d4e268d316aead)).
**This bug is now fixed on the branch;** the reproduction below pins the pre-fix commit so it
still demonstrates the original failure.

## TL;DR

When goproxy MITMs an HTTP/2 connection (`ProxyHttpServer.AllowHTTP2 = true`), it forwards the
upstream response's **headers** and **body** to the client but **drops the response's HTTP/2
trailers**. gRPC carries call status in trailers (`grpc-status` / `grpc-message`), so a gRPC call
through the proxy reaches the server and returns a reply, but the client fails with:

```
rpc error: code = Internal desc = server closed the stream without sending trailers
```

The same affects anything that relies on HTTP/2 trailers (gRPC-Web, Connect streaming, ...).

## Prerequisites

- Go 1.25 or newer (`go version`).

## Run the reproduction

```
go run .
```

Everything runs in-process: a TLS (h2) gRPC Greeter server, a plain goproxy MITM
(`AllowHTTP2 = true`, `AlwaysMitm`), and a gRPC client that tunnels through the proxy via HTTP
CONNECT and calls `SayHello`.

### Expected output (bug present, against `elazarl/goproxy@2ac13d1`)

```
[grpc-server] SayHello name="goproxy" (server was reached)

RPC FAILED: rpc error: code = Internal desc = server closed the stream without sending trailers

BUG REPRODUCED: the call reached the gRPC server (see the log line above),
but its status trailers (grpc-status/grpc-message) were dropped by goproxy's
HTTP/2 MITM, so the client sees 'server closed the stream without sending trailers'.
```

The server log line proves the call went all the way through (h2 client → goproxy MITM → h2
server) and only the trailers are lost. Exit code: `1` = bug reproduced, `0` = trailers preserved
(fixed).

## Root cause

`http2.go`, `handleH2MitmStream` (around lines 170–192): it forwards `resp.Header` and
`io.Copy`s `resp.Body`, but never reads or emits `resp.Trailer`. Trailers are only populated once
the upstream body is fully read, and the handler returns without sending them.

```go
for k, vv := range resp.Header { // headers forwarded
    for _, v := range vv {
        w.Header().Add(k, v)
    }
}
w.WriteHeader(resp.StatusCode)
if resp.Body != nil {
    io.Copy(w, resp.Body) // body forwarded
}
// resp.Trailer is never forwarded  <-- the gap
```

## The fix

See [`suggested-fix.patch`](./suggested-fix.patch). It mirrors the trailer handling that
`handleHttp` in `http.go` already does (the h2 MITM path just never got it). Two parts:

1. **Announce** any pre-known trailers via the `Trailer` header *before* `WriteHeader`, so the
   `http2.Server` commits to sending a trailing HEADERS frame.
2. **After the body**, forward `resp.Trailer`: pre-announced trailers are set by name; the rest
   (HTTP/2 origins / gRPC, which send them *unannounced*) go through `http.TrailerPrefix`. The
   `Flush` forces chunking so small/buffered bodies don't silently drop the trailers.

This streams the body (no buffering).

```go
// before WriteHeader — announce pre-known trailers:
announcedTrailers := len(resp.Trailer)
if announcedTrailers > 0 {
    trailerKeys := make([]string, 0, announcedTrailers)
    for k := range resp.Trailer {
        trailerKeys = append(trailerKeys, k)
    }
    w.Header().Add("Trailer", strings.Join(trailerKeys, ", "))
}
w.WriteHeader(resp.StatusCode)

// ... io.Copy(w, resp.Body) ...

// after the body — forward trailers:
if len(resp.Trailer) > 0 {
    if rc := http.NewResponseController(w); rc != nil {
        _ = rc.Flush()
    }
    if len(resp.Trailer) == announcedTrailers {
        copyHeaders(w.Header(), resp.Trailer, proxy.KeepDestinationHeaders)
    } else {
        for k, vs := range resp.Trailer {
            k = http.TrailerPrefix + k
            for _, v := range vs {
                w.Header().Add(k, v)
            }
        }
    }
}
```

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
3. It now prints `RPC OK: "Hello goproxy (from real gRPC server over h2)"`.

## Notes

- The fix mirrors `handleHttp` in `http.go`: pre-announced trailers are announced via the
  `Trailer` header and set by name; unannounced trailers (how gRPC sends `grpc-status`) use
  `http.TrailerPrefix`. gRPC alone only needs the `TrailerPrefix` branch, but announcing keeps
  the h2 path consistent with the h1 path and `net/http/httputil.ReverseProxy`.
- This reproduction covers **response** trailers (server → client), which is what breaks gRPC.
  Request trailers (client → server) are out of scope.
