# goproxy HTTP/2 MITM drops response trailers

Minimal, self-contained reproduction of an HTTP/2 **trailer-propagation** bug in goproxy's
HTTP/2 MITM support (the `http2-mitm` branch, commit
[`565f717`](https://github.com/elazarl/goproxy/commit/565f717a3d408aea4689811ec593768215fbfd07)).

## TL;DR

When goproxy MITMs an HTTP/2 connection (`ProxyHttpServer.AllowHTTP2 = true`), it forwards the
upstream response's **headers** and **body** to the client but **drops the response's HTTP/2
trailers**. This breaks gRPC, which sends call status as the `grpc-status` / `grpc-message`
trailers — a gRPC client through the proxy fails with:

```
rpc error: code = Internal desc = server closed the stream without sending trailers
```

The same affects gRPC-Web and Connect streaming, or anything relying on HTTP/2 trailers.

## Prerequisites

- Go 1.24 or newer (`go version`).

## Run the reproduction

```
go run .
```

No flags, no setup. The program starts, in-process: an HTTP/2 origin (returns a body **plus
trailers**), a goproxy MITM (`AllowHTTP2 = true`, `AlwaysMitm`), and two clients — one talking to
the origin directly, one talking to it **through the goproxy MITM** — then prints what each sees.

### Expected output (bug present, against `elazarl/goproxy@565f717`)

```
[direct to origin] proto=HTTP/2.0 body="hello from the HTTP/2 origin" trailers=map[Grpc-Message:[OK] Grpc-Status:[0]]
[via goproxy MITM] proto=HTTP/2.0 body="hello from the HTTP/2 origin" trailers=map[]

BUG REPRODUCED: the origin sends HTTP/2 trailers (seen by the direct client),
but they are DROPPED when the response is proxied through goproxy's HTTP/2 MITM.
```

Both requests use HTTP/2; only the proxied one loses the trailers (`trailers=map[]`).
Exit code: `1` = bug reproduced, `0` = trailers preserved (fixed), `2` = inconclusive.

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

See [`suggested-fix.patch`](./suggested-fix.patch). After the body copy — when `resp.Trailer` is
populated — emit the trailers via `http.TrailerPrefix`, which the `http2.Server` sends as trailers
at the end of the stream. This **streams the body (no buffering)** and handles *unannounced*
trailers (how gRPC sends `grpc-status`):

```go
w.WriteHeader(resp.StatusCode)
if resp.Body != nil {
    io.Copy(w, resp.Body)
}
// Forward HTTP/2 response trailers (e.g. gRPC grpc-status/grpc-message).
for k, vv := range resp.Trailer {
    for _, v := range vv {
        w.Header().Add(http.TrailerPrefix+k, v)
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
3. The `[via goproxy MITM]` line now shows `trailers=map[Grpc-Message:[OK] Grpc-Status:[0]]` and
   the program prints `OK: trailers were preserved through the MITM (bug appears fixed).`

## Notes

- The fix uses `http.TrailerPrefix` rather than pre-declaring trailers via the `Trailer` header,
  because gRPC (and many h2 servers) send **unannounced** trailers whose keys/values are only known
  after the body.
- This reproduction covers **response** trailers (server → client), which is what breaks gRPC.
  Request trailers (client → server) are out of scope.
