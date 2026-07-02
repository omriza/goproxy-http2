# goproxy HTTP/2 MITM drops response trailers (breaks gRPC)

Self-contained reproduction of an HTTP/2 **trailer-propagation** bug in goproxy's HTTP/2 MITM
support (the `http2-mitm` branch, commit
[`565f717`](https://github.com/elazarl/goproxy/commit/565f717a3d408aea4689811ec593768215fbfd07)).

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

### Expected output (bug present, against `elazarl/goproxy@565f717`)

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
3. It now prints `RPC OK: "Hello goproxy (from real gRPC server over h2)"`.

## Notes

- The fix uses `http.TrailerPrefix` rather than pre-declaring trailers via the `Trailer` header,
  because gRPC (and many h2 servers) send **unannounced** trailers whose keys/values are only
  known after the body.
- This reproduction covers **response** trailers (server → client), which is what breaks gRPC.
  Request trailers (client → server) are out of scope.
