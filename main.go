// Command goproxy-h2-trailer-repro is a self-contained reproduction of an HTTP/2
// trailer-propagation bug in goproxy's HTTP/2 MITM support (http2-mitm branch,
// commit 565f717).
//
// When goproxy MITMs an HTTP/2 connection (ProxyHttpServer.AllowHTTP2 = true),
// handleH2MitmStream copies the upstream response's headers and body to the client
// but never forwards the upstream response's HTTP/2 *trailers*. Any protocol that
// carries meaning in trailers therefore breaks through the proxy. The canonical
// victim is gRPC, which delivers call status as trailers (grpc-status /
// grpc-message); through the MITM a gRPC client fails with
//
//	rpc error: code = Internal desc = server closed the stream without sending trailers
//
// This program demonstrates the drop with plain net/http (no gRPC dependency):
//   - an HTTP/2 origin that returns a body AND trailers,
//   - a control client that talks to the origin directly (sees the trailers),
//   - a client that talks to the origin THROUGH the goproxy MITM (trailers missing).
package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"

	"github.com/elazarl/goproxy"
)

const (
	trailerStatusKey  = "Grpc-Status"
	trailerStatusVal  = "0"
	trailerMessageKey = "Grpc-Message"
	trailerMessageVal = "OK"
	responseBody      = "hello from the HTTP/2 origin"
)

// startOrigin starts an HTTP/2 TLS origin that returns a body plus HTTP/2 trailers.
// The trailers are sent gRPC-style (unannounced, via http.TrailerPrefix): they are
// only knowable after the body, exactly like gRPC's grpc-status.
func startOrigin() *httptest.Server {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, responseBody)
		w.Header().Set(http.TrailerPrefix+trailerStatusKey, trailerStatusVal)
		w.Header().Set(http.TrailerPrefix+trailerMessageKey, trailerMessageVal)
	})
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	return srv
}

// startMitmProxy starts a goproxy MITM proxy with HTTP/2 enabled, speaking h2 both
// to the client and to the origin (as with a real gRPC path).
func startMitmProxy() *httptest.Server {
	proxy := goproxy.NewProxyHttpServer()
	proxy.AllowHTTP2 = true
	proxy.Tr = &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"h2"},
		},
	}
	proxy.OnRequest().HandleConnect(goproxy.AlwaysMitm)
	return httptest.NewServer(proxy)
}

// fetch performs a GET and returns the negotiated protocol, body, and trailers.
// The body must be fully drained before resp.Trailer is populated.
func fetch(client *http.Client, targetURL string) (proto, body string, trailer http.Header, err error) {
	resp, err := client.Get(targetURL)
	if err != nil {
		return "", "", nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", nil, err
	}
	return resp.Proto, string(b), resp.Trailer, nil
}

func main() {
	log.SetFlags(0)

	origin := startOrigin()
	defer origin.Close()

	mitm := startMitmProxy()
	defer mitm.Close()

	// Control client: straight to the origin (trusts the origin's cert).
	directClient := origin.Client()

	// Repro client: through the goproxy MITM over HTTP/2, trusting goproxy's MITM CA.
	caPool := x509.NewCertPool()
	caPool.AddCert(goproxy.GoproxyCa.Leaf)
	proxyURL, _ := url.Parse(mitm.URL)
	mitmClient := &http.Client{
		Transport: &http.Transport{
			Proxy:             http.ProxyURL(proxyURL),
			ForceAttemptHTTP2: true,
			TLSClientConfig: &tls.Config{
				RootCAs:            caPool,
				InsecureSkipVerify: true, // forged leaf CN won't match 127.0.0.1
				NextProtos:         []string{"h2"},
			},
		},
	}

	fmt.Println("=== goproxy HTTP/2 MITM — trailer propagation reproduction ===")
	fmt.Printf("origin: %s\nproxy:  %s\n\n", origin.URL, mitm.URL)

	dProto, dBody, dTrailer, err := fetch(directClient, origin.URL)
	if err != nil {
		log.Fatalf("direct request failed: %v", err)
	}
	fmt.Printf("[direct to origin] proto=%s body=%q trailers=%v\n",
		dProto, dBody, map[string][]string(dTrailer))

	mProto, mBody, mTrailer, err := fetch(mitmClient, origin.URL)
	if err != nil {
		log.Fatalf("request via MITM failed: %v", err)
	}
	fmt.Printf("[via goproxy MITM] proto=%s body=%q trailers=%v\n\n",
		mProto, mBody, map[string][]string(mTrailer))

	switch {
	case mProto != "HTTP/2.0":
		fmt.Printf("INCONCLUSIVE: request via MITM did not use HTTP/2 (proto=%s); the h2 MITM path was not exercised.\n", mProto)
		os.Exit(2)
	case len(dTrailer) == 0:
		fmt.Println("INCONCLUSIVE: the direct client did not surface trailers; cannot demonstrate the drop.")
		os.Exit(2)
	case len(mTrailer) == 0:
		fmt.Println("BUG REPRODUCED: the origin sends HTTP/2 trailers (seen by the direct client),")
		fmt.Println("but they are DROPPED when the response is proxied through goproxy's HTTP/2 MITM.")
		fmt.Println("For gRPC this manifests as: rpc error: code = Internal desc = server closed the stream without sending trailers")
		os.Exit(1)
	default:
		fmt.Println("OK: trailers were preserved through the MITM (bug appears fixed).")
	}
}
