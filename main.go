// Command grpc reproduces the goproxy HTTP/2 MITM trailer-drop bug with a REAL
// gRPC client and server (google.golang.org/grpc), against plain goproxy — no
// Lumen, no extra proxy layer.
//
// gRPC carries call status in HTTP/2 trailers (grpc-status / grpc-message). Because
// goproxy's h2 MITM handler forwards headers + body but not trailers, the gRPC call
// reaches the server and returns a reply, yet the client fails with:
//
//	rpc error: code = Internal desc = server closed the stream without sending trailers
//
// Everything runs in-process: a TLS h2 gRPC Greeter server, a goproxy MITM
// (AllowHTTP2 = true, AlwaysMitm), and a gRPC client that tunnels through the proxy
// via HTTP CONNECT. Run with: go run .
package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/elazarl/goproxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
)

type greeter struct{ pb.UnimplementedGreeterServer }

func (greeter) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("[grpc-server] SayHello name=%q (server was reached)", in.GetName())
	return &pb.HelloReply{Message: "Hello " + in.GetName() + " (from real gRPC server over h2)"}, nil
}

// selfSignedCert generates an in-memory self-signed cert for 127.0.0.1.
func selfSignedCert() tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatalf("gen key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		log.Fatalf("create cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// startGRPCServer starts a TLS (h2) gRPC Greeter server and returns its address.
func startGRPCServer() string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{selfSignedCert()}, NextProtos: []string{"h2"}}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	pb.RegisterGreeterServer(srv, greeter{})
	go func() { _ = srv.Serve(lis) }()
	return lis.Addr().String()
}

// startMitmProxy starts a plain goproxy MITM with HTTP/2 enabled (client and upstream).
func startMitmProxy() string {
	proxy := goproxy.NewProxyHttpServer()
	proxy.AllowHTTP2 = true
	proxy.Tr = &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}},
	}
	proxy.OnRequest().HandleConnect(goproxy.AlwaysMitm)
	srv := httptest.NewServer(proxy)
	u, _ := url.Parse(srv.URL)
	return u.Host
}

// connectDialer tunnels to the upstream through the goproxy via HTTP CONNECT and
// returns the raw tunneled conn; gRPC's transport credentials then do TLS/h2 over it.
func connectDialer(proxyAddr, upstream string) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) {
		raw, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "tcp", proxyAddr)
		if err != nil {
			return nil, fmt.Errorf("dial proxy: %w", err)
		}
		req := &http.Request{Method: http.MethodConnect, URL: &url.URL{Host: upstream}, Host: upstream, Header: http.Header{}}
		if err := req.Write(raw); err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("write CONNECT: %w", err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(raw), req)
		if err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("read CONNECT response: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			_ = raw.Close()
			return nil, fmt.Errorf("CONNECT failed: %s", resp.Status)
		}
		return raw, nil
	}
}

// portAuthorityCreds advertises an :authority that includes the port, so goproxy's
// h2 MITM (which derives the upstream dial target from :authority) dials host:port
// rather than defaulting to :443. TLS verification still uses the inner config.
type portAuthorityCreds struct {
	credentials.TransportCredentials
	authority string
}

func (c *portAuthorityCreds) Info() credentials.ProtocolInfo {
	info := c.TransportCredentials.Info()
	info.ServerName = c.authority
	return info
}

func (c *portAuthorityCreds) Clone() credentials.TransportCredentials {
	return &portAuthorityCreds{TransportCredentials: c.TransportCredentials.Clone(), authority: c.authority}
}

func main() {
	log.SetFlags(0)

	upstream := startGRPCServer()
	proxyAddr := startMitmProxy()
	fmt.Println("=== goproxy HTTP/2 MITM — real gRPC trailer reproduction ===")
	fmt.Printf("gRPC server: %s\ngoproxy MITM: %s\n\n", upstream, proxyAddr)

	pool := x509.NewCertPool()
	pool.AddCert(goproxy.GoproxyCa.Leaf) // trust the goproxy MITM CA
	creds := &portAuthorityCreds{
		TransportCredentials: credentials.NewTLS(&tls.Config{
			RootCAs:            pool,
			ServerName:         "127.0.0.1",
			InsecureSkipVerify: true,
			NextProtos:         []string{"h2"},
		}),
		authority: upstream,
	}

	conn, err := grpc.NewClient(
		"passthrough:///"+upstream,
		grpc.WithTransportCredentials(creds),
		grpc.WithContextDialer(connectDialer(proxyAddr, upstream)),
	)
	if err != nil {
		log.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	resp, err := pb.NewGreeterClient(conn).SayHello(ctx, &pb.HelloRequest{Name: "goproxy"})
	if err != nil {
		fmt.Printf("\nRPC FAILED: %v\n\n", err)
		if strings.Contains(err.Error(), "without sending trailers") {
			fmt.Println("BUG REPRODUCED: the call reached the gRPC server (see the log line above),")
			fmt.Println("but its status trailers (grpc-status/grpc-message) were dropped by goproxy's")
			fmt.Println("HTTP/2 MITM, so the client sees 'server closed the stream without sending trailers'.")
			os.Exit(1)
		}
		os.Exit(2)
	}
	fmt.Printf("\nRPC OK: %q\n", resp.GetMessage())
	fmt.Println("Trailers were preserved through the MITM (bug appears fixed).")
}
