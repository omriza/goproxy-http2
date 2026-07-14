// Command streaming reproduces a response-streaming bug in goproxy's HTTP/2 MITM
// support: streamed responses are buffered and delivered to the client only when the
// stream closes, instead of incrementally as the origin produces them.
//
// The decisive case is a gRPC bidirectional stream (full-duplex ping-pong): the
// client must receive server message N before it sends message N+1. goproxy's h2
// MITM handler copies the upstream response body with a plain io.Copy and never
// flushes, so Go's HTTP/2 server coalesces the writes and the server's replies sit
// buffered until the stream closes. The client's first Recv therefore blocks until
// the context deadline, and the bidi RPC deadlocks.
//
// Everything runs in-process: a TLS (h2) gRPC TestService server, a plain goproxy
// MITM (AllowHTTP2 = true, AlwaysMitm), and a gRPC client that tunnels through the
// proxy via HTTP CONNECT and runs a full-duplex ping-pong. Run with: go run .
//
// Exit code: 1 = bug reproduced (bidi deadlocked), 0 = streaming works (fixed).
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
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"time"

	"github.com/elazarl/goproxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	pb "google.golang.org/grpc/interop/grpc_testing"
)

// testStreamService implements grpc.testing.TestService. FullDuplexCall is a true
// full-duplex echo: it answers each incoming request immediately with a payload of
// the requested size. If the proxy buffers the response body until the stream
// closes, the client never receives these mid-stream and the RPC deadlocks.
type testStreamService struct {
	pb.UnimplementedTestServiceServer
}

func (testStreamService) FullDuplexCall(stream grpc.BidiStreamingServer[pb.StreamingOutputCallRequest, pb.StreamingOutputCallResponse]) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		size := int32(0)
		if ps := req.GetResponseParameters(); len(ps) > 0 {
			size = ps[0].GetSize()
		}
		if err := stream.Send(&pb.StreamingOutputCallResponse{
			Payload: &pb.Payload{Body: make([]byte, size)},
		}); err != nil {
			return err
		}
	}
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

// startGRPCServer starts a TLS (h2) gRPC TestService server and returns its address.
func startGRPCServer() string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{selfSignedCert()}, NextProtos: []string{"h2"}}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	pb.RegisterTestServiceServer(srv, testStreamService{})
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
	fmt.Println("=== goproxy HTTP/2 MITM — gRPC bidirectional streaming reproduction ===")
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

	// Bounded so a buffering proxy makes the ping-pong fail deterministically
	// (deadline) instead of hanging forever.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	stream, err := pb.NewTestServiceClient(conn).FullDuplexCall(ctx)
	if err != nil {
		log.Fatalf("FullDuplexCall: %v", err)
	}

	sizes := []int32{8, 16, 24, 32, 40}
	for i, s := range sizes {
		if err := stream.Send(&pb.StreamingOutputCallRequest{
			ResponseParameters: []*pb.ResponseParameters{{Size: s}},
		}); err != nil {
			fmt.Printf("\nbidi Send %d failed: %v\n", i, err)
			os.Exit(2)
		}
		// True full-duplex: the reply to message i must arrive before we send i+1.
		resp, err := stream.Recv()
		if err != nil {
			fmt.Printf("\nRPC FAILED on Recv %d: %v\n\n", i, err)
			st, _ := status.FromError(err)
			if st.Code() == codes.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded) {
				fmt.Printf("BUG REPRODUCED: the bidirectional stream deadlocked. The client sent\n")
				fmt.Printf("message %d and waited for the server's reply, but goproxy's HTTP/2 MITM\n", i)
				fmt.Printf("buffered the response body (plain io.Copy, no flush), so the reply never\n")
				fmt.Printf("reached the client mid-stream and the context deadline fired.\n")
				os.Exit(1)
			}
			os.Exit(2)
		}
		if got := len(resp.GetPayload().GetBody()); got != int(s) {
			fmt.Printf("\nbidi resp %d size = %d, want %d\n", i, got, s)
			os.Exit(2)
		}
		fmt.Printf("ping-pong %d OK (echoed %d bytes)\n", i, s)
	}
	if err := stream.CloseSend(); err != nil {
		log.Fatalf("CloseSend: %v", err)
	}
	if _, err := stream.Recv(); err != nil && !errors.Is(err, io.EOF) {
		fmt.Printf("\nfinal Recv = %v, want EOF\n", err)
		os.Exit(2)
	}

	fmt.Printf("\nRPC OK: full-duplex ping-pong completed; every reply arrived mid-stream.\n")
	fmt.Println("Streaming responses were flushed through the MITM (bug appears fixed).")
}
