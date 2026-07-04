// Command ingothttp serves a minimal HTTP query API for an ingot database.
//
// Endpoints:
//
//	GET  /api/v1/query_range?query={name}&start={unix_ms}&end={unix_ms}
//	POST /api/v1/read          (JSON ReadRequest body)
//	GET  /api/v1/status        (DB summary stats)
//
// This is a demo/bridge for Grafana integration, not a full PromQL engine.
// The query parameter is a metric name (matched as __name__=<query>).
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/davidhrinaldo/ingot"
)

func main() {
	var (
		dataDir = flag.String("data", "./data", "ingot data directory")
		addr    = flag.String("addr", ":9001", "HTTP listen address")
	)
	flag.Parse()

	db, err := ingot.Open(*dataDir, ingot.Options{})
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	h := newHandler(db)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/query_range", h.queryRange)
	mux.HandleFunc("/api/v1/read", h.read)
	mux.HandleFunc("/api/v1/status", h.status)

	srv := &http.Server{Addr: *addr, Handler: mux}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		fmt.Fprintln(os.Stderr, "\nshutting down...")
		srv.Close()
	}()

	log.Printf("ingothttp listening on %s (data: %s)", *addr, *dataDir)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
