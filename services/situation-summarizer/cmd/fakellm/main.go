// Command fakellm serves the fake LLM endpoint: MODE is ok, slow, invalid-json or dead. For tests and tests/bonus/ai-isolation.sh only.
package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/brii26/distributed-hazard-coordination/services/situation-summarizer/fakellm"
)

func main() {
	mode := os.Getenv("MODE")
	if mode == "" {
		mode = fakellm.ModeOK
	}

	if !fakellm.Valid(mode) {
		log.Fatalf("MODE must be ok, slow, invalid-json or dead, got %q", mode)
	}

	addr := ":" + func() string {
		if p := os.Getenv("PORT"); p != "" {
			return p
		}

		return "9000"
	}()

	log.Printf("fakellm mode=%s listening on %s", mode, addr)

	srv := &http.Server{Addr: addr, Handler: fakellm.Handler(mode, fakellm.DefaultSleep), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
