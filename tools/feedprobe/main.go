// Command feedprobe measures how the assistants' satellite feed behaves (issue #308).
//
// It only calls GET /v1/session and GET /v1/stream. The group token is read from
// SATELLITE_GROUP_TOKEN and is never printed, logged or written to a file.
package main

import (
	"fmt"
	"io"
	"os"
)

const usage = `feedprobe: measure the satellite feed (issue #308)

  feedprobe session [--poll 5s --for 60s]
  feedprobe capture --from <offset|latest> --duration <d> --out <file.ndjson>
                    [--disconnect-at <d>] [--pause-at <d> --pause-for <d>]
                    [--last-event-id-only] [--last-event-id <id>] [--once]
                    [--retry-fixed <d>] [--bad-token]
  feedprobe analyze --in <file.ndjson> [--session <session.ndjson>]

The token comes from the SATELLITE_GROUP_TOKEN environment variable.
`

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "feedprobe:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("missing subcommand\n\n%s", usage)
	}

	switch args[0] {
	case "session":
		return runSession(args[1:], stdout)
	case "capture":
		return runCapture(args[1:])
	case "analyze":
		return runAnalyze(args[1:], stdout)
	default:
		return fmt.Errorf("unknown subcommand %q\n\n%s", args[0], usage)
	}
}
