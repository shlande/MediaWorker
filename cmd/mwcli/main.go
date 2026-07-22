// mwcli is the MediaWorker command-line client for uploading and downloading
// content via the ingest-worker API and the embedded edge-node distribution layer.
//
// Subcommands:
//
//	upload   — stream a file to an ingest-worker with single-pass SHA-256 hashing
//	download — retrieve a blob via embedded edge node
//
// Usage:
//
//	mwcli upload  -addr <url> -type <image|dash_video> -file <path> [-content-id <id>] [-metadata <json>]
//	mwcli download -config <yaml> -blob <sha256:hex> -out <path> [-wait-timeout 60s] [-req-timeout 120s]
//
// Exit codes: 0 = success, 1 = runtime/API error, 2 = usage error, 3 = hash mismatch.
package main

import (
	"fmt"
	"io"
	"os"
)

const usageText = `mwcli — MediaWorker content upload/download client

Usage:
  mwcli <command> [flags]

Commands:
  upload    Upload a file to an ingest-worker
  download  Download a blob via embedded edge node

Run 'mwcli <command> -h' for command-specific help.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches the subcommand given in args[0] and returns an exit code.
// Factored for testability: callers supply their own stdout/stderr writers.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageText)
		return 2
	}

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "upload":
		return runUpload(cmdArgs, stdout, stderr)
	case "download":
		return runDownload(cmdArgs, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", cmd, usageText)
		return 2
	}
}
