// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0

// Command scouter-db inspects, migrates, verifies and rolls back Scouter file
// DBs. Migration is a verified byte copy; the source DB is never written.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/snowvil/scouter-go/internal/storage/migrate"
)

const usage = `usage: scouter-db <command> [flags]

commands:
  inspect   --db DIR                       read-only DB check and JSON report
  migrate   --src DIR --dst DIR [--dry-run] [--fresh] [--accept-recoverable]
                                           verified copy via DIR.partial, atomic rename
  migrate   --precopy --src DIR --dst DIR [--dry-run] [--fresh]
                                           copy while Java still runs (repeatable);
                                           state in DST.precopy.json, dst not created
  migrate   --finish --src DIR --dst DIR [--dry-run] [--accept-recoverable]
            [--rehash-staging]             after Java stopped: copy and verify only
                                           what changed since the precopy, then rename
  verify    --src DIR --dst DIR [--manifest FILE]
                                           src unchanged, dst checksums and inspection
  rollback  --dst DIR [--manifest FILE]    prove src intact, rename dst aside

common flags:
  --timezone ZONE   Java JVM timezone of the DB (default Asia/Seoul)
  --report FILE     write the JSON result to FILE (default stdout); never inside a DB
  --now SECONDS     Unix seconds for KV TTL state (default: current time)

migrate flags:
  --dry-run             inspect and plan only; write nothing
  --fresh               discard DST.partial staging and start over
  --accept-recoverable  allow src errors a Go writer repairs (torn index tail,
                        gzip count); the files are copied unchanged
  --java-stopped        confirm the Java process exited although lock.dat is
                        younger than 60s
  --rehash-staging      --finish: re-hash reused staging copies instead of
                        trusting their size, time and inode
  --racy-window DUR     --precopy/--finish: files modified this recently are
                        always copied again (default 60s)
  --sample N            keyed query lookups compared per date and category
                        (default 1000)
  --query-limit N       records compared per date per timed category (0 = all)

examples:
  scouter-db inspect --db /backup/scouter-copy --timezone Asia/Seoul --report inspect.json
  scouter-db migrate --src /data/scouter/database --dst /data/scouter-go/database --dry-run
  scouter-db migrate --src /data/scouter/database --dst /data/scouter-go/database --report migrate.json
  # incremental: precopy (repeat while Java runs), stop Java, finish
  scouter-db migrate --precopy --src /data/scouter/database --dst /data/scouter-go/database --report precopy.json
  scouter-db migrate --finish  --src /data/scouter/database --dst /data/scouter-go/database --report migrate.json
  scouter-db verify  --src /data/scouter/database --dst /data/scouter-go/database
  scouter-db rollback --dst /data/scouter-go/database --report rollback.json

exit status: 0 success, 1 problems found or operation refused/failed, 2 usage error.
Stop the Java server (lock.dat older than 5s; migrate/--finish wait 60s or
--java-stopped, below) and any Go writer before migrate/rollback.
migrate --precopy only refuses a live Go writer; --finish requires both stopped.
Before migrate/--finish confirm the Java process has exited (PID gone): Java
flushes gzip blocks, indexes and KV after its lock.dat heartbeat stops and
deletes lock.dat on a normal exit. A lock.dat younger than 60s is refused
unless --java-stopped confirms the exit. Stop any repeating --precopy job
before --finish; one migrate/rollback per DST runs at a time (DST.migrate.lock).
--timezone, --now, --sample and --query-limit must match between precopy and finish.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

type common struct {
	timezone, report string
	now              int64
	location         *time.Location
}

func (c *common) register(fs *flag.FlagSet) {
	fs.StringVar(&c.timezone, "timezone", "Asia/Seoul", "Java JVM timezone of the DB")
	fs.StringVar(&c.report, "report", "", "JSON result file (default stdout)")
	fs.Int64Var(&c.now, "now", 0, "Unix seconds for KV TTL state (default current time)")
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprint(stderr, usage)
		if len(args) == 0 {
			return 2
		}
		return 0
	}
	cmd := args[0]
	fs := flag.NewFlagSet("scouter-db "+cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	var c common
	c.register(fs)
	var db, src, dst, manifest string
	var dryRun, fresh, acceptRecoverable, precopy, finish, javaStopped, rehashStaging bool
	var racyWindow time.Duration
	var queryLimit int64
	var sample int
	switch cmd {
	case "inspect":
		fs.StringVar(&db, "db", "", "DB root to inspect")
	case "migrate":
		fs.StringVar(&src, "src", "", "source (Java) DB root; never written")
		fs.StringVar(&dst, "dst", "", "new destination DB root; absent or empty")
		fs.BoolVar(&dryRun, "dry-run", false, "inspect and plan only; write nothing")
		fs.BoolVar(&fresh, "fresh", false, "discard DST.partial staging and restart")
		fs.BoolVar(&acceptRecoverable, "accept-recoverable", false, "allow src errors a Go writer repairs (torn index tail, gzip count); copied unchanged")
		fs.BoolVar(&precopy, "precopy", false, "copy into DST.partial while Java runs; repeat to shrink the finish")
		fs.BoolVar(&finish, "finish", false, "after Java stopped, copy and verify the changes since --precopy and rename")
		fs.BoolVar(&javaStopped, "java-stopped", false, "confirm the Java process exited although lock.dat is younger than 60s")
		fs.BoolVar(&rehashStaging, "rehash-staging", false, "--finish: re-hash reused staging copies instead of trusting their stat")
		fs.DurationVar(&racyWindow, "racy-window", 0, "precopy/finish: files modified this recently are always copied again (default 60s)")
		fs.Int64Var(&queryLimit, "query-limit", 0, "records per date per timed category to compare (0 = all)")
		fs.IntVar(&sample, "sample", 1000, "keyed query lookups per date and category")
	case "verify":
		fs.StringVar(&src, "src", "", "source DB root (default from manifest)")
		fs.StringVar(&dst, "dst", "", "migrated DB root")
		fs.StringVar(&manifest, "manifest", "", "manifest file (default DST.manifest.json)")
	case "rollback":
		fs.StringVar(&dst, "dst", "", "migrated DB root to rename aside")
		fs.StringVar(&manifest, "manifest", "", "manifest file (default DST.manifest.json)")
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", cmd, usage)
		return 2
	}
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected arguments %v\n", fs.Args())
		return 2
	}
	switch {
	case precopy && finish:
		fmt.Fprintln(stderr, "--precopy and --finish are separate steps")
		return 2
	case precopy && acceptRecoverable:
		fmt.Fprintln(stderr, "--accept-recoverable applies to --finish")
		return 2
	case finish && fresh:
		fmt.Fprintln(stderr, "--fresh discards a precopy; use it with --precopy")
		return 2
	case rehashStaging && !finish:
		fmt.Fprintln(stderr, "--rehash-staging applies to --finish")
		return 2
	case racyWindow < 0:
		fmt.Fprintln(stderr, "--racy-window must be nonnegative")
		return 2
	}
	var err error
	if c.location, err = time.LoadLocation(c.timezone); err != nil || c.timezone == "" {
		fmt.Fprintf(stderr, "invalid --timezone %q: %v\n", c.timezone, err)
		return 2
	}
	if c.now < 0 {
		fmt.Fprintln(stderr, "--now must be nonnegative")
		return 2
	}
	required := map[string][]string{"inspect": {db}, "migrate": {src, dst}, "verify": {dst}, "rollback": {dst}}
	for _, v := range required[cmd] {
		if v == "" {
			fmt.Fprintf(stderr, "missing required path flag\n\n%s", usage)
			return 2
		}
	}
	if c.report != "" {
		roots := []string{db, src, dst}
		if cmd == "migrate" && dst != "" {
			roots = append(roots, migrate.StagingPath(dst))
		}
		if cmd == "verify" || cmd == "rollback" {
			if _, m, err := migrate.LoadManifest(dst, manifest); err == nil {
				roots = append(roots, m.Src)
			}
		}
		for _, root := range roots {
			if root == "" {
				continue
			}
			if err := migrate.CheckOutsideRoot(root, c.report); err != nil {
				fmt.Fprintln(stderr, err)
				return 2
			}
		}
	}
	var result any
	failed := false
	switch cmd {
	case "inspect":
		var rep *migrate.Report
		rep, err = migrate.Inspect(ctx, migrate.InspectOptions{Root: db, Location: c.location, Now: c.now})
		if rep != nil {
			result, failed = rep, rep.HasErrors()
			fmt.Fprintf(stderr, "inspect: %d files, %d bytes, errors=%d warnings=%d infos=%d statuses=%v\n", rep.Summary.Files, rep.Summary.Bytes, rep.Summary.Errors, rep.Summary.Warnings, rep.Summary.Infos, rep.Summary.Statuses)
		}
	case "migrate":
		opt := migrate.MigrateOptions{Src: src, Dst: dst, Location: c.location, Now: c.now, DryRun: dryRun, Fresh: fresh, AcceptRecoverable: acceptRecoverable, QueryLimit: queryLimit, Sample: sample, Progress: stderr,
			JavaStopped: javaStopped, RacyWindow: racyWindow, RehashStaging: rehashStaging}
		if precopy {
			var st *migrate.PrecopyState
			st, err = migrate.Precopy(ctx, opt)
			if st != nil {
				res := precopyResult{Status: st.Status, Rounds: st.Rounds, Error: st.Error, Round: st.Last}
				if !dryRun {
					res.State = migrate.PrecopyStatePath(st.Dst)
				}
				result = res
				if st.Last != nil {
					fmt.Fprintf(stderr, "precopy: status=%s rounds=%d java_active=%v files=%d copied=%d bytes=%d reused=%d racy=%d removed=%d groups inspected=%d reused=%d pending=%d\n", st.Status, st.Rounds, st.Last.JavaActive, st.Last.Files, st.Last.CopiedFiles, st.Last.CopiedBytes, st.Last.ReusedFiles, st.Last.RacyFiles, st.Last.RemovedFiles, st.Last.InspectedGroups, st.Last.ReusedGroups, st.Last.PendingGroups)
				}
			}
			break
		}
		run := migrate.Migrate
		if finish {
			run = migrate.Finish
		}
		var m *migrate.Manifest
		m, err = run(ctx, opt)
		var me *migrate.MigrateError
		if errors.As(err, &me) && me.Report != nil {
			result = struct {
				Manifest *migrate.Manifest `json:"manifest"`
				Report   *migrate.Report   `json:"src_report"`
			}{me.Manifest, me.Report}
		} else if m != nil {
			result = m
		}
		if m != nil {
			fmt.Fprintf(stderr, "migrate: status=%s files=%d bytes=%d copy=%d reused=%d free=%d required=%d accepted=%d\n", m.Status, len(m.Files), m.TotalBytes, m.CopyBytes, m.ReusedFiles, m.FreeBytes, m.RequiredBytes, len(m.AcceptedProblems))
			if f := m.Incremental; f != nil {
				fmt.Fprintf(stderr, "finish: copied=%d files %d bytes, reused=%d files, groups inspected=%d reused=%d, src read=%d bytes, %d ms\n", f.Finish.CopiedFiles, f.Finish.CopiedBytes, f.Finish.ReusedFiles, f.Finish.InspectedGroups, f.Finish.ReusedGroups, f.Finish.SrcReadBytes, f.Finish.TotalMillis)
			}
		}
	case "verify":
		var v *migrate.VerifyResult
		v, err = migrate.Verify(ctx, migrate.VerifyOptions{Src: src, Dst: dst, Manifest: manifest, Location: c.location, Now: c.now})
		if v != nil {
			result, failed = v, v.Failed()
			fmt.Fprintf(stderr, "verify: status=%s post-migration modified=%d added=%d removed=%d\n", v.Status, len(v.PostMigration.Modified), len(v.PostMigration.Added), len(v.PostMigration.Removed))
		}
	case "rollback":
		var r *migrate.RollbackResult
		r, err = migrate.Rollback(ctx, migrate.RollbackOptions{Dst: dst, Manifest: manifest})
		if r != nil {
			result = r
			fmt.Fprintf(stderr, "rollback: status=%s renamed_to=%s\n", r.Status, r.RenamedTo)
		}
	}
	if result != nil {
		if werr := writeResult(c.report, stdout, result); werr != nil {
			fmt.Fprintln(stderr, "write result:", werr)
			return 1
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", cmd, err)
		return 1
	}
	if failed {
		return 1
	}
	return 0
}

// precopyResult is the CLI view of a precopy round; the full state (with
// per-file records and cached group results) stays in DST.precopy.json.
type precopyResult struct {
	Status string              `json:"status"`
	Rounds int                 `json:"rounds"`
	State  string              `json:"state,omitempty"`
	Error  string              `json:"error,omitempty"`
	Round  *migrate.RoundStats `json:"round"`
}

func writeResult(path string, stdout io.Writer, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if path == "" {
		_, err = stdout.Write(b)
		return err
	}
	return os.WriteFile(path, b, 0600)
}
