package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/snowvil/scouter-go/internal/console"
	"github.com/snowvil/scouter-go/internal/server"
)

// modeFlags are the Java Main/TermMain run modes: -console runs the
// interactive console (TermMain), otherwise the <pid>.scouter exit file
// loop of the normal mode runs unless -exit-file=false.
type modeFlags struct {
	console, ansi, noansi, exitFile *bool
}

func registerModeFlags() *modeFlags {
	return &modeFlags{
		console:  flag.Bool("console", false, "interactive console on stdin like Java startcon.sh (TermMain); no <pid>.scouter exit file"),
		ansi:     flag.Bool("ansi", false, "console colors on (wins over -noansi; default on except Windows)"),
		noansi:   flag.Bool("noansi", false, "console colors off"),
		exitFile: flag.Bool("exit-file", true, "Java normal mode: create <pid>.scouter in the working directory (else $HOME/<pid>.scouter.run) and shut down gracefully when it is deleted"),
	}
}

// shutdownSignals are the signals main turns into the graceful shutdown,
// the Java shutdown hook triggers: SIGINT, SIGTERM and SIGHUP. SIGQUIT is
// not one of them: like the JVM thread dump it dumps the goroutines and the
// server keeps running (dumpOnSIGQUIT). A SIGHUP that is already ignored
// (nohup) stays ignored; it must be checked before anything subscribes to
// it.
func shutdownSignals() []os.Signal {
	sigs := []os.Signal{os.Interrupt, syscall.SIGTERM}
	if !signal.Ignored(syscall.SIGHUP) {
		sigs = append(sigs, syscall.SIGHUP)
	}
	return sigs
}

// dumpOnSIGQUIT is the JVM SIGQUIT behaviour (kill -3, or Ctrl-\ also in
// the console's raw mode, which keeps ISIG): every goroutine's stack is
// written to w and the server keeps running. Go's default action would dump
// and exit 2, skipping the graceful close (terminal restore, exit file, DB
// lock, server log). main owns the goroutine; stop unsubscribes and waits
// for it.
func dumpOnSIGQUIT(w io.Writer) (stop func()) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGQUIT)
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-sigs:
				log.Print("SIGQUIT: goroutine dump follows on stderr; the server keeps running")
				if err := pprof.Lookup("goroutine").WriteTo(w, 2); err != nil {
					log.Printf("SIGQUIT: goroutine dump: %v", err)
				}
			case <-quit:
				return
			}
		}
	}()
	return func() {
		signal.Stop(sigs)
		close(quit)
		<-done
	}
}

// ansiEnabled is TermMain: on unless Windows, -ansi wins over -noansi.
func ansiEnabled(goos string, ansi, noansi bool) bool {
	switch {
	case ansi:
		return true
	case noansi:
		return false
	}
	return goos != "windows"
}

// start launches the console or the exit file watcher. It returns the
// context runtime.Run must use: it ends only after the console and the
// watcher have stopped, so neither touches the runtime while it closes.
// stop (called after Run returns) shuts them down, waits for them, restores
// the terminal and deletes the exit file. shutdown cancels ctx (the signal
// context): quit and the exit file deletion use it.
// abort is the part of stop a forced exit (shutdown budget, second signal)
// runs instead, without waiting for anything: it restores the terminal and
// deletes the exit file. It may run concurrently with the console.
// A console or an exit file that cannot be set up is returned as an error
// with nothing left running; main then closes the runtime, the profiles and
// the server log and exits 1 like Java System.exit(1).
func (m *modeFlags) start(ctx context.Context, shutdown context.CancelFunc, rt *server.Runtime, loc *time.Location) (runCtx context.Context, stop, abort func(), err error) {
	var workers sync.WaitGroup
	var cleanups []func()
	abort = func() {}
	if *m.console {
		con, err := console.New(console.Config{Source: rt.ConsoleSource(), Out: os.Stdout, Err: os.Stderr, Location: loc,
			ANSI: ansiEnabled(runtime.GOOS, *m.ansi, *m.noansi), Quit: shutdown, Logf: log.Printf})
		if err != nil {
			return nil, nil, nil, fmt.Errorf("console: %w", err)
		}
		in, err := console.OpenInput(os.Stdin, con.Writer())
		if err != nil {
			_ = con.Close()
			return nil, nil, nil, fmt.Errorf("console: stdin: %w", err)
		}
		abort = func() { _ = in.Close() } // Input.Close runs once
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := con.Run(ctx, in); err != nil && ctx.Err() == nil {
				log.Printf("console: %v", err)
			}
			if err := in.Close(); err != nil {
				log.Printf("console: %v", err)
			}
			// After EOF a running repeat command keeps printing until shutdown.
			<-ctx.Done()
			_ = con.Close()
		}()
	} else if *m.exitFile {
		path, err := createExitFile(".", userHome(), os.Getpid())
		if err != nil {
			return nil, nil, nil, fmt.Errorf("exit file: %w", err)
		}
		log.Printf("exit file %s: delete it to stop the server", path)
		workers.Add(1)
		go func() {
			defer workers.Done()
			watchExitFile(ctx, path, time.Second, func(reason string) {
				log.Printf("exit file %s %s; shutting down", path, reason)
				shutdown()
			})
		}()
		cleanups = append(cleanups, func() { removeExitFile(path) })
		abort = func() { _ = os.Remove(path) }
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	coordinated := make(chan struct{})
	go func() {
		defer close(coordinated)
		<-ctx.Done()
		workers.Wait()
		runCancel()
	}()
	return runCtx, func() {
		shutdown()
		<-coordinated
		for _, f := range cleanups {
			f()
		}
	}, abort, nil
}
