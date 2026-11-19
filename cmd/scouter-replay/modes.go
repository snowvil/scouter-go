package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type generatorReport struct {
	TransactionsPerSecond float64 `json:"transactions_sent_per_second"`
	DatagramsPerSecond    float64 `json:"datagrams_sent_per_second"`
	BytesPerSecond        float64 `json:"bytes_sent_per_second"`
	Note                  string  `json:"note"`
}

// runSink uses the same payload, framing, pacing and independent UDP sockets as
// the server run. Increase --rate across runs to find the generator's ceiling.
// Sink receive counts are deliberately not treated as stored transactions.
func runSink(ctx context.Context, o options) (*report, error) {
	sizes, _ := parseProfileSizes(o.ProfileSizes)
	rep := newReport(o, sizes)
	var err error
	rep.WorkDir = o.WorkDir
	if rep.WorkDir == "" {
		rep.WorkDir, err = os.MkdirTemp("", "scouter-sink-")
	} else {
		err = os.MkdirAll(rep.WorkDir, 0700)
	}
	if err != nil {
		return rep, err
	}
	rep.ReportPath = o.Report
	if rep.ReportPath == "" {
		rep.ReportPath = filepath.Join(rep.WorkDir, "report.json")
	}
	before := readKernelUDP()
	defer func() { rep.KernelUDP = kernelDelta(before, readKernelUDP()) }()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(o.Bind)})
	if err != nil {
		return rep, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 65536)
		for {
			if _, _, err := conn.ReadFromUDP(buf); err != nil {
				return
			}
		}
	}()
	defer func() { conn.Close(); <-done }()
	objects := makeObjects(o.Objects)
	cfg := genConfig{Seed: o.Seed, Objects: objects, ProfileRatio: o.ProfileRatio, ProfileSizes: sizes, TextHitRatio: o.TextHitRatio, TextPool: o.TextPool, TextCardinality: o.TextCardinality, ErrorRatio: o.ErrorRatio}
	sched := newSchedule(time.Now().Add(50*time.Millisecond), o.Rate, o.Warmup, o.Duration)
	end := sched.start.Add(o.Warmup + o.Duration)
	failed, sent := newBitset(sched.total), newBitset(sched.total)
	results := make([]*senderResult, o.Senders)
	var wg sync.WaitGroup
	cpu := selfCPU()
	for k := 0; k < o.Senders; k++ {
		owned := []object{}
		for j := k; j < len(objects); j += o.Senders {
			owned = append(owned, objects[j])
		}
		sc := senderConfig{Index: k, Count: o.Senders, Schedule: sched, HardStop: end.Add(o.MaxOverrun), Tick: o.Tick, HeartbeatInterval: o.HeartbeatInterval, CounterInterval: o.CounterInterval, MaxBytes: o.UDPMaxBytes, Batch: o.Batch, SocketBuffer: o.SocketBuffer, Target: conn.LocalAddr().(*net.UDPAddr), Objects: owned, Failed: failed, Sent: sent}
		wg.Add(1)
		go func() { defer wg.Done(); results[k] = runSender(ctx, sc, newGenerator(cfg, k)) }()
	}
	wg.Wait()
	finish := time.Now()
	aggregateSenders(rep, results, cfg)
	rep.Send.Scheduled = sched.total
	rep.Send.StartedAt, rep.Send.ScheduleEnd, rep.Send.FinishedAt = sched.start, end, finish
	rep.Send.SenderCPU = selfCPU().sub(cpu)
	seconds := finish.Sub(sched.start).Seconds()
	rep.GeneratorOnly = &generatorReport{float64(rep.Send.TxSentOK) / seconds, float64(rep.Send.DatagramsOK) / seconds, float64(rep.Send.DatagramBytes) / seconds, "local discard socket; send completion is not server acceptance/storage TPS; target-paced, increase rate to find ceiling"}
	rep.PassCriteria = "all scheduled transactions sent, zero send/generation/socket errors; no storage verification"
	rep.Verify.Method = "not applicable: generator-only sink"
	rep.Throughput.Definition = "no verified server TPS in sink-only mode"
	rep.Pass = rep.Send.TxSentOK == sched.total && rep.Send.TxFailed == 0 && rep.Send.GenerateErrors == 0 && len(rep.Send.SendErrors) == 0 && len(rep.Send.SocketErrors) == 0
	return rep, ctx.Err()
}

type externalSample struct {
	At         time.Time `json:"at"`
	RSSKiB     int64     `json:"rss_kib"`
	CPUPercent float64   `json:"cpu_percent"`
	Error      string    `json:"error,omitempty"`
}

// Caller owns the stop closure and must join before serializing the report.
func startExternalSamples(pid int, interval time.Duration, rep *report) func() {
	if pid <= 0 || interval <= 0 {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			sample := externalSample{At: time.Now()}
			cctx, ccancel := context.WithTimeout(ctx, 2*time.Second)
			out, err := exec.CommandContext(cctx, "ps", "-o", "rss=,%cpu=", "-p", strconv.Itoa(pid)).Output()
			ccancel()
			fields := strings.Fields(string(out))
			if err != nil {
				sample.Error = err.Error()
			} else if len(fields) != 2 {
				sample.Error = "ps did not return RSS and CPU"
			} else {
				sample.RSSKiB, err = strconv.ParseInt(fields[0], 10, 64)
				if err == nil {
					sample.CPUPercent, err = strconv.ParseFloat(fields[1], 64)
				}
				if err != nil {
					sample.Error = fmt.Sprint(err)
				}
			}
			if len(rep.ExternalSamples) < maxRSSSamples {
				rep.ExternalSamples = append(rep.ExternalSamples, sample)
			} else {
				rep.ExternalSamplesDropped++
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() { cancel(); <-done }
}
