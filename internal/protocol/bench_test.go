package protocol

import "testing"

// Hot-path codec microbenchmarks over Java-generated fixtures. These measure
// codec cost only (no network, queue, dictionary, storage); never convert to TPS.
func BenchmarkDecodePack(b *testing.B) {
	for _, name := range []string{"xlog", "counter", "textpack", "map", "profile", "object"} {
		raw := fixture(b, name)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for b.Loop() {
				if _, err := DecodePack(raw, Limits{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEncodePack(b *testing.B) {
	for _, name := range []string{"xlog", "counter", "textpack", "map", "profile", "object"} {
		raw := fixture(b, name)
		p, err := DecodePack(raw, Limits{})
		if err != nil {
			b.Fatal(err)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for b.Loop() {
				if _, err := EncodePack(p, Limits{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecodeSteps(b *testing.B) {
	raw := fixture(b, "step-sequence")
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := DecodeSteps(raw, Limits{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeSteps(b *testing.B) {
	raw := fixture(b, "step-sequence")
	steps, err := DecodeSteps(raw, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := EncodeSteps(steps, Limits{}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeStepsProfile uses a synthetic Go-encoded sequence shaped like a
// Java Agent profile (mostly METHOD2 with a few SQL3/APICALL2/HASHED_MESSAGE).
// The per-type byte layout is covered by the Java Step fixtures; this only
// measures the allocation pattern of a longer, repetitive profile.
func BenchmarkDecodeStepsProfile(b *testing.B) {
	var steps []Step
	for i := int32(0); i < 64; i++ {
		single := SingleStep{Parent: i - 1, Index: i, StartTime: i * 3, StartCPU: i * 2}
		switch {
		case i%16 == 15:
			steps = append(steps, &APICallStep2{APICallStep: APICallStep{CallStepFields: CallStepFields{SingleStep: single, TxID: int64(i) << 20, Hash: 7000 + i, Elapsed: 12}, Opt: 1, Address: "10.0.0.1:8080"}})
		case i%8 == 7:
			steps = append(steps, &SQLStep3{SQLStep2: SQLStep2{SQLStep: SQLStep{SingleStep: single, Hash: 5000 + i, Elapsed: 4, CPUTime: 1, Param: "1,abc"}, XType: 1}, Updated: 1})
		case i%8 == 3:
			steps = append(steps, &HashedMessageStep{SingleStep: single, Hash: 9000 + i, Time: 2, Value: 1})
		default:
			steps = append(steps, &MethodStep2{MethodStep: MethodStep{SingleStep: single, Hash: 1000 + i, Elapsed: 3, CPUTime: 1}})
		}
	}
	raw, err := EncodeSteps(steps, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := DecodeSteps(raw, Limits{}); err != nil {
			b.Fatal(err)
		}
	}
}
