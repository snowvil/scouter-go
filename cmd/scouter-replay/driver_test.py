"""Driver 판정/탐색 계약 회귀 검사: python3 -m unittest discover -s cmd/scouter-replay -p '*_test.py'."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("driver", Path(__file__).resolve().parents[2] / "scripts/bench-xlog-max.py")
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


def report(rate=500):
    return {"pass": True,
            "send": {"scheduled_transactions": rate * 30, "transactions_sent_ok": rate * 30,
                     "schedule_start": "2026-10-17T00:00:00Z", "schedule_end": "2026-10-17T00:00:30Z",
                     "send_finished": "2026-10-17T00:00:30Z"},
            "verification": {"complete": True, "comparison": {"unique_expected_found": rate * 30}},
            "throughput": {"steady_verified_rate_tps": rate},
            "kernel_udp": {"available": True, "loss": 0},
            "udp_accounting": {"server_rejected_datagrams": 0},
            "drain": {"quiesced": True, "wait_ms": 1100},
            "health_samples": [{"at": f"2026-10-17T00:00:{sec:02d}Z", "udp": {"QueueLen": 0}}
                               for sec in range(1, 30)]}


class DriverTest(unittest.TestCase):
    def setUp(self):
        self.args = driver.parse_args(["--output", "/unused", "--warmup", "0",
                                       "--kernel-loss-fails", "--backlog-growth", "0"])

    def test_classifies_independent_failure_causes(self):
        self.assertTrue(driver.classify(report(), 500, self.args)["passed"])
        for path, value, reason in [
            (("kernel_udp", "loss"), 1, "kernel_udp_loss"),
            (("kernel_udp", "available"), False, "kernel_loss_unobservable"),
            (("udp_accounting", "server_rejected_datagrams"), 1, "server_udp_reject"),
            (("drain", "wait_ms"), 2001, "drain_timeout_or_overload"),
            (("throughput", "steady_verified_rate_tps"), 494, "steady_verified_below_99_percent"),
            (("verification", "comparison", "duplicate_records"), 1, "integrity_error"),
            (("send", "transactions_sent_ok"), 14999, "generator_limited_or_send_error"),
        ]:
            r = report()
            parent = r
            for key in path[:-1]:
                parent = parent[key]
            parent[path[-1]] = value
            self.assertIn(reason, driver.classify(r, 500, self.args)["failure_reasons"])

    def test_default_policy_ignores_host_noise(self):
        # Defaults: host-wide netstat drops without missing XLogs and a small
        # queue wobble are recorded, not failures.
        args = driver.parse_args(["--output", "/unused", "--warmup", "0"])
        r = report()
        r["kernel_udp"]["loss"] = 3
        for s in r["health_samples"][-10:]:
            s["udp"]["QueueLen"] = 10
        decision = driver.classify(r, 500, args)
        self.assertTrue(decision["passed"], decision)
        self.assertIn("kernel_udp_loss_host_wide", decision["unknown"])
        for s in r["health_samples"][-10:]:
            s["udp"]["QueueLen"] = 500
        self.assertIn("ingest_backlog_growth", driver.classify(r, 500, args)["failure_reasons"])
        r = report()
        r["kernel_udp"]["loss"] = 3
        r["verification"]["comparison"]["unique_expected_found"] -= 1
        self.assertIn("kernel_udp_loss", driver.classify(r, 500, args)["failure_reasons"])

    def test_backlog_growth_and_unknown_external(self):
        r = report()
        for s in r["health_samples"][-10:]:
            s["udp"]["QueueLen"] = 10
        self.assertIn("ingest_backlog_growth", driver.classify(r, 500, self.args)["failure_reasons"])
        self.args.kind = "external"
        r["udp_accounting"] = None
        decision = driver.classify(r, 500, self.args)
        self.assertTrue(decision["passed"])
        self.assertEqual(decision["unknown"], ["server_udp_reject", "ingest_backlog"])

    def test_exponential_then_binary_search(self):
        observed = []
        def run(cmd, **kwargs):
            rate = int(cmd[cmd.index("--rate") + 1])
            observed.append(rate)
            r = report(rate)
            if rate > 750:
                r["kernel_udp"]["loss"] = 1
            Path(cmd[cmd.index("--report") + 1]).write_text(json.dumps(r))
            return mock.Mock(returncode=0)
        with tempfile.TemporaryDirectory() as root, mock.patch.object(driver.subprocess, "run", run):
            out = Path(root) / "result"
            self.assertEqual(driver.main(["--output", str(out), "--warmup", "0", "--cooldown", "0",
                                          "--max-attempts", "4", "--max-rate", "2000",
                                          "--kernel-loss-fails"]), 0)
            self.assertEqual(observed, [500, 1000, 750, 875])
            summary = json.loads((out / "summary.json").read_text())
            self.assertEqual(summary["best_passing_rate"], 750)
            self.assertEqual(summary["failing_upper_rate"], 875)

    def test_smoke_bound_and_loopback(self):
        a = driver.parse_args(["--output", "/unused", "--smoke", "--max-rate", "999999"])
        self.assertEqual(a.max_rate, 2000)
        self.assertLessEqual(a.duration + a.warmup, 15)
        with self.assertRaises(SystemExit):
            driver.parse_args(["--output", "/unused", "--kind", "external",
                               "--server-udp", "192.0.2.1:6100", "--server-tcp", "127.0.0.1:6100"])


if __name__ == "__main__":
    unittest.main()
