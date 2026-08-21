# Manual test guide — redesigned live TUI

Step-by-step verification of every feature of the new `internal/tui` renderer.
Complements `RUNBOOK.md` §2 (whole-app test plan); this file covers the run screen.

## 0. Setup (once)

```bash
./scripts/check.sh -r          # gofmt, vet, all tests under -race, build. No Docker needed.
go build -o khaos ./cmd/khaos
./khaos cluster-up             # needs port 8080 free
```

## 1. The centrepiece, at demo size

Terminal ~120×38:

```bash
./khaos run traffic/high-throughput -d 20 -k
```

Look for:

- Header: `khaos · high-throughput`, a green `●` health dot, a green progress bar
  filling left→right, `elapsed 0mNNs / 0m20s`.
- A rounded-border table (same look as `khaos list`): right-aligned, comma-formatted
  `PRODUCED`/`CONSUMED` in green/yellow bold, `LAG` green (small numbers), `MSG/S`
  settling near 2.0k/s per topic, `BYTES` growing.
- Dim nested group rows: `└─ orders-group-1 ×2` (×2 = consumers in the group).
- TOTAL bar: `N produced · 4.0k/s`, `N consumed · 4.0k/s`, `lag N`, `N.N MB · 1.4 MB/s`,
  and a sparkline appearing after ~3 seconds, flat mid-height at steady rate.
- All rules and panes share one right edge with the table.

## 2. Quit keys

Start a run **without** `-d`; verify each of `q`, `Esc`, `Ctrl-C` stops it promptly with
the plain summary and no error line. Any other key must do nothing. (This is the defect
the Go rewrite exists to fix — an infinite run must always stop cleanly.)

## 3. Responsiveness

During a run, resize:

- ~80×24: padding tightens, MSG/S and BYTES yield before topic names truncate, TOTAL bar
  compacts to `↑ N  ↓ N` arrows, nothing wraps.
- Below ~96 cols with `--lag-poll` on: lag headers compact to `SELF` / `BROKER`.
- Short window (~15 rows) : the topic list cuts with a dim `… N more rows` note
  (counts topics *and* group rows); header, TOTAL and `q quit` never scroll away.
- ~40 cols: still no wrapped lines.

## 4. Failure columns from config (the pop-in fix)

```bash
./khaos run testing/consumer-failures -d 20 -k
```

`FAILED` (magenta) and `DLQ` (blue) columns are present **from the very first frame**,
blank until the first simulated failure (blank-instead-of-zero, as in Python), then fill
in without any column shifting. Group rows show their own dim failed/DLQ counts.

## 5. Real broker lag

```bash
./khaos run traffic/consumer-lag -d 30 -k --lag-poll 2s
```

- Headers become `LAG(SELF)` / `LAG(BROKER)` — both from frame one.
- Broker cells start as dim `unknown` (never `0`), then fill with real per-group numbers.
- Lag above 100 turns red; this scenario grows lag, so watch green flip to red.
- TOTAL bar gains a `broker N` segment summing only measured topics.

## 6. Events pane + incidents

```bash
./khaos run chaos/all-incidents -d 60 -k
```

`EVENTS ────` section: dim timestamps; alerts red, warnings yellow, recoveries green,
info dim. During broker stops the health dot flips to `● degraded`, and errors /
rebalances appear on the TOTAL bar. Long or multi-line event messages stay on one line
(newlines collapse to ` · `, then cell-truncate).

## 7. Flows table

```bash
./khaos run flows/order-flow -d 20 -k
```

Second bordered table `FLOW | STARTED | COMPLETED | MSGS | INFLIGHT | ERRORS`, same
style and width as the topic table; yellow `saturated xN` next to the flow name if the
instance pool saturates; many flows cut with `… N more flows`.

## 8. Hidden-counter issues line

```bash
./khaos run testing/duplicate-messages -d 15 -k
```

Under the TOTAL bar a line appears only when non-zero, e.g. `12 duplicates` (also
produce/consume/commit errors when they occur). Absent on a clean run.

## 9. Headless and piping

```bash
./khaos run traffic/high-throughput -d 10 -k --tui off      # structured logs, one progress line / 10s
./khaos run traffic/high-throughput -d 8 -k | cat           # plain text, ZERO escape codes
NO_COLOR=1 ./khaos list                                     # uncoloured table
```

## 10. One-product check

Run `./khaos list` and `./khaos cluster-status` next to a live run: same rounded border,
same grey frame, same palette (cyan names, green ok, magenta headings) everywhere.

## 11. Shutdown drain

End of any `-d` run: header shows yellow `shutting down`, lag briefly turns red while
consumers drain, then the plain summary prints.

## 12. Demo gif

Already re-recorded at `assets/demo.gif`. To redo:

```bash
cd scripts && PATH=$PWD/..:$PATH vhs demo.tape && mv demo.gif ../assets/
```

(Needs the cluster up; the tape ends with `khaos cluster-down`.)

---

Automated coverage for all of the above: `go test ./internal/tui/ -v` — config gates,
`unknown`, the 100 threshold, blank-on-zero, purity, quit keys, width/height bounds,
multi-line events, flow bounding, header priority, and derived-column stability.
