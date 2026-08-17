# 01 — Drift baseline, full table

Every benchmark in [01-drift-baseline](01-drift-baseline.md), Criterion median per iteration,
two identical passes on commit `e75118e`. **Drift** is r2 against r1; **MAD** is r1's median
absolute deviation as a percentage of its median, i.e. the dispersion *within* the first pass.

Generated from the saved `r1` and `r2` Criterion baselines; `target/criterion/` itself is not
committed.

| Benchmark | r1 (ns) | r2 (ns) | drift | MAD |
| --- | ---: | ---: | ---: | ---: |
| `body_throughput/hyper/0` | 9,584 | 9,860 | +2.88% | 0.81% |
| `body_throughput/hyper/1024` | 13,718 | 13,960 | +1.76% | 0.59% |
| `body_throughput/hyper/1048576` | 391,563 | 390,803 | -0.19% | 0.72% |
| `body_throughput/hyper/65536` | 37,084 | 36,465 | -1.67% | 1.41% |
| `body_throughput/ngnet-h2/0` | 10,625 | 10,868 | +2.29% | 0.72% |
| `body_throughput/ngnet-h2/1024` | 13,628 | 13,812 | +1.35% | 0.44% |
| `body_throughput/ngnet-h2/1048576` | 507,416 | 510,369 | +0.58% | 0.77% |
| `body_throughput/ngnet-h2/65536` | 38,934 | 39,799 | +2.22% | 0.50% |
| `concurrent_throughput/hyper/1` | 10,442 | 10,357 | -0.82% | 0.52% |
| `concurrent_throughput/hyper/64` | 487,331 | 485,286 | -0.42% | 0.83% |
| `concurrent_throughput/hyper/8` | 61,785 | 61,137 | -1.05% | 0.92% |
| `concurrent_throughput/ngnet-h2/1` | 11,525 | 11,412 | -0.98% | 0.69% |
| `concurrent_throughput/ngnet-h2/64` | 536,103 | 531,788 | -0.80% | 0.57% |
| `concurrent_throughput/ngnet-h2/8` | 63,631 | 62,983 | -1.02% | 0.70% |
| `concurrent_throughput_multi_thread/hyper/1` | 18,226 | 18,569 | +1.88% | 2.24% |
| `concurrent_throughput_multi_thread/hyper/64` | 671,558 | 672,377 | +0.12% | 2.69% |
| `concurrent_throughput_multi_thread/hyper/8` | 99,347 | 100,146 | +0.80% | 1.16% |
| `concurrent_throughput_multi_thread/ngnet-h2/1` | 19,564 | 19,634 | +0.36% | 1.65% |
| `concurrent_throughput_multi_thread/ngnet-h2/64` | 628,687 | 629,342 | +0.10% | 1.43% |
| `concurrent_throughput_multi_thread/ngnet-h2/8` | 91,380 | 91,381 | +0.00% | 1.08% |
| `serial_latency/hyper` | 9,569 | 9,510 | -0.62% | 0.93% |
| `serial_latency/ngnet-h2` | 10,913 | 10,853 | -0.55% | 1.09% |
| `shared_body/hyper-tokio/0` | 9,394 | 9,610 | +2.29% | 0.64% |
| `shared_body/hyper-tokio/1024` | 13,544 | 13,840 | +2.18% | 0.67% |
| `shared_body/hyper-tokio/1048576` | 384,353 | 428,472 | +11.48% | 0.37% |
| `shared_body/hyper-tokio/65536` | 35,731 | 37,132 | +3.92% | 0.40% |
| `shared_body/ngnet-h2-push/0` | 10,403 | 10,438 | +0.33% | 0.68% |
| `shared_body/ngnet-h2-push/1024` | 13,652 | 13,757 | +0.77% | 0.54% |
| `shared_body/ngnet-h2-push/1048576` | 517,416 | 510,445 | -1.35% | 0.39% |
| `shared_body/ngnet-h2-push/65536` | 39,462 | 40,401 | +2.38% | 0.39% |
| `shared_body/ngnet-h2-shared/0` | 10,618 | 10,584 | -0.32% | 0.76% |
| `shared_body/ngnet-h2-shared/1024` | 12,647 | 12,823 | +1.39% | 0.57% |
| `shared_body/ngnet-h2-shared/1048576` | 473,698 | 469,506 | -0.88% | 0.34% |
| `shared_body/ngnet-h2-shared/65536` | 36,241 | 36,792 | +1.52% | 0.71% |
| `transport_body_throughput/hyper-tokio/0` | 20,651 | 21,168 | +2.50% | 0.51% |
| `transport_body_throughput/hyper-tokio/1024` | 30,599 | 31,881 | +4.19% | 0.42% |
| `transport_body_throughput/hyper-tokio/1048576` | 1,197,783 | 1,220,838 | +1.92% | 0.61% |
| `transport_body_throughput/hyper-tokio/65536` | 114,748 | 116,807 | +1.79% | 0.62% |
| `transport_body_throughput/ngnet-h2-compio/0` | 21,539 | 21,590 | +0.24% | 0.52% |
| `transport_body_throughput/ngnet-h2-compio/1024` | 25,510 | 25,987 | +1.87% | 0.66% |
| `transport_body_throughput/ngnet-h2-compio/1048576` | 1,666,727 | 1,676,382 | +0.58% | 0.97% |
| `transport_body_throughput/ngnet-h2-compio/65536` | 111,550 | 111,726 | +0.16% | 0.37% |
| `transport_body_throughput/ngnet-h2-tokio/0` | 20,981 | 21,319 | +1.61% | 0.67% |
| `transport_body_throughput/ngnet-h2-tokio/1024` | 34,493 | 35,043 | +1.59% | 0.31% |
| `transport_body_throughput/ngnet-h2-tokio/1048576` | 1,397,584 | 1,412,133 | +1.04% | 0.47% |
| `transport_body_throughput/ngnet-h2-tokio/65536` | 104,503 | 106,648 | +2.05% | 0.70% |
| `transport_concurrent_throughput/hyper-tokio/1` | 22,515 | 22,470 | -0.20% | 0.83% |
| `transport_concurrent_throughput/hyper-tokio/64` | 506,500 | 497,959 | -1.69% | 1.47% |
| `transport_concurrent_throughput/hyper-tokio/8` | 74,453 | 74,749 | +0.40% | 0.28% |
| `transport_concurrent_throughput/ngnet-h2-compio/1` | 22,916 | 22,852 | -0.28% | 0.67% |
| `transport_concurrent_throughput/ngnet-h2-compio/64` | 529,128 | 531,405 | +0.43% | 0.72% |
| `transport_concurrent_throughput/ngnet-h2-compio/8` | 73,114 | 73,481 | +0.50% | 0.45% |
| `transport_concurrent_throughput/ngnet-h2-tokio/1` | 21,855 | 21,960 | +0.48% | 0.52% |
| `transport_concurrent_throughput/ngnet-h2-tokio/64` | 547,554 | 549,539 | +0.36% | 0.64% |
| `transport_concurrent_throughput/ngnet-h2-tokio/8` | 74,253 | 73,403 | -1.14% | 0.60% |
| `transport_serial_latency/hyper-tokio` | 20,935 | 21,180 | +1.17% | 0.44% |
| `transport_serial_latency/ngnet-h2-compio` | 22,001 | 21,986 | -0.07% | 0.49% |
| `transport_serial_latency/ngnet-h2-tokio` | 21,188 | 21,265 | +0.36% | 0.59% |
| `transport_shared_body/compio-push/0` | 22,266 | 21,989 | -1.25% | 0.98% |
| `transport_shared_body/compio-push/1024` | 26,462 | 26,329 | -0.50% | 0.58% |
| `transport_shared_body/compio-push/1048576` | 1,674,367 | 1,698,875 | +1.46% | 0.73% |
| `transport_shared_body/compio-push/65536` | 115,777 | 116,351 | +0.50% | 0.20% |
| `transport_shared_body/compio-shared/0` | 22,556 | 22,206 | -1.55% | 0.53% |
| `transport_shared_body/compio-shared/1024` | 26,915 | 26,696 | -0.81% | 0.55% |
| `transport_shared_body/compio-shared/1048576` | 1,610,205 | 1,603,475 | -0.42% | 0.48% |
| `transport_shared_body/compio-shared/65536` | 113,935 | 112,201 | -1.52% | 0.94% |
| `transport_shared_body/hyper-tokio/0` | 21,617 | 21,560 | -0.26% | 0.59% |
| `transport_shared_body/hyper-tokio/1024` | 31,775 | 32,323 | +1.72% | 0.48% |
| `transport_shared_body/hyper-tokio/1048576` | 1,191,430 | 1,211,303 | +1.67% | 0.38% |
| `transport_shared_body/hyper-tokio/65536` | 116,413 | 116,877 | +0.40% | 0.81% |
| `transport_shared_body/tokio-push/0` | 21,629 | 21,339 | -1.34% | 0.74% |
| `transport_shared_body/tokio-push/1024` | 35,284 | 35,490 | +0.58% | 0.35% |
| `transport_shared_body/tokio-push/1048576` | 1,398,147 | 1,395,918 | -0.16% | 0.68% |
| `transport_shared_body/tokio-push/65536` | 105,712 | 106,516 | +0.76% | 0.41% |
| `transport_shared_body/tokio-shared/0` | 21,519 | 21,257 | -1.22% | 0.63% |
| `transport_shared_body/tokio-shared/1024` | 24,862 | 25,049 | +0.75% | 0.41% |
| `transport_shared_body/tokio-shared/1048576` | 1,061,451 | 1,058,209 | -0.31% | 0.75% |
| `transport_shared_body/tokio-shared/65536` | 81,378 | 82,113 | +0.90% | 0.53% |

**78 benchmarks.** Median |drift| 0.90%, mean 1.23%, largest 11.48%.
1 exceeded 5%; 11 exceeded 2%.
