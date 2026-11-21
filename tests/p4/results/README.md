# P4 live run results

`tests/p4/live-run.sh` writes one folder per run here: `live-<UTC time>/` with `summary.txt` (the evidence), `samples.csv` (every sample), the histogram buckets at the start and end of the window, and the aggregator's satellite log lines. The generated files are ignored by git (see `.gitignore`), copy the ones the report cites. This README is the only tracked file.
