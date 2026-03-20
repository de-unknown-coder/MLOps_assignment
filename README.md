# MLOps Task – Rolling Signal Pipeline

## Overview

This project is a simple batch processing pipeline built in Python.  
It reads price data, computes a rolling mean, generates a signal, and outputs metrics.

The goal was not just to make it work, but to make it reproducible, deterministic, and production-ready.

---

## What the pipeline does

1. Reads configuration from `config.yaml`
2. Loads dataset from `data.csv`
3. Validates both inputs properly
4. Computes rolling mean on the `close` column
5. Generates a signal:
   - 1 if `close > rolling_mean`
   - 0 otherwise
6. Calculates metrics:
   - total rows processed
   - signal rate
   - runtime (latency)
7. Writes output to `metrics.json`
8. Logs everything in `run.log`

---

## Project Structure
```text
mlops-task/
│
├── run.py
├── config.yaml
├── data.csv
├── requirements.txt
├── Dockerfile
├── metrics.json
├── run.log
└── README.md
```

---

## How to run locally

Make sure Python is installed.

Install dependencies:
```bash
pip install -r requirements.txt
```
Run the pipeline:
```bash
python run.py
```
---

## How to run using Docker

Build the image:
docker build -t mlops-task .


Run the container:
docker run --rm mlops-task

---

## Example Output (metrics.json)
```json
{
"version": "v1",
"rows_processed": 10000,
"metric": "signal_rate",
"value": 0.4989,
"latency_ms": 200,
"seed": 42,
"status": "success"
}
```
---

## Logging

All steps are logged in `run.log`, including:

- job start
- config loading and validation
- dataset loading
- processing steps
- final metrics
- errors (if any)

---

## Error Handling

The pipeline is designed to fail safely.

If something goes wrong (missing file, bad config, invalid dataset):

- it logs the error
- writes a proper error response in `metrics.json`
- exits with a non-zero code

---

## Notes

- The pipeline is deterministic (fixed seed)
- Works fully inside Docker (no hardcoded paths)
- Handles malformed CSV input before processing

---

## Final Thoughts

This was built as a minimal MLOps-style batch job, focusing on:

- clean structure  
- proper validation  
- reproducibility  
- observability  

Not just getting results, but making sure the system behaves reliably.
