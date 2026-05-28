"""
NEXUS ANALYZER — RESULTS RETRIEVER
Reads the output files from the volume and prints them to logs.
Deploy this as the worker, let it run, copy from Deploy Logs.
"""
import os

OUTPUT_DIR = "/app/output"

for filename in ["nexus_analysis_report_1min.txt", "nexus_recipes_updated_1min.json"]:
    filepath = os.path.join(OUTPUT_DIR, filename)
    print(f"\n{'='*60}", flush=True)
    print(f"FILE: {filename}", flush=True)
    print('='*60, flush=True)
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            print(f.read(), flush=True)
    else:
        print(f"NOT FOUND at {filepath}", flush=True)

print("\nDone.", flush=True)
