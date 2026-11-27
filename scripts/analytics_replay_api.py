#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0.
"""Serve offline analytics replay reports over a read-only loopback API."""

import argparse
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re


RUN_ID = re.compile(r"[0-9a-f]{32}")


def report_at(directory, run_id):
    if not RUN_ID.fullmatch(run_id):
        return None
    path = directory / (run_id + ".json")
    if path.is_symlink() or not path.is_file():
        return None
    try:
        report = json.loads(path.read_text())
        if (report["id"] == run_id and report["format"] == 1
            and all(key in report for key in ("created_at", "dataset", "candidates"))):
            return report
    except (OSError, KeyError, TypeError, ValueError):
        pass
    return None


def reports_in(directory):
    for file in directory.glob("*.json"):
        report = report_at(directory, file.stem)
        if report is not None:
            yield report


def latest_recommendations(directory):
    latest, newest = None, None
    for report in reports_in(directory):
        advisor = report.get("advisor")
        if not isinstance(advisor, dict):
            continue
        try:
            timestamp = datetime.fromisoformat(advisor.get("last_run_at", report["created_at"]))
            if timestamp.utcoffset() is None:
                continue
        except (TypeError, ValueError):
            continue
        key = (timestamp, report["id"])
        if newest is None or key > newest:
            latest, newest = report, key
    if latest is None:
        return {"last_run_at": None, "run_id": None, "recommendations": []}
    return {
        "last_run_at": newest[0].isoformat(),
        "run_id": latest["id"],
        "dataset": latest["dataset"],
        "advisor": latest["advisor"],
        "recommendations": [
            {key: c[key] for key in ("id", "strategy", "status", "advice", "payoff", "view") if key in c}
            for c in latest["candidates"]
        ],
    }


def handler(directory):
    class Reports(BaseHTTPRequestHandler):
        def reply(self, code, value):
            body = json.dumps(value).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            port = self.server.server_port
            if self.headers.get("Host") not in (f"127.0.0.1:{port}", f"localhost:{port}"):
                return self.reply(400, {"error": "invalid_host"})
            path = self.path.split("?")[0].strip("/").split("/")
            if path == ["v1", "recommendations"]:
                return self.reply(200, latest_recommendations(directory))
            if path == ["v1", "runs"]:
                summaries = [{key: report[key] for key in ("id", "created_at", "dataset")}
                             for report in reports_in(directory)]
                summaries.sort(key=lambda item: item["created_at"], reverse=True)
                return self.reply(200, {"runs": summaries})
            if path[:2] == ["v1", "runs"] and len(path) in (3, 5):
                report = report_at(directory, path[2])
                if report and len(path) == 3:
                    return self.reply(200, report)
                if report and len(path) == 5 and path[3] == "candidates":
                    for candidate in report["candidates"]:
                        if candidate["id"] == path[4]:
                            return self.reply(200, candidate)
            return self.reply(404, {"error": "not_found"})

    return Reports


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", required=True, type=Path)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    with ThreadingHTTPServer(("127.0.0.1", args.port), handler(args.reports.resolve())) as server:
        print(f"Analytics replay API listening on http://127.0.0.1:{server.server_port}", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
