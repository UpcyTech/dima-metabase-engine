#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import time
import uuid
from pathlib import Path
from typing import Any

import httpx


WAREHOUSE = "Dima Analytics Lab"


class ProbeError(RuntimeError):
    pass


def load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ProbeError(f"{path} must contain a JSON object")
    return value


def login(base: str, email: str, password: str) -> str:
    r = httpx.post(base + "/api/session", json={"username": email, "password": password}, timeout=30)
    r.raise_for_status()
    token = r.json().get("id")
    if not token:
        raise ProbeError("session token missing")
    return str(token)


def parse_stream(response: httpx.Response) -> dict[str, Any]:
    out = {"text": [], "data": [], "tool_calls": [], "tool_results": [], "errors": [], "starts": [], "finishes": []}
    for line in response.iter_lines():
        if not line:
            continue
        prefix, payload = (line.split(":", 1) + [""])[:2] if ":" in line else ("", line)
        try:
            value = json.loads(payload)
        except Exception:
            value = payload
        if prefix == "0" and isinstance(value, str):
            out["text"].append(value)
        elif prefix == "2":
            out["data"].append(value)
        elif prefix == "9":
            out["tool_calls"].append(value)
        elif prefix == "a":
            out["tool_results"].append(value)
        elif prefix == "3":
            out["errors"].append(value)
        elif prefix == "f":
            out["starts"].append(value)
        elif prefix == "d":
            out["finishes"].append(value)
    out["answer_text"] = "".join(out["text"])
    return out


def generated_queries(parsed: dict[str, Any]) -> list[dict[str, Any]]:
    queries = []
    for part in parsed["data"]:
        if isinstance(part, dict) and part.get("type") == "generated_entity":
            value = part.get("value") or {}
            query = (value.get("query") or {}).get("query")
            if isinstance(query, dict):
                queries.append(query)
    return queries


def tool_error_count(parsed: dict[str, Any]) -> int:
    n = 0
    for item in parsed["tool_results"]:
        if not isinstance(item, dict):
            continue
        if item.get("isError") is True or item.get("is_error") is True or item.get("error"):
            n += 1
    return n


def wait_exact_table(client: httpx.Client, table: str, timeout: int = 240) -> dict[str, Any]:
    deadline = time.time() + timeout
    last: Any = None
    while time.time() < deadline:
        r = client.post("/api/agent/v1/search", json={"term_queries": [table], "semantic_queries": []})
        if r.status_code == 200:
            body = r.json()
            data = body.get("data") or []
            exact = [
                x for x in data
                if isinstance(x, dict)
                and str(x.get("name") or "").casefold() == table.casefold()
                and x.get("database_name") == WAREHOUSE
            ]
            if exact:
                return exact[0]
            last = {"total": body.get("total_count"), "names": [x.get("name") for x in data[:8] if isinstance(x, dict)]}
        else:
            last = {"status": r.status_code, "body": r.text[:500]}
        time.sleep(3)
    raise ProbeError(f"catalog table not ready: {table}: {last}")


def runtime_info(base: str, email: str, password: str) -> tuple[httpx.Client, dict[str, Any]]:
    token = login(base, email, password)
    client = httpx.Client(base_url=base, headers={"X-Metabase-Session": token, "Accept": "application/json"}, timeout=60)
    props = client.get("/api/session/properties")
    props.raise_for_status()
    perms = client.get("/api/metabot/permissions/user-permissions")
    perms.raise_for_status()
    current = client.get("/api/user/current")
    current.raise_for_status()
    anchor = wait_exact_table(client, "satis_siparisleri")
    info = {
        "session_version": (props.json() or {}).get("version"),
        "permissions": perms.json(),
        "current_user": current.json(),
        "warehouse": {
            "database_id": anchor.get("database_id"),
            "database_name": anchor.get("database_name"),
        },
    }
    if info["warehouse"]["database_name"] != WAREHOUSE or not isinstance(info["warehouse"]["database_id"], int):
        raise ProbeError(f"wrong warehouse identity: {info['warehouse']}")
    return client, info


def run_case(client: httpx.Client, runtime: dict[str, Any], case: dict[str, Any], anchor_table: str) -> dict[str, Any]:
    anchor = wait_exact_table(client, anchor_table)
    started = time.perf_counter()
    body = {
        "profile_id": "nlq",
        "message": case["question"],
        "context": {},
        "conversation_id": str(uuid.uuid4()),
        "history": None,
        "state": {},
        "debug": False,
    }
    with client.stream("POST", "/api/metabot/agent-streaming", json=body, timeout=240) as response:
        status = response.status_code
        response.raise_for_status()
        parsed = parse_stream(response)
    latency = time.perf_counter() - started
    queries = generated_queries(parsed)
    final_query = queries[-1] if queries else None
    dataset = None
    if final_query is not None:
        rr = client.post("/api/dataset", json=final_query, timeout=180)
        dataset = {"status": rr.status_code}
        if rr.status_code in (200, 202):
            payload = rr.json()
            data = payload.get("data") or {}
            dataset.update({
                "rows": data.get("rows"),
                "cols": data.get("cols"),
                "database_id": payload.get("database_id"),
                "row_count": len(data.get("rows") or []),
            })
        else:
            dataset["body"] = rr.text[:2000]
    return {
        "case_id": case["id"],
        "question": case["question"],
        "anchor": {
            "name": anchor.get("name"),
            "database_name": anchor.get("database_name"),
            "database_id": anchor.get("database_id"),
            "table_id": anchor.get("id"),
        },
        "status_code": status,
        "latency_seconds": round(latency, 6),
        "answer_text": parsed["answer_text"],
        "stream_errors": parsed["errors"],
        "tool_error_count": tool_error_count(parsed),
        "tool_call_count": len(parsed["tool_calls"]),
        "generated_query_count": len(queries),
        "generated_query": final_query,
        "dataset": dataset,
        "warehouse_database_id": runtime["warehouse"]["database_id"],
    }


def numeric(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def value_equal(a: Any, b: Any, tol: float) -> bool:
    if numeric(a) and numeric(b):
        return math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=tol)
    return a == b


def canonical_pair(row: list[Any]) -> list[Any]:
    if len(row) == 2:
        nums = [v for v in row if numeric(v)]
        texts = [v for v in row if not numeric(v)]
        if len(nums) == 1 and len(texts) == 1:
            return [texts[0], nums[0]]
    return row


def row_equal(a: list[Any], b: list[Any], tol: float) -> bool:
    aa, bb = canonical_pair(a), canonical_pair(b)
    return len(aa) == len(bb) and all(value_equal(x, y, tol) for x, y in zip(aa, bb))


def unordered_equal(observed: list[list[Any]], expected: list[list[Any]], tol: float) -> bool:
    remaining = list(observed)
    for erow in expected:
        match = next((i for i, orow in enumerate(remaining) if row_equal(orow, erow, tol)), None)
        if match is None:
            return False
        remaining.pop(match)
    return not remaining


def score_case(run: dict[str, Any], oracle: dict[str, Any]) -> dict[str, Any]:
    comparison = oracle.get("comparison")
    tol = float(oracle.get("numeric_tolerance") or 0)
    reasons: list[str] = []
    scope_drift = False

    if run.get("status_code") != 202:
        reasons.append(f"agent_status={run.get('status_code')}")
    if run.get("stream_errors"):
        reasons.append("stream_errors")
    if run.get("tool_error_count"):
        reasons.append(f"tool_errors={run['tool_error_count']}")
    query = run.get("generated_query")
    if not isinstance(query, dict):
        reasons.append("no_generated_query")
    else:
        if query.get("database") != run.get("warehouse_database_id"):
            reasons.append("query_database_scope_drift")
            scope_drift = True

    dataset = run.get("dataset")
    observed = dataset.get("rows") if isinstance(dataset, dict) else None
    expected = oracle.get("rows")
    if not isinstance(observed, list):
        reasons.append("no_executed_rows")
        correct = False
    elif comparison == "scalar":
        correct = (
            isinstance(expected, list)
            and len(expected) == 1 and len(expected[0]) == 1
            and len(observed) == 1 and isinstance(observed[0], list) and len(observed[0]) == 1
            and value_equal(observed[0][0], expected[0][0], tol)
        )
    elif comparison == "ordered_rows":
        correct = (
            isinstance(expected, list)
            and len(observed) == len(expected)
            and all(row_equal(o, e, tol) for o, e in zip(observed, expected))
        )
    elif comparison == "unordered_rows":
        correct = isinstance(expected, list) and unordered_equal(observed, expected, tol)
    else:
        correct = False
        reasons.append(f"unsupported_comparison={comparison}")

    if not correct:
        reasons.append("oracle_mismatch")

    silent_wrong = bool(
        not correct
        and isinstance(query, dict)
        and not run.get("stream_errors")
        and not run.get("tool_error_count")
        and bool(run.get("answer_text") or observed is not None)
    )

    return {
        "pass": not reasons,
        "correct": bool(correct),
        "silent_wrong": silent_wrong,
        "dataset_scope_drift": scope_drift,
        "reasons": reasons,
        "observed_rows": observed,
        "expected_rows": expected,
    }


def select_case_maps(corpus: dict[str, Any], oracle: dict[str, Any], selection: dict[str, Any]):
    cases = {x["id"]: x for x in corpus.get("cases") or []}
    oracles = {x["id"]: x for x in oracle.get("results") or []}
    selected = []
    for item in selection.get("cases") or []:
        cid = item["id"]
        if cid not in cases or cid not in oracles:
            raise ProbeError(f"selection case missing from corpus/oracle: {cid}")
        if oracles[cid].get("kind") != "query":
            raise ProbeError(f"C1 only accepts executable query oracle: {cid}")
        selected.append((cases[cid], oracles[cid], item["anchor_table"]))
    return selected


def run_runtime(base: str, email: str, password: str, selected) -> tuple[dict[str, Any], dict[str, Any]]:
    client, info = runtime_info(base, email, password)
    try:
        results = {}
        for case, _oracle, anchor in selected:
            results[case["id"]] = run_case(client, info, case, anchor)
        return info, results
    finally:
        client.close()


def repeat_case(base: str, email: str, password: str, case, oracle, anchor: str, count: int = 2):
    client, info = runtime_info(base, email, password)
    attempts = []
    try:
        for _ in range(count):
            run = run_case(client, info, case, anchor)
            attempts.append({"run": run, "score": score_case(run, oracle)})
    finally:
        client.close()
    return attempts


def stable_pass(initial: bool, repeats: list[dict[str, Any]]) -> tuple[bool | None, list[bool]]:
    outcomes = [initial] + [bool(x["score"]["pass"]) for x in repeats]
    if all(x is True for x in outcomes):
        return True, outcomes
    if all(x is False for x in outcomes):
        return False, outcomes
    return None, outcomes


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stock-url", required=True)
    ap.add_argument("--fork-url", required=True)
    ap.add_argument("--email", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--corpus", type=Path, required=True)
    ap.add_argument("--oracle", type=Path, required=True)
    ap.add_argument("--selection", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()

    corpus, oracle, selection = load(args.corpus), load(args.oracle), load(args.selection)
    expected_fp = selection["corpus_fingerprint"]
    if oracle.get("corpus_fingerprint") != expected_fp:
        raise SystemExit("oracle/corpus fingerprint drift")
    selected = select_case_maps(corpus, oracle, selection)

    stock_info, stock_runs = run_runtime(args.stock_url, args.email, args.password, selected)
    fork_info, fork_runs = run_runtime(args.fork_url, args.email, args.password, selected)

    stock_scores = {}
    fork_scores = {}
    by_id = {case["id"]: (case, o, anchor) for case, o, anchor in selected}
    divergent = []
    for cid, (_case, o, _anchor) in by_id.items():
        stock_scores[cid] = score_case(stock_runs[cid], o)
        fork_scores[cid] = score_case(fork_runs[cid], o)
        if stock_scores[cid]["pass"] != fork_scores[cid]["pass"]:
            divergent.append(cid)

    repeats: dict[str, Any] = {}
    final_stock: dict[str, bool | None] = {}
    final_fork: dict[str, bool | None] = {}
    stochastic = []
    for cid in by_id:
        if cid not in divergent:
            final_stock[cid] = bool(stock_scores[cid]["pass"])
            final_fork[cid] = bool(fork_scores[cid]["pass"])
            continue
        case, o, anchor = by_id[cid]
        sr = repeat_case(args.stock_url, args.email, args.password, case, o, anchor, 2)
        fr = repeat_case(args.fork_url, args.email, args.password, case, o, anchor, 2)
        repeats[cid] = {"stock": sr, "fork": fr}
        sp, so = stable_pass(bool(stock_scores[cid]["pass"]), sr)
        fp, fo = stable_pass(bool(fork_scores[cid]["pass"]), fr)
        final_stock[cid], final_fork[cid] = sp, fp
        if sp is None or fp is None:
            stochastic.append({"case_id": cid, "stock_outcomes": so, "fork_outcomes": fo})

    stock_pass = [cid for cid, v in final_stock.items() if v is True]
    retained = [cid for cid in stock_pass if final_fork.get(cid) is True]
    retention = (len(retained) / len(stock_pass)) if stock_pass else None

    permission_regression = (
        stock_info.get("permissions") != fork_info.get("permissions")
        or bool((stock_info.get("current_user") or {}).get("is_superuser"))
        or bool((fork_info.get("current_user") or {}).get("is_superuser"))
    )
    fork_scope_drifts = [cid for cid, x in fork_scores.items() if x["dataset_scope_drift"]]
    new_silent_wrong = [
        cid for cid in stock_pass
        if final_fork.get(cid) is False and fork_scores[cid].get("silent_wrong")
    ]
    stable_regressions = [
        cid for cid in stock_pass
        if final_fork.get(cid) is False
    ]

    gate = (
        bool(stock_pass)
        and retention == 1.0
        and not new_silent_wrong
        and not permission_regression
        and not fork_scope_drifts
        and not stochastic
    )

    report = {
        "schema_version": "dima_engine_c1_parity_v1",
        "corpus_fingerprint": expected_fp,
        "selected_cases": list(by_id),
        "stock_runtime": stock_info,
        "fork_runtime": fork_info,
        "initial": {
            "stock": {cid: {"run": stock_runs[cid], "score": stock_scores[cid]} for cid in by_id},
            "fork": {cid: {"run": fork_runs[cid], "score": fork_scores[cid]} for cid in by_id},
        },
        "divergent_cases": divergent,
        "repeats": repeats,
        "final_stock_pass": stock_pass,
        "final_fork_retained_stock_pass": retained,
        "fork_capability_retention": retention,
        "new_silent_wrong": new_silent_wrong,
        "permission_regression": permission_regression,
        "dataset_scope_drift": fork_scope_drifts,
        "stochastic_ambiguous": stochastic,
        "stable_regressions": stable_regressions,
        "gate": "GREEN" if gate else "RED",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({
        "gate": report["gate"],
        "stock_pass": stock_pass,
        "retained": retained,
        "fork_capability_retention": retention,
        "divergent_cases": divergent,
        "stochastic_ambiguous": stochastic,
        "new_silent_wrong": new_silent_wrong,
        "permission_regression": permission_regression,
        "dataset_scope_drift": fork_scope_drifts,
    }, ensure_ascii=False, indent=2))
    return 0 if gate else 1


if __name__ == "__main__":
    raise SystemExit(main())
