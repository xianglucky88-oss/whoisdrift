
import json
import sys

from . import pipeline


def _parse(argv):
    mode = "full"
    if "--incremental" in argv:
        mode = "incremental"
    if "--full" in argv:
        mode = "full"
    opts = {"mode": mode, "explain": None, "state_at": None, "basis": "event",
            "reconcile": False}
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("--explain", "--show") and i + 1 < len(argv):
            opts["explain"] = argv[i + 1]
            i += 2
            continue
        if a == "--state-at" and i + 1 < len(argv):
            opts["state_at"] = argv[i + 1]
            i += 2
            continue
        if a == "--domain" and i + 1 < len(argv):
            opts["domain"] = argv[i + 1]
            i += 2
            continue
        if a == "--basis" and i + 1 < len(argv):
            opts["basis"] = argv[i + 1]
            i += 2
            continue
        if a == "--reconcile":
            opts["reconcile"] = True
        i += 1
    return opts


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    opts = _parse(argv)

    if opts["state_at"]:
        domain = opts.get("domain")
        if not domain:
            raise SystemExit("--state-at requires --domain <id>")
        out = pipeline.state_at(domain, opts["state_at"], basis=opts["basis"])
    elif opts["explain"]:
        out = pipeline.explain(opts["explain"])
    elif opts["reconcile"]:
        out = pipeline.reconcile()
    else:
        out = pipeline.run(mode=opts["mode"])
    print(json.dumps(out, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
