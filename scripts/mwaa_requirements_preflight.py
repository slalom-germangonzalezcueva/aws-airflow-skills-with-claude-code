"""Static preflight for requirements/requirements.txt against MWAA constraints."""
import argparse
import re
import sys
import urllib.request

FORBIDDEN = {"apache-airflow", "sqlalchemy", "werkzeug"}
REQ_FILE = "requirements/requirements.txt"


def parse(lines):
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(("-c", "--constraint", "-r")):
            yield line, None, None
            continue
        m = re.match(r"([A-Za-z0-9_.\-]+)\s*(==\s*([^\s;]+))?", line)
        yield line, m.group(1).lower().replace("_", "-"), m.group(3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--airflow-version", default="3.0.6")
    ap.add_argument("--python-version", default="3.12")
    ap.add_argument("--skip-resolver", action="store_true", help="only run static checks")
    args = ap.parse_args()

    errors = []
    reqs = list(parse(open(REQ_FILE).read().splitlines()))
    for line, name, _ in reqs:
        if name is None:
            errors.append(f"directive not supported by deploy pipe: {line}")
        elif name in FORBIDDEN:
            errors.append(f"do not pin {name} on MWAA Airflow {args.airflow_version}")

    url = (f"https://raw.githubusercontent.com/apache/airflow/constraints-{args.airflow_version}"
           f"/constraints-{args.python_version}.txt")
    constraints = {}
    for l in urllib.request.urlopen(url).read().decode().splitlines():
        if "==" in l:
            n, v = l.split("==")
            constraints[n.lower().replace("_", "-")] = v
    for line, name, ver in reqs:
        if name and ver and name in constraints and constraints[name] != ver:
            errors.append(f"{name}=={ver} differs from constraints ({constraints[name]})")

    for e in errors:
        print("ERROR:", e)
    if not args.skip_resolver and not errors:
        print("Resolver check not implemented; run pip install --dry-run with the constraints file.")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
