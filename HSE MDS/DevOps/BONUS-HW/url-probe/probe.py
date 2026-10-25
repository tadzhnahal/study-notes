"""url-probe: проверяет, отвечают ли URL, и пишет результат в формате Prometheus.

Запуск: python probe.py <файл со списком URL>

Переменные окружения:
  PROBE_TIMEOUT     таймаут одной проверки в секундах (по умолчанию 5)
  PROBE_OUTPUT_DIR  каталог, куда записать url_probe.prom; если не задан, метрики печатаются в stdout

Код возврата: 0 - все URL живы, 1 - хотя бы один не отвечает, 2 - ошибка запуска.
"""

import os
import sys
import tempfile
import time
import urllib.error
import urllib.request

OUTPUT_FILE = "url_probe.prom"

METRICS = [
    ("probe_success", "1, если получен ответ с кодом 2xx/3xx, иначе 0"),
    ("probe_http_status_code", "Код ответа HTTP, 0 - ответа нет"),
    ("probe_duration_seconds", "Сколько заняла проверка"),
]


def load_urls(path):
    with open(path, encoding="utf-8") as f:
        lines = (line.strip() for line in f)
        return [line for line in lines if line and not line.startswith("#")]


def check(url, timeout):
    request = urllib.request.Request(url, headers={"User-Agent": "url-probe/1.0"})
    start = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            code = response.status
    except urllib.error.HTTPError as e:
        code = e.code
    except (OSError, ValueError) as e:
        print(f"{url}: {e}", file=sys.stderr)
        code = 0
    duration = time.monotonic() - start
    return {
        "probe_success": int(200 <= code < 400),
        "probe_http_status_code": code,
        "probe_duration_seconds": round(duration, 3),
    }


def escape(value):
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render(results):
    lines = []
    for name, help_text in METRICS:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        for url, values in results.items():
            lines.append(f'{name}{{url="{escape(url)}"}} {values[name]}')
    lines.append("# HELP probe_last_run_timestamp_seconds Время последнего запуска")
    lines.append("# TYPE probe_last_run_timestamp_seconds gauge")
    lines.append(f"probe_last_run_timestamp_seconds {time.time():.0f}")
    return "\n".join(lines) + "\n"


def write_atomically(directory, text):
    # пишем во временный файл рядом и подменяем целиком: читатель не увидит файл недописанным
    fd, tmp_path = tempfile.mkstemp(dir=directory, suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    os.chmod(tmp_path, 0o644)
    os.replace(tmp_path, os.path.join(directory, OUTPUT_FILE))


def main():
    if len(sys.argv) != 2:
        print("usage: probe.py <urls-file>", file=sys.stderr)
        return 2
    try:
        urls = load_urls(sys.argv[1])
    except OSError as e:
        print(f"cannot read urls file: {e}", file=sys.stderr)
        return 2
    timeout = float(os.environ.get("PROBE_TIMEOUT", "5"))

    results = {}
    for url in urls:
        results[url] = check(url, timeout)
        r = results[url]
        status = "OK  " if r["probe_success"] else "FAIL"
        print(
            f"{status} {r['probe_http_status_code']:3} {r['probe_duration_seconds']:6.3f}s {url}",
            file=sys.stderr,
        )

    text = render(results)
    output_dir = os.environ.get("PROBE_OUTPUT_DIR")
    if output_dir:
        write_atomically(output_dir, text)
    else:
        sys.stdout.write(text)

    return 0 if all(r["probe_success"] for r in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
