from __future__ import annotations

from pathlib import Path

from parsers.measurement_conditions_parser import parse_header_key_values


def parse_ivium_method_file(file_path: str | Path) -> tuple[dict[str, object], str]:
    path = Path(file_path)
    raw_bytes = path.read_bytes()
    for encoding in ("mbcs", "cp932", "utf-8-sig", "utf-8", "latin1"):
        try:
            text = raw_bytes.decode(encoding, errors="ignore")
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw_bytes.decode("latin1", errors="ignore")

    for marker in ("\r\n", "\r", "\x00", "\x1c", "\x1e"):
        text = text.replace(marker, "\n")
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    return parse_header_key_values(lines), "\n".join(lines)
