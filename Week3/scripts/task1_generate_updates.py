from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

from common import UPDATE_MANIFEST_PATH, ensure_updates_dir

import generate_taxi_trips_update
import generate_weather_update
import generate_air_quality_update


GENERATORS = {
    "taxi_trips": generate_taxi_trips_update.generate,
    "weather": generate_weather_update.generate,
    "air_quality": generate_air_quality_update.generate,
}


def _load_existing_manifest() -> dict:
    if not os.path.exists(UPDATE_MANIFEST_PATH):
        return {"datasets": {}}
    try:
        with open(UPDATE_MANIFEST_PATH, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        os.replace(UPDATE_MANIFEST_PATH, UPDATE_MANIFEST_PATH + ".bak")
        return {"datasets": {}}
    if "datasets" not in data or not isinstance(data["datasets"], dict):
        data["datasets"] = {}
    return data


def main(argv: list[str]) -> int:
    ensure_updates_dir()

    selected = argv[1:] if len(argv) > 1 else list(GENERATORS)
    unknown = [d for d in selected if d not in GENERATORS]
    if unknown:
        print(f"Unknown datasets: {unknown}. Available: {list(GENERATORS)}",
              file=sys.stderr)
        return 2

    manifest = _load_existing_manifest()
    now_iso = datetime.now(timezone.utc).isoformat()
    manifest["last_run_at"] = now_iso
    manifest["last_run_datasets"] = selected

    for name in selected:
        print(f"\n=== GENERATING UPDATE FOR {name} ===")
        result = GENERATORS[name]()
        result["generated_at"] = now_iso
        manifest["datasets"][name] = result

    with open(UPDATE_MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2, default=str)

    print(f"\nManifest written to {UPDATE_MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
