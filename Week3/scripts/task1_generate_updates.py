from __future__ import annotations

import json
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


def main(argv: list[str]) -> int:
    ensure_updates_dir()

    selected = argv[1:] if len(argv) > 1 else list(GENERATORS)
    unknown = [d for d in selected if d not in GENERATORS]
    if unknown:
        print(f"Unknown datasets: {unknown}. Available: {list(GENERATORS)}",
              file=sys.stderr)
        return 2

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "datasets": {},
    }

    for name in selected:
        print(f"\GENERATING UPDATE FOR {name}")
        result = GENERATORS[name]()
        manifest["datasets"][name] = result

    with open(UPDATE_MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2, default=str)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
