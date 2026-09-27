from __future__ import annotations

import os

from common import WEEK1_DELTA
from validation import (
    AllowedValuesRule,
    NumericRangeRule,
    PredicateRule,
    ReferenceIntegrityRule,
    ValidationRule,
)


_TAXI_ZONE_DELTA = os.path.join(WEEK1_DELTA, "taxi_zone")

EXTRA_RULES: dict[str, list[ValidationRule]] = {
    "taxi_zone": [],

    "weather": [
        NumericRangeRule("humidity", lo=0.0, hi=100.0),
        NumericRangeRule("prcp", lo=0.0, hi=None),
    ],

    "air_quality": [
        NumericRangeRule("aqi", lo=0, hi=500),
    ],

    "taxi_trips": [

        ReferenceIntegrityRule(
            field="pu_location_id",
            ref_delta_path=_TAXI_ZONE_DELTA,
            ref_column="location_id",
            rule_id="ref[pu_location_id->taxi_zone]",
        ),
        ReferenceIntegrityRule(
            field="do_location_id",
            ref_delta_path=_TAXI_ZONE_DELTA,
            ref_column="location_id",
            rule_id="ref[do_location_id->taxi_zone]",
        ),
        PredicateRule(
            "predicate[dropoff_after_pickup]",
            "dropoff_time_local > pickup_time_local",
        ),
        AllowedValuesRule(
            "ratecode_id",
            values=[1, 2, 3, 4, 5, 6, 99],
        ),
    ],
}
