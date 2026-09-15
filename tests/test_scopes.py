"""Two watchers sharing a state scope is the failure that never announces
itself: each reads the other's incidents as missing from its own snapshot, so
the pair flaps between cleared and re-declared on every poll, forever.

The shipped config is safe from it only by accident — its two PulsePoint
watchers and its two wildfire watchers happen to sit in different groups. These
tests pin the property rather than the accident.
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scanner_transcribe_gpu import check_scopes, poller_scope


class Scopes(unittest.TestCase):
    def test_same_agency_different_prefixes_do_not_collide(self):
        a = poller_scope("vancouver-island",
                         {"type": "pulsepoint", "agency": "EMS1201",
                          "unit_prefix": ["1"]})
        b = poller_scope("vancouver-island",
                         {"type": "pulsepoint", "agency": "EMS1201",
                          "unit_prefix": ["3"]})
        self.assertNotEqual(a, b)

    def test_same_type_different_groups_do_not_collide(self):
        a = poller_scope("vancouver-island", {"type": "nanaimo_fire"})
        b = poller_scope("interior", {"type": "nanaimo_fire"})
        self.assertNotEqual(a, b)

    def test_wildfire_scope_follows_the_fire_centre(self):
        a = poller_scope("interior", {"type": "bc_wildfire",
                                      "fire_centre_code": "50"})
        b = poller_scope("interior", {"type": "bc_wildfire",
                                      "fire_centre_code": "25"})
        c = poller_scope("interior", {"type": "bc_wildfire"})
        self.assertNotEqual(a, b)
        self.assertNotEqual(a, c)

    def test_scope_is_stable_for_an_unchanged_config(self):
        cfg = {"type": "pulsepoint", "agency": "EMS1201", "unit_prefix": ["1"]}
        self.assertEqual(poller_scope("g", cfg), poller_scope("g", dict(cfg)),
                         "the scope is a durable key; it must not drift between "
                         "restarts for the same config")

    def test_missing_optional_fields_are_handled(self):
        self.assertTrue(poller_scope("g", {"type": "pulsepoint"}))
        self.assertTrue(poller_scope("g", {"type": "bc_wildfire"}))


class CheckScopes(unittest.TestCase):
    def test_shipped_config_has_no_collisions(self):
        raw = json.loads(Path(__file__).resolve().parent.parent
                         .joinpath("config.json").read_text())
        groups = {name: {"pollers": g.get("pollers", [])}
                  for name, g in raw.get("groups", {}).items()}
        check_scopes(groups)  # must not exit

    def test_duplicate_watchers_are_refused_at_startup(self):
        cfg = {"type": "pulsepoint", "agency": "EMS1201", "unit_prefix": ["1"]}
        groups = {"vancouver-island": {"pollers": [cfg, dict(cfg)]}}
        with self.assertRaises(SystemExit):
            check_scopes(groups)

    def test_distinct_watchers_are_allowed(self):
        groups = {"vancouver-island": {"pollers": [
            {"type": "pulsepoint", "agency": "EMS1201", "unit_prefix": ["1"]},
            {"type": "pulsepoint", "agency": "EMS1201", "unit_prefix": ["3"]},
            {"type": "bc_wildfire", "fire_centre_code": "50"},
        ]}}
        check_scopes(groups)  # must not exit


if __name__ == "__main__":
    unittest.main()
