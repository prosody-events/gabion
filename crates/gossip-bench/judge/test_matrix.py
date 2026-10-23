#!/usr/bin/env python3
"""Tests for matrix.py's build step.

Run: python3 -m unittest discover -s crates/gossip-bench/judge
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from matrix import executable_from


def artifact(name: str, kind: str, executable: str | None) -> str:
    return json.dumps({"reason": "compiler-artifact", "target": {"name": name, "kind": [kind]},
                       "executable": executable})


class ExecutableFromTest(unittest.TestCase):
    def test_picks_the_gossip_bench_binary_among_cargo_messages(self):
        messages = "\n".join([
            "not json",
            artifact("gabion", "lib", None),
            artifact("gossip-bench", "lib", None),
            artifact("gossip-bench", "bin", "/t/release/gossip-bench"),
            json.dumps({"reason": "build-finished", "success": True}),
        ])
        self.assertEqual(executable_from(messages), Path("/t/release/gossip-bench"))

    def test_a_build_without_the_binary_is_an_error(self):
        with self.assertRaises(SystemExit):
            executable_from(artifact("gossip-bench", "lib", None))


if __name__ == "__main__":
    unittest.main()
