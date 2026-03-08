from __future__ import annotations

import numpy as np
import pytest


stitch = pytest.importorskip("rivcam.stitch")


def test_dev_best_overlap_offset_finds_expected_shift() -> None:
    prev_tail = [np.full((4, 4), i, dtype=np.uint8) for i in range(10)]
    curr_head = [np.full((4, 4), 255, dtype=np.uint8) for _ in range(2)] + [
        np.full((4, 4), i, dtype=np.uint8) for i in range(3, 10)
    ]

    best = stitch._dev_best_overlap_offset(
        prev_tail,
        curr_head,
        min_offset=0,
        max_offset=5,
        min_match_frames=5,
    )
    assert best is not None
    off, score, matched = best
    assert off == 2
    assert matched >= 7
    assert score == pytest.approx(0.0)


def test_dev_best_overlap_offset_handles_no_candidate() -> None:
    prev_tail = [np.zeros((4, 4), dtype=np.uint8) for _ in range(4)]
    curr_head = [np.ones((4, 4), dtype=np.uint8) * 255 for _ in range(3)]

    best = stitch._dev_best_overlap_offset(
        prev_tail,
        curr_head,
        min_offset=0,
        max_offset=2,
        min_match_frames=5,
    )
    assert best is None
