"""Contract tests for the pytest harness itself.

The harness exposes two mutually exclusive ways of ending a run:

* ``*_ntimes`` waits until a message count is reached (or the timeout expires);
* ``*_for_seconds`` samples for a duration and never waits for a count.

Node-RED's rate-limiting specs need the second form, and they check the spacing between
outputs, so only that form annotates messages with ``_arrival_ms``. These tests pin both
halves of that contract down so the two cannot silently drift apart.
"""
import pytest

from tests import run_single_node_for_seconds, run_single_node_with_msgs_ntimes


@pytest.mark.describe('Test harness contract')
class TestHarnessContract:

    @pytest.mark.asyncio
    @pytest.mark.it('count mode returns exactly the requested number of messages')
    async def test_count_mode_waits_for_a_count(self):
        node = {"type": "change", "rules": []}
        msgs = [{"payload": i} for i in range(4)]
        out = await run_single_node_with_msgs_ntimes(node, msgs, 2)
        assert len(out) == 2

    @pytest.mark.asyncio
    @pytest.mark.it('count mode reports no arrival offsets')
    async def test_count_mode_has_no_arrival_offsets(self):
        node = {"type": "change", "rules": []}
        out = await run_single_node_with_msgs_ntimes(node, [{"payload": "x"}], 1)
        assert all('_arrival_ms' not in m for m in out)

    @pytest.mark.asyncio
    @pytest.mark.it('sampling mode never waits for a count')
    async def test_sampling_mode_does_not_wait_for_a_count(self):
        node = {"type": "change", "rules": []}
        msgs = [{"payload": i} for i in range(4)]
        # Count mode would need to see all 4; sampling returns as soon as the window closes.
        out = await run_single_node_for_seconds(node, msgs, 0.5)
        assert len(out) == 4

    @pytest.mark.asyncio
    @pytest.mark.it('sampling mode reports arrival offsets relative to the first output')
    async def test_sampling_mode_reports_arrival_offsets(self):
        node = {"type": "change", "rules": []}
        out = await run_single_node_for_seconds(node, [{"payload": 0}, {"payload": 1}], 0.5)
        stamps = [m['_arrival_ms'] for m in out]
        assert len(stamps) == 2
        assert stamps[0] == 0.0
        assert stamps[1] >= stamps[0]
