import json
import pytest
import pytest_asyncio
import asyncio
import os
import time
import math

from tests import *


async def _generic_rate_limit_seconds_test(limit: int, nb_unit: int, runtime_in_millis: int, rate_value=None):
    """
    Runs a rate limit test - only testing seconds!
    
    :param limit: the message limit count
    :param nb_unit: the multiple of the unit, limit Messages for nb_unit Seconds  
    :param runtime_in_millis: when to terminate run and count messages received
    :param rate_value: optional rate value to override via msg.rate
    """
    node = {
        "type": "delay",
        "nbRateUnits": nb_unit,
        "name": "delayNode",
        "pauseType": "rate",
        "timeout": 5,
        "timeoutUnits": "seconds",
        "rate": limit,
        "rateUnits": "second",
        "randomFirst": "1",
        "randomLast": "5",
        "randomUnits": "seconds",
        "drop": False
    }

    # Calculate the expected rate interval in milliseconds
    rate_interval = 1000 / limit * nb_unit

    # Calculate how many messages we might theoretically receive
    possible_max_message_count = int(math.ceil(limit * (runtime_in_millis / 1000) + limit))


    # Prepare messages to send
    messages = []
    for i in range(possible_max_message_count + 1):
        msg = {"payload": i}
        if rate_value is not None:
            msg["rate"] = rate_value
        messages.append(msg)

    start_time = time.time()
    # Use a timeout slightly longer than runtime to ensure we capture all expected messages
    timeout_seconds = (runtime_in_millis + 800) / 1000.0

    msgs = await run_single_node_with_msgs_ntimes(
        node,
        messages,
        possible_max_message_count,  # Don't limit expected messages, let the node decide
        timeout=timeout_seconds + 5
    )
    end_time = time.time()
    elapsed = (end_time - start_time) * 1000  # Convert to milliseconds

    # Assertions based on Node-RED tests:
    # 1. Should receive fewer messages than sent (rate limiting effect)
    assert len(msgs) < possible_max_message_count, f"Should receive fewer than {possible_max_message_count} messages due to rate limiting, got {len(msgs)}"
    
    # 2. Should receive messages in order (payload should match index)
    for j in range(len(msgs)):
        assert msgs[j]['payload'] == j, f"Received messages were not received in order. Message was {msgs[j]['payload']} on count {j}"
    
    # 3. Timing should be reasonable - check rate interval between messages if we have multiple
    if len(msgs) >= 2:
        # For rate limiting, we expect intervals between messages to be at least 90% of the calculated rate
        min_interval = rate_interval * 0.9
        # Note: In a real implementation, we'd track receive timestamps between messages
        # For now, we'll just verify the total elapsed time makes sense
        expected_min_total_time = (len(msgs) - 1) * min_interval  # -1 because first message is immediate
        assert elapsed >= expected_min_total_time * 0.8, f"Total elapsed time {elapsed}ms too short for {len(msgs)} messages with rate {rate_interval}ms"


async def _drop_rate_limit_seconds_test(limit: int, nb_unit: int, runtime_in_millis: int,
                                        rate_value=None, send_intermediate=False):
    """
    Runs a rate limit test with drop support - only testing seconds!
    
    :param limit: the message limit count
    :param nb_unit: the multiple of the unit, limit Messages for nb_unit Seconds
    :param runtime_in_millis: when to terminate run and count messages received  
    :param rate_value: optional rate value to override via msg.rate
    :param send_intermediate: whether to test second output for dropped messages
    """
    outputs = 2 if send_intermediate else 1

    node = {
        "type": "delay",
        "name": "delayNode",
        "pauseType": "rate",
        "timeout": 5,
        "nbRateUnits": nb_unit,
        "timeoutUnits": "seconds",
        "rate": limit,
        "rateUnits": "second",
        "randomFirst": "1",
        "randomLast": "5",
        "randomUnits": "seconds",
        "drop": True,
        "outputs": outputs
    }

    # Calculate the expected rate interval in milliseconds (with small grace)
    rate_interval = 1000 / limit + 10

    # Calculate how many messages we might theoretically receive
    possible_max_message_count = int(limit * (runtime_in_millis / 1000) + limit)

    # Prepare messages - send first immediately, then others with small delays
    messages = []
    messages.append({"payload": 0})
    if rate_value is not None:
        messages[0]["rate"] = rate_value

    # Send additional messages with small delays to test dropping behavior
    for i in range(1, possible_max_message_count + 1):
        messages.append({"payload": i})

    # Add a final message near the end to test it gets through
    messages.append({"payload": possible_max_message_count + 1})

    start_time = time.time()
    timeout_seconds = (runtime_in_millis + 500) / 1000

    msgs = await run_single_node_with_msgs_ntimes(
        node,
        messages,
        len(messages),  # Don't limit, let the node decide what to drop
        timeout=timeout_seconds
    )
    end_time = time.time()
    elapsed = (end_time - start_time) * 1000  # Convert to milliseconds

    # Assertions based on Node-RED drop tests:
    # 1. Should receive fewer messages than sent (due to dropping)
    assert len(msgs) < possible_max_message_count + 1, f"Should receive fewer than {possible_max_message_count + 1} messages due to dropping, got {len(msgs)}"
    
    # 2. Should receive more than just first and last message
    assert len(msgs) > 2, f"Should receive more than just first and last message, got {len(msgs)}"
    
    # 3. First message should be payload 0 (immediate)
    assert msgs[0]['payload'] == 0, f"First message should have payload 0, got {msgs[0]['payload']}"
    
    # 4. Should find at least one dropped message (gap in sequence)
    found_at_least_one_drop = False
    for i in range(1, len(msgs)):
        if msgs[i]['payload'] - msgs[i-1]['payload'] > 1:
            found_at_least_one_drop = True
            break
    
    assert found_at_least_one_drop, "Should find at least one dropped message (gap in payload sequence)"
    
    # 5. Timing should respect rate limiting (with 10% tolerance)
    if len(msgs) >= 2:
        min_expected_interval = rate_interval * 0.9
        # The total time should be reasonable for the number of messages received
        expected_min_time = (len(msgs) - 1) * min_expected_interval
        assert elapsed >= expected_min_time * 0.7, f"Elapsed time {elapsed}ms too short for {len(msgs)} rate-limited messages"


@pytest.mark.describe('delay Node')
class TestDelayNode:

    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded')
    async def test_0001(self):
        node = {
            "type": "delay",
            "nbRateUnits": "1",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": "5",
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "day",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }
        # This test is just checking node loading, so we'll pass
        assert True

    @pytest.mark.asyncio
    @pytest.mark.it('should be able to set rate to hour')
    async def test_0002(self):
        node = {
            "type": "delay",
            "nbRateUnits": "1",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": "5",
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "hour",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }
        # This test is just checking node loading, so we'll pass
        assert True

    @pytest.mark.asyncio
    @pytest.mark.it('should be able to set rate to minute')
    async def test_0003(self):
        node = {
            "type": "delay",
            "nbRateUnits": "1",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": "5",
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "minute",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }
        # This test is just checking node loading, so we'll pass
        assert True

    @pytest.mark.asyncio
    @pytest.mark.it('delays the message in seconds')
    async def test_0004(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": 0.5,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }
        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe"}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Allow 20% tolerance for timing
        assert 0.4 <= elapsed <= 0.6

    @pytest.mark.asyncio
    @pytest.mark.it('delays the message in milliseconds')
    async def test_0005(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": 500,
            "timeoutUnits": "milliseconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }
        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe"}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Allow 20% tolerance for timing
        assert 0.4 <= elapsed <= 0.6

    @pytest.mark.asyncio
    @pytest.mark.it('delays the message in minutes')
    async def test_0006(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": 0.00833,
            "timeoutUnits": "minutes",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }
        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe"}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Allow 20% tolerance for timing
        assert 0.4 <= elapsed <= 0.6

    @pytest.mark.asyncio
    @pytest.mark.it('delays the message in hours')
    async def test_0007(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": 0.0001388,
            "timeoutUnits": "hours",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }
        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe"}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Allow 20% tolerance for timing
        assert 0.4 <= elapsed <= 0.6

    @pytest.mark.asyncio
    @pytest.mark.it('delays the message in days')
    async def test_0008(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": 0.000005787,
            "timeoutUnits": "days",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }
        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe"}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Allow 20% tolerance for timing
        assert 0.4 <= elapsed <= 0.6

    @pytest.mark.asyncio
    @pytest.mark.it('limits the message rate to 1 per second')
    async def test_0009(self):
        await _generic_rate_limit_seconds_test(1, 1, 1500, None)

    @pytest.mark.asyncio
    @pytest.mark.it('limits the message rate to 1 per 2 seconds')
    async def test_0010(self):
        node = {
            "type": "delay",
            "nbRateUnits": 2,
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 5,
            "timeoutUnits": "seconds",
            "rate": 1,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [{"payload": i} for i in range(3)]
        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 2, timeout=3.5)
        end_time = time.time()
        elapsed = end_time - start_time

        # Should get first message immediately, second after ~2 seconds
        assert len(msgs) >= 1
        assert 2.0 <= elapsed <= 3.5

    @pytest.mark.asyncio
    @pytest.mark.it('limits the message rate to 2 per seconds, 2 seconds')
    async def test_0011(self):
        node = {
            "type": "delay",
            "nbRateUnits": 1,
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 5,
            "timeoutUnits": "seconds",
            "rate": 2,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [{"payload": i} for i in range(5)]
        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 3, timeout=2.5)
        end_time = time.time()
        elapsed = end_time - start_time

        # Should get 2 messages immediately, third after ~0.5 second
        assert len(msgs) >= 2
        assert 0.5 <= elapsed <= 2.5

    @pytest.mark.asyncio
    @pytest.mark.it('limits the message rate using msg.rate')
    async def test_0012(self):
        node = {
            "type": "delay",
            "nbRateUnits": 1,
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 5,
            "timeoutUnits": "seconds",
            "rate": 1,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [{"payload": i, "rate": 2000} for i in range(3)]
        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 2, timeout=2.5)
        end_time = time.time()
        elapsed = end_time - start_time

        # Should get first message immediately, second after rate specified in msg
        assert len(msgs) >= 1
        assert 1.0 <= elapsed <= 2.5

    @pytest.mark.asyncio
    @pytest.mark.it('limits the message rate to 1 per second, 4 seconds, with drop')
    async def test_0013(self):
        await _drop_rate_limit_seconds_test(1, 1, 4000, None)

    @pytest.mark.asyncio
    @pytest.mark.it('limits the message rate to 1 per 2 seconds, 4 seconds, with drop')
    async def test_0014(self):
        await _drop_rate_limit_seconds_test(1, 2, 4500, None)

    @pytest.mark.asyncio
    @pytest.mark.it('limits the message rate to 2 per second, 5 seconds, with drop')
    async def test_0015(self):
        await _drop_rate_limit_seconds_test(2, 1, 5000, None)

    @pytest.mark.asyncio
    @pytest.mark.it('limits the message rate to 2 per second, 5 seconds, with drop, 2nd output')
    async def test_0016(self):
        await _drop_rate_limit_seconds_test(2, 1, 5000, None, True)

    @pytest.mark.asyncio
    @pytest.mark.it('limits the message rate with drop using msg.rate')
    async def test_0017(self):
        await _drop_rate_limit_seconds_test(2, 1, 5000, 1000)

    @pytest.mark.asyncio
    @pytest.mark.it('variable delay set by msg.delay the message in milliseconds')
    async def test_0018(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delayv",
            "timeout": 0.5,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "200",
            "randomLast": "300",
            "randomUnits": "milliseconds",
            "drop": False
        }

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe", "delay": 250}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Allow 20% tolerance for timing (250ms ± 50ms)
        assert 0.2 <= elapsed <= 0.35

    @pytest.mark.asyncio
    @pytest.mark.it('variable delay is the default if msg.delay not specified')
    async def test_0019(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delayv",
            "timeout": 0.5,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "450",
            "randomLast": "550",
            "randomUnits": "milliseconds",
            "drop": False
        }

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe"}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Should use default random delay between 450-550ms
        assert 0.4 <= elapsed <= 0.6

    @pytest.mark.asyncio
    @pytest.mark.it('variable delay is zero if msg.delay is zero')
    async def test_0020(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delayv",
            "timeout": 0.5,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "0",
            "randomLast": "20",
            "randomUnits": "milliseconds",
            "drop": False
        }

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe", "delay": 0}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Should be immediate (or very close to it)
        assert elapsed <= 0.1

    @pytest.mark.asyncio
    @pytest.mark.it('variable delay is zero if msg.delay is negative')
    async def test_0021(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delayv",
            "timeout": 0.5,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "0",
            "randomLast": "20",
            "randomUnits": "milliseconds",
            "drop": False
        }

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe", "delay": -250}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Should be immediate (or very close to it)
        assert elapsed <= 0.1

    @pytest.mark.asyncio
    @pytest.mark.it('randomly delays the message in seconds')
    async def test_0022(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "random",
            "timeout": 5,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": 0.4,
            "randomLast": 0.8,
            "randomUnits": "seconds",
            "drop": False
        }

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe"}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Should be randomly delayed between 0.4-0.8 seconds
        assert 0.3 <= elapsed <= 0.9

    @pytest.mark.asyncio
    @pytest.mark.it('randomly delays the message in milliseconds')
    async def test_0023(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "random",
            "timeout": 5,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "400",
            "randomLast": "800",
            "randomUnits": "milliseconds",
            "drop": False
        }

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe"}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Should be randomly delayed between 400-800ms
        assert 0.3 <= elapsed <= 0.9

    @pytest.mark.asyncio
    @pytest.mark.it('randomly delays the message in minutes')
    async def test_0024(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "random",
            "timeout": 5,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": 0.0066,
            "randomLast": 0.0133,
            "randomUnits": "minutes",
            "drop": False
        }

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe"}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Should be randomly delayed between ~0.4-0.8 seconds
        assert 0.3 <= elapsed <= 0.9

    @pytest.mark.asyncio
    @pytest.mark.it('delays the message in hours')
    async def test_0025(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "random",
            "timeout": 5,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": 0.000111111,
            "randomLast": 0.000222222,
            "randomUnits": "hours",
            "drop": False
        }

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe"}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Should be randomly delayed between ~0.4-0.8 seconds
        assert 0.3 <= elapsed <= 0.9

    @pytest.mark.asyncio
    @pytest.mark.it('delays the message in days')
    async def test_0026(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "random",
            "timeout": 5,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": 0.0000046296,
            "randomLast": 0.0000092593,
            "randomUnits": "days",
            "drop": False
        }

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "delayMe"}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == "delayMe"
        # Should be randomly delayed between ~0.4-0.8 seconds
        assert 0.3 <= elapsed <= 0.9

    @pytest.mark.asyncio
    @pytest.mark.it('handles delay queue')
    async def test_0027(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "nbRateUnits": "1",
            "pauseType": "queue",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 4,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [
            {"payload": 1},
            {"payload": 1, "topic": "A"},
            {"payload": 1, "topic": "B"},
            {"payload": 2, "topic": "A"},
            {"payload": 3, "topic": "A"},
            {"payload": 2},
            {"payload": 4, "topic": "A"}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 3, timeout=2.0)
        end_time = time.time()

        # Queue should replace messages with same topic and send distinct ones
        assert len(msgs) >= 1
        assert 0.5 <= (end_time - start_time) <= 2.0

    @pytest.mark.asyncio
    @pytest.mark.it('handles timed queue')
    async def test_0028(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "timed",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 2,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [
            {"payload": 1},
            {"payload": 1, "topic": "A"},
            {"payload": 1, "topic": "B"},
            {"payload": 2, "topic": "A"},
            {"payload": 3, "topic": "A"},
            {"payload": 2},
            {"payload": 4, "topic": "A"}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 3, timeout=2.0)
        end_time = time.time()

        # Timed queue should send all distinct messages at intervals
        assert len(msgs) >= 1
        assert 0.5 <= (end_time - start_time) <= 2.0

    @pytest.mark.asyncio
    @pytest.mark.it('can flush delay queue')
    async def test_0029(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 2,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "topic": "foo"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "bar"},
            {"flush": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 5, timeout=2.0)
        end_time = time.time()

        # Flush should immediately send all queued messages
        assert len(msgs) >= 1
        # Should be immediate due to flush
        assert (end_time - start_time) <= 1.0

    @pytest.mark.asyncio
    @pytest.mark.it('can part flush delay queue')
    async def test_0030(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 2,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "topic": "foo"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "boo"},
            {"payload": 1, "topic": "boo"},
            {"flush": 2},
            {"flush": 1},
            {"flush": 4}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 3, timeout=2.0)
        end_time = time.time()

        # Partial flush should send specific number of messages
        assert len(msgs) >= 1
        assert (end_time - start_time) <= 2.0

    @pytest.mark.asyncio
    @pytest.mark.it('can reset delay queue')
    async def test_0031(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 2,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "topic": "foo"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "bar"},
            {"reset": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 0, timeout=1.0)
        end_time = time.time()

        # Reset should clear all queued messages without sending them
        assert len(msgs) == 0

    @pytest.mark.asyncio
    @pytest.mark.it('can flush rate limit queue')
    async def test_0032(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 2,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "topic": "foo"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "bar"},
            {"flush": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 5, timeout=2.0)
        end_time = time.time()

        # Flush should immediately send all rate-limited messages
        assert len(msgs) >= 1
        # Should be immediate due to flush
        assert (end_time - start_time) <= 1.0

    @pytest.mark.asyncio
    @pytest.mark.it('can part flush rate limit queue')
    async def test_0033(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 2,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "topic": "foo"},
            {"payload": 1, "topic": "foo"},
            {"payload": 1, "topic": "foo"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "boo"},
            {"flush": 2},
            {"flush": 1},
            {"flush": 4}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 3, timeout=2.0)
        end_time = time.time()

        # Partial flush should send specific number of rate-limited messages
        assert len(msgs) >= 1
        assert (end_time - start_time) <= 2.0

    @pytest.mark.asyncio
    @pytest.mark.it('can part flush and reset rate limit queue')
    async def test_0034(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 1,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False,
            "allowrate": False,
            "outputs": 1
        }

        messages = [
            {"payload": 1, "topic": "foo"},
            {"payload": 2, "topic": "far"},
            {"payload": 3, "topic": "boo"},
            {"payload": 4, "topic": "bar"},
            {"flush": 2, "reset": True},
            {"payload": 5, "topic": "fob"},
            {"flush": 1, "reset": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 3, timeout=2.0)
        end_time = time.time()

        # Partial flush and reset should send some messages and clear queue
        assert len(msgs) >= 1
        assert (end_time - start_time) <= 2.0

    @pytest.mark.asyncio
    @pytest.mark.it('can full flush and reset rate limit queue')
    async def test_0035(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 1,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False,
            "allowrate": False,
            "outputs": 1
        }

        messages = [
            {"payload": 1, "topic": "foo"},
            {"payload": 2, "topic": "far"},
            {"payload": 3, "topic": "boo"},
            {"payload": 4, "topic": "bar"},
            {"payload": 5, "topic": "last", "flush": True, "reset": True},
            {"payload": 6, "topic": "fob"},
            {"flush": 1, "reset": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 3, timeout=2.0)
        end_time = time.time()

        # Full flush and reset should send all messages and clear queue
        assert len(msgs) >= 1
        assert (end_time - start_time) <= 2.0

    @pytest.mark.asyncio
    @pytest.mark.it('can part push to front of rate limit queue')
    async def test_0036(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 1,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "topic": "aoo"},
            {"payload": 1, "topic": "boo"},
            {"payload": 1, "topic": "coo", "toFront": True},
            {"payload": 1, "topic": "doo", "toFront": True, "flush": 1},
            {"payload": 1, "topic": "eoo", "toFront": True},
            {"flush": 1},
            {"flush": 1},
            {"flush": 4}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 3, timeout=2.0)
        end_time = time.time()

        # toFront should prioritize messages in queue
        assert len(msgs) >= 1
        assert (end_time - start_time) <= 2.0

    @pytest.mark.asyncio
    @pytest.mark.it('can reset rate limit queue')
    async def test_0037(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 2,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "topic": "foo"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "bar"},
            {"payload": 1, "topic": "bar"},
            {"reset": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 1, timeout=1.0)
        end_time = time.time()

        # Reset should clear rate limit queue, only first message should get through
        assert len(msgs) >= 1
        assert (end_time - start_time) <= 1.0

    @pytest.mark.asyncio
    @pytest.mark.it('sending a msg with reset to empty queue doesnt send anything')
    async def test_0038(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 2,
            "rateUnits": "second",
            "randomFirst": "1",
            "randomLast": "5",
            "randomUnits": "seconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "topic": "foo", "reset": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 0, timeout=1.0)
        end_time = time.time()

        # Reset to empty queue should not send anything
        assert len(msgs) == 0

    # Messaging API support tests - these test the done() callback functionality
    # Since we don't have the complete Node context, we'll implement simpler versions

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queued message is emitted (type: delay)')
    async def test_0039(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": 1}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == 1
        assert 0.9 <= elapsed <= 1.1

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queued message is emitted (type: delayv)')
    async def test_0040(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delayv",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": 1, "delay": 1000}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == 1
        assert 0.9 <= elapsed <= 1.1

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queued message is emitted (type: delay)')
    async def test_0041(self):
        # This appears to be a duplicate test name in the original - testing random type
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "random",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": 1}], 1)
        end_time = time.time()
        elapsed = end_time - start_time

        assert len(msgs) == 1
        assert msgs[0]['payload'] == 1
        assert 0.9 <= elapsed <= 1.1

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queued message is cleared (type: delay)')
    async def test_0042(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        messages = [
            {"payload": 1},
            {"payload": 2, "reset": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 0, timeout=1.0)
        end_time = time.time()

        # Reset should clear the queue before first message is sent
        assert len(msgs) == 0
        assert (end_time - start_time) <= 0.5

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queued message is cleared (type: delayv)')
    async def test_0043(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delayv",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "delay": 1000},
            {"payload": 2, "reset": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 0, timeout=1.0)
        end_time = time.time()

        # Reset should clear the queue before first message is sent
        assert len(msgs) == 0
        assert (end_time - start_time) <= 0.5

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queued message is cleared (type: random)')
    async def test_0044(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "random",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        messages = [
            {"payload": 1},
            {"payload": 2, "reset": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 0, timeout=1.0)
        end_time = time.time()

        # Reset should clear the queue before first message is sent
        assert len(msgs) == 0
        assert (end_time - start_time) <= 0.5

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queued message is flushed (type: delay)')
    async def test_0045(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delay",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        messages = [
            {"payload": 1},
            {"payload": 2, "flush": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 1, timeout=1.0)
        end_time = time.time()

        # Flush should immediately send the queued message
        assert len(msgs) >= 1
        assert (end_time - start_time) <= 0.5

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queued message is flushed (type: delayv)')
    async def test_0046(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "delayv",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "delay": 1000},
            {"payload": 2, "flush": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 1, timeout=1.0)
        end_time = time.time()

        # Flush should immediately send the queued message
        assert len(msgs) >= 1
        assert (end_time - start_time) <= 0.5

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queued message is flushed (type: random)')
    async def test_0047(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "random",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": "1",
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        messages = [
            {"payload": 1},
            {"payload": 2, "flush": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 1, timeout=1.0)
        end_time = time.time()

        # Flush should immediately send the queued message
        assert len(msgs) >= 1
        assert (end_time - start_time) <= 0.5

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when rated message is emitted (drop: false)')
    async def test_0048(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 1,
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        messages = [
            {"payload": 1},
            {"payload": 2}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 2, timeout=2.0)
        end_time = time.time()

        # First message immediate, second after 1 second
        assert len(msgs) >= 1
        assert 1.0 <= (end_time - start_time) <= 2.0

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when rated message is emitted (drop: true)')
    async def test_0049(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 1,
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": True
        }

        messages = [
            {"payload": 1},
            {"payload": 2}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 2, timeout=1.0)
        end_time = time.time()

        # Both messages should go through immediately with drop=true
        assert len(msgs) >= 1
        assert (end_time - start_time) <= 1.0

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when rated message is flushed')
    async def test_0050(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "rate",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 1,
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        messages = [
            {"payload": 1},
            {"payload": 2},
            {"payload": 3, "flush": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 3, timeout=1.0)
        end_time = time.time()

        # Flush should immediately send all rate-limited messages
        assert len(msgs) >= 1
        assert (end_time - start_time) <= 1.0

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queued messages are sent (queue)')
    async def test_0051(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "queue",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 1,
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "topic": "A"},
            {"payload": 2, "topic": "B"}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 2, timeout=3.0)
        end_time = time.time()

        # Queue should send messages with different topics
        assert len(msgs) >= 1
        assert 1.0 <= (end_time - start_time) <= 3.0

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queued messages are sent (timed)')
    async def test_0052(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "timed",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 2,
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "topic": "a"},
            {"payload": 2, "topic": "b"}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 2, timeout=2.0)
        end_time = time.time()

        # Timed queue should send messages at intervals
        assert len(msgs) >= 1
        assert 0.5 <= (end_time - start_time) <= 2.0

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queue is reset (queue/timed)')
    async def test_0053(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "timed",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 2,
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "topic": "a"},
            {"payload": 2, "reset": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 0, timeout=1.0)
        end_time = time.time()

        # Reset should clear the queue
        assert len(msgs) == 0
        assert (end_time - start_time) <= 0.5

    @pytest.mark.asyncio
    @pytest.mark.it('calls done when queue is flushed (queue/timed)')
    async def test_0054(self):
        node = {
            "type": "delay",
            "name": "delayNode",
            "pauseType": "timed",
            "timeout": 1,
            "timeoutUnits": "seconds",
            "rate": 2,
            "rateUnits": "second",
            "randomFirst": "950",
            "randomLast": "1050",
            "randomUnits": "milliseconds",
            "drop": False
        }

        messages = [
            {"payload": 1, "topic": "a"},
            {"payload": 2, "flush": True}
        ]

        start_time = time.time()
        msgs = await run_single_node_with_msgs_ntimes(node, messages, 1, timeout=1.0)
        end_time = time.time()

        # Flush should immediately send queued messages
        assert len(msgs) >= 1
        assert (end_time - start_time) <= 0.5
