"""Node-RED 5.0.9 CSV leading-empty regression specs."""
import json
import pytest
from tests import run_single_node_with_msgs_ntimes


async def _check(config, payload, expected):
    node = {"type": "csv", "spec": "legacy", "temp": "a,b,c", "multi": "mult",
            "include_empty_strings": False, **config}
    result = await run_single_node_with_msgs_ntimes(node, [{"payload": payload}], 1)
    assert result[0]["payload"] == expected


@pytest.mark.describe("CSV leading empty columns (Legacy Mode)")
class TestCsvLeadingEmpty:
    @pytest.mark.asyncio
    @pytest.mark.it("handles a leading delimiter without a final newline")
    async def test_no_final_newline(self):
        await _check({"include_null_values": True}, ",1,2", [{"a": None, "b": 1, "c": 2}])

    @pytest.mark.asyncio
    @pytest.mark.it("handles consecutive empty columns")
    async def test_consecutive(self):
        await _check({"include_null_values": True}, ",,2\n", [{"a": None, "b": None, "c": 2}])

    @pytest.mark.asyncio
    @pytest.mark.it("does not turn a missing first field into an empty string")
    async def test_missing(self):
        await _check({"include_null_values": False, "include_empty_strings": True}, ",1,2", [{"b": 1, "c": 2}])

    @pytest.mark.asyncio
    @pytest.mark.it("keeps a quoted empty first field as an empty string")
    async def test_quoted(self):
        await _check({"include_null_values": True, "include_empty_strings": True}, '\"\",1,2', [{"a": "", "b": 1, "c": 2}])

    @pytest.mark.asyncio
    @pytest.mark.it("omits a quoted empty field when empty strings are disabled")
    async def test_omit(self):
        await _check({"include_null_values": True}, '\"\",1,2\n', [{"b": 1, "c": 2}])


def _add_delimiter_tests():
    for si, sep in enumerate([",", "\t"]):
        for ei, ending in enumerate(["\n", "\r\n", "\r"]):
            for include in [True, False]:
                def make_test(sep, ending, include):
                    async def test(self):
                        expected = [{"b": 1, "c": 2}, {"b": 3, "c": 4}]
                        if include:
                            for row in expected:
                                row["a"] = None
                        payload = sep + "1" + sep + "2" + ending + sep + "3" + sep + "4" + ending
                        await _check({"sep": sep, "include_null_values": include}, payload, expected)
                    return test
                title = (f"handles a leading {json.dumps(sep)} with line ending {json.dumps(ending)} "
                         f"and include_null_values={str(include).lower()}")
                test = pytest.mark.it(title)(pytest.mark.asyncio(make_test(sep, ending, include)))
                setattr(TestCsvLeadingEmpty, f"test_delimiter_{si}_{ei}_{include}", test)


_add_delimiter_tests()
