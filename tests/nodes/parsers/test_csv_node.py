import pytest
from tests import *

@pytest.mark.describe('CSV node (Legacy Mode)')
class TestCsvNodeLegacyMode:
    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded with defaults')
    async def test_json_string_to_object(self):
        pass

    @pytest.mark.describe('csv to json')
    class TestCsvToJson:

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple csv string to a javascript object')
        async def test_buffer_json_string_to_object(self):
            # JavaScript equivalent:
            # var flow = [ { id:"n1", type:"csv", temp:"a,b,c,d", wires:[["n2"]] },
            #     {id:"n2", type:"helper"} ];
            # var testString = "1,2,3,4"+String.fromCharCode(10);
            # n1.emit("input", {payload:testString});
            # Expected: msg.payload = { a: 1, b: 2, c: 3, d: 4 }
            # Expected: msg.columns = "a,b,c,d"

            flows = [
                {"id": "100", "type": "tab"},
                {"id": "101", "z": "100", "type": "csv", "temp": "a,b,c,d", "wires": [["102"]]},
                {"id": "102", "z": "100", "type": "test-once"}
            ]

            # Test string with newline character (equivalent to String.fromCharCode(10))
            test_string = "1,2,3,4\n"

            injections = [
                {"nid": "101", "msg": {"payload": test_string}}
            ]

            msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)

            # Verify the output
            assert len(msgs) == 1
            msg = msgs[0]

            # Check payload - should be converted to object
            assert msg["payload"] == {"a": 1, "b": 2, "c": 3, "d": 4}

            # Check columns property
            assert msg["columns"] == "a,b,c,d"

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple string to a javascript object with | separator (no template)')
        async def test_convert_with_pipe_separator_no_template(self):
            # JavaScript equivalent:
            # var flow = [ { id:"n1", type:"csv", sep:"|", wires:[["n2"]] },
            #     {id:"n2", type:"helper"} ];
            # var testString = "1|2|3|4"+String.fromCharCode(10);
            # Expected: msg.payload = { col1: 1, col2: 2, col3: 3, col4: 4 }
            # Expected: msg.columns = "col1,col2,col3,col4"

            flows = [
                {"id": "100", "type": "tab"},
                {"id": "1", "z": "100", "type": "csv", "sep": "|", "wires": [["2"]]},
                {"id": "2", "z": "100", "type": "test-once"}
            ]

            # Test string with pipe separator
            test_string = "1|2|3|4\n"

            injections = [
                {"nid": "1", "msg": {"payload": test_string}}
            ]

            msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)

            # Verify the output
            assert len(msgs) == 1
            msg = msgs[0]

            # Check payload - should be converted to object with default column names
            assert msg["payload"] == {"col1": 1, "col2": 2, "col3": 3, "col4": 4}

            # Check columns property
            assert msg["columns"] == "col1,col2,col3,col4"

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple string to a javascript object with tab separator (with template)')
        async def test_convert_with_tab_separator_with_template(self):
            # JavaScript equivalent:
            # var flow = [ { id:"n1", type:"csv", sep:"\t", temp:"A,B,,D", wires:[["n2"]] },
            #     {id:"n2", type:"helper"} ];
            # var testString = "1\t2\t3\t4"+String.fromCharCode(10);
            # Expected: msg.payload = { A: 1, B: 2, D: 4 }
            # Expected: msg.columns = "A,B,D"

            flows = [
                {"id": "100", "type": "tab"},
                {"id": "1", "z": "100", "type": "csv", "sep": "\t", "temp": "A,B,,D", "wires": [["2"]]},
                {"id": "2", "z": "100", "type": "test-once"}
            ]

            # Test string with tab separator
            test_string = "1\t2\t3\t4\n"

            injections = [
                {"nid": "1", "msg": {"payload": test_string}}
            ]

            msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)

            # Verify the output
            assert len(msgs) == 1
            msg = msgs[0]

            # Check payload - should skip the third column (empty in template)
            assert msg["payload"] == {"A": 1, "B": 2, "D": 4}

            # Check columns property
            assert msg["columns"] == "A,B,D"

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple string to a javascript object with space separator (with spaced template)')
        async def test_convert_with_space_separator_with_spaced_template(self):
            # JavaScript equivalent:
            # var flow = [ { id:"n1", type:"csv", sep:" ", temp:"A, B, , D", wires:[["n2"]] },
            #     {id:"n2", type:"helper"} ];
            # var testString = "1 2 3 4"+String.fromCharCode(10);
            # Expected: msg.payload = { A: 1, B: 2, D: 4 }
            # Expected: msg.columns = "A,B,D"

            flows = [
                {"id": "100", "type": "tab"},
                {"id": "1", "z": "100", "type": "csv", "sep": " ", "temp": "A, B, , D", "wires": [["2"]]},
                {"id": "2", "z": "100", "type": "test-once"}
            ]

            # Test string with space separator
            test_string = "1 2 3 4\n"

            injections = [
                {"nid": "1", "msg": {"payload": test_string}}
            ]

            msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)

            # Verify the output
            assert len(msgs) == 1
            msg = msgs[0]

            # Check payload - should skip the third column (empty in template)
            assert msg["payload"] == {"A": 1, "B": 2, "D": 4}

            # Check columns property
            assert msg["columns"] == "A,B,D"

        @pytest.mark.asyncio
        @pytest.mark.it('should remove quotes and whitespace from template')
        async def test_remove_quotes_and_whitespace_from_template(self):
            # JavaScript equivalent:
            # var flow = [ { id:"n1", type:"csv", temp:'"a",  "b" , " c "," d  " ', wires:[["n2"]] },
            #     {id:"n2", type:"helper"} ];
            # var testString = "1,2,3,4"+String.fromCharCode(10);
            # Expected: msg.payload = { a: 1, b: 2, c: 3, d: 4 }

            flows = [
                {"id": "100", "type": "tab"},
                {"id": "1", "z": "100", "type": "csv", "temp": '"a",  "b" , " c "," d  " ', "wires": [["2"]]},
                {"id": "2", "z": "100", "type": "test-once"}
            ]

            # Test string with comma separator
            test_string = "1,2,3,4\n"

            injections = [
                {"nid": "1", "msg": {"payload": test_string}}
            ]

            msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)

            # Verify the output
            assert len(msgs) == 1
            msg = msgs[0]

            # Check payload - quotes and whitespace should be removed from template
            assert msg["payload"] == {"a": 1, "b": 2, "c": 3, "d": 4}

            # Check columns property (should be clean column names)
            assert msg["columns"] == "a,b,c,d"
