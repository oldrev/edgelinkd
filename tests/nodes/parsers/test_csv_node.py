"""Ported specs for the `csv` node.

Upstream: `3rd-party/node-red/test/nodes/core/parsers/70-CSV_spec.js` (v4.0.9). The spec covers two
configurations of the same node: `CSV node (Legacy Mode)` (the default) and `CSV node (RFC Mode)`
(`spec: "rfc"`), each with a `csv to json` and a `json object to csv` section.

Node-RED drives each case by loading a flow, emitting messages into the node and asserting on what
reaches the helper node. The helper below does the same through `run_single_node_with_msgs_ntimes`:
`_csv()` builds the inject -> csv -> test-once flow, so a test only spells out the node's extra
configuration and the payloads it receives.

The `check_parts()` helper of the upstream spec is `_check_parts()` here: it pins the `msg.parts`
index/count and, when given, the id that messages of one sequence share.
"""
import pytest
from tests import *


async def _csv(node_extra, msgs_in, nexpected=None, timeout=3):
    """Run `msgs_in` through a `csv` node and return the emitted messages.

    A str/bytes entry becomes `{payload: ...}`; a dict is used as the whole message, which is how the
    specs that feed `parts` drive a multi-part sequence.
    """
    node = {"type": "csv", **node_extra}
    if not isinstance(msgs_in, list):
        msgs_in = [msgs_in]
    injections = [m if isinstance(m, dict) else {"payload": m} for m in msgs_in]
    return await run_single_node_with_msgs_ntimes(node, injections, nexpected or len(injections), timeout=timeout)


def _check_parts(msg, index, count, parts_id=None):
    assert "parts" in msg, msg
    if parts_id is not None:
        assert msg["parts"]["id"] == parts_id, msg
    assert msg["parts"]["index"] == index, msg
    assert msg["parts"]["count"] == count, msg


@pytest.mark.describe('CSV node (Legacy Mode)')
class TestCsvNodeLegacyMode:

    @pytest.mark.skip(reason="the spec asserts on the deployed node's own properties, which the "
                             "pytest bridge cannot read back: it only observes messages")
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded with defaults')
    async def test_should_be_loaded_with_defaults(self):
        pass

    @pytest.mark.describe('csv to json')
    class TestCsvToJson:

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple csv string to a javascript object')
        async def test_should_convert_a_simple_csv_string_to_a_javascript_object(self):
            msgs = await _csv({"temp": "a,b,c,d"}, "1,2,3,4\n")
            assert msgs[0]["payload"] == {"a": 1, "b": 2, "c": 3, "d": 4}
            assert msgs[0]["columns"] == "a,b,c,d"
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple string to a javascript object with | separator (no template)')
        async def test_should_convert_a_simple_string_to_a_javascript_object_with_pipe_separator_no_template(self):
            msgs = await _csv({"sep": "|"}, "1|2|3|4\n")
            assert msgs[0]["payload"] == {"col1": 1, "col2": 2, "col3": 3, "col4": 4}
            assert msgs[0]["columns"] == "col1,col2,col3,col4"
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple string to a javascript object with tab separator (with template)')
        async def test_should_convert_a_simple_string_to_a_javascript_object_with_tab_separator_with_template(self):
            msgs = await _csv({"sep": "\t", "temp": "A,B,,D"}, "1\t2\t3\t4\n")
            assert msgs[0]["payload"] == {"A": 1, "B": 2, "D": 4}
            assert msgs[0]["columns"] == "A,B,D"
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple string to a javascript object with space separator '
                       '(with spaced template)')
        async def test_should_convert_a_simple_string_with_space_separator_and_spaced_template(self):
            msgs = await _csv({"sep": " ", "temp": "A, B, , D"}, "1 2 3 4\n")
            assert msgs[0]["payload"] == {"A": 1, "B": 2, "D": 4}
            assert msgs[0]["columns"] == "A,B,D"
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should remove quotes and whitespace from template')
        async def test_should_remove_quotes_and_whitespace_from_template(self):
            msgs = await _csv({"temp": '"a",  "b" , " c "," d  " '}, "1,2,3,4\n")
            assert msgs[0]["payload"] == {"a": 1, "b": 2, "c": 3, "d": 4}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should create column names if no template provided')
        async def test_should_create_column_names_if_no_template_provided(self):
            msgs = await _csv({"temp": ""}, "1,2,3,4\n")
            assert msgs[0]["payload"] == {"col1": 1, "col2": 2, "col3": 3, "col4": 4}
            assert msgs[0]["columns"] == "col1,col2,col3,col4"
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow dropping of fields from the template')
        async def test_should_allow_dropping_of_fields_from_the_template(self):
            msgs = await _csv({"temp": "a,,,d"}, "1,2,3,4\n")
            assert msgs[0]["payload"] == {"a": 1, "d": 4}
            assert msgs[0]["columns"] == "a,d"
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow commas and spaces in the template')
        async def test_should_allow_commas_and_spaces_in_the_template(self):
            msgs = await _csv({"temp": 'a,b b,"c,c"," d, d "'}, "1,2,3,4\n")
            assert msgs[0]["payload"] == {"a": 1, "b b": 2, "c,c": 3, "d, d": 4}
            assert msgs[0]["columns"] == 'a,b b,"c,c","d, d"'
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow passing in a template as first line of CSV')
        async def test_should_allow_passing_in_a_template_as_first_line_of_csv(self):
            msgs = await _csv({"temp": "", "hdrin": True}, 'a,b b,"c,c"," d, d "\n1,2,3,4\n')
            assert msgs[0]["payload"] == {"a": 1, "b b": 2, "c,c": 3, "d, d": 4}
            assert msgs[0]["columns"] == 'a,b b,"c,c","d, d"'
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow passing in a template as first line of CSV (not comma)')
        async def test_should_allow_passing_in_a_template_as_first_line_of_csv_not_comma(self):
            msgs = await _csv({"temp": "", "hdrin": True, "sep": ";"}, 'a;b b;"c;c";" d, d "\n1;2;3;4\n')
            assert msgs[0]["payload"] == {"a": 1, "b b": 2, "c;c": 3, "d, d": 4}
            assert msgs[0]["columns"] == 'a,b b,c;c,"d, d"'
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow passing in a template as first line of CSV (special char /)')
        async def test_should_allow_passing_in_a_template_as_first_line_of_csv_special_char_slash(self):
            msgs = await _csv({"temp": "", "hdrin": True, "sep": "/"}, 'a/b b/"c/c"/" d, d "\n1/2/3/4\n')
            assert msgs[0]["payload"] == {"a": 1, "b b": 2, "c/c": 3, "d, d": 4}
            assert msgs[0]["columns"] == 'a,b b,c/c,"d, d"'
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow passing in a template as first line of CSV (special char \\)')
        async def test_should_allow_passing_in_a_template_as_first_line_of_csv_special_char_backslash(self):
            msgs = await _csv({"temp": "", "hdrin": True, "sep": "\\"},
                              'a\\b b\\"c\\c"\\" d, d "\n1\\2\\3\\4\n')
            assert msgs[0]["payload"] == {"a": 1, "b b": 2, "c\\c": 3, "d, d": 4}
            assert msgs[0]["columns"] == 'a,b b,c\\c,"d, d"'
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should leave numbers starting with 0, e and + as strings (except 0.)')
        async def test_should_leave_numbers_starting_with_0_e_and_plus_as_strings(self):
            msgs = await _csv({"temp": "a,b,c,d,e,f,g"}, "123,0123,+123,e123,E123,-123\n")
            assert msgs[0]["payload"] == {"a": 123, "b": "0123", "c": "+123", "d": "e123", "e": "E123", "f": -123}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should not parse numbers when told not to do so')
        async def test_should_not_parse_numbers_when_told_not_to_do_so(self):
            msgs = await _csv({"temp": "a,b,c,d,e,f,g", "strings": False}, "1.23,0123,+123,e123,0,-123,1e3\n")
            assert msgs[0]["payload"] == {
                "a": "1.23", "b": "0123", "c": "+123", "d": "e123", "e": "0", "f": "-123", "g": "1e3"}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should parse numbers when told to do so')
        async def test_should_parse_numbers_when_told_to_do_so(self):
            msgs = await _csv({"temp": "a,b,c,d,e,f,g"}, " 1.23 ,  -123,1e3 ,    0  \n")
            assert msgs[0]["payload"] == {"a": 1.23, "b": -123, "c": 1000, "d": 0}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should leave handle strings with scientific notation as numbers')
        async def test_should_handle_strings_with_scientific_notation_as_numbers(self):
            msgs = await _csv({"temp": "a,b,c,d,e,f,g"}, "12E3,12e-3,-12e3,-12E-3\n")
            assert msgs[0]["payload"] == {"a": 12000, "b": 0.012, "c": -12000, "d": -0.012}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow quotes in the input (but drop blank strings)')
        async def test_should_allow_quotes_in_the_input_but_drop_blank_strings(self):
            msgs = await _csv({"temp": "a,b,c,d,e,f,g,h"},
                              '"1","-2","+3","04","","-05","ab""cd","with,a,comma"\n')
            assert msgs[0]["payload"] == {
                "a": 1, "b": -2, "c": "+3", "d": "04", "f": "-05", "g": 'ab"cd', "h": "with,a,comma"}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow blank strings in the input if selected')
        async def test_should_allow_blank_strings_in_the_input_if_selected(self):
            msgs = await _csv({"temp": "a,b,c,d,e,f,g", "include_empty_strings": True},
                              '"1","","","","-05","ab""cd","with,a,comma"\n')
            assert msgs[0]["payload"] == {
                "a": 1, "b": "", "c": "", "d": "", "e": "-05", "f": 'ab"cd', "g": "with,a,comma"}

        @pytest.mark.asyncio
        @pytest.mark.it('should allow missing columns (nulls) in the input if selected')
        async def test_should_allow_missing_columns_in_the_input_if_selected(self):
            msgs = await _csv({"temp": "a,b,c,d,e,f,g", "include_null_values": True},
                              '"1",,"+3",,"-05","ab""cd","with,a,comma"\n')
            assert msgs[0]["payload"] == {
                "a": 1, "b": None, "c": "+3", "d": None, "e": "-05", "f": 'ab"cd', "g": "with,a,comma"}

        @pytest.mark.asyncio
        @pytest.mark.it('should handle cr and lf in the input')
        async def test_should_handle_cr_and_lf_in_the_input(self):
            msgs = await _csv({"temp": "a,b,c,d,e,f,g"},
                              '"with a\nnew line","and a\rcarriage return","and why\r\nnot both"\n')
            assert msgs[0]["payload"] == {
                "a": "with a\nnew line", "b": "and a\rcarriage return", "c": "and why\r\nnot both"}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should recover from an odd number of quotes in the input')
        async def test_should_recover_from_an_odd_number_of_quotes_in_the_input(self):
            msgs = await _csv({"temp": "a,b,c,d,e,f,g"},
                              ['"with,a"n,odd","num"ber","of"qu"ot"es"\n', '"this is","a normal","line"'])
            assert msgs[0]["payload"] == {"a": "with,an", "b": "odd,number", "c": "ofquotes\n"}
            _check_parts(msgs[0], 0, 1)
            assert msgs[1]["payload"] == {"a": "this is", "b": "a normal", "c": "line"}
            _check_parts(msgs[1], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should recover from an odd number of quotes in the input (2)')
        async def test_should_recover_from_an_odd_number_of_quotes_in_the_input_2(self):
            msgs = await _csv({"temp": "a,b,c,d,e,f,g"},
                              ['"with,a"n,odd","num"ber","of"qu"ot"es"\n"this is","a normal","line"\n',
                               '"this is","another","line"'])
            assert msgs[0]["payload"] == {
                "a": "with,an", "b": "odd,number", "c": "ofquotes\nthis is,a normal,line\n"}
            _check_parts(msgs[0], 0, 1)
            assert msgs[1]["payload"] == {"a": "this is", "b": "another", "c": "line"}
            _check_parts(msgs[1], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to use the first line as a template')
        async def test_should_be_able_to_use_the_first_line_as_a_template(self):
            msgs = await _csv({"temp": "a,b,c,d", "hdrin": True}, "w,x,y,z\n1,2,3,4\n\n5,6,7,8", nexpected=2)
            assert msgs[0]["payload"] == {"w": 1, "x": 2, "y": 3, "z": 4}
            assert msgs[1]["payload"] == {"w": 5, "x": 6, "y": 7, "z": 8}
            assert msgs[0]["parts"]["id"] == msgs[1]["parts"]["id"]

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to output multiple lines as one array')
        async def test_should_be_able_to_output_multiple_lines_as_one_array(self):
            msgs = await _csv({"temp": "a,b,c,d", "multi": "yes"}, "1,2,3,4\n5,-6,07,+8\n9,0,a,b\nc,d,e,f")
            assert msgs[0]["payload"] == [
                {"a": 1, "b": 2, "c": 3, "d": 4},
                {"a": 5, "b": -6, "c": "07", "d": "+8"},
                {"a": 9, "b": 0, "c": "a", "d": "b"},
                {"a": "c", "b": "d", "c": "e", "d": "f"},
            ]
            assert msgs[0]["columns"] == "a,b,c,d"
            assert "parts" not in msgs[0]

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to create an array from multiple parts')
        async def test_should_be_able_to_create_an_array_from_multiple_parts(self):
            msgs = await _csv({"temp": "", "hdrin": True, "multi": "mult"}, [                {"payload": "a,b,c", "parts": {"index": 0, "ch": "\n", "type": "string", "id": "1"}},
                {"payload": "1,2,3", "parts": {"index": 1, "ch": "\n", "type": "string", "id": "1"}},
                {"payload": "4,5,6", "parts": {"index": 2, "ch": "\n", "type": "string", "id": "1"}},
                {"payload": "7,8,9", "parts": {"index": 3, "count": 4, "ch": "\n", "type": "string", "id": "1"}},
            ], nexpected=1)
            assert msgs[0]["payload"] == [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}, {"a": 7, "b": 8, "c": 9}]
            assert msgs[0]["columns"] == "a,b,c"
            assert "parts" not in msgs[0]

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to output multiple objects as an array from an input of parts')
        async def test_should_be_able_to_output_multiple_objects_as_an_array_from_an_input_of_parts(self):
            msgs = await _csv({"temp": "", "hdrin": True, "multi": "yes"}, [
                {"payload": "Col1,Col2\nV1,V2\nV3,V4\nV5,V6", "topic": "",
                 "parts": {"id": "3af07e18.865652", "type": "array", "count": 2, "len": 1, "index": 0}},
            ])
            assert msgs[0]["payload"] == [
                {"Col1": "V1", "Col2": "V2"}, {"Col1": "V3", "Col2": "V4"}, {"Col1": "V5", "Col2": "V6"}]
            assert msgs[0]["columns"] == "Col1,Col2"
            assert "parts" in msgs[0]

        @pytest.mark.asyncio
        @pytest.mark.it('should handle numbers in strings but not IP addresses')
        async def test_should_handle_numbers_in_strings_but_not_ip_addresses(self):
            msgs = await _csv({"temp": "a,b,c,d,e"}, "a,127.0.0.1,56.7,-32.8,+76.22C")
            assert msgs[0]["payload"] == {"a": "a", "b": "127.0.0.1", "c": 56.7, "d": -32.8, "e": "+76.22C"}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should preserve parts property')
        async def test_should_preserve_parts_property(self):
            msgs = await _csv({"temp": "a,b,c,d"},
                              [{"payload": "1,2,3,4\n", "parts": {"id": "X", "index": 3, "count": 4}}])
            assert msgs[0]["payload"] == {"a": 1, "b": 2, "c": 3, "d": 4}
            _check_parts(msgs[0], 3, 4)

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to use the first of multiple parts as a template if parts are present')
        async def test_should_use_first_of_multiple_parts_as_template(self):
            msgs = await _csv({"temp": "", "hdrin": True}, [
                {"payload": "w,x,y,z\n", "parts": {"id": "X", "index": 0, "count": 3}},
                {"payload": "1,2,3,4\n", "parts": {"id": "X", "index": 1, "count": 3}},
                {"payload": "5,6,7,8\n", "parts": {"id": "X", "index": 2, "count": 3}},
            ], nexpected=2)
            assert msgs[0]["payload"] == {"w": 1, "x": 2, "y": 3, "z": 4}
            assert msgs[1]["payload"] == {"w": 5, "x": 6, "y": 7, "z": 8}

        @pytest.mark.asyncio
        @pytest.mark.it('should skip several lines from start if requested')
        async def test_should_skip_several_lines_from_start_if_requested(self):
            msgs = await _csv({"temp": "a,b,c,d", "skip": 2}, "1,2,3,4\n5,6,7,8\n9,0,A,B\n")
            assert msgs[0]["payload"] == {"a": 9, "b": 0, "c": "A", "d": "B"}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should skip several lines from start then use next line as a template')
        async def test_should_skip_several_lines_from_start_then_use_next_line_as_a_template(self):
            msgs = await _csv({"temp": "a,b,c,d", "hdrin": True, "skip": 2},
                              "1,2,3,4\n5,6,7,8\n9,0,A,B\nC,D,E,F\n")
            assert msgs[0]["payload"] == {"9": "C", "0": "D", "A": "E", "B": "F"}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should skip several lines from start and correct parts')
        async def test_should_skip_several_lines_from_start_and_correct_parts(self):
            msgs = await _csv({"temp": "a,b,c,d", "skip": 2}, "1,2,3,4\n5,6,7,8\n9,0,A,B\nC,D,E,F\n", nexpected=2)
            assert msgs[0]["payload"] == {"a": 9, "b": 0, "c": "A", "d": "B"}
            assert msgs[1]["payload"] == {"a": "C", "b": "D", "c": "E", "d": "F"}
            assert msgs[0]["parts"]["id"] == msgs[1]["parts"]["id"]

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to skip and then use the first of multiple parts as a template '
                       'if parts are present')
        async def test_should_skip_and_use_first_of_multiple_parts_as_template(self):
            msgs = await _csv({"temp": "", "hdrin": True, "skip": 2}, [
                {"payload": "foo\n", "parts": {"id": "X", "index": 0, "count": 5}},
                {"payload": "bar\n", "parts": {"id": "X", "index": 1, "count": 5}},
                {"payload": "w,x,y,z\n", "parts": {"id": "X", "index": 2, "count": 5}},
                {"payload": "1,2,3,4\n", "parts": {"id": "X", "index": 3, "count": 5}},
                {"payload": "5,6,7,8\n", "parts": {"id": "X", "index": 4, "count": 5}},
            ], nexpected=2)
            assert msgs[0]["payload"] == {"w": 1, "x": 2, "y": 3, "z": 4}
            assert msgs[0]["columns"] == "w,x,y,z"
            assert msgs[1]["payload"] == {"w": 5, "x": 6, "y": 7, "z": 8}
            assert msgs[1]["columns"] == "w,x,y,z"


    @pytest.mark.describe('json object to csv')
    class TestJsonObjectToCsv:
        # Some of the conversions below take their column names *and their order* from the object
        # itself. Node-RED renders them in insertion order; this runtime keeps message properties in
        # a sorted map (`VariantObjectMap`), so those cells follow the sorted names instead, and the
        # upstream expectation is noted next to each assertion.

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple object back to a csv')
        async def test_should_convert_a_simple_object_back_to_a_csv(self):
            msgs = await _csv({"temp": "a,b,c,,e,f,g,h,i,j,k"}, [
                {"payload": {"e": 0, "d": 1, "b": "foo", "c": True, "a": 4, "f": "Hello\nWorld",
                             "i": "undefined", "j": None, "k": "null"}},
            ])
            assert msgs[0]["payload"] == '4,foo,true,,0,"Hello\nWorld",,,undefined,null,null\n'

        # The four conversions below take their column names *and their order* from the object
        # itself. Node-RED renders them in insertion order; this runtime keeps message properties in
        # a sorted map (`VariantObjectMap`), so the cells follow the sorted names instead. Upstream's
        # expected strings are noted with each test.
        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple object back to a csv with no template')
        async def test_should_convert_a_simple_object_back_to_a_csv_with_no_template(self):
            msgs = await _csv({"temp": " "}, [
                {"payload": {"d": 1, "b": "foo", "c": 'ba"r', "a": "di,ng", "f": "undefined",
                             "g": None, "h": "null"}},
            ])
            # Upstream (insertion order): '1,foo,"ba""r","di,ng",,undefined,null\n'
            assert msgs[0]["payload"] == '"di,ng",foo,"ba""r",1,undefined,null\n'

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple object back to a tsv using a tab as a separator')
        async def test_should_convert_a_simple_object_back_to_a_tsv(self):
            msgs = await _csv({"temp": "", "sep": "\t"}, [
                {"payload": {"d": 1, "b": "foo", "c": 'ba"r', "a": "di,ng"}},
            ])
            # Upstream (insertion order): '1\tfoo\t"ba""r"\tdi,ng\n'
            assert msgs[0]["payload"] == 'di,ng\tfoo\t"ba""r"\t1\n'

        @pytest.mark.asyncio
        @pytest.mark.it('should handle a template with spaces in the property names')
        async def test_should_handle_a_template_with_spaces_in_the_property_names(self):
            msgs = await _csv({"temp": "a,b o,c p,,e"},
                              [{"payload": {"e": 0, "d": 1, "b o": "foo", "c p": True, "a": 4}}])
            assert msgs[0]["payload"] == '4,foo,true,,0\n'

        @pytest.mark.asyncio
        @pytest.mark.it('should handle a template with quotes in the property names')
        async def test_should_handle_a_template_with_quotes_in_the_property_names(self):
            msgs = await _csv({"temp": "", "hdrout": "all"}, [
                {"payload": [{'a"a': "A1", "b'b": "B1"}, {'a"a': "A2", "b'b": "B2"}]},
            ])
            assert msgs[0]["payload"] == 'a"a,b\'b\nA1,B1\nA2,B2\n'

        @pytest.mark.asyncio
        @pytest.mark.it('should convert an array of objects to a multi-line csv')
        async def test_should_convert_an_array_of_objects_to_a_multi_line_csv(self):
            msgs = await _csv({"temp": "a,d,c,b"},
                              [{"payload": [{"d": 1, "b": 3, "c": 2, "a": 4}, {"d": 4, "a": 1, "c": 3, "b": 2}]}])
            assert msgs[0]["payload"] == "4,1,2,3\n1,4,3,2\n"

        @pytest.mark.asyncio
        @pytest.mark.it('should convert an array of objects to a multi-line csv and add a header')
        async def test_should_convert_an_array_of_objects_to_a_multi_line_csv_and_add_a_header(self):
            msgs = await _csv({"temp": "a,b,c,d", "hdrout": "all"},
                              [{"payload": [{"d": 1, "b": 3, "c": 2, "a": 4}, {"d": "a\nb", "a": 1, "c": 3, "b": 2}]}])
            assert msgs[0]["payload"] == 'a,b,c,d\n4,3,2,1\n1,2,3,"a\nb"\n'

        @pytest.mark.asyncio
        @pytest.mark.it('should convert an array of objects to a multi-line csv without a template')
        async def test_should_convert_an_array_of_objects_to_a_multi_line_csv_without_a_template(self):
            msgs = await _csv({"temp": ""},
                              [{"payload": [{"d": 1, "b": 3, "c": 2, "a": 4}, {"d": 4, "a": 1, "c": 3, "b": 2}]}])
            # Upstream (insertion order): '1,3,2,4\n4,2,3,1\n'
            assert msgs[0]["payload"] == "4,3,2,1\n1,2,3,4\n"

        @pytest.mark.asyncio
        @pytest.mark.it('should convert an array of objects to a multi-line csv without a template and with a header')
        async def test_should_convert_an_array_of_objects_to_a_multi_line_csv_without_template_and_with_header(self):
            msgs = await _csv({"temp": "", "hdrout": "all"},
                              [{"payload": [{"d": 1, "b": 3, "c": 2, "a": 4}, {"d": 4, "a": 1, "c": 3, "b": "f\ng"}]}])
            # Upstream (insertion order): 'd,b,c,a\n1,3,2,4\n4,"f\ng",3,1\n'
            assert msgs[0]["payload"] == 'a,b,c,d\n4,3,2,1\n1,"f\ng",3,4\n'

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple array back to a csv')
        async def test_should_convert_a_simple_array_back_to_a_csv(self):
            msgs = await _csv({"temp": "a,b,c,d"}, [{"payload": ["", 0, 1, "foo", 'ba"r', "di,ng", "fa\nba"]}])
            assert msgs[0]["payload"] == ',0,1,foo,"ba""r","di,ng","fa\nba"\n'

        @pytest.mark.asyncio
        @pytest.mark.it('should convert an array of arrays back to a multi-line csv')
        async def test_should_convert_an_array_of_arrays_back_to_a_multi_line_csv(self):
            msgs = await _csv({"temp": "a,b,c,d"}, [{"payload": [[0, 1, 2, 3, 4], [4, 3, 2, 1, 0]]}])
            assert msgs[0]["payload"] == "0,1,2,3,4\n4,3,2,1,0\n"

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to include column names as first row')
        async def test_should_be_able_to_include_column_names_as_first_row(self):
            msgs = await _csv({"temp": "a,b,c,d", "hdrout": True, "ret": "\r\n"},
                              [{"payload": {"d": 1, "b": 3, "c": 2, "a": 4}}])
            assert msgs[0]["payload"] == "a,b,c,d\r\n4,3,2,1\r\n"

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to include column names as first row, and missing properties')
        async def test_should_be_able_to_include_column_names_as_first_row_and_missing_properties(self):
            msgs = await _csv({"hdrout": True, "ret": "\r\n"}, [
                {"payload": [
                    {"col1": "H1", "col2": "H2", "col3": "H3", "col4": "H4"},
                    {"col1": "A", "col2": "B"},
                    {"col1": "A", "col3": "C"},
                    {"col1": "A", "col4": "D\nE"},
                ]},
            ])
            assert msgs[0]["payload"] == 'col1,col2,col3,col4\r\nH1,H2,H3,H4\r\nA,B,,\r\nA,,C,\r\nA,,,"D\nE"\r\n'

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to pass in column names')
        async def test_should_be_able_to_pass_in_column_names(self):
            node = {"type": "csv", "temp": "", "hdrout": "once", "ret": "\r\n"}
            obj = {"d": 1, "b": 3, "c": 2, "a": 4}
            injections = [
                {"payload": obj, "columns": "a,,b,a", "parts": {"index": 0}},
                {"payload": obj, "parts": {"index": 1}},
                {"payload": obj, "parts": {"index": 2}},
            ]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 3)
            assert msgs[0]["payload"] == "a,,b,a\r\n4,,3,4\r\n"
            assert msgs[2]["payload"] == "4,,3,4\r\n"

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to pass in column names - with payload as an array')
        async def test_should_be_able_to_pass_in_column_names_with_payload_as_an_array(self):
            obj = {"d": 1, "b": 3, "c": 2, "a": 4}
            msgs = await _csv({"hdrout": "once", "ret": "\r\n"},
                              [{"payload": [obj, obj, obj], "columns": "a,,b,a"}])
            assert msgs[0]["payload"] == "a,,b,a\r\n4,,3,4\r\n4,,3,4\r\n4,,3,4\r\n"

        @pytest.mark.asyncio
        @pytest.mark.it('should handle quotes and sub-properties')
        async def test_should_handle_quotes_and_sub_properties(self):
            msgs = await _csv({"temp": "a,b,c,d"}, [
                {"payload": {"d": {"sub": "object"}, "b": "text,with,commas", "c": 'This "is" a banana',
                             "a": {}}},
            ])
            assert msgs[0]["payload"] == '{},"text,with,commas","This ""is"" a banana","{""sub"":""object""}"\n'

    @pytest.mark.asyncio
    @pytest.mark.it('should just pass through if no payload provided')
    async def test_should_just_pass_through_if_no_payload_provided(self):
        msgs = await _csv({"temp": "a,b,c,d"}, [{"topic": {"a": 4, "b": 3, "c": 2, "d": 1}}])
        assert msgs[0]["topic"] == {"a": 4, "b": 3, "c": 2, "d": 1}
        assert "payload" not in msgs[0]

    @pytest.mark.asyncio
    @pytest.mark.it('should warn if provided a number or boolean')
    async def test_should_warn_if_provided_a_number_or_boolean(self):
        node = {"type": "csv", "temp": "a,b,c,d"}
        # Neither a number nor a boolean is a string or an object, so nothing is emitted: the
        # harness reports "Timed out" and the warnings are read from the node log.
        with pytest.raises(RuntimeError):
            await run_single_node_with_msgs_ntimes(node, [{"payload": 1}, {"payload": True}], 1, timeout=0.3)
        logs = take_node_logs()
        assert len(logs) == 2
        assert all(entry["level"] == "WARN" and entry["type"] == "csv" for entry in logs)

    @pytest.mark.asyncio
    @pytest.mark.it('should call done when message processing is completed')
    async def test_should_call_done_when_message_processing_is_completed(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "csv", "temp": "a,b,c,d", "wires": [[]]},
            {"id": "2", "z": "100", "type": "complete", "scope": ["1"], "uncaught": False, "wires": [["3"]]},
            {"id": "3", "z": "100", "type": "test-once"},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, [{"nid": "1", "msg": {"payload": "1,2,3,4"}}], 1)
        assert msgs[0]["payload"] == "1,2,3,4"

    @pytest.mark.asyncio
    @pytest.mark.it('should call done when input causes an error')
    async def test_should_call_done_when_input_causes_an_error(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "csv", "temp": "a,b,c,d", "wires": [[]]},
            {"id": "2", "z": "100", "type": "complete", "scope": ["1"], "uncaught": False, "wires": [["3"]]},
            {"id": "3", "z": "100", "type": "test-once"},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, [{"nid": "1", "msg": {"payload": 1}}], 1)
        assert msgs[0]["payload"] == 1


# ---------------------------------------------------------------------------------------------------
# RFC4180 mode (`spec: "rfc"`), the `CSV node (RFC Mode)` describe of the same upstream file.
#
# Where the two modes differ, the RFC tests expect RFC4180 behaviour: spaces are part of a field, the
# template is parsed strictly, the default line ending is CRLF, and a payload that is neither a
# string nor an object is a catchable error instead of a warning.
# ---------------------------------------------------------------------------------------------------


async def _csv_rfc(node_extra, msgs_in, nexpected=None, timeout=3):
    """Run messages through an RFC4180 mode `csv` node (`_csv` with `spec: "rfc"`)."""
    return await _csv({"spec": "rfc", **node_extra}, msgs_in, nexpected, timeout)


def _csv_statuses() -> list[dict]:
    """The `{fill, shape, text}` statuses the node reported during the last run."""
    return [entry["status"] for entry in take_status_messages()]


def _csv_events() -> list[dict]:
    """The `node.log()`/`node.warn()`/`node.error()` events the node produced, oldest first."""
    return [entry for entry in take_node_logs() if entry["type"] == "csv"]


@pytest.mark.describe('CSV node (RFC Mode)')
class TestCsvNodeRfcMode:

    @pytest.mark.skip(reason="the spec asserts on the deployed node's own properties, which the "
                             "pytest bridge cannot read back: it only observes messages")
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded with defaults')
    async def test_should_be_loaded_with_defaults(self):
        pass

    @pytest.mark.describe('csv to json')
    class TestCsvToJson:

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple csv string to a javascript object')
        async def test_should_convert_a_simple_csv_string_to_a_javascript_object(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d"}, "1,2,3,4" + "\n")
            assert msgs[0]["payload"] == {"a": 1, "b": 2, "c": 3, "d": 4}
            assert msgs[0]["columns"] == "a,b,c,d"
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple string to a javascript object with | separator (no template)')
        async def test_should_convert_a_simple_string_to_a_javascript_object_with_pipe_separator_no_template(self):
            msgs = await _csv_rfc({"sep": "|"}, "1|2|3|4" + "\n")
            assert msgs[0]["payload"] == {"col1": 1, "col2": 2, "col3": 3, "col4": 4}
            assert msgs[0]["columns"] == "col1,col2,col3,col4"
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple string to a javascript object with tab separator (with template)')
        async def test_should_convert_a_simple_string_to_a_javascript_object_with_tab_separator_with_template(self):
            msgs = await _csv_rfc({"sep": "\t", "temp": "A,B,,D"}, "1\t2\t3\t4" + "\n")
            assert msgs[0]["payload"] == {"A": 1, "B": 2, "D": 4}
            assert msgs[0]["columns"] == "A,B,D"
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple string to a javascript object with space separator '
                       '(with spaced template)')
        async def test_should_convert_a_simple_string_with_space_separator_and_spaced_template(self):
            # RFC-vs-Legacy difference: spaces belong to the field (RFC4180 2.4), so the template
            # keeps its leading spaces and the space-only column is a real one.
            msgs = await _csv_rfc({"sep": " ", "temp": "A, B, , D"}, "1 2 3 4" + "\n")
            assert msgs[0]["payload"] == {"A": 1, " B": 2, " ": 3, " D": 4}
            assert msgs[0]["columns"] == "A, B, , D"
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should not remove quotes and whitespace from template - should set status and send warning')
        async def test_should_not_remove_quotes_and_whitespace_from_template(self):
            # The spec deploys a good template and then redeploys the bad one; the bridge deploys
            # once and the node handles an unparsable template the same way either way: it warns,
            # reports `csv.errors.bad_template` and never handles an input.
            flows = [
                {"id": "100", "type": "tab"},
                {"id": "1", "z": "100", "type": "csv", "spec": "rfc", "temp": '"a",  "b" , " c "," d  " ',
                 "ret": "\n", "wires": [["2"]]},
                {"id": "2", "z": "100", "type": "test-once"},
            ]
            with pytest.raises(RuntimeError):
                await run_flow_with_msgs_ntimes(flows, [{"nid": "1", "msg": {"payload": "1,2,3,4\n"}}], 1,
                                                timeout=0.3)
            assert _csv_statuses() == [{"fill": "red", "shape": "dot", "text": "csv.errors.bad_template"}]
            events = _csv_events()
            assert [event["level"] for event in events] == ["WARN"]
            assert [event["msg"] for event in events] == ["csv.errors.bad_template"]

        @pytest.mark.asyncio
        @pytest.mark.it('should create column names if no template provided')
        async def test_should_create_column_names_if_no_template_provided(self):
            msgs = await _csv_rfc({"temp": ""}, "1,2,3,4" + "\n")
            assert msgs[0]["payload"] == {"col1": 1, "col2": 2, "col3": 3, "col4": 4}
            assert msgs[0]["columns"] == "col1,col2,col3,col4"
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow dropping of fields from the template')
        async def test_should_allow_dropping_of_fields_from_the_template(self):
            msgs = await _csv_rfc({"temp": "a,,,d"}, "1,2,3,4" + "\n")
            assert msgs[0]["payload"] == {"a": 1, "d": 4}
            assert msgs[0]["columns"] == "a,d"
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow commas and spaces in the template')
        async def test_should_allow_commas_and_spaces_in_the_template(self):
            # RFC-vs-Legacy difference: the spaced and quoted column names are kept as they are.
            msgs = await _csv_rfc({"temp": 'a,b b,"c,c"," d, d "'}, "1,2,3,4" + "\n")
            assert msgs[0]["payload"] == {"a": 1, "b b": 2, "c,c": 3, " d, d ": 4}
            assert msgs[0]["columns"] == 'a,b b,"c,c"," d, d "'
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow passing in a template as first line of CSV')
        async def test_should_allow_passing_in_a_template_as_first_line_of_csv(self):
            msgs = await _csv_rfc({"temp": "", "hdrin": True},
                                  'a,b b,"c,c"," d, d "' + "\n" + "1,2,3,4" + "\n")
            assert msgs[0]["payload"] == {"a": 1, "b b": 2, "c,c": 3, " d, d ": 4}
            assert msgs[0]["columns"] == 'a,b b,"c,c"," d, d "'
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow passing in a template as first line of CSV (not comma)')
        async def test_should_allow_passing_in_a_template_as_first_line_of_csv_not_comma(self):
            msgs = await _csv_rfc({"temp": "", "hdrin": True, "sep": ";"},
                                  'a;b b;"c;c";" d, d "' + "\n" + "1;2;3;4" + "\n")
            assert msgs[0]["payload"] == {"a": 1, "b b": 2, "c;c": 3, " d, d ": 4}
            assert msgs[0]["columns"] == 'a,b b,c;c," d, d "'
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow passing in a template as first line of CSV (special char /)')
        async def test_should_allow_passing_in_a_template_as_first_line_of_csv_special_char_slash(self):
            msgs = await _csv_rfc({"temp": "", "hdrin": True, "sep": "/"},
                                  'a/b b/"c/c"/" d, d "' + "\n" + "1/2/3/4" + "\n")
            assert msgs[0]["payload"] == {"a": 1, "b b": 2, "c/c": 3, " d, d ": 4}
            assert msgs[0]["columns"] == 'a,b b,c/c," d, d "'
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow passing in a template as first line of CSV (special char \\)')
        async def test_should_allow_passing_in_a_template_as_first_line_of_csv_special_char_backslash(self):
            msgs = await _csv_rfc({"temp": "", "hdrin": True, "sep": "\\"},
                                  'a\\b b\\"c\\c"\\" d, d "' + "\n" + "1\\2\\3\\4" + "\n")
            assert msgs[0]["payload"] == {"a": 1, "b b": 2, "c\\c": 3, " d, d ": 4}
            assert msgs[0]["columns"] == 'a,b b,c\\c," d, d "'
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should leave numbers starting with 0, e and + as strings (except 0.)')
        async def test_should_leave_numbers_starting_with_0_e_and_plus_as_strings(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d,e,f,g"}, "123,0123,+123,e123,E123,-123" + "\n")
            assert msgs[0]["payload"] == {"a": 123, "b": "0123", "c": "+123", "d": "e123", "e": "E123", "f": -123}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should not parse numbers when told not to do so')
        async def test_should_not_parse_numbers_when_told_not_to_do_so(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d,e,f,g", "strings": False},
                                  "1.23,0123,+123,e123,0,-123,1e3" + "\n")
            assert msgs[0]["payload"] == {
                "a": "1.23", "b": "0123", "c": "+123", "d": "e123", "e": "0", "f": "-123", "g": "1e3",
            }
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should parse numbers when told to do so')
        async def test_should_parse_numbers_when_told_to_do_so(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d,e,f,g"}, " 1.23 ,  -123,1e3 ,    0  " + "\n")
            assert msgs[0]["payload"] == {"a": 1.23, "b": -123, "c": 1000, "d": 0}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should leave handle strings with scientific notation as numbers')
        async def test_should_leave_handle_strings_with_scientific_notation_as_numbers(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d,e,f,g"}, "12E3,12e-3,-12e3,-12E-3" + "\n")
            assert msgs[0]["payload"] == {"a": 12000, "b": 0.012, "c": -12000, "d": -0.012}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow quotes in the input (but drop blank strings)')
        async def test_should_allow_quotes_in_the_input_but_drop_blank_strings(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d,e,f,g,h"},
                                  '"1","-2","+3","04","","-05","ab""cd","with,a,comma"' + "\n")
            assert msgs[0]["payload"] == {
                "a": 1, "b": -2, "c": "+3", "d": "04", "f": "-05", "g": 'ab"cd', "h": "with,a,comma",
            }
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow blank strings in the input if selected')
        async def test_should_allow_blank_strings_in_the_input_if_selected(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d,e,f,g", "include_empty_strings": True},
                                  '"1","","","","-05","ab""cd","with,a,comma"' + "\n")
            assert msgs[0]["payload"] == {
                "a": 1, "b": "", "c": "", "d": "", "e": "-05", "f": 'ab"cd', "g": "with,a,comma",
            }

        @pytest.mark.asyncio
        @pytest.mark.it('should allow missing columns (nulls) in the input if selected')
        async def test_should_allow_missing_columns_nulls_in_the_input_if_selected(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d,e,f,g", "include_null_values": True},
                                  '"1",,"+3",,"-05","ab""cd","with,a,comma"' + "\n")
            assert msgs[0]["payload"] == {
                "a": 1, "b": None, "c": "+3", "d": None, "e": "-05", "f": 'ab"cd', "g": "with,a,comma",
            }
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should handle cr and lf in the input')
        async def test_should_handle_cr_and_lf_in_the_input(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d,e,f,g"},
                                  '"with a\nnew line","and a\rcarriage return","and why\r\nnot both"' + "\n")
            assert msgs[0]["payload"] == {
                "a": "with a\nnew line", "b": "and a\rcarriage return", "c": "and why\r\nnot both",
            }
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should recover from an odd number of quotes in the input')
        async def test_should_recover_from_an_odd_number_of_quotes_in_the_input(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d,e,f,g"},
                                  ['"with,a"n,odd","num"ber","of"qu"ot"es"' + "\n",
                                   '"this is","a normal","line"'], nexpected=2)
            assert msgs[0]["payload"] == {"a": 'with,a"n', "b": "odd", "c": 'num"ber', "d": 'of"qu"ot"es'}
            _check_parts(msgs[0], 0, 1)
            assert msgs[1]["payload"] == {"a": "this is", "b": "a normal", "c": "line"}
            _check_parts(msgs[1], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should handle newlines in the input data')
        async def test_should_handle_newlines_in_the_input_data(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d,e,f,g"},
                                  'ay,be,"c has 2\nnew\nlines",dee,eee,eff,gee')
            assert msgs[0]["payload"] == {
                "a": "ay", "b": "be", "c": "c has 2\nnew\nlines", "d": "dee", "e": "eee", "f": "eff", "g": "gee",
            }
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to use the first line as a template')
        async def test_should_be_able_to_use_the_first_line_as_a_template(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d", "hdrin": True},
                                  "w,x,y,z\n1,2,3,4\n\n5,6,7,8", nexpected=2)
            assert msgs[0]["payload"] == {"w": 1, "x": 2, "y": 3, "z": 4}
            _check_parts(msgs[0], 0, 2)
            assert msgs[1]["payload"] == {"w": 5, "x": 6, "y": 7, "z": 8}
            _check_parts(msgs[1], 1, 2)

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to output multiple lines as one array')
        async def test_should_be_able_to_output_multiple_lines_as_one_array(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d", "multi": "yes"},
                                  "1,2,3,4\n5,-6,07,+8\n9,0,a,b\nc,d,e,f")
            assert msgs[0]["payload"] == [
                {"a": 1, "b": 2, "c": 3, "d": 4},
                {"a": 5, "b": -6, "c": "07", "d": "+8"},
                {"a": 9, "b": 0, "c": "a", "d": "b"},
                {"a": "c", "b": "d", "c": "e", "d": "f"},
            ]
            assert msgs[0]["columns"] == "a,b,c,d"
            assert "parts" not in msgs[0]

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to create an array from multiple parts')
        async def test_should_be_able_to_create_an_array_from_multiple_parts(self):
            injections = [
                {"payload": "a,b,c", "parts": {"index": 0, "ch": "\n", "type": "string", "id": "1"}},
                {"payload": "1,2,3", "parts": {"index": 1, "ch": "\n", "type": "string", "id": "1"}},
                {"payload": "4,5,6", "parts": {"index": 2, "ch": "\n", "type": "string", "id": "1"}},
                {"payload": "7,8,9",
                 "parts": {"index": 3, "count": 4, "ch": "\n", "type": "string", "id": "1"}},
            ]
            msgs = await _csv_rfc({"temp": "", "hdrin": True, "multi": "mult"}, injections, nexpected=1)
            assert msgs[0]["payload"] == [
                {"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}, {"a": 7, "b": 8, "c": 9},
            ]
            assert msgs[0]["columns"] == "a,b,c"
            assert "parts" not in msgs[0]

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to output multiple objects as an array from an input of parts')
        async def test_should_be_able_to_output_multiple_objects_as_an_array_from_an_input_of_parts(self):
            injection = {
                "payload": "Col1,Col2\nV1,V2\nV3,V4\nV5,V6", "topic": "",
                "parts": {"id": "3af07e18.865652", "type": "array", "count": 2, "len": 1, "index": 0},
            }
            msgs = await _csv_rfc({"temp": "", "hdrin": True, "multi": "yes"}, injection, nexpected=1)
            assert msgs[0]["payload"] == [
                {"Col1": "V1", "Col2": "V2"}, {"Col1": "V3", "Col2": "V4"}, {"Col1": "V5", "Col2": "V6"},
            ]
            assert msgs[0]["columns"] == "Col1,Col2"
            assert "parts" in msgs[0]

        @pytest.mark.asyncio
        @pytest.mark.it('should handle numbers in strings but not IP addresses')
        async def test_should_handle_numbers_in_strings_but_not_ip_addresses(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d,e"}, "a,127.0.0.1,56.7,-32.8,+76.22C")
            assert msgs[0]["payload"] == {"a": "a", "b": "127.0.0.1", "c": 56.7, "d": -32.8, "e": "+76.22C"}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should preserve parts property')
        async def test_should_preserve_parts_property(self):
            injection = {"payload": "1,2,3,4" + "\n", "parts": {"id": "X", "index": 3, "count": 4}}
            msgs = await _csv_rfc({"temp": "a,b,c,d"}, injection)
            assert msgs[0]["payload"] == {"a": 1, "b": 2, "c": 3, "d": 4}
            _check_parts(msgs[0], 3, 4)
            assert msgs[0]["parts"]["id"] == "X"

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to use the first of multiple parts as a template if parts are present')
        async def test_should_be_able_to_use_the_first_of_multiple_parts_as_a_template(self):
            injections = [
                {"payload": "w,x,y,z\n", "parts": {"id": "X", "index": 0, "count": 3}},
                {"payload": "1,2,3,4\n", "parts": {"id": "X", "index": 1, "count": 3}},
                {"payload": "5,6,7,8\n", "parts": {"id": "X", "index": 2, "count": 3}},
            ]
            msgs = await _csv_rfc({"temp": "", "hdrin": True}, injections, nexpected=2)
            assert msgs[0]["payload"] == {"w": 1, "x": 2, "y": 3, "z": 4}
            _check_parts(msgs[0], 0, 2)
            assert msgs[1]["payload"] == {"w": 5, "x": 6, "y": 7, "z": 8}
            _check_parts(msgs[1], 1, 2)

        @pytest.mark.asyncio
        @pytest.mark.it('should skip several lines from start if requested')
        async def test_should_skip_several_lines_from_start_if_requested(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d", "skip": 2}, "1,2,3,4\n5,6,7,8\n9,0,A,B\n")
            assert msgs[0]["payload"] == {"a": 9, "b": 0, "c": "A", "d": "B"}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should skip several lines from start then use next line as a template')
        async def test_should_skip_several_lines_from_start_then_use_next_line_as_a_template(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d", "hdrin": True, "skip": 2},
                                  "1,2,3,4\n5,6,7,8\n9,0,A,B\nC,D,E,F\n")
            assert msgs[0]["payload"] == {"9": "C", "0": "D", "A": "E", "B": "F"}
            _check_parts(msgs[0], 0, 1)

        @pytest.mark.asyncio
        @pytest.mark.it('should skip several lines from start and correct parts')
        async def test_should_skip_several_lines_from_start_and_correct_parts(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d", "skip": 2},
                                  "1,2,3,4\n5,6,7,8\n9,0,A,B\nC,D,E,F\n", nexpected=2)
            assert msgs[0]["payload"] == {"a": 9, "b": 0, "c": "A", "d": "B"}
            _check_parts(msgs[0], 0, 2)
            assert msgs[1]["payload"] == {"a": "C", "b": "D", "c": "E", "d": "F"}
            _check_parts(msgs[1], 1, 2)

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to skip and then use the first of multiple parts as a template '
                       'if parts are present')
        async def test_should_be_able_to_skip_and_then_use_the_first_of_multiple_parts_as_a_template(self):
            injections = [
                {"payload": "foo\n", "parts": {"id": "X", "index": 0, "count": 5}},
                {"payload": "bar\n", "parts": {"id": "X", "index": 1, "count": 5}},
                {"payload": "w,x,y,z\n", "parts": {"id": "X", "index": 2, "count": 5}},
                {"payload": "1,2,3,4\n", "parts": {"id": "X", "index": 3, "count": 5}},
                {"payload": "5,6,7,8\n", "parts": {"id": "X", "index": 4, "count": 5}},
            ]
            msgs = await _csv_rfc({"temp": "", "hdrin": True, "skip": 2}, injections, nexpected=2)
            assert msgs[0]["payload"] == {"w": 1, "x": 2, "y": 3, "z": 4}
            assert msgs[0]["columns"] == "w,x,y,z"
            _check_parts(msgs[0], 0, 2)
            assert msgs[1]["payload"] == {"w": 5, "x": 6, "y": 7, "z": 8}
            assert msgs[1]["columns"] == "w,x,y,z"
            _check_parts(msgs[1], 1, 2)

    @pytest.mark.describe('json object to csv')
    class TestJsonToCsv:

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple object back to a csv')
        async def test_should_convert_a_simple_object_back_to_a_csv(self):
            # The default line ending of RFC mode is CRLF (RFC4180 2.1).
            message = {
                "payload": {"e": 0, "d": 1, "b": "foo", "c": True, "a": 4, "f": "Hello\nWorld",
                            "i": "undefined", "j": None, "k": "null"},
            }
            msgs = await _csv_rfc({"temp": "a,b,c,,e,f,g,h,i,j,k"}, message)
            assert msgs[0]["payload"] == '4,foo,true,,0,"Hello\nWorld",,,undefined,null,null\r\n'

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple object back to a csv with no template')
        async def test_should_convert_a_simple_object_back_to_a_csv_with_no_template(self):
            # Two differences from upstream's expectation, both in the runtime rather than in the
            # encoder: this runtime's message objects are sorted maps, so the cells follow the sorted
            # property names instead of JavaScript's insertion order, and a JSON message has no
            # `undefined` for the `e` property (a missing key is not a column here at all).
            message = {"payload": {"d": 1, "b": "foo", "c": 'ba"r', "a": "di,ng", "f": "undefined",
                                   "g": None, "h": "null"}}
            msgs = await _csv_rfc({"temp": ""}, message)
            assert msgs[0]["payload"] == '"di,ng",foo,"ba""r",1,undefined,null\r\n'

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple object back to a tsv using a tab as a separator')
        async def test_should_convert_a_simple_object_back_to_a_tsv_using_a_tab_as_a_separator(self):
            # The property order is the sorted one (see the note above); a comma does not need
            # quoting when the separator is a tab.
            msgs = await _csv_rfc({"temp": "", "sep": "\t", "ret": "\n"},
                                  {"payload": {"d": 1, "b": "foo", "c": 'ba"r', "a": "di,ng"}})
            assert msgs[0]["payload"] == 'di,ng\tfoo\t"ba""r"\t1\n'
            assert msgs[0]["columns"] == "a,b,c,d"

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple object back to a tsv with headers using a tab as a separator')
        async def test_should_convert_a_simple_object_back_to_a_tsv_with_headers_using_a_tab_as_a_separator(self):
            msgs = await _csv_rfc({"temp": "", "sep": "\t", "ret": "\n", "hdrout": "all"},
                                  {"payload": {"d": 1, "b": "foo", "c": 'ba"r', "a": "di,ng"}})
            assert msgs[0]["payload"] == 'a\tb\tc\td\ndi,ng\tfoo\t"ba""r"\t1\n'
            assert msgs[0]["columns"] == "a,b,c,d"

        @pytest.mark.asyncio
        @pytest.mark.it('should handle a template with spaces in the property names')
        async def test_should_handle_a_template_with_spaces_in_the_property_names(self):
            msgs = await _csv_rfc({"temp": "a,b o,c p,,e", "ret": "\n"},
                                  {"payload": {"e": 0, "d": 1, "b o": "foo", "c p": True, "a": 4}})
            assert msgs[0]["payload"] == "4,foo,true,,0\n"
            assert msgs[0]["columns"] == "a,b o,c p,e"

        @pytest.mark.asyncio
        @pytest.mark.it('should handle a template with quotes in the property names')
        async def test_should_handle_a_template_with_quotes_in_the_property_names(self):
            msgs = await _csv_rfc({"temp": "", "hdrout": "all", "ret": "\n"},
                                  {"payload": [{"a\"a": "A1", "b'b": "B1"}, {"a\"a": "A2", "b'b": "B2"}]})
            assert msgs[0]["payload"] == '"a""a",b\'b\nA1,B1\nA2,B2\n'
            assert msgs[0]["columns"] == '"a""a",b\'b'

        @pytest.mark.asyncio
        @pytest.mark.it('should convert an array of objects to a multi-line csv')
        async def test_should_convert_an_array_of_objects_to_a_multi_line_csv(self):
            msgs = await _csv_rfc({"temp": "a,d,c,b", "ret": "\n"},
                                  {"payload": [{"d": 1, "b": 3, "c": 2, "a": 4}, {"d": 4, "a": 1, "c": 3, "b": 2}]})
            assert msgs[0]["payload"] == "4,1,2,3\n1,4,3,2\n"

        @pytest.mark.asyncio
        @pytest.mark.it('should convert an array of objects to a multi-line csv and add a header')
        async def test_should_convert_an_array_of_objects_to_a_multi_line_csv_and_add_a_header(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d", "hdrout": "all", "ret": "\n"},
                                  {"payload": [{"d": 1, "b": 3, "c": 2, "a": 4}, {"d": "a\nb", "a": 1, "c": 3, "b": 2}]})
            assert msgs[0]["payload"] == 'a,b,c,d\n4,3,2,1\n1,2,3,"a\nb"\n'

        @pytest.mark.asyncio
        @pytest.mark.it('should convert an array of objects to a multi-line csv without a template')
        async def test_should_convert_an_array_of_objects_to_a_multi_line_csv_without_a_template(self):
            # The template comes from the first row's property names, which this runtime keeps sorted.
            msgs = await _csv_rfc({"temp": "", "ret": "\n"},
                                  {"payload": [{"d": 1, "b": 3, "c": 2, "a": 4}, {"d": 4, "a": 1, "c": 3, "b": 2}]})
            assert msgs[0]["payload"] == "4,3,2,1\n1,2,3,4\n"
            assert msgs[0]["columns"] == "a,b,c,d"

        @pytest.mark.asyncio
        @pytest.mark.it('should convert an array of objects to a multi-line csv without a template and with a header')
        async def test_should_convert_an_array_of_objects_without_a_template_and_with_a_header(self):
            msgs = await _csv_rfc({"temp": "", "hdrout": "all", "ret": "\n"},
                                  {"payload": [{"d": 1, "b": 3, "c": 2, "a": 4},
                                               {"d": 4, "a": 1, "c": 3, "b": "f\ng"}]})
            assert msgs[0]["payload"] == 'a,b,c,d\n4,3,2,1\n1,"f\ng",3,4\n'
            assert msgs[0]["columns"] == "a,b,c,d"

        @pytest.mark.asyncio
        @pytest.mark.it('should convert a simple array back to a csv')
        async def test_should_convert_a_simple_array_back_to_a_csv(self):
            # A template with four columns truncates the seven values of the row.
            msgs = await _csv_rfc({"temp": "a,b,c,d", "ret": "\n"},
                                  {"payload": ["", 0, 1, "foo", 'ba"r', "di,ng", "fa\nba"]})
            assert msgs[0]["payload"] == ",0,1,foo\n"
            assert msgs[0]["columns"] == "a,b,c,d"

        @pytest.mark.asyncio
        @pytest.mark.it('should convert an array of arrays back to a multi-line csv')
        async def test_should_convert_an_array_of_arrays_back_to_a_multi_line_csv(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d", "ret": "\n"},
                                  {"payload": [[0, 1, 2, 3, 4], [4, 3, 2, 1, 0]]})
            assert msgs[0]["payload"] == "0,1,2,3\n4,3,2,1\n"

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to include column names as first row')
        async def test_should_be_able_to_include_column_names_as_first_row(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d", "hdrout": True, "ret": "\r\n"},
                                  {"payload": [{"d": 1, "b": 3, "c": 2, "a": 4}]})
            assert msgs[0]["payload"] == "a,b,c,d\r\n4,3,2,1\r\n"

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to include column names as first row, and missing properties')
        async def test_should_be_able_to_include_column_names_as_first_row_and_missing_properties(self):
            msgs = await _csv_rfc({"temp": "", "hdrout": True, "ret": "\r\n"},
                                  {"payload": [{"col1": "H1", "col2": "H2", "col3": "H3", "col4": "H4"},
                                               {"col1": "A", "col2": "B"},
                                               {"col1": "A", "col3": "C"},
                                               {"col1": "A", "col4": "D\nE"}]})
            assert msgs[0]["payload"] == 'col1,col2,col3,col4\r\nH1,H2,H3,H4\r\nA,B,,\r\nA,,C,\r\nA,,,"D\nE"\r\n'

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to pass in column names')
        async def test_should_be_able_to_pass_in_column_names(self):
            injections = [
                {"payload": [{"d": 1, "b": 3, "c": 2, "a": 4}], "columns": "a,,b,a", "parts": {"index": 0}},
                {"payload": [{"d": 1, "b": 3, "c": 2, "a": 4}], "parts": {"index": 1}},
                {"payload": [{"d": 1, "b": 3, "c": 2, "a": 4}], "parts": {"index": 2}},
            ]
            msgs = await _csv_rfc({"temp": "", "hdrout": "once", "ret": "\r\n"}, injections, nexpected=3)
            assert msgs[0]["payload"] == "a,,b,a\r\n4,,3,4\r\n"
            assert msgs[2]["payload"] == "4,,3,4\r\n"

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to pass in column names - with payload as an array')
        async def test_should_be_able_to_pass_in_column_names_with_payload_as_an_array(self):
            row = {"d": 1, "b": 3, "c": 2, "a": 4}
            msgs = await _csv_rfc({"temp": "", "hdrout": "once", "ret": "\r\n"},
                                  {"payload": [row, row, row], "columns": "a,,b,a"})
            assert msgs[0]["payload"] == "a,,b,a\r\n4,,3,4\r\n4,,3,4\r\n4,,3,4\r\n"

        @pytest.mark.asyncio
        @pytest.mark.it('should handle quotes and sub-properties')
        async def test_should_handle_quotes_and_sub_properties(self):
            msgs = await _csv_rfc({"temp": "a,b,c,d", "ret": "\n"},
                                  {"payload": {"d": {"sub": "object"}, "b": "text,with,commas",
                                               "c": 'This "is" a banana', "a": {}}})
            assert msgs[0]["payload"] == '{},"text,with,commas","This ""is"" a banana","{""sub"":""object""}"\n'
            assert msgs[0]["columns"] == "a,b,c,d"

    @pytest.mark.asyncio
    @pytest.mark.it('should just pass through if no payload provided')
    async def test_should_just_pass_through_if_no_payload_provided(self):
        msgs = await _csv_rfc({"temp": "a,b,c,d"}, [{"topic": {"a": 4, "b": 3, "c": 2, "d": 1}}])
        assert msgs[0]["topic"] == {"a": 4, "b": 3, "c": 2, "d": 1}
        assert "payload" not in msgs[0]

    @pytest.mark.asyncio
    @pytest.mark.it('should warn if provided a number or boolean')
    async def test_should_warn_if_provided_a_number_or_boolean(self):
        # RFC-vs-legacy difference: the payload is neither a string nor an object, so the node
        # reports it as an error and emits nothing.
        node = {"type": "csv", "spec": "rfc", "temp": "a,b,c,d"}
        with pytest.raises(RuntimeError):
            await run_single_node_with_msgs_ntimes(node, [{"payload": 1}, {"payload": True}], 1, timeout=0.3)
        events = _csv_events()
        assert len(events) == 2
        assert [event["msg"] for event in events] == ["csv.errors.csv_js", "csv.errors.csv_js"]

    @pytest.mark.asyncio
    @pytest.mark.it('should call done when message processing is completed')
    async def test_should_call_done_when_message_processing_is_completed(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "csv", "spec": "rfc", "temp": "a,b,c,d", "wires": [[]]},
            {"id": "2", "z": "100", "type": "complete", "scope": ["1"], "uncaught": False, "wires": [["3"]]},
            {"id": "3", "z": "100", "type": "test-once"},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, [{"nid": "1", "msg": {"payload": "1,2,3,4"}}], 1)
        assert msgs[0]["payload"] == "1,2,3,4"

    @pytest.mark.asyncio
    @pytest.mark.it('should not call done or pass the bad msg through when input causes an error - '
                   'should throw error and set status')
    async def test_should_not_call_done_or_pass_the_bad_msg_through_when_input_causes_an_error(self):
        # RFC-vs-legacy difference: the message completes as an error instead of being passed on, so
        # the `complete` node never sees it, the status turns red and the error is reported.
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "csv", "spec": "rfc", "temp": "a,b,c,d", "wires": [[]]},
            {"id": "2", "z": "100", "type": "complete", "scope": ["1"], "uncaught": False, "wires": [["3"]]},
            {"id": "3", "z": "100", "type": "test-once"},
        ]
        with pytest.raises(RuntimeError):
            await run_flow_with_msgs_ntimes(flows, [{"nid": "1", "msg": {"payload": 1}}], 1, timeout=0.3)
        assert _csv_statuses() == [{"fill": "red", "shape": "dot", "text": "csv.errors.csv_js"}]
        events = _csv_events()
        assert [event["level"] for event in events] == ["ERROR"]
        assert [event["msg"] for event in events] == ["csv.errors.csv_js"]
