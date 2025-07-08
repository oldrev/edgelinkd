import json
import pytest

from .. import *


@pytest.mark.describe('subflow')
class TestSubflow:

    @pytest.mark.asyncio
    @pytest.mark.it('''should define subflow''')
    async def test_0001(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "subflow:200", "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once", "wires": []},
            # Subflow
            {"id": "200", "type": "subflow", "name": "Subflow", "info": "", "in": [
                {"wires": [{"id": "3"}]}], "out": [{"wires": [{"id": "3", "port": 0}]}]},
            {"id": "3", "z": "200", "type": "function",
                "func": "return msg;", "wires": []}
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "hello"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["payload"] == "hello"

    @pytest.mark.asyncio
    @pytest.mark.it('should pass data to/from subflow')
    async def test_it_should_pass_data_to_from_subflow(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "subflow:200", "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once", "wires": []},
            # Subflow
            {"id": "200", "type": "subflow", "name": "Subflow", "info": "", "in": [
                {"wires": [{"id": "3"}]}], "out": [{"wires": [{"id": "3", "port": 0}]}]},
            {"id": "3", "z": "200", "type": "function",
                "func": "msg.payload = msg.payload+'bar'; return msg;", "wires": []}
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["payload"] == "foobar"

    @pytest.mark.asyncio
    @pytest.mark.it('''should pass data to/from nested subflow''')
    async def test_0003(self):
        flows = [
            {"id": "100", "type": "tab", "info": ""},
            {"id": "1", "z": "100", "type": "subflow:200", "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once", "wires": []},
            # Subflow1
            {"id": "200", "type": "subflow", "name": "Subflow1", "info": "",
             "in": [{"wires": [{"id": "3"}]}],
             "out": [{"wires": [{"id": "4", "port": 0}]}]
             },
            {"id": "3", "z": "200", "type": "subflow:300",
             "wires": [["4"]]},
            {"id": "4", "z": "200", "type": "function",
             "func": "msg.payload = msg.payload+'baz'; return msg;", "wires": []},
            # Subflow2
            {"id": "300", "type": "subflow", "name": "Subflow2", "info": "",
             "in": [{"wires": [{"id": "5"}]}],
             "out": [{"wires": [{"id": "5", "port": 0}]}]},
            {"id": "5", "z": "300", "type": "function",
             "func": "msg.payload=msg.payload+'bar'; return msg;", "wires": []}
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["payload"] == "foobarbaz"

    @pytest.mark.asyncio
    @pytest.mark.it('should access env var of subflow template')
    async def test_0004(self):
        flows = [
            {"id": "100", "type": "tab", "label": "",
                "disabled": False, "info": ""},
            {"id": "1", "z": "100", "type": "subflow:200", "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once", "wires": []},
            # Subflow
            {"id": "200", "type": "subflow", "name": "Subflow", "info": "",
             "env": [
                 {"name": "K", "type": "str", "value": "V"}
             ],
             "in": [{"wires": [{"id": "3"}]}],
             "out": [{"wires": [{"id": "3", "port": 0}]
                      }]
             },
            {"id": "3", "type": "change", "z": "200",
                "rules": [{"t": "set", "p": "V", "pt": "msg", "to": "K", "tot": "env"}],
                "name": "set-env-node", "wires": []},
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["V"] == "V"

    @pytest.mark.asyncio
    @pytest.mark.it('should access env var of subflow instance')
    async def test_0005(self):
        flows = [
            {"id": "999", "type": "tab", "label": "",
                "disabled": False, "info": ""},
            {"id": "1", "z": "999", "type": "subflow:100", "env": [
                {"name": "K", "type": "str", "value": "V"}
            ], "wires": [["2"]]},
            {"id": "2", "z": "999", "type": "test-once", "wires": []},
            # Subflow
            {"id": "100", "type": "subflow", "name": "Subflow", "info": "",
             "in": [{"wires": [{"id": "101"}]}],
             "out": [{"wires": [{"id": "101", "port": 0}]
                      }]
             },
            {"id": "101", "type": "change", "z": "100",
                "rules": [{"t": "set", "p": "V", "pt": "msg", "to": "K", "tot": "env"}],
                "name": "set-env-node", "wires": []},
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["V"] == "V"

    @pytest.mark.asyncio
    @pytest.mark.it('should access last env var with same name')
    async def test_0006(self):
        flows = [
            {"id": "999", "type": "tab", "label": "",
                "disabled": False, "info": ""},
            {"id": "1", "z": "999", "type": "subflow:100", "env": [
                {"name": "K", "type": "str", "value": "V0"},
                {"name": "X", "type": "str", "value": "VX"},
                {"name": "K", "type": "str", "value": "V1"}
            ], "wires": [["2"]]},
            {"id": "2", "z": "999", "type": "test-once", "wires": []},
            # Subflow
            {"id": "100", "type": "subflow", "name": "Subflow", "info": "",
             "in": [{"wires": [{"id": "101"}]}],
             "out": [{"wires": [{"id": "101", "port": 0}]}]
             },
            {"id": "101", "type": "change", "z": "100",
                "rules": [{"t": "set", "p": "V", "pt": "msg", "to": "K", "tot": "env"}],
                "name": "set-env-node", "wires": []},
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["V"] == "V1"

    @pytest.mark.asyncio
    @pytest.mark.it('should access typed value of env var')
    async def test_0007(self):
        flows = [
            {"id": "999", "type": "tab", "label": "",
                "disabled": False, "info": ""},
            {
                "id": "1", "z": "999", "type": "subflow:100",
                "env": [
                    {"name": "KN", "type": "num", "value": "100"},
                    {"name": "KB", "type": "bool", "value": "true"},
                    {"name": "KJ", "type": "json", "value": "[1,2,3]"},
                    {"name": "Kb", "type": "bin", "value": "[65,65]"},
                    {"name": "Ke", "type": "env", "value": "KS"},
                    # FIXME {"name": "Kj", "type": "jsonata", "value": "1+2"}
                ],
                "wires": [["2"]]},
            {"id": "2", "z": "999", "type": "test-once", "wires": []},
            {"id": "100", "type": "subflow", "name": "Subflow", "info": "",
             "in": [{"wires": [{"id": "101"}]}],
             "out": [{"wires": [{"id": "101", "port": 0}]}],
             "env": [{"name": "KS", "type": "str", "value": "STR"}]
             },
            {"id": "101", "z": "100", "type": "function",
                "func": "msg.VE = env.get('Ke'); msg.VS = env.get('KS'); msg.VN = env.get('KN'); msg.VB = env.get('KB'); msg.VJ = env.get('KJ'); msg.Vb = env.get('Kb'); /*msg.Vj = env.get('Kj');*/ return msg;",
                "wires": []
             }
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert len(msgs) == 1
        msg = msgs[0]
        assert msg["VS"] == "STR"
        assert msg["VN"] == 100
        assert msg["VB"] == True
        assert msg["VJ"] == [1, 2, 3]
        assert msg["Vb"] == [65, 65]
        assert msg["VE"] == "STR"
        # assert msg["Vj"] == 3 #FIXME

    @pytest.mark.asyncio
    @pytest.mark.it('should overwrite env var of subflow template by env var of subflow instance')
    async def test_0008(self):
        flows = [
            {"id": "999", "type": "tab", "label": "",
                "disabled": False, "info": ""},
            {"id": "1", "z": "999", "type": "subflow:100", "env": [
                {"name": "K", "type": "str", "value": "V"},
            ], "wires": [["2"]]},
            {"id": "2", "z": "999", "type": "test-once", "wires": []},
            # Subflow
            {"id": "100", "type": "subflow", "name": "Subflow", "info": "",
             "env": [
                 {"name": "K", "type": "str", "value": "TV"},
             ],
             "in": [{"wires": [{"id": "101"}]}],
             "out": [{"wires": [{"id": "101", "port": 0}]}]
             },
            {"id": "101", "type": "function", "z": "100",
             "func": "msg.V = env.get('K'); return msg;", "wires": []},
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["V"] == "V"

    @pytest.mark.asyncio
    @pytest.mark.it('should access env var of parent subflow template')
    async def test_0009(self):
        flows = [
            {"id": "999", "type": "tab", "label": "",
                "disabled": False, "info": ""},
            {"id": "998", "z": "999", "type": "test-once", "wires": []},
            {"id": "1", "z": "999",
                "type": "subflow:100", "wires": [["998"]]},
            # Subflow1
            {
                "id": "100",
                "type": "subflow",
                "name": "Subflow1",
                "info": "",
                "env": [
                    {"name": "K", "type": "str", "value": "V"}
                ],
                "in": [{"wires": [{"id": "101"}]}],
                "out": [{"wires": [{"id": "102", "port": 0}]}]
            },
            {"id": "101", "z": "100",
                "type": "subflow:200", "wires": [["102"]]},
            {
                "id": "102",
                "z": "100",
                "type": "function",
                "func": "return msg;",
                "wires": []
            },
            # Subflow2
            {
                "id": "200",
                "type": "subflow",
                "name": "Subflow2",
                "info": "",
                "in": [{"wires": [{"id": "201"}]}],
                "out": [{"wires": [{"id": "201", "port": 0}]}]
            },
            {"id": "201", "type": "change", "z": "200",
                "rules": [{"t": "set", "p": "V", "pt": "msg", "to": "K", "tot": "env"}],
                "name": "set-env-node", "wires": []},
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["V"] == "V"

    @pytest.mark.asyncio
    @pytest.mark.it('should access env var of parent subflow instance')
    async def test_0010(self):
        flows = [
            {"id": "999", "type": "tab", "label": "",
                "disabled": False, "info": ""},
            {"id": "1", "z": "999", "type": "subflow:100",
             "env": [
                 {"name": "K", "type": "str", "value": "V"}
             ],
             "wires": [["2"]]},
            {"id": "2", "z": "999", "type": "test-once", "wires": []},
            # Subflow1
            {"id": "100", "type": "subflow", "name": "Subflow1", "info": "",
             "in": [{"wires": [{"id": "101"}]}],
             "out": [{"wires": [{"id": "102", "port": 0}]}]
             },
            {"id": "101", "z": "100", "type": "subflow:200",
             "wires": [["102"]]},
            {"id": "102", "z": "100", "type": "function",
             "func": "return msg;", "wires": []},
            # Subflow2
            {"id": "200", "type": "subflow", "name": "Subflow2", "info": "",
             "in": [{"wires": [{"id": "201"}]}],
             "out": [{"wires": [{"id": "201", "port": 0}]}]
             },
            {"id": "201", "type": "change", "z": "200",
                "rules": [{"t": "set", "p": "V", "pt": "msg", "to": "K", "tot": "env"}],
                "name": "set-env-node", "wires": []},
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["V"] == "V"

    @pytest.mark.asyncio
    @pytest.mark.it('should access env var of tab')
    async def test_0011(self):
        flows = [
            {"id": "999", "type": "tab", "label": "", "disabled": False, "info": "",
                "env": [{"name": "K", "type": "str", "value": "V"}]
             },
            {"id": "1", "z": "999", "type": "subflow:100", "wires": [["2"]]},
            {"id": "2", "z": "999", "type": "test-once", "wires": []},
            # Subflow 1
            {
                "id": "100",
                "type": "subflow",
                "name": "Subflow",
                "info": "",
                "env": [],
                "in": [{"wires": [{"id": "101"}]}],
                "out": [{"wires": [{"id": "101", "port": 0}]}]
            },
            {"id": "101", "type": "change", "z": "100",
                "rules": [{"t": "set", "p": "V", "pt": "msg", "to": "K", "tot": "env"}],
                "name": "set-env-node", "wires": []},

        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["V"] == "V"

    @pytest.mark.asyncio
    @pytest.mark.it('should access env var of group')
    async def test_0012(self):
        flows = [
            {"id": "999", "type": "tab", "label": "",
                "disabled": False, "info": ""},
            {
                "id": "1000",
                "z": "999",
                "type": "group",
                "env": [{"name": "K", "type": "str", "value": "V"}]
            },
            {"id": "1", "z": "999", "g": "1000",
                "type": "subflow:100", "wires": [["2"]]},
            {"id": "2", "z": "999", "type": "test-once", "wires": []},
            {"id": "100", "type": "subflow", "name": "Subflow", "info": "", "env": [],
             "in": [{"wires": [{"id": "101"}]}],
             "out": [{"wires": [{"id": "101", "port": 0}]}]},
            {"id": "101", "type": "change", "z": "100",
             "rules": [{"t": "set", "p": "V", "pt": "msg", "to": "K", "tot": "env"}],
             "name": "set-env-node", "wires": []},
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["V"] == "V"

    @pytest.mark.asyncio
    @pytest.mark.it('should access env var of nested group')
    async def test_0013(self):
        flows = [
            {"id": "999", "type": "tab", "label": "",
                "disabled": False, "info": ""},
            {"id": "1000", "z": "999", "type": "group", "env": [
                {"name": "K", "type": "str", "value": "V"}
            ]},
            {"id": "2000", "z": "999", "g": "1000", "type": "group", "env": []},
            {"id": "1", "z": "999", "g": "2000",
                "type": "subflow:100", "wires": [["2"]]},
            {"id": "2", "z": "999", "type": "test-once", "wires": []},
            {"id": "100", "type": "subflow", "name": "Subflow", "info": "", "env": [],
                "in": [{"wires": [{"id": "101"}]}],
                "out": [{"wires": [{"id": "101", "port": 0}]}]
             },
            {"id": "101", "type": "change", "z": "100",
             "rules": [{"t": "set", "p": "V", "pt": "msg", "to": "K", "tot": "env"}],
             "name": "set-env-node", "wires": []},
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["V"] == "V"

    @pytest.mark.asyncio
    @pytest.mark.it('should access NR_NODE_PATH env var within subflow instance')
    async def test_0014(self):
        flows = [
            {"id": "999", "type": "tab", "label": "",
                "disabled": False, "info": ""},
            {"id": "1", "z": "999", "type": "subflow:100",
                "env": [], "wires": [["2"]]},
            {"id": "2", "z": "999", "type": "test-once", "wires": []},
            {"id": "100", "type": "subflow", "name": "Subflow", "info": "",
                "in": [{"wires": [{"id": "101"}]}],
                "out": [{"wires": [{"id": "101", "port": 0}]}]
             },
            {"id": "101", "type": "change", "z": "100",
             "rules": [{"t": "set", "p": "payload", "pt": "msg", "to": "NR_NODE_PATH", "tot": "env"}],
             "name": "set-env-node", "wires": []},
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        payload = msgs[0]["payload"]
        assert payload.count('/') == 2
