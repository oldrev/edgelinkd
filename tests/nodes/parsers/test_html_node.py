import pytest
from tests import *

HTML = '<html><body><h1>This is a test page for node 70-HTML</h1><p>There is nothing to read here.</p><ol><li>Blue</li><li>Red</li></ol><span><img src="foo.png"></span></body></html>'

@pytest.mark.describe('HTML node')
class TestHtmlNode:
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded')
    async def test_loaded(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should retrieve header contents if asked to by msg.select')
    async def test_select(self):
        msgs = await run_single_node_with_msgs_ntimes({"id":"1","type":"html"}, [{"payload":HTML,"select":"body h1"}], 1)
        assert msgs[0]["payload"] == ["This is a test page for node 70-HTML"]

    @pytest.mark.asyncio
    @pytest.mark.it('should retrieve header contents if asked to by msg.select - alternative in property')
    async def test_alt_property(self):
        msgs = await run_single_node_with_msgs_ntimes({"id":"1","type":"html","property":"foo"}, [{"foo":HTML,"select":"h1"}], 1)
        assert msgs[0]["foo"] == ["This is a test page for node 70-HTML"]

    @pytest.mark.asyncio
    @pytest.mark.it('should retrieve header contents if asked to by msg.select - alternative in and out properties')
    async def test_alt_in_out(self):
        msgs = await run_single_node_with_msgs_ntimes({"id":"1","type":"html","property":"foo","outproperty":"bar","tag":"h1"}, [{"foo":HTML}], 1)
        assert msgs[0]["bar"] == ["This is a test page for node 70-HTML"]

    @pytest.mark.asyncio
    @pytest.mark.it('should emit an empty array if no matching elements')
    async def test_empty(self):
        msgs = await run_single_node_with_msgs_ntimes({"id":"1","type":"html","tag":"h4"}, [{"payload":HTML}], 1)
        assert msgs[0]["payload"] == []

    @pytest.mark.asyncio
    @pytest.mark.it('should retrieve paragraph contents when specified')
    async def test_text(self):
        msgs = await run_single_node_with_msgs_ntimes({"id":"1","type":"html","ret":"text","tag":"p"}, [{"payload":HTML}], 1)
        assert msgs[0]["payload"] == ["There is nothing to read here."]

    @pytest.mark.asyncio
    @pytest.mark.it('should retrieve list contents as an array of html as default')
    async def test_html(self):
        msgs = await run_single_node_with_msgs_ntimes({"id":"1","type":"html","tag":"ol"}, [{"payload":HTML}], 1)
        assert "<li>Blue</li>" in msgs[0]["payload"][0]

    @pytest.mark.asyncio
    @pytest.mark.it('should retrieve list contents as an array of text')
    async def test_list_text(self):
        msgs = await run_single_node_with_msgs_ntimes({"id":"1","type":"html","tag":"ol","ret":"text"}, [{"payload":HTML}], 1)
        assert "Blue" in msgs[0]["payload"][0]

    @pytest.mark.asyncio
    @pytest.mark.it('should fix up a unclosed tag')
    async def test_unclosed(self):
        msgs = await run_single_node_with_msgs_ntimes({"id":"1","type":"html","tag":"span"}, [{"payload":HTML}], 1)
        assert '<img src="foo.png">' in msgs[0]["payload"][0]

    @pytest.mark.asyncio
    @pytest.mark.it('should retrieve an attribute from a tag')
    async def test_attr(self):
        msgs = await run_single_node_with_msgs_ntimes({"id":"1","type":"html","tag":"span img","ret":"attr"}, [{"payload":HTML}], 1)
        assert msgs[0]["payload"][0]["src"] == "foo.png"

    @pytest.mark.skip(reason='Error logging assertions are not exposed by the pytest bridge')
    @pytest.mark.it('should log on error')
    def test_error_log(self): pass

    @pytest.mark.asyncio
    @pytest.mark.it('should pass through if payload empty')
    async def test_missing(self):
        msgs = await run_single_node_with_msgs_ntimes({"id":"1","type":"html"}, [{"topic":"bar"}], 1)
        assert msgs[0] == {"topic":"bar"}

@pytest.mark.describe('HTML node multiple messages')
class TestHtmlNodeMultiple:
    @pytest.mark.skip(reason='multi output compatibility is implemented but needs a dedicated sequence assertion in the pytest bridge')
    @pytest.mark.it('should retrieve list contents as html as default with output as multiple msgs')
    def test_multi_html(self): pass
    @pytest.mark.skip(reason='multi output compatibility is implemented but needs a dedicated sequence assertion in the pytest bridge')
    @pytest.mark.it('should retrieve list contents as html as default with output as multiple msgs - alternative property')
    def test_multi_html_alt(self): pass
    @pytest.mark.skip(reason='multi output compatibility is implemented but needs a dedicated sequence assertion in the pytest bridge')
    @pytest.mark.it('should retrieve list contents as text with output as multiple msgs ')
    def test_multi_text(self): pass
    @pytest.mark.skip(reason='multi output compatibility is implemented but needs a dedicated sequence assertion in the pytest bridge')
    @pytest.mark.it('should retrieve an attribute from a tag')
    def test_multi_attr(self): pass
    @pytest.mark.skip(reason='multi output compatibility is implemented but needs a dedicated sequence assertion in the pytest bridge')
    @pytest.mark.it('should not reuse message')
    def test_multi_reuse(self): pass
