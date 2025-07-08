import pytest
import json
import pytest_jsonreport.serialize


def _make_collectitem(item):
    """Return JSON-serializable collection item."""
    json_item = {
        'nodeid': item.nodeid,
        'type': item.__class__.__name__,
    }
    for marker in item.iter_markers():
        if isinstance(marker, pytest.Mark):
            if marker.name == "it":
                it = marker.args[0] if marker.args else None
                if it != None:
                    path = [x[1] for x in _parent_marks(item)] + [it]
                    json_item["title"] = it
                    json_item["fullTitle"] = " ".join(path)
    try:
        location = item.location
    except AttributeError:
        pass
    else:
        json_item['lineno'] = location[1]
    return json_item

pytest_jsonreport.serialize.make_collectitem = _make_collectitem



"""

#@pytest.hookimpl(trylast=True, hookwrapper=True, optionalhook=True)
@pytest.hookimpl(trylast=True, optionalhook=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()

    # only add this during call instead of during any stage
    print(call.when)
    if call.when == 'call':
        test_info = {
            "title": None,
            "fullTitle": None
        }

        # Extract it and describe markers
        #print(_parent_marks(item))
        for marker in item.iter_markers():
            if isinstance(marker, pytest.Mark):
                if marker.name == "it":
                    test_info["title"] = marker.args[0] if marker.args else None
                    path = [x[1] for x in _parent_marks(item)] + [test_info["title"]]
                    test_info["fullTitle"] = " ".join(path)
        #report.test_metadata = test_info
        print(test_info)
        return test_info
    else:
        return {}
"""

def _parent_marks(item):
    markers = []
    for m in item.iter_markers():
        if m.name in ("describe", "context"):
            try:
                markers.append((m.name, m.args[0]))
            except IndexError:
                pass
    return list(reversed(markers))