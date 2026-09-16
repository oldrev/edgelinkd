"""Ported specs for the local file-system context store.

Upstream: `3rd-party/node-red/test/unit/@node-red/runtime/lib/nodes/context/localfilesystem_spec.js`
(v4.0.9). The upstream spec drives the store object directly rather than through a flow, so these
tests go through the `edgelink.ContextStore` bridge in `crates/pymod/src/context.rs`, which
exposes the very same `ContextStore` the runtime uses.

Like the upstream spec, every test works inside one scratch directory under `tests/resources`
(upstream's `resourcesDir`) which is emptied before and after each test.

Two things about the bridge shape the bodies:

* Node-RED's store API is callback based and has a synchronous path when the cache is disabled.
  Ours is a Rust trait whose operations are all `async`, so every call is awaited, and the specs
  that only exercise the callback contract are skipped.
* A missing value and a stored JSON `null` are different things — upstream's `undefined` versus
  `null`. `get()` therefore raises `KeyError` when the key is not set and returns `None` for a
  stored `null`; `get_many()`, which mirrors Node-RED's multi-key form, uses `None` for unset
  entries because it has no per-key error channel.

`clean()` takes the runtime's element ids, the 16-digit hex strings a scope is built from, which
is why the `#clean` specs name their scopes through `red_id()`.
"""

import asyncio
import json
import os
import shutil

import pytest
import pytest_asyncio

from tests import *

RESOURCES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'resources', 'context'))

# The base directory name the store uses when its options do not say otherwise.
DEFAULT_CONTEXT_BASE = 'context'


@pytest.fixture(autouse=True)
def resources():
    """The upstream `before()`/`afterEach()` pair: a clean scratch directory for each test."""
    shutil.rmtree(RESOURCES_DIR, ignore_errors=True)
    os.makedirs(RESOURCES_DIR, exist_ok=True)
    yield RESOURCES_DIR
    shutil.rmtree(RESOURCES_DIR, ignore_errors=True)


def make_options(directory, **extra):
    """The options Node-RED's `LocalFileSystem({dir, cache: false})` spells out."""
    options = {'dir': str(directory), 'cache': False}
    options.update(extra)
    return options


async def open_store(options):
    store = edgelink.ContextStore.create('localfilesystem', 'file', options)
    await store.open()
    return store


def context_dir(directory, base=DEFAULT_CONTEXT_BASE):
    return os.path.join(directory, base)


def scope_file(directory, scope, base=DEFAULT_CONTEXT_BASE):
    """Where a scope is filed, mirroring Node-RED's `getStoragePath`."""
    if ':' in scope:
        node_id, flow_id = scope.split(':', 1)
        return os.path.join(context_dir(directory, base), flow_id, f'{node_id}.json')
    if scope == 'global':
        return os.path.join(context_dir(directory, base), 'global', 'global.json')
    return os.path.join(context_dir(directory, base), scope, 'flow.json')


def write_scope(directory, scope, value, base=DEFAULT_CONTEXT_BASE):
    path = scope_file(directory, scope, base)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fp:
        json.dump(value, fp)
    return path


def read_scope(directory, scope, base=DEFAULT_CONTEXT_BASE):
    with open(scope_file(directory, scope, base), encoding='utf-8') as fp:
        return json.load(fp)


def remove_scope(directory, scope, base=DEFAULT_CONTEXT_BASE):
    """Drop a scope file, tolerating its absence the way Node-RED's `fs.remove` does."""
    try:
        os.remove(scope_file(directory, scope, base))
    except FileNotFoundError:
        pass


async def wait_for_scope_file(directory, scope, timeout=3.0):
    """Wait for a cached scope to reach the disk, and return what it holds."""
    path = scope_file(directory, scope)
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if os.path.exists(path):
            return read_scope(directory, scope)
        await asyncio.sleep(0.05)
    raise AssertionError(f'Timed out waiting for the flush of {path}')


@pytest_asyncio.fixture
async def store(resources):
    """A store over a clean directory, with the cache off, as the upstream `beforeEach` builds."""
    store = await open_store(make_options(resources))
    yield store
    await store.clean([])
    await store.close()


@pytest.mark.describe('localfilesystem')
class TestLocalFileSystem:

    @pytest.mark.describe('#get/set')
    class TestGetSet:

        @pytest.mark.asyncio
        @pytest.mark.it('should store property')
        async def test_0001(self, store):
            with pytest.raises(KeyError):
                await store.get('nodeX', 'foo')

            await store.set('nodeX', 'foo', 'test')
            assert await store.get('nodeX', 'foo') == 'test'

        @pytest.mark.asyncio
        @pytest.mark.it('should store property - creates parent properties')
        async def test_0002(self, store):
            await store.set('nodeX', 'foo.bar', 'test')
            assert await store.get('nodeX', 'foo') == {'bar': 'test'}

        @pytest.mark.asyncio
        @pytest.mark.it('should store local scope property')
        async def test_0003(self, store, resources):
            await store.set('abc:def', 'foo.bar', 'test')
            assert await store.get('abc:def', 'foo') == {'bar': 'test'}
            # A local scope is filed in its flow's directory, like Node-RED's `<flow>/<node>.json`.
            assert os.path.isfile(scope_file(resources, 'abc:def'))

        @pytest.mark.asyncio
        @pytest.mark.it('should delete property')
        async def test_0004(self, store):
            await store.set('nodeX', 'foo.abc.bar1', 'test1')
            await store.set('nodeX', 'foo.abc.bar2', 'test2')
            assert await store.get('nodeX', 'foo.abc') == {'bar1': 'test1', 'bar2': 'test2'}

            await store.remove('nodeX', 'foo.abc.bar1')
            assert await store.get('nodeX', 'foo.abc') == {'bar2': 'test2'}

            await store.remove('nodeX', 'foo.abc')
            with pytest.raises(KeyError):
                await store.get('nodeX', 'foo.abc')

            await store.remove('nodeX', 'foo')
            with pytest.raises(KeyError):
                await store.get('nodeX', 'foo')

        @pytest.mark.asyncio
        @pytest.mark.it('should not shared context with other scope')
        async def test_0005(self, store):
            with pytest.raises(KeyError):
                await store.get('nodeX', 'foo')
            with pytest.raises(KeyError):
                await store.get('nodeY', 'foo')

            await store.set('nodeX', 'foo', 'testX')
            await store.set('nodeY', 'foo', 'testY')

            assert await store.get('nodeX', 'foo') == 'testX'
            assert await store.get('nodeY', 'foo') == 'testY'

        @pytest.mark.asyncio
        @pytest.mark.it('should store string')
        async def test_0006(self, store):
            await store.set('nodeX', 'foo', 'bar')
            value = await store.get('nodeX', 'foo')
            assert isinstance(value, str)
            assert value == 'bar'

            # A string that looks like a number must stay a string.
            await store.set('nodeX', 'foo', '1')
            value = await store.get('nodeX', 'foo')
            assert isinstance(value, str)
            assert value == '1'

        @pytest.mark.asyncio
        @pytest.mark.it('should store number')
        async def test_0007(self, store):
            await store.set('nodeX', 'foo', 1)
            value = await store.get('nodeX', 'foo')
            assert isinstance(value, int) and not isinstance(value, bool)
            assert value == 1

        @pytest.mark.asyncio
        @pytest.mark.it('should store null')
        async def test_0008(self, store):
            await store.set('nodeX', 'foo', None)
            # `None` is a stored JSON null here, not a missing key, which raises `KeyError`.
            assert await store.get('nodeX', 'foo') is None

        @pytest.mark.asyncio
        @pytest.mark.it('should store boolean')
        async def test_0009(self, store):
            await store.set('nodeX', 'foo', True)
            value = await store.get('nodeX', 'foo')
            assert isinstance(value, bool) and value is True

            await store.set('nodeX', 'foo', False)
            value = await store.get('nodeX', 'foo')
            assert isinstance(value, bool) and value is False

        @pytest.mark.asyncio
        @pytest.mark.it('should store object')
        async def test_0010(self, store):
            await store.set('nodeX', 'foo', {'obj': 'bar'})
            assert await store.get('nodeX', 'foo') == {'obj': 'bar'}

        @pytest.mark.asyncio
        @pytest.mark.it('should store array')
        async def test_0011(self, store):
            await store.set('nodeX', 'foo', ['a', 'b', 'c'])
            assert await store.get('nodeX', 'foo') == ['a', 'b', 'c']
            assert await store.get('nodeX', 'foo[1]') == 'b'

        @pytest.mark.asyncio
        @pytest.mark.it('should store array of arrays')
        async def test_0012(self, store):
            await store.set('nodeX', 'foo', [['a', 'b', 'c'], [1, 2, 3, 4], [True, False]])
            value = await store.get('nodeX', 'foo')
            assert [len(item) for item in value] == [3, 4, 2]
            assert await store.get('nodeX', 'foo[1]') == [1, 2, 3, 4]

        @pytest.mark.asyncio
        @pytest.mark.it('should store array of objects')
        async def test_0013(self, store):
            await store.set('nodeX', 'foo', [{'obj': 'bar1'}, {'obj': 'bar2'}, {'obj': 'bar3'}])
            value = await store.get('nodeX', 'foo')
            assert [isinstance(item, dict) for item in value] == [True, True, True]
            assert await store.get('nodeX', 'foo[1]') == {'obj': 'bar2'}

        @pytest.mark.asyncio
        @pytest.mark.it('should set/get multiple values')
        async def test_0014(self, store):
            await store.set_many('nodeX', ['one', 'two', 'three'], ['test1', 'test2', 'test3'])
            assert await store.get_many('nodeX', ['one', 'two']) == ['test1', 'test2']

        @pytest.mark.asyncio
        @pytest.mark.it('should set/get multiple values - get unknown')
        async def test_0015(self, store):
            await store.set_many('nodeX', ['one', 'two', 'three'], ['test1', 'test2', 'test3'])
            assert await store.get_many('nodeX', ['one', 'two', 'unknown']) == ['test1', 'test2', None]

        @pytest.mark.asyncio
        @pytest.mark.it('should set/get multiple values - single value providd')
        async def test_0016(self, store):
            await store.set_many('nodeX', ['one', 'two', 'three'], 'test1')
            assert await store.get_many('nodeX', ['one', 'two']) == ['test1', None]

        @pytest.mark.asyncio
        @pytest.mark.it('should throw error if bad key included in multiple keys - get')
        async def test_0017(self, store):
            await store.set_many('nodeX', ['one', 'two', 'three'], ['test1', 'test2', 'test3'])
            with pytest.raises((ValueError, RuntimeError)):
                await store.get_many('nodeX', ['one', '.foo', 'three'])

        @pytest.mark.asyncio
        @pytest.mark.it('should throw error if bad key included in multiple keys - set')
        async def test_0018(self, store):
            with pytest.raises((ValueError, RuntimeError)):
                await store.set_many('nodeX', ['one', '.foo', 'three'], ['test1', 'test2', 'test3'])

            # Check 'one' didn't get set as a result
            with pytest.raises(KeyError):
                await store.get('nodeX', 'one')

        @pytest.mark.asyncio
        @pytest.mark.it('should throw an error when getting a value with invalid key')
        async def test_0019(self, store):
            await store.set('nodeX', 'foo', 'bar')
            with pytest.raises((ValueError, RuntimeError)):
                await store.get('nodeX', ' ')

        @pytest.mark.asyncio
        @pytest.mark.it('should throw an error when setting a value with invalid key')
        async def test_0020(self, store):
            with pytest.raises((ValueError, RuntimeError)):
                await store.set('nodeX', ' ', 'bar')

        @pytest.mark.skip(reason="the callback-based store API is out of scope: the Rust "
                                 "ContextStore trait is async-only and takes no callback argument")
        @pytest.mark.asyncio
        @pytest.mark.it('should throw an error when callback of get() is not a function')
        async def test_0021(self, store):
            pass

        @pytest.mark.skip(reason="the callback-based store API is out of scope: the Rust "
                                 "ContextStore trait is async-only and takes no callback argument")
        @pytest.mark.asyncio
        @pytest.mark.it('should throw an error when callback of get() is not specified')
        async def test_0022(self, store):
            pass

        @pytest.mark.skip(reason="the callback-based store API is out of scope: the Rust "
                                 "ContextStore trait is async-only and takes no callback argument")
        @pytest.mark.asyncio
        @pytest.mark.it('should throw an error when callback of set() is not a function')
        async def test_0023(self, store):
            pass

        @pytest.mark.skip(reason="the callback-based store API is out of scope: the Rust "
                                 "ContextStore trait is async-only and takes no callback argument")
        @pytest.mark.asyncio
        @pytest.mark.it('should not throw an error when callback of set() is not specified')
        async def test_0024(self, store):
            pass

        @pytest.mark.asyncio
        @pytest.mark.it('should handle empty context file')
        async def test_0025(self, store, resources):
            path = scope_file(resources, 'nodeX')
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as fp:
                fp.write('')

            with pytest.raises(KeyError):
                await store.get('nodeX', 'foo')

            await store.set('nodeX', 'foo', 'test')
            assert await store.get('nodeX', 'foo') == 'test'

        @pytest.mark.asyncio
        @pytest.mark.it('should throw an error when reading corrupt context file')
        async def test_0026(self, store, resources):
            path = scope_file(resources, 'nodeX')
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as fp:
                fp.write('{abc')

            with pytest.raises(RuntimeError):
                await store.get('nodeX', 'foo')

    @pytest.mark.describe('#keys')
    class TestKeys:

        @pytest.mark.asyncio
        @pytest.mark.it('should enumerate context keys')
        async def test_0001(self, store):
            assert await store.keys('nodeX') == []

            await store.set('nodeX', 'foo', 'bar')
            assert await store.keys('nodeX') == ['foo']

            await store.set('nodeX', 'abc.def', 'bar')
            assert await store.keys('nodeX') == ['abc', 'foo']

        @pytest.mark.asyncio
        @pytest.mark.it('should enumerate context keys in each scopes')
        async def test_0002(self, store):
            assert await store.keys('nodeX') == []
            assert await store.keys('nodeY') == []

            await store.set('nodeX', 'foo', 'bar')
            await store.set('nodeY', 'hoge', 'piyo')

            assert await store.keys('nodeX') == ['foo']
            assert await store.keys('nodeY') == ['hoge']

        @pytest.mark.skip(reason="the callback-based store API is out of scope: the Rust "
                                 "ContextStore trait is async-only and takes no callback argument")
        @pytest.mark.asyncio
        @pytest.mark.it('should throw an error when callback of keys() is not a function')
        async def test_0003(self, store):
            pass

        @pytest.mark.skip(reason="the callback-based store API is out of scope: the Rust "
                                 "ContextStore trait is async-only and takes no callback argument")
        @pytest.mark.asyncio
        @pytest.mark.it('should throw an error when callback of keys() is not specified')
        async def test_0004(self, store):
            pass

    @pytest.mark.describe('#delete')
    class TestDelete:

        @pytest.mark.asyncio
        @pytest.mark.it('should delete context')
        async def test_0001(self, store):
            with pytest.raises(KeyError):
                await store.get('nodeX', 'foo')
            with pytest.raises(KeyError):
                await store.get('nodeY', 'foo')

            await store.set('nodeX', 'foo', 'testX')
            await store.set('nodeY', 'foo', 'testY')
            assert await store.get('nodeX', 'foo') == 'testX'
            assert await store.get('nodeY', 'foo') == 'testY'

            await store.delete('nodeX')

            with pytest.raises(KeyError):
                await store.get('nodeX', 'foo')
            assert await store.get('nodeY', 'foo') == 'testY'

    @pytest.mark.describe('#clean')
    class TestClean:
        flow1 = red_id('flow1')
        flow2 = red_id('flow2')
        nodeX = red_id('nodeX')
        nodeY = red_id('nodeY')

        @pytest.mark.asyncio
        @pytest.mark.it('should clean unnecessary context')
        async def test_0001(self, store):
            await store.set('global', 'foo', 'testGlobal')
            await store.set(f'{self.nodeX}:{self.flow1}', 'foo', 'testX')
            await store.set(f'{self.nodeY}:{self.flow2}', 'foo', 'testY')

            assert await store.get(f'{self.nodeX}:{self.flow1}', 'foo') == 'testX'
            assert await store.get(f'{self.nodeY}:{self.flow2}', 'foo') == 'testY'

            await store.clean([])

            with pytest.raises(KeyError):
                await store.get(f'{self.nodeX}:{self.flow1}', 'foo')
            with pytest.raises(KeyError):
                await store.get(f'{self.nodeY}:{self.flow2}', 'foo')
            assert await store.get('global', 'foo') == 'testGlobal'

        @pytest.mark.asyncio
        @pytest.mark.it('should not clean active context')
        async def test_0002(self, store):
            await store.set('global', 'foo', 'testGlobal')
            await store.set(f'{self.nodeX}:{self.flow1}', 'foo', 'testX')
            await store.set(f'{self.nodeY}:{self.flow2}', 'foo', 'testY')

            await store.clean([self.flow1, self.nodeX])

            assert await store.get(f'{self.nodeX}:{self.flow1}', 'foo') == 'testX'
            with pytest.raises(KeyError):
                await store.get(f'{self.nodeY}:{self.flow2}', 'foo')
            assert await store.get('global', 'foo') == 'testGlobal'

    @pytest.mark.describe('#if cache is enabled')
    class TestCacheEnabled:

        @pytest.mark.asyncio
        @pytest.mark.it('should load contexts into the cache')
        async def test_0001(self, resources):
            write_scope(resources, 'global', {'key': 'global'})
            write_scope(resources, 'flow', {'key': 'flow'})
            write_scope(resources, 'node:flow', {'key': 'node'})

            store = await open_store(make_options(resources, cache=True))

            # The values now live in the cache, so removing the files must not lose them.
            for scope in ['global', 'flow', 'node:flow']:
                os.remove(scope_file(resources, scope))

            assert await store.get('global', 'key') == 'global'
            assert await store.get('flow', 'key') == 'flow'
            assert await store.get('node:flow', 'key') == 'node'

            await store.close()

        @pytest.mark.asyncio
        @pytest.mark.it('should store property to the cache')
        async def test_0002(self, resources):
            store = await open_store(make_options(resources, cache=True, flushInterval=1))
            await store.set('global', 'foo', 'bar')

            # The write is batched, so nothing has reached the disk yet.
            assert not os.path.exists(scope_file(resources, 'global'))

            assert await wait_for_scope_file(resources, 'global') == {'foo': 'bar'}

            os.remove(scope_file(resources, 'global'))
            assert await store.get('global', 'foo') == 'bar'

            await store.close()

        @pytest.mark.asyncio
        @pytest.mark.it('should enumerate context keys in the cache')
        async def test_0003(self, resources):
            write_scope(resources, 'global', {'foo': 'bar'})
            store = await open_store(make_options(resources, cache=True, flushInterval=2))

            os.remove(scope_file(resources, 'global'))
            assert await store.keys('global') == ['foo']

            await store.set('global', 'foo2', 'bar2')
            # Still batched: nothing has been flushed since the write above.
            remove_scope(resources, 'global')
            assert await store.keys('global') == ['foo', 'foo2']

            await store.close()

        @pytest.mark.asyncio
        @pytest.mark.it('should delete context in the cache')
        async def test_0004(self, resources):
            store = await open_store(make_options(resources, cache=True, flushInterval=2))
            await store.set('global', 'foo', 'bar')
            assert await store.get('global', 'foo') == 'bar'

            await store.delete('global')

            with pytest.raises(KeyError):
                await store.get('global', 'foo')

            await store.close()

        @pytest.mark.asyncio
        @pytest.mark.it('should clean unnecessary context in the cache')
        async def test_0005(self, resources):
            flow_a = red_id('flowA')
            flow_b = red_id('flowB')
            write_scope(resources, flow_a, {'key': 'flowA'})
            write_scope(resources, flow_b, {'key': 'flowB'})

            store = await open_store(make_options(resources, cache=True, flushInterval=2))
            assert await store.get(flow_a, 'key') == 'flowA'
            assert await store.get(flow_b, 'key') == 'flowB'

            await store.clean([flow_a])

            assert await store.get(flow_a, 'key') == 'flowA'
            with pytest.raises(KeyError):
                await store.get(flow_b, 'key')

            await store.close()

    @pytest.mark.describe('Configuration')
    class TestConfiguration:

        @pytest.mark.asyncio
        @pytest.mark.it('should change a base directory')
        async def test_0001(self, store, resources):
            different_base_context = await open_store(make_options(resources, base='contexts2'))
            await different_base_context.set('node2', 'foo2', 'bar2')
            assert await different_base_context.get('node2', 'foo2') == 'bar2'

            with pytest.raises(KeyError):
                await store.get('node2', 'foo2')

            assert os.path.isfile(scope_file(resources, 'node2', base='contexts2'))
            await different_base_context.close()

        @pytest.mark.asyncio
        @pytest.mark.it('should use userDir')
        async def test_0002(self, store, resources):
            # No `dir`: the runtime's home directory is what Node-RED calls `settings.userDir`.
            user_dir_context = await open_store(
                {'base': 'contexts2', 'cache': False, 'settings': {'userDir': str(resources)}})
            await user_dir_context.set('node2', 'foo2', 'bar2')
            assert await user_dir_context.get('node2', 'foo2') == 'bar2'

            with pytest.raises(KeyError):
                await store.get('node2', 'foo2')

            assert os.path.isfile(scope_file(resources, 'node2', base='contexts2'))
            await user_dir_context.close()

        @pytest.mark.asyncio
        @pytest.mark.it('should use NODE_RED_HOME')
        async def test_0003(self, store, resources, monkeypatch):
            # `EDGELINK_HOME` is EdgeLinkd's `NODE_RED_HOME`.
            monkeypatch.setenv('EDGELINK_HOME', str(resources))
            home_context = await open_store({'base': 'contexts2', 'cache': False})
            await home_context.set('node2', 'foo2', 'bar2')
            assert await home_context.get('node2', 'foo2') == 'bar2'

            with pytest.raises(KeyError):
                await store.get('node2', 'foo2')

            assert os.path.isfile(scope_file(resources, 'node2', base='contexts2'))
            await home_context.close()

        @pytest.mark.asyncio
        @pytest.mark.it('should use HOME_PATH')
        async def test_0004(self, store, resources, monkeypatch):
            monkeypatch.delenv('EDGELINK_HOME', raising=False)
            monkeypatch.delenv('HOME', raising=False)
            monkeypatch.delenv('USERPROFILE', raising=False)
            monkeypatch.setenv('HOMEPATH', str(resources))
            home_context = await open_store({'base': 'contexts2', 'cache': False})
            await home_context.set('node2', 'foo2', 'bar2')
            assert await home_context.get('node2', 'foo2') == 'bar2'

            with pytest.raises(KeyError):
                await store.get('node2', 'foo2')

            assert os.path.isfile(os.path.join(resources, '.edgelinkd', 'contexts2', 'node2', 'flow.json'))
            await home_context.close()

        @pytest.mark.asyncio
        @pytest.mark.it('should use HOME_PATH')
        async def test_0005(self, store, resources, monkeypatch):
            monkeypatch.delenv('EDGELINK_HOME', raising=False)
            monkeypatch.setenv('HOME', str(resources))
            home_context = await open_store({'base': 'contexts2', 'cache': False})
            await home_context.set('node2', 'foo2', 'bar2')
            assert await home_context.get('node2', 'foo2') == 'bar2'

            with pytest.raises(KeyError):
                await store.get('node2', 'foo2')

            assert os.path.isfile(os.path.join(resources, '.edgelinkd', 'contexts2', 'node2', 'flow.json'))
            await home_context.close()
