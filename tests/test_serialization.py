import pytest
from aiorpc.apis import JsonSerializer, serialize, deserialize, SimpleBlockStream


vals = [
    1, 2, 0, -3, -(2 ** 50), 2 ** 50, 1.1234124515, -12.4124154, None, True, False,
    "asfagfagahra", "йцуйккнеукхгзщкешъфпфрваф",
    b"1wefq34521436523\x00\x12\x33",
]


@pytest.mark.asyncio
async def test_simple_serialization():

    name = "test1"

    for val in vals:
        stream = serialize(name, [val], {}, JsonSerializer(), SimpleBlockStream())
        dname, dval, dkw = await deserialize(stream, False, JsonSerializer(), SimpleBlockStream())
        assert val == dval[0]
        assert dname == name
        assert dkw == {}


@pytest.mark.asyncio
async def test_collection_serialization():

    name = "test1"

    cvals = [vals, {"a": 12, "b": 13, "": True, "5": [12, [4, [None, {" ": 2}]]]}]

    for val in cvals:
        stream = serialize(name, [val], {"1": val}, JsonSerializer(), SimpleBlockStream())
        dname, dval, dkw = await deserialize(stream, False, JsonSerializer(), SimpleBlockStream())
        assert val == dval[0]
        assert dname == name
        assert dkw == {"1": val}
