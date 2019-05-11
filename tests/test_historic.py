import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from aiorpc.plugins.ceph import get_historic, previous_ops
from cephlib import CephRelease, unpack_historic


TEST_FOLDER = Path(__file__).parent
HISTORIC_PATH = TEST_FOLDER / 'historic_dump.json'


class MockCephCLI:
    def __init__(self, *args, **kwargs) -> None:
        pass

    async def get_historic(self, osd_id: int) -> Any:
        return json.load(HISTORIC_PATH.open())


@pytest.mark.asyncio
async def test_historic():
    with patch('aiorpc.plugins.ceph.CephCLI', MockCephCLI):
        data = await get_historic([1], size=20, duration=600, ceph_extra_args=[], cmd_timeout=20,
                                  release_i=CephRelease.luminous.value,
                                  min_duration=0,
                                  packer_name='msgpack')

        raw_data = await MockCephCLI().get_historic(1)
        obj_names = []
        for op in raw_data['ops']:
            if op['description'].startswith("osd_op("):
                obj_names.append(op['description'].split()[2])
            elif op['description'].startswith("osd_repop("):
                obj_names.append(op['description'].split()[3])
            else:
                raise ValueError(f"Unknonw op {op['description']}")

        it, _ = unpack_historic(data)
        assert [op['obj_name'] for op in it] == obj_names

        previous_ops.clear()
