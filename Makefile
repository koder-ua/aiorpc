.PHONY: mypy pylint pylint_e whl

ALL_FILES=$(shell find aiorpc/ -type f -name '*.py')
STUBS="/home/koder/workspace/typeshed"
PYLINT_FMT=--msg-template={path}:{line}: [{msg_id}({symbol}), {obj}] {msg}
CURR_PATH=$(dir $(abspath $(lastword $(MAKEFILE_LIST))))


mypy:
		MYPYPATH=${STUBS} python3 -m mypy --ignore-missing-imports --follow-imports=skip ${ALL_FILES}

mypy_collect:
		MYPYPATH=${STUBS} python3 -m mypy --ignore-missing-imports --follow-imports=skip ${COLLECT_FILES}

pylint:
		python3 -m pylint '${PYLINT_FMT}' --rcfile=pylint.rc ${ALL_FILES}

pylint_e:
		python3 -m pylint -E '${PYLINT_FMT}' --rcfile=pylint.rc ${ALL_FILES}

whl:
		python3 setup.py sdist bdist_wheel