#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly ARCHNAME="${0}"
readonly CMD="${1:-}"
readonly INSTALL_ROOT_MARKER=".install_root"
readonly TAR_OPT="{FILL_TAR_OPT}"

function help() {
    echo "${ARCHNAME} --list|--help|-h|--install [INSTALLATION_PATH(%DEFPATH%)]"
}

if [[ "${CMD}" == "--help" ]] || [[ "${CMD}" == "-h" ]] ; then
    help
    exit 0
fi

readonly ARCH_CONTENT_POS=$(awk '/^__ARCHIVE_BELOW__/ {print NR + 1; exit 0; }' "${ARCHNAME}")

if [[ "${CMD}" == "--list" ]] ; then
    tail "--lines=+${ARCH_CONTENT_POS}" "${ARCHNAME}" | tar "${TAR_OPT}" --list --verbose
    exit 0
fi

if [[ "${CMD}" == "--install" ]] ; then
    readonly INSTALL_PATH="${2:-%DEFPATH%}"

    if [[ "${INSTALL_PATH}" == "" ]] ; then
        help
        exit 1
    fi

    mkdir --parents "${INSTALL_PATH}"
    tail "--lines=+${ARCH_CONTENT_POS}" "${ARCHNAME}" | tar --extract "${TAR_OPT}" --directory "${INSTALL_PATH}"
    cp "${ARCHNAME}" "${INSTALL_PATH}/distribution.sh"
    touch "${INSTALL_PATH}/${INSTALL_ROOT_MARKER}"
    exit 0
fi

# get here only if not option provided
help
exit 1

__ARCHIVE_BELOW__
