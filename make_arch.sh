#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset


# params
CODE_ROOT=$(dirname "${0}")
readonly OUTPUT="${1:-/tmp/aiorpc_service.sh}"
readonly SRC_ROOT="${2:-/home/koder/workspace}"

# settings
readonly TAR_PACK_OPT="--xz"
readonly IMAGE="ubuntu:16.04"
LOCAL_PACKAGES="${SRC_ROOT}/aiorpc ${SRC_ROOT}/cephlib ${SRC_ROOT}/koder_utils ${SRC_ROOT}/ceph_report"
LOCAL_PACKAGES+=" ${SRC_ROOT}/xmlbuilder3"
readonly LOCAL_PACKAGES

# not importans settings
readonly TMP_ROOT=/opt/root
readonly UNPACK_SH="${CODE_ROOT}/unpack.sh"
readonly NEEDED_PACKAGES="libpython3.7-minimal libpython3.7-stdlib python3.7-distutils python3.7-minimal"
readonly PIPOPTS="--no-warn-script-location"
readonly PYTHON="${TMP_ROOT}/usr/bin/python3.7"
DOCKER_ID="no-such-id"  # will be set later after container spawned


if [[ "${TAR_PACK_OPT}" == "--xz" ]] ; then
    readonly ARCH=package.tar.xz
else
    if [[ "${TAR_PACK_OPT}" != "--gzip" ]] ; then
        echo "Only xz or gzip supported as tar archivers. Set TAR_PACK_OPT(=${TAR_PACK_OPT}) to either --xz or --gzip"
        exit 1
    fi
    readonly ARCH=package.tar.gz
fi

function start_container {
    local -r image="${1}"
    local -r did=$(docker run -d "${image}" sleep 36000000)
    echo "${did:0:12}"
}

function remove_container {
    docker kill "${DOCKER_ID}"
    docker rm "${DOCKER_ID}"
}

function dexec {
    docker exec --interactive "${DOCKER_ID}" "${@}"
}

function clear {
    dexec rm --recursive --force "${TMP_ROOT}"
}

function install_tools {
    dexec apt update
    dexec apt install --assume-yes unzip curl libexpat1 xz-utils software-properties-common
}

function download_and_install_python37 {
    dexec apt-add-repository --yes ppa:deadsnakes/ppa
    dexec apt update
    dexec rm -rf '/var/cache/apt/archives/*python3.7*.deb'
    files=$(dexec apt install --quiet --assume-yes --download-only $NEEDED_PACKAGES 2>/dev/null | \
            awk '/^Get:/ {print "/var/cache/apt/archives/" $5 "_" $7 "_" $6 ".deb"}')
    echo $files

    for file in ${files} ; do
        dexec dpkg-deb --extract "${file}" "${TMP_ROOT}"
    done
}

function install_pip {
    dexec curl --silent --output /tmp/get-pip.py https://bootstrap.pypa.io/get-pip.py
    dexec "${PYTHON}" /tmp/get-pip.py --prefix /opt/root/usr
    dexec mkdir --parents /opt/root/usr/local/lib/python3.7/dist-packages
    echo /opt/root/usr/lib/python3.7/site-packages | dexec tee /opt/root/usr/local/lib/python3.7/dist-packages/pip.pth
}

function install_local_packages {
    local -r local_packages="${1}"
    for package in ${local_packages} ; do
        local name=$(basename "${package}")
        dexec rm --recursive --force "/tmp/${name}"
        docker cp "${package}" "${DOCKER_ID}:/tmp"
        dexec "${PYTHON}" -m pip install ${PIPOPTS} "/tmp/${name}" --no-deps
    done
}

function install_local_packages_deps {
    local -r local_packages="${1}"
    for package in ${local_packages} ; do
        local name=$(basename ${package})
        dexec "${PYTHON}" -m pip install ${PIPOPTS} -r "/tmp/${name}/requirements.txt"
    done
    dexec cp --recursive /tmp/aiorpc/aiorpc_service_files /opt/root/usr/local/lib/python3.7/dist-packages
    dexec cp --recursive /tmp/ceph_report/ceph_report_files /opt/root/usr/local/lib/python3.7/dist-packages
}

function remove_pip {
    dexec rm --recursive --force /opt/root/usr/lib/python3.7/site-packages
}

function remove_extra_files {
    dexec rm --recursive --force "${TMP_ROOT}/usr/share"
    local -r pycache_dirs=$(dexec find "${TMP_ROOT}" -type d -iregex '.*/__pycache__$')
    dexec rm --recursive --force ${pycache_dirs}
    local -r dist_info_dirc=$(dexec find "${TMP_ROOT}" -type d -iregex '.*/[^/]+\.dist-info$')
    dexec rm --recursive --force ${dist_info_dirc}
    local -r c_files=$(dexec find "${TMP_ROOT}" -type f -iregex '.*/[^/]+\.c$')
    dexec rm --force ${c_files}
    local -r pyx_files=$(dexec find "${TMP_ROOT}" -type f -iregex '.*/[^/]+\.pyx$')
    dexec rm --force ${pyx_files}
    local -r pxi_files=$(dexec find "${TMP_ROOT}" -type f -iregex '.*/[^/]+\.pxi$')
    dexec rm --force ${pxi_files}
    local -r pyi_files=$(dexec find "${TMP_ROOT}" -type f -iregex '.*/[^/]+\.pyi$')
    dexec rm --force ${pyi_files}
}

function pack {
    dexec rm --force "/tmp/${ARCH}"
    dexec tar --create "${TAR_PACK_OPT}" --file "/tmp/${ARCH}" --directory "${TMP_ROOT}" .
}

function make_self_unpack_arch {
    docker cp "${DOCKER_ID}:/tmp/${ARCH}" /tmp
    cat "${UNPACK_SH}" | sed "s/{FILL_TAR_OPT}/${TAR_PACK_OPT}/g" > "${OUTPUT}"
    cat "/tmp/${ARCH}" >> "${OUTPUT}"
}

function show_result_props {
    local size=$(ls -l --human-readable "${OUTPUT}" | awk '{print $5}')
    local md5=$(md5sum "${OUTPUT}" | awk '{print $1}')
    echo "${OUTPUT}  size=${size}   md5=${md5}"
}

echo ">>>> Starting docker container from image $IMAGE"
DOCKER_ID=$(start_container "${IMAGE}")
echo ">>>> Container started with id=$DOCKER_ID"
# clear

echo ">>>> Installing packages"
install_tools  >/dev/null

echo ">>>> Installing python37"
download_and_install_python37  >/dev/null

echo ">>>> Installing pip"
install_pip  >/dev/null

echo ">>>> Installing packages from local machine"
install_local_packages "${LOCAL_PACKAGES}"  >/dev/null

echo ">>>> Installing dependencies"
install_local_packages_deps "${LOCAL_PACKAGES}"  >/dev/null

echo ">>>> Removing pip"
remove_pip  >/dev/null

echo ">>>> Removing extra files"
remove_extra_files  >/dev/null

echo ">>>> Packiing $TMP_ROOT"
pack  >/dev/null

echo ">>>> Making result archive"
make_self_unpack_arch  >/dev/null

echo ">>>> Removing container"
remove_container  >/dev/null

echo
echo
show_result_props
