#!/bin/sh

set -e

DOWNLOAD_DIR="${HOME}/tmp"
INSTALL_PREFIX="${HOME}/opt"

PACKAGE_NAME="${MPI_LIBRARY:?"MPI_LIBRARY not set!"}-${MPI_LIBRARY_VERSION:?"MPI_LIBRARY_VERSION not set!"}"
INSTALL_PREFIX="${INSTALL_PREFIX}/${PACKAGE_NAME}"
TARBALL_NAME="${PACKAGE_NAME}.tar.gz"

if [ -d "${INSTALL_PREFIX}" ]
then
  echo "MPI library already installed: ${PACKAGE_NAME}"
  exit 0
fi

case "$MPI_LIBRARY" in
  openmpi)
    OPENMPI_SHORTVERSION=$(expr "${MPI_LIBRARY_VERSION}" ":" "\(.\{1,\}\..\{1,\}\)\..\{1,\}")
    SOURCE_URL="http://www.open-mpi.org/software/ompi/v${OPENMPI_SHORTVERSION}/downloads/${TARBALL_NAME}"
    ;;
  mpich)
    SOURCE_URL="http://www.mpich.org/static/downloads/${MPI_LIBRARY_VERSION}/${TARBALL_NAME}"
    ;;
esac

echo "Installing MPI library: ${PACKAGE_NAME}"
echo "into: ${INSTALL_PREFIX}"
echo "Download URL: ${SOURCE_URL}"
echo "Tarball name: ${TARBALL_NAME}"

mkdir -p "${DOWNLOAD_DIR}"
cd "${DOWNLOAD_DIR}"
rm -rf "${TARBALL_NAME}"
wget --no-check-certificate -O "${TARBALL_NAME}" "${SOURCE_URL}"
rm -rf "${PACKAGE_NAME}"
tar -xzf "${TARBALL_NAME}"

cd "${PACKAGE_NAME}"
mkdir -p "${INSTALL_PREFIX}"
./configure --enable-shared --prefix="${INSTALL_PREFIX}"
make -j 2
make -j 2 install
