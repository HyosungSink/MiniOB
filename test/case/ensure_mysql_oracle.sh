#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"

build_dir="${MINIOB_MYSQL_BUILD_DIR:-${repo_root}/../miniob-mysql-build}"
data_dir="${MINIOB_MYSQL_DATA_DIR:-${repo_root}/../miniob-mysql-data}"
socket_path="${MINIOB_MYSQL_SOCKET:-/tmp/miniob2026-mysql.sock}"
pid_file="${MINIOB_MYSQL_PID_FILE:-/tmp/miniob2026-mysql.pid}"
jobs="${MINIOB_MYSQL_BUILD_JOBS:-2}"

mysqld="${build_dir}/runtime_output_directory/mysqld"
mysqladmin="${build_dir}/runtime_output_directory/mysqladmin"

if [[ -x "${mysqladmin}" ]] && [[ -S "${socket_path}" ]]; then
  if "${mysqladmin}" --socket="${socket_path}" -uroot ping >/dev/null 2>&1; then
    echo "MySQL oracle is already running on ${socket_path}"
    exit 0
  fi
fi

if [[ ! -d "${repo_root}/mysql-server/sql" ]]; then
  git -C "${repo_root}" submodule update --init mysql-server
fi

if [[ ! -f "${build_dir}/build.ninja" ]]; then
  cmake -S "${repo_root}/mysql-server" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DWITH_UNIT_TESTS=OFF \
    -DWITH_ROUTER=OFF \
    -DWITH_NDB=OFF \
    -DWITH_NDBCLUSTER_STORAGE_ENGINE=OFF \
    -DWITH_AUTHENTICATION_LDAP=OFF \
    -DWITH_AUTHENTICATION_KERBEROS=OFF \
    -DWITH_AUTHENTICATION_WEBAUTHN=OFF \
    -DWITH_AUTHENTICATION_CLIENT_PLUGINS=OFF \
    -DWITH_FIDO=none \
    -DWITH_PROTOBUF=bundled \
    -DWITH_SSL=system
fi

cmake --build "${build_dir}" --target mysqld mysql mysqladmin -j"${jobs}"

if [[ ! -x "${mysqld}" || ! -x "${mysqladmin}" ]]; then
  echo "MySQL build did not produce mysqld/mysqladmin under ${build_dir}" >&2
  exit 1
fi

if [[ ! -d "${data_dir}/mysql" ]]; then
  rm -rf "${data_dir}"
  mkdir -p "${data_dir}"
  "${mysqld}" --initialize-insecure \
    --user=root \
    --datadir="${data_dir}" \
    --basedir="${build_dir}"
fi

rm -f "${socket_path}" "${pid_file}"

"${mysqld}" \
  --user=root \
  --datadir="${data_dir}" \
  --basedir="${build_dir}" \
  --socket="${socket_path}" \
  --pid-file="${pid_file}" \
  --skip-networking \
  --mysqlx=OFF \
  --log-error="${data_dir}/mysqld.err" \
  --daemonize

"${mysqladmin}" --socket="${socket_path}" -uroot ping
echo "MySQL oracle is running on ${socket_path}"
