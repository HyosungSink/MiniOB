#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import argparse
import datetime
import decimal
import math
import re
from pathlib import Path
from typing import Optional

import pymysql


DEFAULT_TEST_DIR = Path(__file__).resolve().parent / "test"
DEFAULT_RESULT_DIR = Path(__file__).resolve().parent / "result"


class MysqlMiniobResultGenerator:
  def __init__(self, socket_path: str, case_name: str, charset: str = "utf8mb4"):
    self.socket_path = socket_path
    self.charset = charset
    self.database = "miniob_oracle_" + re.sub(r"[^0-9a-zA-Z_]", "_", case_name)
    self.connections = {}
    self.current_name = "default"

  def __enter__(self):
    admin = self._connect(database=None)
    try:
      with admin.cursor() as cursor:
        cursor.execute(f"drop database if exists `{self.database}`")
        cursor.execute(f"create database `{self.database}`")
    finally:
      admin.close()

    self.connections["default"] = self._connect(database=self.database)
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    for conn in self.connections.values():
      conn.close()
    self.connections.clear()

    admin = self._connect(database=None)
    try:
      with admin.cursor() as cursor:
        cursor.execute(f"drop database if exists `{self.database}`")
    finally:
      admin.close()

  def _connect(self, database: Optional[str]):
    return pymysql.connect(
      unix_socket=self.socket_path,
      user="root",
      database=database,
      charset=self.charset,
      autocommit=True,
    )

  def connect(self, name: str):
    name = name.strip()
    if not name:
      raise RuntimeError("empty connection name")
    if name in self.connections:
      raise RuntimeError(f"connection already exists: {name}")
    self.connections[name] = self._connect(database=self.database)

  def switch_connection(self, name: str):
    name = name.strip()
    if name not in self.connections:
      raise RuntimeError(f"unknown connection: {name}")
    self.current_name = name

  def run_sql(self, sql: str) -> list[str]:
    conn = self.connections[self.current_name]
    try:
      with conn.cursor() as cursor:
        cursor.execute(sql.rstrip().rstrip(";"))
        if cursor.description is None:
          return ["SUCCESS"]

        headers = [field[0] for field in cursor.description]
        rows = cursor.fetchall()
        result = [" | ".join(headers)]
        for row in rows:
          result.append(" | ".join(self._format_value(value) for value in row))
        return result
    except pymysql.Error:
      return ["FAILURE"]

  @staticmethod
  def _format_value(value) -> str:
    if value is None:
      return "NULL"
    if isinstance(value, datetime.datetime):
      return value.isoformat(sep=" ")
    if isinstance(value, datetime.date):
      return value.isoformat()
    if isinstance(value, decimal.Decimal):
      return format(value.normalize(), "f").rstrip("0").rstrip(".") or "0"
    if isinstance(value, float):
      if not math.isfinite(value):
        return str(value)
      rounded = decimal.Decimal(str(value)).quantize(decimal.Decimal("0.01"), rounding=decimal.ROUND_HALF_UP)
      return format(rounded.normalize(), "f").rstrip("0").rstrip(".") or "0"
    if isinstance(value, bytes):
      return value.decode("utf-8", errors="replace")
    return str(value)


def write_line(output: list[str], text: str):
  output.append(text.upper())


def normalize_sql_result(lines: list[str]) -> list[str]:
  text = "\n".join(lines).strip()
  return text.split("\n") if text else []


def render_case(socket_path: str, case_path: Path) -> str:
  output: list[str] = []
  with MysqlMiniobResultGenerator(socket_path, case_path.stem) as runner:
    for raw_line in case_path.read_text(encoding="utf-8").splitlines():
      line = raw_line.strip()
      if not line:
        write_line(output, "")
        continue
      if line.startswith("#"):
        continue
      if line.startswith("--"):
        command_line = line[2:].lstrip()
        command, _, arg = command_line.partition(" ")
        if command == "echo":
          write_line(output, arg)
        elif command == "observer-args":
          continue
        elif command == "case-state":
          continue
        elif command == "connect":
          runner.connect(arg)
        elif command == "connection":
          runner.switch_connection(arg)
        elif command == "sort":
          write_line(output, arg)
          for result_line in sorted(normalize_sql_result(runner.run_sql(arg))):
            write_line(output, result_line)
        elif command.startswith("ensure"):
          write_line(output, command_line)
        else:
          raise RuntimeError(f"unsupported command in {case_path}: {line}")
        continue

      write_line(output, line)
      if re.match(r"set\s+(hash_join|use_cascade)\s*=", line, flags=re.IGNORECASE) or \
          re.match(r"analyze\s+table\s+", line, flags=re.IGNORECASE):
        write_line(output, "SUCCESS")
        continue
      for result_line in normalize_sql_result(runner.run_sql(line)):
        write_line(output, result_line)

  return "\n".join(output) + "\n"


def discover_cases(test_dir: Path, case_names: list[str], include_fulltext: bool) -> list[Path]:
  if case_names:
    cases = []
    for name in case_names:
      case = test_dir / (name if name.endswith(".test") else f"{name}.test")
      if not case.is_file():
        raise FileNotFoundError(case)
      cases.append(case)
    return cases

  cases = sorted(test_dir.glob("miniob2025-*.test"))
  if not include_fulltext:
    cases = [case for case in cases if "full-text-index" not in case.name]
  return cases


def main():
  parser = argparse.ArgumentParser(description="Generate MiniOB-style .result files from the competition MySQL server.")
  parser.add_argument("--socket", required=True, help="Unix socket of the running competition MySQL server")
  parser.add_argument("--test-dir", type=Path, default=DEFAULT_TEST_DIR)
  parser.add_argument("--result-dir", type=Path, default=DEFAULT_RESULT_DIR)
  parser.add_argument("--case", action="append", default=[], help="Case name or .test filename; defaults to all miniob2025 cases")
  parser.add_argument("--include-fulltext", action="store_true", help="Also generate full-text results from MySQL")
  args = parser.parse_args()

  cases = discover_cases(args.test_dir, args.case, args.include_fulltext)
  args.result_dir.mkdir(parents=True, exist_ok=True)
  for case in cases:
    result_path = args.result_dir / f"{case.stem}.result"
    result_path.write_text(render_case(args.socket, case), encoding="utf-8")
    print(result_path)


if __name__ == "__main__":
  main()
