/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved. */

#pragma once

#include "common/sys/rc.h"

class SQLStageEvent;

class DropIndexExecutor
{
public:
  DropIndexExecutor() = default;
  ~DropIndexExecutor() = default;

  RC execute(SQLStageEvent *sql_event);
};
