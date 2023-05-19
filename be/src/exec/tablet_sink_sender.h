// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/tablet_sink.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "common/tracer.h"
#include "exec/data_sink.h"
#include "exec/tablet_info.h"
#include "exec/tablet_sink_index_channel.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/mem_tracker.h"
#include "util/bitmap.h"
#include "util/compression/block_compression.h"
#include "util/raw_container.h"
#include "util/ref_count_closure.h"
#include "util/reusable_closure.h"
#include "util/threadpool.h"

namespace starrocks {

class Bitmap;
class MemTracker;
class RuntimeProfile;
class RowDescriptor;
class TupleDescriptor;
class ExprContext;
class TExpr;

namespace stream_load {
// TabletSinkSender will control one index/table's send chunks.
class TabletSinkSender {
public:
private:
    Status _send_chunk(Chunk* chunk);

    Status _send_chunk_with_colocate_index(Chunk* chunk);

    Status _send_chunk_by_node(Chunk* chunk, IndexChannel* channel, std::vector<uint16_t>& selection_idx);

    Status _fill_auto_increment_id(Chunk* chunk);

    Status _fill_auto_increment_id_internal(Chunk* chunk, SlotDescriptor* slot, int64_t table_id);

private:
    bool _enable_replicated_storage{false};
    std::set<int64_t> _partition_ids;
    OlapTableLocationParam* _location = nullptr;
    // index_channel
    std::vector<std::unique_ptr<IndexChannel>> _channels;
    std::vector<OlapTablePartition*> _partitions;
    std::vector<uint32_t> _tablet_indexes;
    // one chunk selection index for partition validation and data validation
    std::vector<uint16_t> _validate_select_idx;
    // one chunk selection for BE node
    std::vector<uint32_t> _node_select_idx;
    std::vector<int64_t> _tablet_ids;
    OlapTablePartitionParam* _vectorized_partition = nullptr;
    std::vector<std::vector<int64_t>> _index_tablet_ids;
    // Store the output expr comput result column
    std::unique_ptr<Chunk> _output_chunk;
};

} // namespace stream_load
} // namespace starrocks
