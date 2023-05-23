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
    TabletSinkSender(OlapTableLocationParam* location, OlapTablePartitionParam* vectorized_partition,
                     std::vector<IndexChannel*> channels, bool colocate_mv_index, bool enable_replicated_storage)
            : _location(location),
              _vectorized_partition(vectorized_partition),
              _channels(std::move(channels)),
              _colocate_mv_index(colocate_mv_index),
              _enable_replicated_storage(enable_replicated_storage) {}
    ~TabletSinkSender() = default;

public:
    Status send_chunk(const std::vector<OlapTablePartition*>& partitions, const std::vector<uint32_t>& tablet_indexes,
                      const std::vector<uint16_t>& validate_select_idx, Chunk* chunk) {
        if (_colocate_mv_index) {
            return _send_chunk_with_colocate_index(partitions, tablet_indexes, validate_select_idx, chunk);
        } else {
            return _send_chunk(partitions, tablet_indexes, validate_select_idx, chunk);
        }
    }
    Status try_close(RuntimeState* state);

    void for_each_node_channel(const std::function<void(NodeChannel*)>& func) {
        for (auto& it : _node_channels) {
            func(it.second.get());
        }
    }

    void for_each_index_channel(const std::function<void(NodeChannel*)>& func) {
        for (auto& index_channel : _channels) {
            index_channel->for_each_node_channel(func);
        }
    }

private:
    Status _send_chunk(const std::vector<OlapTablePartition*>& partitions, const std::vector<uint32_t>& tablet_indexes,
                       const std::vector<uint16_t>& validate_select_idx, Chunk* chunk);

    Status _send_chunk_with_colocate_index(const std::vector<OlapTablePartition*>& _partitions,
                                           const std::vector<uint32_t>& tablet_indexes,
                                           const std::vector<uint16_t>& validate_select_idx, Chunk* chunk);

    Status _send_chunk_by_node(Chunk* chunk, IndexChannel* channel, const std::vector<uint16_t>& selection_idx);

    void _mark_as_failed(const NodeChannel* ch) { _failed_channels.insert(ch->node_id()); }
    bool _is_failed_channel(const NodeChannel* ch) { return _failed_channels.count(ch->node_id()) != 0; }
    bool _has_intolerable_failure() {
        if (_write_quorum_type == TWriteQuorumType::ALL) {
            return _failed_channels.size() > 0;
        } else if (_write_quorum_type == TWriteQuorumType::ONE) {
            return _failed_channels.size() >= _num_repicas;
        } else {
            return _failed_channels.size() >= ((_num_repicas + 1) / 2);
        }
    }

private:
    OlapTableLocationParam* _location = nullptr;
    // partition schema
    OlapTablePartitionParam* _vectorized_partition = nullptr;
    // index_channel
    std::vector<IndexChannel*> _channels;
    bool _colocate_mv_index{false};
    bool _enable_replicated_storage{false};
    TWriteQuorumType::type _write_quorum_type = TWriteQuorumType::MAJORITY;
    std::unordered_map<int64_t, std::unique_ptr<NodeChannel>> _node_channels;
    int _num_repicas = -1;

    // one chunk selection for BE node
    std::vector<uint32_t> _node_select_idx;
    std::vector<int64_t> _tablet_ids;
    std::vector<std::vector<int64_t>> _index_tablet_ids;
    std::set<int64_t> _failed_channels;
};

} // namespace stream_load
} // namespace starrocks
