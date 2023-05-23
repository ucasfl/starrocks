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

#include "exec/tablet_sink_sender.h"

#include "agent/master_info.h"
#include "agent/utils.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/statusor.h"
#include "config.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/stream_epoch_manager.h"
#include "exec/tablet_sink.h"
#include "exprs/expr.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "serde/protobuf_serde.h"
#include "simd/simd.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "types/hll.h"
#include "util/brpc_stub_cache.h"
#include "util/compression/compression_utils.h"
#include "util/defer_op.h"
#include "util/thread.h"
#include "util/thrift_rpc_helper.h"
#include "util/uid_util.h"

namespace starrocks::stream_load {

Status TabletSinkSender::_send_chunk(const std::vector<OlapTablePartition*>& partitions,
                                     const std::vector<uint32_t>& tablet_indexes,
                                     const std::vector<uint16_t>& validate_select_idx, Chunk* chunk) {
    size_t num_rows = chunk->num_rows();
    size_t selection_size = validate_select_idx.size();
    if (selection_size == 0) {
        return Status::OK();
    }
    _tablet_ids.resize(num_rows);
    if (num_rows > selection_size) {
        size_t index_size = partitions[validate_select_idx[0]]->indexes.size();
        for (size_t i = 0; i < index_size; ++i) {
            for (size_t j = 0; j < selection_size; ++j) {
                uint16_t selection = validate_select_idx[j];
                _tablet_ids[selection] = partitions[selection]->indexes[i].tablets[tablet_indexes[selection]];
            }
            RETURN_IF_ERROR(_send_chunk_by_node(chunk, _channels[i], validate_select_idx));
        }
    } else { // Improve for all rows are selected

        size_t index_size = partitions[0]->indexes.size();
        for (size_t i = 0; i < index_size; ++i) {
            for (size_t j = 0; j < num_rows; ++j) {
                _tablet_ids[j] = partitions[j]->indexes[i].tablets[tablet_indexes[j]];
            }
            RETURN_IF_ERROR(_send_chunk_by_node(chunk, _channels[i], validate_select_idx));
        }
    }
    return Status::OK();
}

Status TabletSinkSender::_send_chunk_with_colocate_index(const std::vector<OlapTablePartition*>& partitions,
                                                         const std::vector<uint32_t>& tablet_indexes,
                                                         const std::vector<uint16_t>& validate_select_idx,
                                                         Chunk* chunk) {
    Status err_st = Status::OK();
    size_t num_rows = chunk->num_rows();
    size_t selection_size = validate_select_idx.size();
    if (selection_size == 0) {
        return Status::OK();
    }
    if (num_rows > selection_size) {
        size_t index_size = partitions[validate_select_idx[0]]->indexes.size();
        _index_tablet_ids.resize(index_size);
        for (size_t i = 0; i < index_size; ++i) {
            _index_tablet_ids[i].resize(num_rows);
            for (size_t j = 0; j < selection_size; ++j) {
                uint16_t selection = validate_select_idx[j];
                _index_tablet_ids[i][selection] = partitions[selection]->indexes[i].tablets[tablet_indexes[selection]];
            }
        }
    } else { // Improve for all rows are selected
        size_t index_size = partitions[0]->indexes.size();
        _index_tablet_ids.resize(index_size);
        for (size_t i = 0; i < index_size; ++i) {
            _index_tablet_ids[i].resize(num_rows);
            for (size_t j = 0; j < num_rows; ++j) {
                _index_tablet_ids[i][j] = partitions[j]->indexes[i].tablets[tablet_indexes[j]];
            }
        }
    }
    return Status::OK();
}

Status TabletSinkSender::_send_chunk_by_node(Chunk* chunk, IndexChannel* channel,
                                             const std::vector<uint16_t>& selection_idx) {
    Status err_st = Status::OK();
    for (auto& it : channel->_node_channels) {
        int64_t be_id = it.first;
        _node_select_idx.clear();
        _node_select_idx.reserve(selection_idx.size());
        for (unsigned short selection : selection_idx) {
            std::vector<int64_t>& be_ids = channel->_tablet_to_be.find(_tablet_ids[selection])->second;
            if (_enable_replicated_storage) {
                // TODO(meegoo): add backlist policy
                // first replica is primary replica, which determined by FE now
                // only send to primary replica when enable replicated storage engine
                if (be_ids[0] == be_id) {
                    _node_select_idx.emplace_back(selection);
                }
            } else {
                if (std::find(be_ids.begin(), be_ids.end(), be_id) != be_ids.end()) {
                    _node_select_idx.emplace_back(selection);
                }
            }
        }
        NodeChannel* node = it.second.get();
        auto st = node->add_chunk(chunk, _tablet_ids, _node_select_idx, 0, _node_select_idx.size());

        if (!st.ok()) {
            LOG(WARNING) << node->name() << ", tablet add chunk failed, " << node->print_load_info()
                         << ", node=" << node->node_info()->host << ":" << node->node_info()->brpc_port
                         << ", errmsg=" << st.get_error_msg();
            channel->mark_as_failed(node);
            err_st = st;
            // we only send to primary replica, if it fail whole load fail
            if (_enable_replicated_storage) {
                return err_st;
            }
        }
        if (channel->has_intolerable_failure()) {
            return err_st;
        }
    }
    return Status::OK();
}

Status TabletSinkSender::try_close(RuntimeState* state) {
    Status err_st = Status::OK();
    bool intolerable_failure = false;
    if (_colocate_mv_index) {
        for_each_node_channel([this, &err_st, &intolerable_failure](NodeChannel* ch) {
            if (!this->is_failed_channel(ch)) {
                auto st = ch->try_close();
                if (!st.ok()) {
                    LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                                 << ", load_info=" << ch->print_load_info() << ", error_msg=" << st.get_error_msg();
                    err_st = st;
                    this->mark_as_failed(ch);
                }
            }
            if (this->has_intolerable_failure()) {
                intolerable_failure = true;
            }
        });
    } else {
        for (auto& index_channel : _channels) {
            if (index_channel->has_incremental_node_channel()) {
                // close initial node channel and wait it done
                index_channel->for_each_initial_node_channel(
                        [&index_channel, &err_st, &intolerable_failure](NodeChannel* ch) {
                            if (!index_channel->is_failed_channel(ch)) {
                                auto st = ch->try_close(true);
                                if (!st.ok()) {
                                    LOG(WARNING) << "close initial channel failed. channel_name=" << ch->name()
                                                 << ", load_info=" << ch->print_load_info()
                                                 << ", error_msg=" << st.get_error_msg();
                                    err_st = st;
                                    index_channel->mark_as_failed(ch);
                                }
                            }
                            if (index_channel->has_intolerable_failure()) {
                                intolerable_failure = true;
                            }
                        });

                if (intolerable_failure) {
                    break;
                }

                bool is_initial_node_channel_close_done = true;
                index_channel->for_each_initial_node_channel([&is_initial_node_channel_close_done](NodeChannel* ch) {
                    is_initial_node_channel_close_done &= ch->is_close_done();
                });

                // close initial node channel not finish, can not close incremental node channel
                if (!is_initial_node_channel_close_done) {
                    break;
                }

                // close incremental node channel
                index_channel->for_each_incremental_node_channel(
                        [&index_channel, &err_st, &intolerable_failure](NodeChannel* ch) {
                            if (!index_channel->is_failed_channel(ch)) {
                                auto st = ch->try_close();
                                if (!st.ok()) {
                                    LOG(WARNING) << "close incremental channel failed. channel_name=" << ch->name()
                                                 << ", load_info=" << ch->print_load_info()
                                                 << ", error_msg=" << st.get_error_msg();
                                    err_st = st;
                                    index_channel->mark_as_failed(ch);
                                }
                            }
                            if (index_channel->has_intolerable_failure()) {
                                intolerable_failure = true;
                            }
                        });

            } else {
                index_channel->for_each_node_channel([&index_channel, &err_st, &intolerable_failure](NodeChannel* ch) {
                    if (!index_channel->is_failed_channel(ch)) {
                        auto st = ch->try_close();
                        if (!st.ok()) {
                            LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                                         << ", load_info=" << ch->print_load_info()
                                         << ", error_msg=" << st.get_error_msg();
                            err_st = st;
                            index_channel->mark_as_failed(ch);
                        }
                    }
                    if (index_channel->has_intolerable_failure()) {
                        intolerable_failure = true;
                    }
                });
            }
        }
    }

    if (intolerable_failure) {
        return err_st;
    } else {
        return Status::OK();
    }
}
} // namespace starrocks::stream_load
