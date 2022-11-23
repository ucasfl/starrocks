// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.server.GlobalStateMgr;
import org.apache.iceberg.TableScan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class AsyncManifestCacheScheduler extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(AsyncManifestCacheScheduler.class);

    private final ExecutorService icebergManifestCacheRefreshExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
            Config.iceberg_async_manifest_cache_refresh_threads, Integer.MAX_VALUE, "iceberg_async_manifest_refresh_pool", true);

    // (DbId, TableId) for a collection of objects marked with "async_manifest_cache.enable" = "true" on the table
    private final Set<Pair<Long, Long>> asyncManifestCacheTableInfo = Sets.newConcurrentHashSet();

    private boolean initialize = false;

    public AsyncManifestCacheScheduler(String name, long intervalMs) {
        super(name, intervalMs);
        this.initialize = false;
    }

    public void registerAsyncManifestCacheTable(Long dbId, Long tableId) {
        asyncManifestCacheTableInfo.add(new Pair<>(dbId, tableId));
    }

    public void removeAsyncManifestCacheTable(Long dbId, Long tableId) {
        asyncManifestCacheTableInfo.remove(new Pair<>(dbId, tableId));
    }

    private void executeAsyncManifestCache() {
        Iterator<Pair<Long, Long>> iterator = asyncManifestCacheTableInfo.iterator();

        while (iterator.hasNext()) {
            Pair<Long, Long> tableInfo = iterator.next();
            Long dbId = tableInfo.first;
            Long tableId = tableInfo.second;
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                iterator.remove();
                continue;
            }

            IcebergTable icebergTable;

            db.readLock();
            try {
                icebergTable = (IcebergTable) db.getTable(tableId);
                if (icebergTable == null || !icebergTable.getIcebergAsyncManifestCacheEnable().equals("true")) {
                    iterator.remove();
                    continue;
                }

                org.apache.iceberg.Table underlineIcebergTable = icebergTable.getIcebergTable();
                TableScan tableScan = underlineIcebergTable.newScan();
                icebergManifestCacheRefreshExecutor.submit(tableScan::planFiles);
            } finally {
                LOG.info("Async refresh manifest cache of Iceberg table, dbId: {}, tableId: {}", dbId, tableId);
                db.readUnlock();
            }
        }
    }

    private void initAsyncManifestCacheTable() {
        for (Long dbId : GlobalStateMgr.getCurrentState().getDbIds()) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                continue;
            }
            db.readLock();
            try {
                for (Table table : GlobalStateMgr.getCurrentState().getDb(dbId).getTables()) {
                    if (table instanceof IcebergTable && IcebergUtil.isAsyncManifestCacheTable((IcebergTable) table)) {
                        registerAsyncManifestCacheTable(db.getId(), table.getId());
                    }
                }
            } finally {
                db.readUnlock();
            }
        }
        initialize = true;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!initialize) {
            initAsyncManifestCacheTable();
        }
        setInterval(Config.aync_iceberg_manifest_cache_interval_seconds * 1000L);
        executeAsyncManifestCache();
    }
}
