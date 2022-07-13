/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedClientPool implements ClientPool<IMetaStoreClient, TException> {

  private static Cache<String, HiveClientPool> clientPoolCache;

  private final Configuration conf;
  private final String metastoreUri;
  private final int clientPoolSize;
  private final long evictionInterval;
  private static final Logger LOG = LoggerFactory.getLogger(CachedClientPool.class);

  CachedClientPool(Configuration conf, Map<String, String> properties) {
    this.conf = conf;
    this.metastoreUri = conf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
    this.clientPoolSize = PropertyUtil.propertyAsInt(properties,
            CatalogProperties.CLIENT_POOL_SIZE,
            CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
    this.evictionInterval = PropertyUtil.propertyAsLong(properties,
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);
    init();
  }

  @VisibleForTesting
  @SuppressWarnings({"Slf4jConstantLogMessage"})
  synchronized HiveClientPool clientPool() {
    SparkSession sparkSession = SparkSession.getActiveSession().get();
    SparkContext sc = sparkSession.sparkContext();
    SQLConf sqlConf = sparkSession.sqlContext().conf();
    String sqlUser = "";
    if (sqlConf.hiveSubmitSqlHasView()) {
      sqlUser = sqlConf.hiveRealSubmitUser();
    } else if (sc.conf().getBoolean("spark.proxyuser.enabled", false)) {
      sqlUser = sc.getLocalProperty("spark.sql.user");
    } else {
      sqlUser = sparkSession.sparkContext().sparkUser();
    }
    if (sqlUser == null || "".equals(sqlUser)) {
      sqlUser = sparkSession.sparkContext().sparkUser();
    }
    UserGroupInformation proxyUser = UserGroupInformation.createRemoteUser(sqlUser);
    LOG.info("Iceberg catalog to " + metastoreUri + " with user " + proxyUser.getShortUserName());
    try {
      return proxyUser.doAs(
              (PrivilegedExceptionAction<HiveClientPool>) () ->
                      clientPoolCache.get(metastoreUri + proxyUser.getShortUserName(), k -> new HiveClientPool(clientPoolSize, conf))
      );
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e);
    }
    return clientPoolCache.get(metastoreUri + proxyUser.getShortUserName(), k -> new HiveClientPool(clientPoolSize, conf));
  }

  private synchronized void init() {
    if (clientPoolCache == null) {
      clientPoolCache = Caffeine.newBuilder().expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
              .removalListener((key, value, cause) -> ((HiveClientPool) value).close())
              .build();
    }
  }

  @VisibleForTesting
  static Cache<String, HiveClientPool> clientPoolCache() {
    return clientPoolCache;
  }

  @Override
  public <R> R run(Action<R, IMetaStoreClient, TException> action) throws TException, InterruptedException {
    return clientPool().run(action);
  }

  @Override
  public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
      throws TException, InterruptedException {
    return clientPool().run(action, retry);
  }
}
