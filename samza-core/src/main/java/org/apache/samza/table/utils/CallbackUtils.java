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

package org.apache.samza.table.utils;

import org.apache.samza.metrics.Timer;
import org.apache.samza.table.TableOpCallback;
import org.slf4j.Logger;


/**
 * Helper class to invoke table callbacks and update the matching timer metric.
 * It also catches any possible exceptions thrown by the callbacks on behalf of
 * the underlying reader/write function in case exceptions are not properly by them.
 */
public final class CallbackUtils {
  private final Logger logger;
  private final String tableId;

  public CallbackUtils(String tableId, Logger logger) {
    this.tableId = tableId;
    this.logger = logger;
  }

  /**
   * Invoke a callback with the return {@code value} and {@code error} from the async method.
   * The specified Timer metric is updated with the time taken by the callback to return.
   * Caller should ensure callback and metric are both valid to save null validation cost.
   * @param callback application callback
   * @param value value of the table record (nullable for write-only operation)
   * @param error error exception thrown by the table function when it fails (nullable)
   * @param metric timer metric to update the call time
   */
  public void invoke(TableOpCallback callback, Object value, Throwable error, Timer metric) {
    try {
      long startNs = System.nanoTime();
      callback.onComplete(value, error);
      metric.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      logger.warn("Exception thrown by callback, table=" + tableId, e);
    }
  }
}
