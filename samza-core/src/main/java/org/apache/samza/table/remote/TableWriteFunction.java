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

package org.apache.samza.table.remote;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.functions.ClosableFunction;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.TableOpCallback;


/**
 * A function object to be used with a {@link RemoteReadWriteTable} implementation. It encapsulates the functionality
 * of writing table record(s) for a provided set of key(s) to the store.
 *
 * <p> Instances of {@link TableWriteFunction} are meant to be serializable. ie. any non-serializable state
 * (eg: network sockets) should be marked as transient and recreated inside readObject().
 *
 * <p> Implementations are expected to be thread-safe.
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
@InterfaceStability.Unstable
public interface TableWriteFunction<K, V> extends Serializable, InitableFunction, ClosableFunction {
  /**
   * Store single table {@code record} with specified {@code key}. This method must be thread-safe.
   *
   * The key is deleted if record is {@code null}.
   *
   * @param key key for the table record
   * @param record table record to be written
   */
  void put(K key, V record);

  /**
   * Asynchronously store single table {@code record} with specified {@code key}. This method must be thread-safe.
   *
   * The key is deleted if record is {@code null}.
   *
   * Default implementation delegates to the synchronous method.
   *
   * @param key key for the table record
   * @param record table record to be written
   * @param callback method to be invoked when the put is done or fails.
   */
  default void put(K key, V record, TableOpCallback callback) {
    try {
      put(key, record);
      callback.onComplete(null, null);
    } catch (Exception e) {
      callback.onComplete(null, e);
    }
  }

  /**
   * Store the table {@code records} with specified {@code keys}. This method must be thread-safe.
   *
   * A key is deleted if its corresponding record is {@code null}.
   *
   * @param records table records to be written
   */
  default void putAll(List<Entry<K, V>> records) {
    records.forEach(e -> put(e.getKey(), e.getValue()));
  }

  /**
   * Asynchronously store the table {@code records} with specified {@code keys}. This method must be thread-safe.
   *
   * A key is deleted if its corresponding record is {@code null}.
   *
   * Default implementation delegates to the synchronous method.
   *
   * @param records table records to be written
   * @param callback method to be invoked when the put is done or fails.
   */
  default void putAll(List<Entry<K, V>> records, TableOpCallback callback) {
    try {
      putAll(records);
      callback.onComplete(null, null);
    } catch (Exception e) {
      callback.onComplete(null, e);
    }
  }

  /**
   * Delete the {@code record} with specified {@code key} from the remote store
   * @param key key to the table record to be deleted
   */
  void delete(K key);

  /**
   * Asynchronously delete the {@code record} with specified {@code key} from the remote store
   * Default implementation delegates to the synchronous method.
   * @param key key to the table record to be deleted
   * @param callback method to be invoked when the put is done or fails.
   */
  default void delete(K key, TableOpCallback callback) {
    try {
      delete(key);
      callback.onComplete(null, null);
    } catch (Exception e) {
      callback.onComplete(null, e);
    }
  }

  /**
   * Delete all {@code records} with the specified {@code keys} from the remote store
   * @param keys keys for the table records to be written
   */
  default void deleteAll(Collection<K> keys) {
    keys.stream().forEach(k -> delete(k));
  }

  /**
   * Asynchronously delete all {@code records} with the specified {@code keys} from the remote store
   * Default implementation delegates to the synchronous method.
   * @param keys keys for the table records to be written
   * @param callback method to be invoked when the put is done or fails.
   */
  default void deleteAll(List<K> keys, TableOpCallback callback) {
    try {
      deleteAll(keys);
      callback.onComplete(null, null);
    } catch (Exception e) {
      callback.onComplete(null, e);
    }
  }

  /**
   * Flush the remote store (optional)
   */
  default void flush() {
  }

  // optionally implement readObject() to initialize transient states
}
