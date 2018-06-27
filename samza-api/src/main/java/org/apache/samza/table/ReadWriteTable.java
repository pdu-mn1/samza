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
package org.apache.samza.table;

import java.util.List;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.storage.kv.Entry;

/**
 *
 * A table that supports get, put and delete by one or more keys
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
@InterfaceStability.Unstable
public interface ReadWriteTable<K, V> extends ReadableTable<K, V> {

  /**
   * Updates the mapping of the specified key-value pair;
   * Associates the specified {@code key} with the specified {@code value}.
   *
   * The key is deleted from the table if value is {@code null}.
   *
   * @param key the key with which the specified {@code value} is to be associated.
   * @param value the value with which the specified {@code key} is to be associated.
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  void put(K key, V value);

  /**
   * Asynchronously Updates the mapping of the specified key-value pair;
   * Associates the specified {@code key} with the specified {@code value}.
   * The key is deleted from the table if value is {@code null}.
   * Default implementation delegates to the synchronous method.
   *
   * @param key the key with which the specified {@code value} is to be associated.
   * @param value the value with which the specified {@code key} is to be associated.
   * @param callback method to be invoked when the put is done or fails.
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  default void put(K key, V value, TableOpCallback callback) {
    try {
      put(key, value);
      callback.onComplete(null, null);
    } catch (Exception e) {
      callback.onComplete(null, e);
    }
  }

  /**
   * Updates the mappings of the specified key-value {@code entries}.
   *
   * A key is deleted from the table if its corresponding value is {@code null}.
   *
   * @param entries the updated mappings to put into this table.
   * @throws NullPointerException if any of the specified {@code entries} has {@code null} as key.
   */
  void putAll(List<Entry<K, V>> entries);

  /**
   * Asynchronously updates the mappings of the specified key-value {@code entries}.
   * A key is deleted from the table if its corresponding value is {@code null}.
   * Default implementation delegates to the synchronous method.
   *
   * @param entries the updated mappings to put into this table.
   * @param callback method to be invoked when the putAll is done or fails.
   * @throws NullPointerException if any of the specified {@code entries} has {@code null} as key.
   */
  default void putAll(List<Entry<K, V>> entries, TableOpCallback callback) {
    try {
      putAll(entries);
      callback.onComplete(null, null);
    } catch (Exception e) {
      callback.onComplete(null, e);
    }
  }

  /**
   * Deletes the mapping for the specified {@code key} from this table (if such mapping exists).
   *
   * @param key the key for which the mapping is to be deleted.
   * @throws NullPointerException if the specified {@code key} is {@code null}.
   */
  void delete(K key);

  /**
   * Asynchronously deletes the mapping for the specified {@code key} from this table (if such mapping exists).
   * Default implementation delegates to the synchronous method.
   * @param key the key for which the mapping is to be deleted.
   * @param callback method to be invoked when the delete is done or fails.
   * @throws NullPointerException if the specified {@code key} is {@code null}.
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
   * Deletes the mappings for the specified {@code keys} from this table.
   *
   * @param keys the keys for which the mappings are to be deleted.
   * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
   */
  void deleteAll(List<K> keys);

  /**
   * Asynchronously deletes the mappings for the specified {@code keys} from this table.
   * Default implementation delegates to the synchronous method.
   * @param keys the keys for which the mappings are to be deleted.
   * @param callback method to be invoked when the deleteAll is done or fails.
   * @throws NullPointerException if the specified {@code keys} list, or any of the keys, is {@code null}.
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
   * Flushes the underlying store of this table, if applicable.
   */
  void flush();

}
