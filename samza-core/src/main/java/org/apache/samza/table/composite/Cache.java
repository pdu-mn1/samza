package org.apache.samza.table.composite;

import java.io.Serializable;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.task.TaskContext;


public interface Cache<K, V> extends Serializable {
  V get(K key);
  void put(K key, V value);
  void invalidate(K key);

  /**
   * Initialize Cache with container and task context
   * @param containerContext Samza container context
   * @param taskContext nullable for global table
   */
  default void init(SamzaContainerContext containerContext, TaskContext taskContext) {
  }
}
