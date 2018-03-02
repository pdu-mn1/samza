package org.apache.samza.table.composite;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.operators.KV;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableSpec;
import org.apache.samza.task.TaskContext;


public class CachedTableProvider implements TableProvider {
  static final String CACHE = "cache";
  public static final String INNER_TABLE_ID = "innerTableId";

  private final String innerTableId;
  private final TableSpec tableSpec;

  public CachedTableProvider(TableSpec tableSpec) {
    innerTableId = tableSpec.getConfig().get(INNER_TABLE_ID);
    this.tableSpec = tableSpec;
  }

  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    this.taskContext = taskContext;
  }

  TaskContext taskContext;

  @Override
  public Table getTable() {
    Cache cache = deserializeObject(CACHE);
    Table<KV<?, ?>> innerTable = taskContext.getTable(innerTableId);

    if (innerTable instanceof ReadWriteTable) {
      return new CachedReadWriteTable((ReadWriteTable) innerTable, cache);
    } else {
      return new CachedReadableTable((ReadableTable) innerTable, cache);
    }
  }

  private <T> T deserializeObject(String key) {
    String entry = tableSpec.getConfig().getOrDefault(key, "");
    if (entry.isEmpty()) {
      return null;
    }

    try {
      byte [] bytes = Base64.getDecoder().decode(entry);
      return (T) new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
    } catch (Exception e) {
      String errMsg = "Failed to deserialize " + key;
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public Map<String, String> generateConfig(Map<String, String> config) {
    return Collections.emptyMap();
  }

  @Override
  public void close() {
  }
}
