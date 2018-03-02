package org.apache.samza.table.composite;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.apache.samza.table.remote.RemoteTableProviderFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;


public class CachedTableDescriptor<K, V> extends BaseTableDescriptor<K, V, CachedTableDescriptor<K, V>> {
  private String innerTableId;
  private Cache<K, V> cache;

  /**
   * Constructs a table descriptor instance

   * @param tableId Id of the table
   */
  protected CachedTableDescriptor(String tableId) {
    super(tableId);
  }

  @Override
  public TableSpec getTableSpec() {
    validate();

    if (cache == null) {
      cache = new GuavaCache<>(innerTableId, CacheBuilder.newBuilder().build());
    }

    Map<String, String> tableSpecConfig = new HashMap<>();
    generateTableSpecConfig(tableSpecConfig);

    tableSpecConfig.put(CachedTableProvider.INNER_TABLE_ID, serializeObject("inner table id", tableId));
    tableSpecConfig.put(CachedTableProvider.CACHE, serializeObject("cache instance", cache));

    return new TableSpec(tableId, serde, CachedTableProviderFactory.class.getName(), tableSpecConfig);
  }

  /**
   * Helper method to serialize Java objects as Base64 strings
   * @param name name of the object (for error reporting)
   * @param object object to be serialized
   * @return Base64 representation of the object
   */
  static <T> String serializeObject(String name, T object) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(object);
      return Base64.getEncoder().encodeToString(baos.toByteArray());
    } catch (IOException e) {
      throw new SamzaException("Failed to serialize " + name, e);
    }
  }

  public CachedTableDescriptor<K, V> withInnerTable(Table<KV<K, V>> table) {
    innerTableId = ((TableImpl) table).getTableSpec().getId();
    return this;
  }

  public CachedTableDescriptor<K, V> withCache(Cache<K, V> cache) {
    this.cache = cache;
    return this;
  }

  @Override
  protected void validate() {
    super.validate();
    Preconditions.checkNotNull(innerTableId, "innerTableId is required.");
  }
}
