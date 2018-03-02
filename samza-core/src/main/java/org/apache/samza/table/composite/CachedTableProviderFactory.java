package org.apache.samza.table.composite;

import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableProviderFactory;
import org.apache.samza.table.TableSpec;


public class CachedTableProviderFactory implements TableProviderFactory {
  @Override
  public TableProvider getTableProvider(TableSpec tableSpec) {
    return new CachedTableProvider(tableSpec);
  }
}
