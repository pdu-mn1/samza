package org.apache.samza.table.retry;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.samza.SamzaException;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;

import com.google.common.base.Preconditions;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;


/**
 *
 * @param <K>
 * @param <V>
 */
public class RetriableTableIO<K, V> implements TableReadFunction<K,V>, TableWriteFunction<K, V> {
  private final TableReadFunction<K, V> readFn;
  private final TableWriteFunction<K, V> writeFn;
  private final ScheduledExecutorService retryExecutor;
  private final RetryPolicy readPolicy;
  private final RetryPolicy writePolicy;

  private final Counter retryCount;
  private final Counter successCount;
  private final Timer retryTimer;

  public RetriableTableIO(String tableId, TableRetryPolicy policy, TableReadFunction<K, V> readFn,
      TableWriteFunction<K, V> writeFn, ScheduledExecutorService retryExecutor) {
    Preconditions.checkNotNull(tableId);
    Preconditions.checkNotNull(policy);
    Preconditions.checkArgument(readFn != null || writeFn != null);
    Preconditions.checkNotNull(retryExecutor);

    this.readFn = readFn;
    this.writeFn = writeFn;
    this.retryExecutor = retryExecutor;

    this.readPolicy = policy.toRetryPolicy(readFn::isRetriable);
    this.writePolicy = policy.toRetryPolicy(writeFn::isRetriable);

    this.retryCount = new Counter(tableId + "_retryCount");
    this.successCount = new Counter(tableId + "_successCount");
    this.retryTimer = new Timer(tableId + "_retryTimer");
  }

  @Override
  public CompletableFuture<V> getAsync(K key) {
    return Failsafe.with(readPolicy).with(retryExecutor)
        .onRetry(e -> retryCount.inc())
        .onSuccess((e, ctx) -> {
            if (ctx.getExecutions() > 1) {
              retryTimer.update(ctx.getElapsedTime().toMillis());
            } else {
              successCount.inc();
            }
          })
        .future(k -> readFn.getAsync(key))
        .exceptionally(e -> {
            throw new SamzaException("Failed to get the record for " + key + " after retries.", e);
          });
  }

  @Override
  public CompletableFuture<Map<K, V>> getAllAsync(Collection<K> keys) {
    return Failsafe.with(readPolicy).with(retryExecutor)
        .future(k -> readFn.getAllAsync(keys))
        .exceptionally(e -> {
            throw new SamzaException("Failed to get the records for " + keys + " after retries.", e);
          });
  }

  @Override
  public boolean isRetriable(Throwable exception) {
    throw new IllegalStateException("Should never be called.");
  }

  @Override
  public CompletableFuture<Void> putAsync(K key, V record) {
    return Failsafe.with(writePolicy).with(retryExecutor)
        .future(k -> writeFn.putAsync(key, record))
        .exceptionally(e -> {
            throw new SamzaException("Failed to get the record for " + key + " after retries.", e);
          });
  }

  @Override
  public CompletableFuture<Void> putAllAsync(Collection<Entry<K, V>> records) {
    return Failsafe.with(writePolicy).with(retryExecutor)
        .future(k -> writeFn.putAllAsync(records))
        .exceptionally(e -> {
            throw new SamzaException("Failed to put records after retries.", e);
          });
  }

  @Override
  public CompletableFuture<Void> deleteAsync(K key) {
    return Failsafe.with(writePolicy).with(retryExecutor)
        .future(k -> writeFn.deleteAsync(key))
        .exceptionally(e -> {
            throw new SamzaException("Failed to delete the record for " + key + " after retries.", e);
          });
  }

  @Override
  public CompletableFuture<Void> deleteAllAsync(Collection<K> keys) {
    return Failsafe.with(writePolicy).with(retryExecutor)
        .future(k -> writeFn.deleteAllAsync(keys))
        .exceptionally(e -> {
            throw new SamzaException("Failed to delete the records for " + keys + " after retries.", e);
          });
  }
}
