package org.apache.samza.table.remote;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.samza.SamzaException;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.TableOpCallback;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import scala.Function1;
import scala.Option;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;


/**
 * Helper class to handle table IO requests with rate limiting and retries on behalf of the remote table.
 *
 * It provides two sets of APIs: blocking and non-blocking, ie. execute{Entries} and execute{Entries}Async.
 * Blocking requests are executed immediately whereas non-blocking requests are buffered in a blocking queue.
 * If the blocking queue is filled up, further non-blocking requests will be blocked to apply back pressure.
 *
 * In either mode, multiple versions of execute() are provided for the possible CRUD operations, ie.
 * (key), (key, value), (keys), (entries).
 *
 * Both read and write operations should share the same RequestManager instance for each remote table.
 *
 * @param <K> type of the table key
 * @param <V> type of the table record
 */
public class RequestManager<K, V> implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(RequestManager.class);

  private volatile boolean shutdown = false;
  private final String tableId;

  private Counter retryCounter;

  @VisibleForTesting
  final BlockingQueue<Request> requestQueue;
  final RetryHelper retryHelper;

  private final ScheduledExecutorService schedExecutor;

  public static class RetryHelper extends ExponentialSleepStrategy {
    private int maxRetryCount;

    public RetryHelper(int maxRetryCount, double backOffMultiplier, long initialDelayMs, long maximumDelayMs) {
      super(backOffMultiplier, initialDelayMs, maximumDelayMs);
      this.maxRetryCount = maxRetryCount;
    }

    public int getMaxRetryCount() {
      return maxRetryCount;
    }
  }

  /**
   * Wrapper of a queued request
   */
  static class Request {
    final int credits;
    final Runnable runnable;
    final Throttler throttler;

    long retryCount;
    long previousBackoff;

    public Request(int credits, Runnable runnable, Throttler throttler) {
      this.credits = credits;
      this.runnable = runnable;
      this.throttler = throttler;
      this.retryCount = 0;
      this.previousBackoff = 0;
    }
  }

  class RetryCallback<T> implements TableOpCallback<T> {
    private final TableOpCallback<T> userCallback;
    private Request request;

    public RetryCallback(TableOpCallback<T> userCallback) {
      this.userCallback = userCallback;
    }

    public void setRequest(Request request) {
      this.request = request;
    }

    @Override
    public void onComplete(T result, Throwable error) {
      if (error != null && request.retryCount < retryHelper.getMaxRetryCount()) {
        long backoffMs = retryHelper.getNextDelay(request.previousBackoff);
        request.previousBackoff = backoffMs;
        LOG.warn("{}: retrying after {}ms, tableId={}", backoffMs, tableId);
        schedExecutor.schedule(() -> requestQueue.add(request), backoffMs, TimeUnit.MILLISECONDS);
        retryCounter.inc();
        return;
      }

      userCallback.onComplete(result, error);
    }
  }

  public RequestManager(String tableId, int maxRequests, RetryHelper retryHelper) {
    this.requestQueue = new ArrayBlockingQueue<>(maxRequests);
    this.retryHelper = retryHelper;
    this.tableId = tableId;
    this.schedExecutor = Executors.newSingleThreadScheduledExecutor();

    Executors.newSingleThreadExecutor((arg) -> {
      Thread thread = new Thread(arg);
      thread.setName(tableId + "-request-manager");
      thread.setDaemon(true);
      return thread;
    }).submit(this);
  }

  public void setRetryCounter(Counter retryCounter) {
    this.retryCounter = retryCounter;
  }

  @Override
  public void run() {
    while (!shutdown) {
      try {
        Request request = requestQueue.take();
        request.throttler.throttle(request.credits);
        request.runnable.run();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for async request, shutdown=" + shutdown);
        if (shutdown) {
          break;
        }
      } catch (Exception e) {
        LOG.error("Failed to submit async request for table={}", tableId);
        // TODO:
      }
    }
    LOG.info("RequestManager has exited, table={}", tableId);
  }

  /**
   * Throttle the request given a table record (key, value)
   * @param key key of the table record
   */
  public <R> R execute(K key, Supplier<R> method, Throttler<K, V> throttler) {
    return doExecute(throttler.getCredits(key, null), method, throttler);
  }

  /**
   * Throttle the request given a table record (key, value)
   * @param key key of the table record
   * @param value value of the table record
   */
  public void execute(K key, V value, Runnable method, Throttler<K, V> throttler) {
    doExecute(throttler.getCredits(key, value), () -> { method.run(); return null; }, throttler);
  }

  /**
   * Throttle the request given a list of table keys
   * @param keys list of keys
   */
  public <R> R execute(List<K> keys, Supplier<R> method, Throttler<K, V> throttler) {
    return doExecute(throttler.getCredits(keys), method, throttler);
  }

  /**
   * Throttle the request given a list of table records
   * @param entries list of records
   */
  // Have to be renamed because of type erasure
  public void executeEntries(List<Entry<K, V>> entries, Runnable method, Throttler<K, V> throttler) {
    doExecute(throttler.getEntryCredits(entries), () -> { method.run(); return (Void) null; }, throttler);
  }

  private <R> R doExecute(int credits, Supplier<R> method, Throttler<K, V> throttler) {
    final Exception [] exceptions = new Exception[1];
    Option<R> result = retryHelper.<R>run(new AbstractFunction1<ExponentialSleepStrategy.RetryLoop, R>() {
      @Override
      public R apply(ExponentialSleepStrategy.RetryLoop loop) {
        throttler.throttle(credits);
        R value = method.get();
        loop.done();
        return value;
      }
    }, new AbstractFunction2<Exception, ExponentialSleepStrategy.RetryLoop, BoxedUnit>() {
      @Override
      public BoxedUnit apply(Exception exception, ExponentialSleepStrategy.RetryLoop loop) {
        if (loop.sleepCount() < retryHelper.getMaxRetryCount()) {
          LOG.warn("Retrying ...");
          retryCounter.inc();
        } else {
          LOG.error("Failed to execute table operation after all retries, table={}.", tableId);
          exceptions[0] = exception;
          loop.done();
        }
        return null;
      }
    });

    if (exceptions[0] != null) {
      throw new SamzaException("Table operation failed.", exceptions[0]);
    }

    return result.get();
  }

  /**
   * Asynchronously throttle the request given a table record
   * @param key key of the table record
   */
  public void executeAsync(K key, Runnable runnable, Throttler<K, V> throttler) {
    executeAsync(key, null, runnable, throttler);
  }

  /**
   * Asynchronously throttle the request given a table record
   * @param key key of the table record
   * @param value value of the table record
   */
  public void executeAsync(K key, V value, Runnable runnable, Throttler<K, V> throttler) {
    requestQueue.add(new Request(throttler.getCredits(key, value), runnable, throttler));
  }

  /**
   * Asynchronously throttle the request given a list of table keys
   * @param keys list of keys
   */
  public void executeAsync(List<K> keys, Runnable runnable, Throttler<K, V> throttler) {
    requestQueue.add(new Request(throttler.getCredits(keys), runnable, throttler));
  }

  /**
   * Asynchronously throttle the request given a list of table records
   * @param entries list of records
   */
  // Have to be renamed because of type erasure
  public void executeEntriesAsync(List<Entry<K, V>> entries, Runnable runnable, Throttler<K, V> throttler) {
    requestQueue.add(new Request(throttler.getEntryCredits(entries), runnable, throttler));
  }

  public <T> RetryCallback<T> decorate(TableOpCallback<T> userCallback) {
    return new RetryCallback<>(userCallback);
  }

  public void shutdown() {
    this.shutdown = true;
  }
}
