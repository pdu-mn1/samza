package org.apache.samza.table.retry;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.samza.SamzaException;

import net.jodah.failsafe.RetryPolicy;


/**
 * Simple policy class for table IO retry with 3 backoff types and 3 termination conditions.
 */
public class TableRetryPolicy {
  enum BackoffType { NONE, FIXED, RANDOM, EXPONENTIAL }

  private long sleepMs;
  private long randomMinMs;
  private long randomMaxMs;
  private double exponentialFactor;
  private long exponentialMaxSleepMs;
  private long jitterMs;
  private int maxAttempts;
  private long maxDelayMs;
  private BackoffType backoffType = BackoffType.NONE;

  public TableRetryPolicy withFixedBackoff(long sleepMs) {
    this.sleepMs = sleepMs;
    this.backoffType = BackoffType.FIXED;
    return this;
  }

  public TableRetryPolicy withRandomBackoff(long minSleepMs, long maxSleepMs) {
    this.randomMinMs = minSleepMs;
    this.randomMaxMs = maxSleepMs;
    this.backoffType = BackoffType.RANDOM;
    return this;
  }

  public TableRetryPolicy withExponentialBackoff(long sleepMs, long maxSleepMs, double factor) {
    this.sleepMs = sleepMs;
    this.exponentialMaxSleepMs = maxSleepMs;
    this.exponentialFactor = factor;
    this.backoffType = BackoffType.EXPONENTIAL;
    return this;
  }

  public TableRetryPolicy withJitter(long jitterMs) {
    this.jitterMs = jitterMs;
    return this;
  }

  public TableRetryPolicy withStopAfterAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
    return this;
  }

  public TableRetryPolicy withStopAfterDelay(long maxDelayMs) {
    this.maxDelayMs = maxDelayMs;
    return this;
  }

  RetryPolicy toRetryPolicy(Predicate<Throwable> isRetriable) {
    if (backoffType == BackoffType.NONE) {
      return null;
    }

    RetryPolicy policy = new RetryPolicy();
    policy.withMaxDuration(maxDelayMs, TimeUnit.MILLISECONDS);
    policy.withMaxRetries(maxAttempts);
    policy.withJitter(jitterMs, TimeUnit.MILLISECONDS);
    policy.retryOn((e) -> isRetriable.test(e));

    switch (backoffType) {
      case FIXED:
        policy.withDelay(sleepMs, TimeUnit.MILLISECONDS);
        break;

      case RANDOM:
        policy.withDelay(randomMinMs, randomMaxMs, TimeUnit.MILLISECONDS);
        break;

      case EXPONENTIAL:
        policy.withBackoff(sleepMs, exponentialMaxSleepMs, TimeUnit.MILLISECONDS, exponentialFactor);
        break;

      default:
        throw new SamzaException("Unknown retry policy type.");
    }

    return policy;
  }
}
