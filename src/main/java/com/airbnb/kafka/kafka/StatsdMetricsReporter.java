/*
 * Copyright (c) 2015.  Airbnb.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.airbnb.kafka.kafka09;

import com.airbnb.metrics.Dimension;
import com.airbnb.metrics.KafkaStatsDReporter;
import com.airbnb.metrics.StatsDMetricsRegistry;
import com.airbnb.metrics.StatsDReporter;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import kafka.metrics.KafkaMetricsReporter;
import org.slf4j.LoggerFactory;
import kafka.metrics.KafkaMetricsReporterMBean;


public class StatsdMetricsReporter implements KafkaMetricsReporter, KafkaMetricsReporterMBean {
  private static final org.slf4j.Logger log = LoggerFactory.getLogger(StatsDReporter.class);

  public static final String REPORTER_NAME = "kafka-statsd-metrics-0.5";

  public static final String STATSD_REPORTER_ENABLED = "external.kafka.statsd.reporter.enabled";
  public static final String STATSD_HOST = "external.kafka.statsd.host";
  public static final String STATSD_PORT = "external.kafka.statsd.port";
  public static final String STATSD_METRICS_PREFIX = "external.kafka.statsd.metrics.prefix";
  public static final String POLLING_INTERVAL_SECS = "kafka.metrics.polling.interval.secs";
  public static final String STATSD_DIMENSION_ENABLED = "external.kafka.statsd.dimension.enabled";

  private static final String METRIC_PREFIX = "kafka.";
  private static final int POLLING_PERIOD_IN_SECONDS = 10;

  private boolean enabled;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private String host;
  private int port;
  private String prefix;
  private long pollingPeriodInSeconds;
  private EnumSet<Dimension> metricDimensions;
  private StatsDClient statsd;
  private Map<String, KafkaMetric> kafkaMetrics;
  private StatsDMetricsRegistry registry;
  private KafkaStatsDReporter underlying = null;

  public boolean isRunning() {
    return running.get();
  }

  @Override
  public void init(VerifiableProperties verifiableProperties) {
    registry = new StatsDMetricsRegistry();
    configure(verifiableProperties)
    if (enabled) {
      startReporter(POLLING_PERIOD_IN_SECONDS);
    } else {
      log.warn("KafkaStatsDReporter is disabled");
    }
  }

  private String getMetricName(final KafkaMetric metric) {
    MetricName metricName = metric.metricName();

    return METRIC_PREFIX + metricName.group() + "." + metricName.name();
  }

  @Override
  public void metricChange(final KafkaMetric metric) {
    String name = getMetricName(metric);

    StringBuilder strBuilder = new StringBuilder();

    for (String key : metric.metricName().tags().keySet()) {
      strBuilder.append(key).append(":").append(metric.metricName().tags().get(key)).append(",");
    }

    if (strBuilder.length() > 0) {
      strBuilder.deleteCharAt(strBuilder.length() - 1);
    }

    registry.register(name, metric, strBuilder.toString());
    log.debug("metrics name: {}", name);
  }

  @Override
  public void metricRemoval(KafkaMetric metric) {
    String name = getMetricName(metric);

    registry.unregister(name);
  }

  @Override
  public void close() {
    stopReporter();
  }

  @Override
  public void configure(VerifiableProperties verifiableProperties) {
    enabled = configs.containsKey(STATSD_REPORTER_ENABLED) ?
      Boolean.valueOf((String) verifiableProperties.getProperty(STATSD_REPORTER_ENABLED)) : false;
    host = configs.containsKey(STATSD_HOST) ?
      (String) verifiableProperties.getProperty(STATSD_HOST) : "localhost";
    port = configs.containsKey(STATSD_PORT) ?
      Integer.valueOf((String) verifiableProperties.getProperty(STATSD_PORT)) : 8125;
    prefix = configs.containsKey(STATSD_METRICS_PREFIX) ?
      (String) verifiableProperties.getProperty(STATSD_METRICS_PREFIX) : "";
    pollingPeriodInSeconds = configs.containsKey(POLLING_INTERVAL_SECS) ?
      Integer.valueOf((String) verifiableProperties.getProperty(POLLING_INTERVAL_SECS)) : 10;
    metricDimensions = Dimension.fromConfigs(configs, STATSD_DIMENSION_ENABLED);
  }

  public void startReporter(long pollingPeriodInSeconds) {
    if (pollingPeriodInSeconds <= 0) {
      throw new IllegalArgumentException("Polling period must be greater than zero");
    }

    synchronized (running) {
      if (running.get()) {
        log.warn("KafkaStatsDReporter: {} is already running", REPORTER_NAME);
      } else {
        statsd = createStatsd();
        underlying = new KafkaStatsDReporter(statsd, registry);
        underlying.start(pollingPeriodInSeconds, TimeUnit.SECONDS);
        log.info(
          "Started KafkaStatsDReporter: {} with host={}, port={}, polling_period_secs={}, prefix={}",
          REPORTER_NAME, host, port, pollingPeriodInSeconds, prefix
        );
        running.set(true);
      }
    }
  }

  private StatsDClient createStatsd() {
    try {
      return new NonBlockingStatsDClient(prefix, host, port);
    } catch (StatsDClientException ex) {
      log.error("KafkaStatsDReporter cannot be started");
      throw ex;
    }
  }

  private void stopReporter() {
    if (!enabled) {
      log.warn("KafkaStatsDReporter is disabled");
    } else {
      synchronized (running) {
        if (running.get()) {
          try {
            underlying.shutdown();
          } catch (InterruptedException e) {
            log.warn("Stop reporter exception: {}", e);
          }
          statsd.stop();
          running.set(false);
          log.info("Stopped KafkaStatsDReporter with host={}, port={}", host, port);
        } else {
          log.warn("KafkaStatsDReporter is not running");
        }
      }
    }
  }
}
