/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.contrib.awsxray;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.contrib.awsxray.GetSamplingTargetsRequest.SamplingBoostStatisticsDocument;
import io.opentelemetry.contrib.awsxray.GetSamplingTargetsRequest.SamplingStatisticsDocument;
import io.opentelemetry.contrib.awsxray.GetSamplingTargetsResponse.SamplingTargetDocument;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

final class SamplingRuleApplier {

  private static final Logger logger = Logger.getLogger(SamplingRuleApplier.class.getName());

  private static final Map<String, String> XRAY_CLOUD_PLATFORM;

  static {
    Map<String, String> xrayCloudPlatform = new HashMap<>();
    xrayCloudPlatform.put(ResourceAttributes.CloudPlatformValues.AWS_EC2, "AWS::EC2::Instance");
    xrayCloudPlatform.put(ResourceAttributes.CloudPlatformValues.AWS_ECS, "AWS::ECS::Container");
    xrayCloudPlatform.put(ResourceAttributes.CloudPlatformValues.AWS_EKS, "AWS::EKS::Container");
    xrayCloudPlatform.put(
        ResourceAttributes.CloudPlatformValues.AWS_ELASTIC_BEANSTALK,
        "AWS::ElasticBeanstalk::Environment");
    xrayCloudPlatform.put(
        ResourceAttributes.CloudPlatformValues.AWS_LAMBDA, "AWS::Lambda::Function");
    XRAY_CLOUD_PLATFORM = Collections.unmodifiableMap(xrayCloudPlatform);
  }

  private final String clientId;
  private final String ruleName;
  private final String serviceName;
  private final Clock clock;
  private final Sampler reservoirSampler;
  private final long reservoirEndTimeNanos;
  private final double fixedRate;
  private final Sampler fixedRateSampler;
  private final boolean borrowing;

  // Adaptive sampling related configs
  private final int errorsCapturedPerSecond;
  private final RateLimiter errorCaptureRateLimiter;
  private final double boostedFixedRate;
  private final Long boostEndTimeNanos;
  private final Sampler boostedFixedRateSampler;
  private final List<String> rulesWatchingThisService;

  private final Map<String, Matcher> attributeMatchers;
  private final Matcher urlPathMatcher;
  private final Matcher serviceNameMatcher;
  private final Matcher httpMethodMatcher;
  private final Matcher hostMatcher;
  private final Matcher serviceTypeMatcher;
  private final Matcher resourceArnMatcher;

  private final Statistics statistics;

  private final long nextSnapshotTimeNanos;

  SamplingRuleApplier(String clientId, GetSamplingRulesResponse.SamplingRule rule, @Nullable String serviceName, List<String> rulesWatchingThisService, Clock clock) {
    this.clientId = clientId;
    this.clock = clock;
    String ruleName = rule.getRuleName();
    if (ruleName == null) {
      // The AWS API docs mark this as an optional field but in practice it seems to always be
      // present, and sampling
      // targets could not be computed without it. For now provide an arbitrary fallback just in
      // case the AWS API docs
      // are correct.
      ruleName = "default";
    }
    this.ruleName = ruleName;

    // TODO: @majanjua - Ensure choosing a default should be the correct behaviour
    this.serviceName = serviceName == null ? "default" : serviceName;

    // We don't have a SamplingTarget so are ready to report a snapshot right away.
    nextSnapshotTimeNanos = clock.nanoTime();

    // We either have no reservoir sampling or borrow until we get a quota so have no end time.
    reservoirEndTimeNanos = Long.MAX_VALUE;

    if (rule.getReservoirSize() > 0) {
      // Until calling GetSamplingTargets, the default is to borrow 1/s if reservoir size is
      // positive.
      reservoirSampler = createRateLimited(1);
      borrowing = true;
    } else {
      // No reservoir sampling, we will always use the fixed rate.
      reservoirSampler = Sampler.alwaysOff();
      borrowing = false;
    }
    fixedRate = rule.getFixedRate();
    fixedRateSampler = createFixedRate(fixedRate);

    // TODO: @majanjua - Remove all the parsing once object is passed properly
    String anomalySamplingConfig = rule.getAnomalySampling();
    int errorsCapturedPerSecondTemp;
    if (anomalySamplingConfig != null) {
      errorsCapturedPerSecondTemp = 1;
    }
    else {
      errorsCapturedPerSecondTemp = 0;
    }

    errorsCapturedPerSecond = errorsCapturedPerSecondTemp;
    errorCaptureRateLimiter = new RateLimiter(errorsCapturedPerSecond, errorsCapturedPerSecond, clock);

    boostedFixedRate = fixedRate;
    boostedFixedRateSampler = createFixedRate(fixedRate);
    boostEndTimeNanos = clock.nanoTime();
    this.rulesWatchingThisService = rulesWatchingThisService;

    if (rule.getAttributes().isEmpty()) {
      attributeMatchers = Collections.emptyMap();
    } else {
      attributeMatchers =
          rule.getAttributes().entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> toMatcher(e.getValue())));
    }

    urlPathMatcher = toMatcher(rule.getUrlPath());
    serviceNameMatcher = toMatcher(rule.getServiceName());
    httpMethodMatcher = toMatcher(rule.getHttpMethod());
    hostMatcher = toMatcher(rule.getHost());
    serviceTypeMatcher = toMatcher(rule.getServiceType());
    resourceArnMatcher = toMatcher(rule.getResourceArn());

    statistics = new Statistics();
  }

  private SamplingRuleApplier(
      String clientId,
      String ruleName,
      String serviceName,
      Clock clock,
      Sampler reservoirSampler,
      long reservoirEndTimeNanos,
      double fixedRate,
      Sampler fixedRateSampler,
      boolean borrowing,
      int errorsCapturedPerSecond,
      List<String> rulesWatchingThisService,
      double boostedFixedRate,
      Long boostEndTimeNanos,
      Map<String, Matcher> attributeMatchers,
      Matcher urlPathMatcher,
      Matcher serviceNameMatcher,
      Matcher httpMethodMatcher,
      Matcher hostMatcher,
      Matcher serviceTypeMatcher,
      Matcher resourceArnMatcher,
      Statistics statistics,
      long nextSnapshotTimeNanos) {
    this.clientId = clientId;
    this.ruleName = ruleName;
    this.serviceName = serviceName;
    this.clock = clock;
    this.reservoirSampler = reservoirSampler;
    this.reservoirEndTimeNanos = reservoirEndTimeNanos;
    this.fixedRate = fixedRate;
    this.fixedRateSampler = fixedRateSampler;
    this.borrowing = borrowing;
    this.errorsCapturedPerSecond = errorsCapturedPerSecond;
    this.rulesWatchingThisService = rulesWatchingThisService;
    this.boostedFixedRate = boostedFixedRate;
    this.boostEndTimeNanos = boostEndTimeNanos;
    this.attributeMatchers = attributeMatchers;
    this.urlPathMatcher = urlPathMatcher;
    this.serviceNameMatcher = serviceNameMatcher;
    this.httpMethodMatcher = httpMethodMatcher;
    this.hostMatcher = hostMatcher;
    this.serviceTypeMatcher = serviceTypeMatcher;
    this.resourceArnMatcher = resourceArnMatcher;
    this.statistics = statistics;
    this.nextSnapshotTimeNanos = nextSnapshotTimeNanos;

    this.errorCaptureRateLimiter = new RateLimiter(errorsCapturedPerSecond, errorsCapturedPerSecond, clock);
    this.boostedFixedRateSampler = createFixedRate(this.boostedFixedRate);

  }

  @SuppressWarnings("deprecation") // TODO
  boolean matches(Attributes attributes, Resource resource) {
    int matchedAttributes = 0;
    String httpTarget = null;
    String httpUrl = null;
    String httpMethod = null;
    String host = null;

    for (Map.Entry<AttributeKey<?>, Object> entry : attributes.asMap().entrySet()) {
      if (entry.getKey().equals(SemanticAttributes.HTTP_TARGET)) {
        httpTarget = (String) entry.getValue();
      } else if (entry.getKey().equals(SemanticAttributes.HTTP_URL)) {
        httpUrl = (String) entry.getValue();
      } else if (entry.getKey().equals(SemanticAttributes.HTTP_METHOD)) {
        httpMethod = (String) entry.getValue();
      } else if (entry.getKey().equals(SemanticAttributes.NET_HOST_NAME)) {
        host = (String) entry.getValue();
      } else if (entry.getKey().equals(SemanticAttributes.HTTP_HOST)) {
        // TODO (trask) remove support for deprecated http.host attribute
        host = (String) entry.getValue();
      }

      Matcher matcher = attributeMatchers.get(entry.getKey().getKey());
      if (matcher == null) {
        continue;
      }
      if (matcher.matches(entry.getValue().toString())) {
        matchedAttributes++;
      } else {
        return false;
      }
    }
    // All attributes in the matched attributes must have been present in the span to be a match.
    if (matchedAttributes != attributeMatchers.size()) {
      return false;
    }

    // URL Path may be in either http.target or http.url
    if (httpTarget == null && httpUrl != null) {
      int schemeEndIndex = httpUrl.indexOf("://");
      // Per spec, http.url is always populated with scheme://host/target. If scheme doesn't
      // match, assume it's bad instrumentation and ignore.
      if (schemeEndIndex > 0) {
        int pathIndex = httpUrl.indexOf('/', schemeEndIndex + "://".length());
        if (pathIndex < 0) {
          // No path, equivalent to root path.
          httpTarget = "/";
        } else {
          httpTarget = httpUrl.substring(pathIndex);
        }
      }
    }

    return urlPathMatcher.matches(httpTarget)
        && serviceNameMatcher.matches(resource.getAttribute(ResourceAttributes.SERVICE_NAME))
        && httpMethodMatcher.matches(httpMethod)
        && hostMatcher.matches(host)
        && serviceTypeMatcher.matches(getServiceType(resource))
        && resourceArnMatcher.matches(getArn(attributes, resource));
  }

  SamplingResult shouldSample(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {
    // Incrementing requests first ensures sample / borrow rate are positive.
    statistics.requests.increment();
    boolean reservoirExpired = clock.nanoTime() >= reservoirEndTimeNanos;
    SamplingResult result =
        !reservoirExpired
            ? reservoirSampler.shouldSample(
                parentContext, traceId, name, spanKind, attributes, parentLinks)
            : SamplingResult.create(SamplingDecision.DROP);
    if (result.getDecision() != SamplingDecision.DROP) {
      // We use the result from the reservoir sampler if it worked.
      if (borrowing) {
        statistics.borrowed.increment();
      }
      statistics.sampled.increment();
      return result;
    }

    logger.log(Level.SEVERE, "Clock.nanoTime()={0}; boostEndTimeNanos={1}", new Object[]{clock.nanoTime(), boostEndTimeNanos});
    if (clock.nanoTime() < boostEndTimeNanos) {
      logger.log(Level.SEVERE, "Using boosted sampler");
      result = boostedFixedRateSampler.shouldSample(
          parentContext, traceId, name, spanKind, attributes, parentLinks);
    }
    else {
      logger.log(Level.SEVERE, "Using fixed rate sampler");
      result =
          fixedRateSampler.shouldSample(
              parentContext, traceId, name, spanKind, attributes, parentLinks);
    }
    if (result.getDecision() != SamplingDecision.DROP) {
      statistics.sampled.increment();
    }
    return result;
  }

  void countTrace() {
    statistics.traces.increment();
  }

  void captureError(ReadableSpan span, SpanData spanData, Consumer<ReadableSpan> spanBatcher) {
    // Record encountered error
    statistics.errors.increment();

    if (spanBatcher == null) {
      throw new IllegalStateException("Programming bug - Span exporter is null");
    }
    logger.log(Level.SEVERE, "Error span is being: Sampled={0}; ParentSampled={1}", new Object[] {
        span.getSpanContext().isSampled(),
        span.getParentSpanContext().isSampled()
    });
    if (span.getSpanContext().isSampled()) {
      statistics.errorsSampled.increment();
      return;
    }

    if (errorCaptureRateLimiter.trySpend(1)) {
      logger.log(Level.SEVERE, "Error span is being CAPTURED due to error, after being left unsampled");
      // Let BatchSpanProcessor accept the span directly to export accordingly
      spanBatcher.accept(span);
    }
    else {
      logger.log(Level.SEVERE, "Error span is being RATE-LIMITED despite error, after being left unsampled");
    }
  }

  @Nullable
  SamplingRuleStatisticsSnapshot snapshot(Date now) {
    if (clock.nanoTime() < nextSnapshotTimeNanos) {
      return null;
    }
    long totalCount = statistics.requests.sumThenReset();
    long sampledCount = statistics.sampled.sumThenReset();
    long borrowCount = statistics.borrowed.sumThenReset();
    long traceCount = statistics.traces.sumThenReset();
    long errorCount = statistics.errors.sumThenReset();
    long errorSampledCount = statistics.errorsSampled.sumThenReset();
    SamplingStatisticsDocument samplingStatistics = SamplingStatisticsDocument.newBuilder()
        .setClientId(clientId)
        .setRuleName(ruleName)
        .setTimestamp(now)
        // Resetting requests first ensures that sample / borrow rate are positive after the reset.
        // Snapshotting is not concurrent so this ensures they are always positive.
        .setRequestCount(totalCount)
        .setSampledCount(sampledCount)
        .setBorrowCount(borrowCount)
        .build();
    List<SamplingBoostStatisticsDocument> samplingBoostStatistics = rulesWatchingThisService.stream().map(
        watcherRuleName -> {
          return SamplingBoostStatisticsDocument.newBuilder()
              .setClientId(clientId)
              .setRuleName(watcherRuleName)
              .setServiceName(serviceName)
              .setTimestamp(now)
              .setTotalCount(traceCount)
              .setErrorCount(errorCount)
              .setErrorSampledCount(errorSampledCount)
              .build();
        })
        .filter(doc -> doc.getTotalCount() > 0) // TODO: @majanjua - Aggregate better
        .collect(Collectors.toList());
    // TODO: @majanjua - Remove this extra logic, it assumes there is only 1 boost document for now
    List<String> samplingBoostStatsDocs = samplingBoostStatistics.stream().map(doc -> {
      return "{"
          + "\"RuleName\":\"" + doc.getRuleName() + "\","
          + "\"ServiceName\":\"" + doc.getServiceName() + "\","
          + "\"ErrorCount\":" + doc.getErrorCount() + ","
          + "\"TotalCount\":" + doc.getTotalCount() + ","
          + "\"ErrorCountSampled\":" + doc.getErrorSampledCount()
          + "}";
    })
    .collect(Collectors.toList());
    return new SamplingRuleStatisticsSnapshot(samplingStatistics, samplingBoostStatsDocs);
  }

  long getNextSnapshotTimeNanos() {
    return nextSnapshotTimeNanos;
  }

  // currentNanoTime is passed in to ensure all uses of withTarget are used with the same baseline time reference
  SamplingRuleApplier withTarget(SamplingTargetDocument target, Date now, long currentNanoTime) {
    Sampler newFixedRateSampler = createFixedRate(target.getFixedRate());
    Sampler newReservoirSampler = Sampler.alwaysOff();
    long newReservoirEndTimeNanos = currentNanoTime;
    // Not well documented but a quota should always come with a TTL
    if (target.getReservoirQuota() != null && target.getReservoirQuotaTtl() != null) {
      newReservoirSampler = createRateLimited(target.getReservoirQuota());
      newReservoirEndTimeNanos =
          currentNanoTime
              + Duration.between(now.toInstant(), target.getReservoirQuotaTtl().toInstant())
                  .toNanos();
    }
    long intervalNanos =
        target.getIntervalSecs() != null
            ? TimeUnit.SECONDS.toNanos(target.getIntervalSecs())
            : AwsXrayRemoteSampler.DEFAULT_TARGET_INTERVAL_NANOS;
    long newNextSnapshotTimeNanos = currentNanoTime + intervalNanos;

    // TODO: @majanjua - Clean logic when the response is no longer a string
    double newBoostedFixedRate = fixedRate;
    long newBoostEndTimeNanos = currentNanoTime;
    if (target.getSamplingBoost() != null) {
      String samplingBoostMap = target.getSamplingBoost();
      try {
        @SuppressWarnings("unchecked")
        Map<String, Object> boostMap = new ObjectMapper().readValue(samplingBoostMap, Map.class);
        if (boostMap.containsKey("BoostRate") && boostMap.containsKey("BoostRateTTL")) {
          logger.log(Level.SEVERE, "CHECK - Boosting");
          newBoostedFixedRate = Double.parseDouble(boostMap.get("BoostRate").toString());
          newBoostEndTimeNanos = currentNanoTime + Duration.between(now.toInstant(), Instant.parse(boostMap.get("BoostRateTTL").toString())).toNanos();
        }
      } catch (JsonProcessingException e) {
        logger.log(Level.SEVERE, "Error parsing boost map: {0}", samplingBoostMap);
      }
    }

    return new SamplingRuleApplier(
        clientId,
        ruleName,
        serviceName,
        clock,
        newReservoirSampler,
        newReservoirEndTimeNanos,
        fixedRate,
        newFixedRateSampler,
        /* borrowing= */ false,
        errorsCapturedPerSecond,
        rulesWatchingThisService,
        newBoostedFixedRate,
        newBoostEndTimeNanos,
        attributeMatchers,
        urlPathMatcher,
        serviceNameMatcher,
        httpMethodMatcher,
        hostMatcher,
        serviceTypeMatcher,
        resourceArnMatcher,
        statistics,
        newNextSnapshotTimeNanos);
  }

  SamplingRuleApplier withNextSnapshotTimeNanos(long newNextSnapshotTimeNanos) {
    return new SamplingRuleApplier(
        clientId,
        ruleName,
        serviceName,
        clock,
        reservoirSampler,
        reservoirEndTimeNanos,
        fixedRate,
        fixedRateSampler,
        borrowing,
        errorsCapturedPerSecond,
        rulesWatchingThisService,
        boostedFixedRate,
        boostEndTimeNanos,
        attributeMatchers,
        urlPathMatcher,
        serviceNameMatcher,
        httpMethodMatcher,
        hostMatcher,
        serviceTypeMatcher,
        resourceArnMatcher,
        statistics,
        newNextSnapshotTimeNanos);
  }

  String getRuleName() {
    return ruleName;
  }

  @Nullable
  private static String getArn(Attributes attributes, Resource resource) {
    String arn = resource.getAttributes().get(ResourceAttributes.AWS_ECS_CONTAINER_ARN);
    if (arn != null) {
      return arn;
    }
    String cloudPlatform = resource.getAttributes().get(ResourceAttributes.CLOUD_PLATFORM);
    if (ResourceAttributes.CloudPlatformValues.AWS_LAMBDA.equals(cloudPlatform)) {
      return getLambdaArn(attributes, resource);
    }
    return null;
  }

  @Nullable
  private static String getLambdaArn(Attributes attributes, Resource resource) {
    String arn = resource.getAttributes().get(ResourceAttributes.CLOUD_RESOURCE_ID);
    if (arn != null) {
      return arn;
    }
    return attributes.get(ResourceAttributes.CLOUD_RESOURCE_ID);
  }

  @Nullable
  private static String getServiceType(Resource resource) {
    String cloudPlatform = resource.getAttributes().get(ResourceAttributes.CLOUD_PLATFORM);
    if (cloudPlatform == null) {
      return null;
    }
    return XRAY_CLOUD_PLATFORM.get(cloudPlatform);
  }

  private static Matcher toMatcher(String globPattern) {
    if (globPattern.equals("*")) {
      return TrueMatcher.INSTANCE;
    }

    for (int i = 0; i < globPattern.length(); i++) {
      char c = globPattern.charAt(i);
      if (c == '*' || c == '?') {
        return new PatternMatcher(toRegexPattern(globPattern));
      }
    }

    return new StringMatcher(globPattern);
  }

  private static Pattern toRegexPattern(String globPattern) {
    int tokenStart = -1;
    StringBuilder patternBuilder = new StringBuilder();
    for (int i = 0; i < globPattern.length(); i++) {
      char c = globPattern.charAt(i);
      if (c == '*' || c == '?') {
        if (tokenStart != -1) {
          patternBuilder.append(Pattern.quote(globPattern.substring(tokenStart, i)));
          tokenStart = -1;
        }
        if (c == '*') {
          patternBuilder.append(".*");
        } else {
          // c == '?'
          patternBuilder.append(".");
        }
      } else {
        if (tokenStart == -1) {
          tokenStart = i;
        }
      }
    }
    if (tokenStart != -1) {
      patternBuilder.append(Pattern.quote(globPattern.substring(tokenStart)));
    }
    return Pattern.compile(patternBuilder.toString());
  }

  private interface Matcher {
    boolean matches(@Nullable String s);
  }

  private enum TrueMatcher implements Matcher {
    INSTANCE;

    @Override
    public boolean matches(@Nullable String s) {
      return true;
    }

    @Override
    public String toString() {
      return "TrueMatcher";
    }
  }

  private static class StringMatcher implements Matcher {

    private final String target;

    StringMatcher(String target) {
      this.target = target;
    }

    @Override
    public boolean matches(@Nullable String s) {
      if (s == null) {
        return false;
      }
      return target.equalsIgnoreCase(s);
    }

    @Override
    public String toString() {
      return target;
    }
  }

  private static class PatternMatcher implements Matcher {
    private final Pattern pattern;

    PatternMatcher(Pattern pattern) {
      this.pattern = pattern;
    }

    @Override
    public boolean matches(@Nullable String s) {
      if (s == null) {
        return false;
      }
      return pattern.matcher(s).matches();
    }

    @Override
    public String toString() {
      return pattern.toString();
    }
  }

  private Sampler createRateLimited(int numPerSecond) {
    return Sampler.parentBased(new RateLimitingSampler(numPerSecond, clock));
  }

  private static Sampler createFixedRate(double rate) {
    return Sampler.parentBased(Sampler.traceIdRatioBased(rate));
  }

  // We keep track of sampling requests and decisions to report to X-Ray to allow it to allocate
  // quota from the central reservoir. We do not lock around updates because sampling is called on
  // the hot, highly-contended path and locking would have significant overhead. The actual possible
  // error should not be off to significantly affect quotas in practice.
  private static class Statistics {
    final LongAdder requests = new LongAdder();
    final LongAdder sampled = new LongAdder();
    final LongAdder borrowed = new LongAdder();
    final LongAdder traces = new LongAdder();
    final LongAdder errors = new LongAdder();
    final LongAdder errorsSampled = new LongAdder();
  }

  static class SamplingRuleStatisticsSnapshot {
    final SamplingStatisticsDocument statisticsDocument;
    final List<String> boostStatisticsDocument;
    // final SamplingBoostStatisticsDocument boostStatisticsDocument;

    SamplingRuleStatisticsSnapshot(SamplingStatisticsDocument statisticsDocument, List<String> boostStatisticsDocument) {
      this.statisticsDocument = statisticsDocument;
      this.boostStatisticsDocument = boostStatisticsDocument;
    }

    SamplingStatisticsDocument getStatisticsDocument() {
      return statisticsDocument;
    }

    List<String> getBoostStatisticsDocument() {
      return boostStatisticsDocument;
    }
  }
}
