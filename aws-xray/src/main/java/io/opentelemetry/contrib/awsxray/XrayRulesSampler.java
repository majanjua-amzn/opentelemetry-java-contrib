/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.contrib.awsxray;

import static io.opentelemetry.semconv.HttpAttributes.HTTP_RESPONSE_STATUS_CODE;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.contrib.awsxray.GetSamplingTargetsResponse.SamplingTargetDocument;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

// import io.opentelemetry.api.common.AttributeKey;

final class XrayRulesSampler implements Sampler {

  private static final Logger logger = Logger.getLogger(XrayRulesSampler.class.getName());

  private final String clientId;
  private final Resource resource;
  private final Clock clock;
  private final Sampler fallbackSampler;
  private final SamplingRuleApplier[] ruleAppliers;

  XrayRulesSampler(
      String clientId,
      Resource resource,
      Clock clock,
      Sampler fallbackSampler,
      List<GetSamplingRulesResponse.SamplingRule> rules) {
    this(
        clientId,
        resource,
        clock,
        fallbackSampler,
        generateSamplingRuleAppliers(clientId, resource.getAttribute(ResourceAttributes.SERVICE_NAME), clock, rules));
  }

  private XrayRulesSampler(
      String clientId,
      Resource resource,
      Clock clock,
      Sampler fallbackSampler,
      SamplingRuleApplier[] ruleAppliers) {
    this.clientId = clientId;
    this.resource = resource;
    this.clock = clock;
    this.fallbackSampler = fallbackSampler;
    this.ruleAppliers = ruleAppliers;
  }

  private static SamplingRuleApplier[] generateSamplingRuleAppliers(
      String clientId,
      @Nullable String serviceName,
      Clock clock,
      List<GetSamplingRulesResponse.SamplingRule> rules) {
    List<String> rulesWatchingThisService = rules.stream()
            .filter(rule -> {
                // TODO: @majanjua - This should check if the rule's `ServiceToMonitorForFault` includes this service's name
                // Right now this assumes any rule with boost map is watching the current service
                return rule.getSamplingRateBoost() != null; })
            .map(rule -> rule.getRuleName())
            .collect(Collectors.toList());
    return rules.stream()
            // Lower priority value takes precedence so normal ascending sort.
            .sorted(Comparator.comparingInt(GetSamplingRulesResponse.SamplingRule::getPriority))
            .map(rule -> new SamplingRuleApplier(clientId, rule, serviceName, rulesWatchingThisService, clock))
            .toArray(SamplingRuleApplier[]::new);
  }

  @Override
  public SamplingResult shouldSample(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {
    logger.log(Level.SEVERE, "SPAN COUNTER BLABLA");
    for (SamplingRuleApplier applier : ruleAppliers) {
      if (applier.matches(attributes, resource)) {
        SamplingResult result = applier.shouldSample(
            parentContext, traceId, name, spanKind, attributes, parentLinks);
        // AttributeKey<String> samplingRuleKey = AttributeKey.stringKey("aws.xray.sampling_rule");
        // if (attributes.get(samplingRuleKey) == null) {
        //   result = SamplingResult.create(result.getDecision(), Attributes.of(samplingRuleKey, applier.getRuleName()));
        //   logger.log(Level.SEVERE, "Propagation test - Adding sampling rule name to span attributes: {0}", applier.getRuleName());
        // } else {
        //   logger.log(Level.SEVERE, "Propagation test - Sampling rule name already exists in span attributes: {0}", attributes.get(samplingRuleKey));
        // }
        return result;
      }
    }

    // In practice, X-Ray always returns a Default rule that matches all requests so it is a bug in
    // our code or X-Ray to reach here, fallback just in case.
    logger.log(
        Level.FINE,
        "No sampling rule matched the request. "
            + "This is a bug in either the OpenTelemetry SDK or X-Ray.");
    return fallbackSampler.shouldSample(
        parentContext, traceId, name, spanKind, attributes, parentLinks);
  }

  @Override
  public String getDescription() {
    return "XrayRulesSampler{" + Arrays.toString(ruleAppliers) + "}";
  }

  void adaptSampling(ReadableSpan span, SpanData spanData, Consumer<ReadableSpan> spanBatcher) {
    if (spanData.getAttributes().get(HTTP_RESPONSE_STATUS_CODE) == null) {
      return;
    }
    Long statusCode = spanData.getAttributes().get(HTTP_RESPONSE_STATUS_CODE);
    SpanContext parentContext = spanData.getParentSpanContext();
    boolean isLocalRootSpan = parentContext == null || !parentContext.isValid() || parentContext.isRemote();
    if (statusCode == null || statusCode > 299 || isLocalRootSpan) {
      for (SamplingRuleApplier applier : ruleAppliers) {
        if (applier.matches(spanData.getAttributes(), resource)) {
          if (isLocalRootSpan) {
            applier.countTrace();
          }
          if (statusCode == null || statusCode > 299) {
            applier.captureError(span, spanData, spanBatcher);
          }
          return;
        }
      }
    }
  }

  List<SamplingRuleApplier.SamplingRuleStatisticsSnapshot> snapshot(Date now) {
    return Arrays.stream(ruleAppliers)
        .map(rule -> rule.snapshot(now))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  long nextTargetFetchTimeNanos() {
    return Arrays.stream(ruleAppliers)
        .mapToLong(SamplingRuleApplier::getNextSnapshotTimeNanos)
        .min()
        // There is always at least one rule in practice so this should never be exercised.
        .orElseGet(() -> clock.nanoTime() + AwsXrayRemoteSampler.DEFAULT_TARGET_INTERVAL_NANOS);
  }

  XrayRulesSampler withTargets(
      Map<String, SamplingTargetDocument> ruleTargets,
      Set<String> requestedTargetRuleNames,
      Date now) {
    long currentNanoTime = clock.nanoTime();
    long defaultNextSnapshotTimeNanos =
        currentNanoTime + AwsXrayRemoteSampler.DEFAULT_TARGET_INTERVAL_NANOS;
    SamplingRuleApplier[] newAppliers =
        Arrays.stream(ruleAppliers)
            .map(
                rule -> {
                  SamplingTargetDocument target = ruleTargets.get(rule.getRuleName());
                  if (target != null) {
                    return rule.withTarget(target, now, currentNanoTime);
                  }
                  if (requestedTargetRuleNames.contains(rule.getRuleName())) {
                    // In practice X-Ray should return a target for any rule we requested but
                    // do a defensive check here in case. If we requested a target but got nothing
                    // back assume the default interval.
                    return rule.withNextSnapshotTimeNanos(defaultNextSnapshotTimeNanos);
                  }
                  // Target not requested, will be updated in a future target fetch.
                  return rule;
                })
            .toArray(SamplingRuleApplier[]::new);
    return new XrayRulesSampler(clientId, resource, clock, fallbackSampler, newAppliers);
  }
}
