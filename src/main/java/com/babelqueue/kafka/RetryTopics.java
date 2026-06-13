package com.babelqueue.kafka;

import com.babelqueue.BabelQueueException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The SDK-owned retry/delay topology for one work topic (Contract §6.4–§6.5). Kafka has no
 * native delay, DLQ, or per-message retry, so BabelQueue layers them on tiered delay topics
 * {@code <topic>.retry.<n>} (each mapped to a delay tier, ascending) plus an opt-in
 * {@code <topic>.dlq}. A delay or a release with no tiers configured raises rather than
 * silently dropping.
 */
public final class RetryTopics {

    /** A single delay tier: the {@code <topic>.retry.<n>} topic and the wall-clock delay it holds for. */
    public record Tier(String topic, Duration delay) {}

    private final String workTopic;
    private final List<Tier> tiers;
    private final String dlqTopic;

    private RetryTopics(String workTopic, List<Tier> tiers, String dlqTopic) {
        this.workTopic = workTopic;
        this.tiers = tiers;
        this.dlqTopic = dlqTopic;
    }

    /** Start a topology for {@code workTopic} (DLQ defaults to {@code <workTopic>.dlq}). */
    public static Builder forTopic(String workTopic) {
        return new Builder(workTopic);
    }

    public String workTopic() {
        return workTopic;
    }

    /** The DLQ topic, or {@code null} if dead-lettering is disabled. */
    public String dlqTopic() {
        return dlqTopic;
    }

    public boolean hasTiers() {
        return !tiers.isEmpty();
    }

    public List<Tier> tiers() {
        return tiers;
    }

    /**
     * The smallest tier whose delay is &ge; {@code delay} (the topic a delayed message is
     * routed to). Raises {@link BabelQueueException} if no tiers are configured or the delay
     * exceeds the largest tier — delays are never silently clamped or dropped (§6.4).
     */
    public Tier tierForDelay(Duration delay) {
        requireTiers();
        for (Tier tier : tiers) {
            if (tier.delay().compareTo(delay) >= 0) {
                return tier;
            }
        }
        throw new BabelQueueException(
            "Requested Kafka delay " + delay.toMillis() + "ms exceeds the largest retry tier ("
                + tiers.get(tiers.size() - 1).delay().toMillis() + "ms).");
    }

    /**
     * The tier for a retry at {@code attempt} (0-based): attempt 0 → the smallest tier, then up
     * one tier per attempt, clamped to the largest. Raises if no tiers are configured (§6.5).
     */
    public Tier tierForAttempt(int attempt) {
        requireTiers();
        int index = Math.min(Math.max(attempt, 0), tiers.size() - 1);
        return tiers.get(index);
    }

    private void requireTiers() {
        if (tiers.isEmpty()) {
            throw new BabelQueueException(
                "Kafka retry/delay requires retry topics; none are configured for '" + workTopic + "'.");
        }
    }

    /** Builder for {@link RetryTopics}; tiers may be added in any order (sorted by delay on build). */
    public static final class Builder {
        private final String workTopic;
        private final List<Duration> delays = new ArrayList<>();
        private String dlqTopic;

        private Builder(String workTopic) {
            this.workTopic = workTopic;
            this.dlqTopic = workTopic + ".dlq";
        }

        /** Add a delay tier; tiers are numbered {@code .retry.1}, {@code .retry.2}, … by ascending delay. */
        public Builder tier(Duration delay) {
            this.delays.add(delay);
            return this;
        }

        /** Override the DLQ topic name (default {@code <workTopic>.dlq}). */
        public Builder dlqTopic(String dlqTopic) {
            this.dlqTopic = dlqTopic;
            return this;
        }

        /** Disable dead-lettering — terminal failures degrade to commit-and-drop (§6.5). */
        public Builder withoutDlq() {
            this.dlqTopic = null;
            return this;
        }

        public RetryTopics build() {
            List<Duration> sorted = new ArrayList<>(delays);
            sorted.sort(Comparator.naturalOrder());
            List<Tier> tiers = new ArrayList<>(sorted.size());
            for (int i = 0; i < sorted.size(); i++) {
                tiers.add(new Tier(workTopic + ".retry." + (i + 1), sorted.get(i)));
            }
            return new RetryTopics(workTopic, List.copyOf(tiers), dlqTopic);
        }
    }
}
