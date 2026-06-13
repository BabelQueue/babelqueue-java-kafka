package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.BabelQueueException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** §6.4–§6.5 retry/delay topology: tier naming, delay→tier, attempt→tier, raises, DLQ. */
class RetryTopicsTest {

    @Test
    void tiersAreNamedAndSortedAscending() {
        RetryTopics rt = RetryTopics.forTopic("orders")
            .tier(Duration.ofMinutes(5))
            .tier(Duration.ofSeconds(5))
            .tier(Duration.ofSeconds(30))
            .build();

        assertEquals("orders.dlq", rt.dlqTopic());
        assertTrue(rt.hasTiers());
        List<RetryTopics.Tier> tiers = rt.tiers();
        assertEquals(3, tiers.size());
        assertEquals("orders.retry.1", tiers.get(0).topic());
        assertEquals(Duration.ofSeconds(5), tiers.get(0).delay());
        assertEquals("orders.retry.2", tiers.get(1).topic());
        assertEquals(Duration.ofSeconds(30), tiers.get(1).delay());
        assertEquals("orders.retry.3", tiers.get(2).topic());
        assertEquals(Duration.ofMinutes(5), tiers.get(2).delay());
    }

    @Test
    void tierForDelayPicksSmallestSufficient() {
        RetryTopics rt = RetryTopics.forTopic("orders")
            .tier(Duration.ofSeconds(5)).tier(Duration.ofSeconds(30)).build();

        assertEquals("orders.retry.1", rt.tierForDelay(Duration.ofSeconds(3)).topic());
        assertEquals("orders.retry.1", rt.tierForDelay(Duration.ofSeconds(5)).topic());
        assertEquals("orders.retry.2", rt.tierForDelay(Duration.ofSeconds(10)).topic());
    }

    @Test
    void tierForDelayTooLargeRaises() {
        RetryTopics rt = RetryTopics.forTopic("orders").tier(Duration.ofSeconds(5)).build();
        assertThrows(BabelQueueException.class, () -> rt.tierForDelay(Duration.ofMinutes(1)));
    }

    @Test
    void tierForAttemptClampsToLast() {
        RetryTopics rt = RetryTopics.forTopic("orders")
            .tier(Duration.ofSeconds(5)).tier(Duration.ofSeconds(30)).build();

        assertEquals("orders.retry.1", rt.tierForAttempt(0).topic());
        assertEquals("orders.retry.2", rt.tierForAttempt(1).topic());
        assertEquals("orders.retry.2", rt.tierForAttempt(9).topic());
    }

    @Test
    void noTiersRaisesOnUse() {
        RetryTopics rt = RetryTopics.forTopic("orders").build();
        assertFalse(rt.hasTiers());
        assertThrows(BabelQueueException.class, () -> rt.tierForAttempt(0));
        assertThrows(BabelQueueException.class, () -> rt.tierForDelay(Duration.ofSeconds(1)));
    }

    @Test
    void withoutDlqDisablesDeadLettering() {
        RetryTopics rt = RetryTopics.forTopic("orders").withoutDlq().build();
        assertNull(rt.dlqTopic());
    }

    @Test
    void customDlqTopic() {
        RetryTopics rt = RetryTopics.forTopic("orders").dlqTopic("orders-dead").build();
        assertEquals("orders-dead", rt.dlqTopic());
    }
}
