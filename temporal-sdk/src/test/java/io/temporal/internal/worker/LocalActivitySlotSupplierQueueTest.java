package io.temporal.internal.worker;

import static org.junit.Assert.*;

import com.uber.m3.tally.NoopScope;
import io.temporal.api.common.v1.ActivityType;
import io.temporal.api.workflowservice.v1.PollActivityTaskQueueResponse;
import io.temporal.internal.statemachines.ExecuteLocalActivityParameters;
import io.temporal.worker.tuning.FixedSizeSlotSupplier;
import io.temporal.worker.tuning.LocalActivitySlotInfo;
import io.temporal.worker.tuning.SlotPermit;
import io.temporal.worker.tuning.SlotReleaseReason;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

/**
 * Verifies that the queue error path and permit abandonment cannot release the same slot permit
 * twice. Both competitors funnel through {@link LocalActivityExecutionContext#takePermit} inside
 * the execution-context monitor, so exactly one of them performs the release regardless of order.
 */
public class LocalActivitySlotSupplierQueueTest {

  private final TrackingSlotSupplier<LocalActivitySlotInfo> slotSupplier =
      new TrackingSlotSupplier<>(new FixedSizeSlotSupplier<>(1), new NoopScope());
  private LocalActivitySlotSupplierQueue queue;

  @After
  public void tearDown() throws Exception {
    if (queue != null) {
      queue.shutdown(new ShutdownManager(), true).get(10, TimeUnit.SECONDS);
    }
  }

  private LocalActivityAttemptTask newAttemptTask(CountDownLatch completed) {
    ExecuteLocalActivityParameters params =
        new ExecuteLocalActivityParameters(
            PollActivityTaskQueueResponse.newBuilder()
                .setActivityId("la")
                .setActivityType(ActivityType.newBuilder().setName("activity")),
            null,
            System.currentTimeMillis(),
            null,
            true,
            java.time.Duration.ofSeconds(10),
            null,
            null);
    LocalActivityExecutionContext context =
        new LocalActivityExecutionContext(
            params, (LocalActivityResult r) -> completed.countDown(), null);
    return new LocalActivityAttemptTask(
        context,
        PollActivityTaskQueueResponse.newBuilder().setActivityId("la").setAttempt(1),
        null);
  }

  private void awaitCompletion(LocalActivityAttemptTask task) throws Exception {
    assertTrue(
        "execution context did not complete",
        task.getExecutionContext().getExecutionResult().get(10, TimeUnit.SECONDS) != null);
  }

  private void assertExactlyOnceAccounting() {
    assertEquals(
        "issuedSlots must return to zero; a double release drives it negative",
        0,
        slotSupplier.getIssuedSlots());
    assertEquals(0, slotSupplier.getUsedSlotCount());
  }

  /**
   * Timeout abandonment takes the permit first; the queue error path triggered by a failed dispatch
   * must then find nothing to release.
   */
  @Test(timeout = 30_000)
  public void abandonBeforeQueueErrorReleasesPermitExactlyOnce() throws Exception {
    queue =
        new LocalActivitySlotSupplierQueue(
            slotSupplier,
            task -> {
              // Simulate the timeout callback abandoning the just-assigned permit.
              LocalActivityExecutionContext ctx = task.getExecutionContext();
              synchronized (ctx) {
                SlotPermit taken = ctx.takePermit();
                if (taken != null) {
                  slotSupplier.releaseSlot(SlotReleaseReason.abandoned(), taken);
                }
              }
              throw new RuntimeException("simulated executor dispatch failure");
            });
    CountDownLatch completed = new CountDownLatch(1);
    LocalActivityAttemptTask task = newAttemptTask(completed);

    queue.submitAttempt(new SlotReservationData("q", "id", "b"), false, task);
    queue.start();

    assertTrue(completed.await(10, TimeUnit.SECONDS));
    assertExactlyOnceAccounting();
  }

  /**
   * The queue error path releases first; a subsequent timeout abandonment must observe an empty
   * permit reference and release nothing.
   */
  @Test(timeout = 30_000)
  public void queueErrorBeforeAbandonReleasesPermitExactlyOnce() throws Exception {
    queue =
        new LocalActivitySlotSupplierQueue(
            slotSupplier,
            task -> {
              throw new RuntimeException("simulated executor dispatch failure");
            });
    CountDownLatch completed = new CountDownLatch(1);
    LocalActivityAttemptTask task = newAttemptTask(completed);

    queue.submitAttempt(new SlotReservationData("q", "id", "b"), false, task);
    queue.start();

    assertTrue(completed.await(10, TimeUnit.SECONDS));

    // A timeout that fires after the error release must not release the same permit again.
    LocalActivityExecutionContext ctx = task.getExecutionContext();
    synchronized (ctx) {
      assertNull("the permit must already be taken by the error path", ctx.takePermit());
    }
    assertExactlyOnceAccounting();
  }
}
