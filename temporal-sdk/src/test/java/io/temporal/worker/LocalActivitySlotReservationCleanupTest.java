package io.temporal.worker;

import static org.junit.Assert.*;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.api.enums.v1.TimeoutType;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.RetryOptions;
import io.temporal.failure.TimeoutFailure;
import io.temporal.testUtils.CountingSlotSupplier;
import io.temporal.testing.internal.SDKTestWorkflowRule;
import io.temporal.worker.tuning.*;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

/**
 * Regression test for the local activity slot-reservation cleanup. Covers the case where a local
 * activity execution reaches a terminal result (scheduleToClose timeout) while its slot reservation
 * is still pending in {@code LocalActivitySlotSupplierQueue}.
 *
 * <p>The single LA slot is held by a latch-controlled blocking activity. A victim local activity
 * with a short scheduleToClose times out while its reservation is pending. The test verifies the
 * pending reservation is actually aborted, that the victim is never granted a slot, and that a
 * subsequent local activity is not blocked behind the stale reservation.
 */
public class LocalActivitySlotReservationCleanupTest {

  private final CountDownLatch blockerStarted = new CountDownLatch(1);
  private final CountDownLatch releaseBlocker = new CountDownLatch(1);

  private final AbortRecordingSlotSupplier localActivitySlotSupplier =
      new AbortRecordingSlotSupplier(1);

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder()
          .setWorkerOptions(
              WorkerOptions.newBuilder()
                  .setWorkerTuner(
                      new CompositeTuner(
                          new FixedSizeSlotSupplier<WorkflowSlotInfo>(10),
                          new FixedSizeSlotSupplier<ActivitySlotInfo>(10),
                          localActivitySlotSupplier,
                          new FixedSizeSlotSupplier<NexusSlotInfo>(10)))
                  .build())
          .setActivityImplementations(new TestActivityImpl(blockerStarted, releaseBlocker))
          .setWorkflowTypes(TestWorkflowImpl.class)
          .setUseTimeskipping(false)
          .build();

  @After
  public void releaseBlockingActivity() {
    releaseBlocker.countDown();
  }

  @Test(timeout = 90_000)
  public void timedOutLocalActivityAbortsItsPendingSlotReservation() throws Exception {
    WorkflowClient client = testWorkflowRule.getWorkflowClient();
    String taskQueue = testWorkflowRule.getTaskQueue();

    // Occupy the only local activity slot with a latch-controlled blocking activity.
    startWorkflow(client, taskQueue, "block-workflow", "block");
    assertTrue("blocking activity never started", blockerStarted.await(10, TimeUnit.SECONDS));

    try {
      // Queue the victim. Its scheduleToClose fires while the slot reservation is still pending.
      WorkflowStub victim = startWorkflow(client, taskQueue, "victim-workflow", "victim");
      try {
        victim.getResult(30, TimeUnit.SECONDS, String.class);
        fail("victim workflow should have failed with scheduleToClose timeout");
      } catch (TimeoutException e) {
        fail("victim workflow did not terminate within the wait bound");
      } catch (WorkflowFailedException e) {
        assertTrue(
            "expected TIMEOUT_TYPE_SCHEDULE_TO_CLOSE but got: " + e, hasScheduleToCloseTimeout(e));
      }

      // The pending reservation must actually be aborted, not just left to be discarded later.
      assertEquals(1, localActivitySlotSupplier.abortCalls.get());

      // The victim was never granted a slot: only the blocker's reservation completed, and only
      // the blocker's permit was marked used. Nothing was released yet.
      assertEquals(1, localActivitySlotSupplier.reservedCount.get());
      assertEquals(1, localActivitySlotSupplier.usedCount.get());
      assertEquals(0, localActivitySlotSupplier.releasedCount.get());
    } finally {
      releaseBlocker.countDown();
    }

    // A subsequent local activity must not be blocked behind the stale reservation.
    WorkflowStub third = startWorkflow(client, taskQueue, "third-workflow", "quick");
    assertEquals("quick", third.getResult(30, TimeUnit.SECONDS, String.class));

    // Accounting is balanced: two grants total (blocker + third), two uses, two releases.
    // The aborted victim reservation contributed no grant and no release.
    assertEquals(1, localActivitySlotSupplier.abortCalls.get());
    assertEquals(2, localActivitySlotSupplier.reservedCount.get());
    assertEquals(2, localActivitySlotSupplier.usedCount.get());
    assertEquals(2, localActivitySlotSupplier.releasedCount.get());
  }

  private WorkflowStub startWorkflow(
      WorkflowClient client, String taskQueue, String workflowId, String input) {
    WorkflowStub workflow =
        client.newUntypedWorkflowStub(
            "TestWorkflow",
            WorkflowOptions.newBuilder().setTaskQueue(taskQueue).setWorkflowId(workflowId).build());
    workflow.start(input);
    return workflow;
  }

  private static boolean hasScheduleToCloseTimeout(Throwable e) {
    while (e != null) {
      if (e instanceof TimeoutFailure
          && ((TimeoutFailure) e).getTimeoutType() == TimeoutType.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE) {
        return true;
      }
      e = e.getCause();
    }
    return false;
  }

  @ActivityInterface
  public interface TestActivity {
    @ActivityMethod
    String execute(String input);
  }

  public static class TestActivityImpl implements TestActivity {
    private final CountDownLatch started;
    private final CountDownLatch release;

    public TestActivityImpl(CountDownLatch started, CountDownLatch release) {
      this.started = started;
      this.release = release;
    }

    @Override
    public String execute(String input) {
      if ("block".equals(input)) {
        started.countDown();
        try {
          // Bounded so the test can never leave a blocked activity behind on failure.
          release.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return input;
    }
  }

  @WorkflowInterface
  public interface TestWorkflow {
    @WorkflowMethod
    String testWorkflow(String input);
  }

  public static class TestWorkflowImpl implements TestWorkflow {
    @Override
    public String testWorkflow(String input) {
      LocalActivityOptions.Builder options =
          LocalActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(30))
              .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build());
      if ("victim".equals(input)) {
        options.setScheduleToCloseTimeout(Duration.ofSeconds(2));
      }
      TestActivity activity = Workflow.newLocalActivityStub(TestActivity.class, options.build());
      return activity.execute(input);
    }
  }

  /**
   * Counts {@code abortReservation} invocations on reservations it hands out. The returned wrapper
   * mirrors {@code SlotSupplierFuture.fromCompletableFuture} but delegates the abort to the inner
   * {@code SlotSupplierFuture#abortReservation()} rather than {@code cancel()}, which {@code
   * SlotSupplierFuture} does not support. If the inner reservation already completed normally, the
   * abort returns its permit to the caller, matching the standard contract.
   */
  private static class AbortRecordingSlotSupplier
      extends CountingSlotSupplier<LocalActivitySlotInfo> {
    final AtomicInteger abortCalls = new AtomicInteger();

    AbortRecordingSlotSupplier(int numSlots) {
      super(numSlots);
    }

    @Override
    public SlotSupplierFuture reserveSlot(SlotReserveContext<LocalActivitySlotInfo> ctx)
        throws Exception {
      SlotSupplierFuture inner = super.reserveSlot(ctx);
      SlotSupplierFuture wrapper =
          new SlotSupplierFuture() {
            @Override
            public SlotPermit abortReservation() {
              abortCalls.incrementAndGet();
              if (completeExceptionally(new CancellationException("Reservation aborted"))) {
                return inner.abortReservation();
              }
              return join();
            }
          };
      inner.whenComplete(
          (permit, throwable) -> {
            if (throwable != null) {
              wrapper.completeExceptionally(throwable);
            } else {
              wrapper.complete(permit);
            }
          });
      return wrapper;
    }
  }
}
