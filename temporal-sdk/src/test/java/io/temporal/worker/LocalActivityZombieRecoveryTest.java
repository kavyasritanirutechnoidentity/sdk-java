package io.temporal.worker;

import static org.junit.Assert.*;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.LocalActivityOptions;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

/**
 * Coverage for abandoned Local Activity executor permit recovery ({@code
 * setLocalActivityAbandonTimedOutExecutions} + {@code setLocalActivityMaxAbandonedExecutions}).
 * With one normal slot and one abandoned-execution allowance, a timed-out attempt whose user code
 * stays blocked releases its permit so another Local Activity can execute on a spare executor
 * thread. Timed-out user code keeps running but no longer counts towards the concurrency limit;
 * this does not interrupt or terminate user code.
 */
public class LocalActivityZombieRecoveryTest {

  private final CountDownLatch firstBlockStarted = new CountDownLatch(1);
  private final CountDownLatch firstBlockReturn = new CountDownLatch(1);
  private final CountDownLatch secondBlockStarted = new CountDownLatch(1);
  private final CountDownLatch secondBlockReturn = new CountDownLatch(1);
  private final Map<String, String> threadByInput = new ConcurrentHashMap<>();
  private final AtomicInteger retryBlockInvocations = new AtomicInteger();
  private final CountingSlotSupplier<LocalActivitySlotInfo> slotSupplier =
      new CountingSlotSupplier<>(1);

  private WorkerOptions workerOptions(boolean abandonEnabled) {
    WorkerOptions.Builder builder =
        WorkerOptions.newBuilder()
            .setWorkerTuner(
                new CompositeTuner(
                    new FixedSizeSlotSupplier<WorkflowSlotInfo>(10),
                    new FixedSizeSlotSupplier<ActivitySlotInfo>(10),
                    slotSupplier,
                    new FixedSizeSlotSupplier<NexusSlotInfo>(10)))
            .setLocalActivityAbandonTimedOutExecutions(abandonEnabled)
            // A positive allowance must not expand executor capacity when abandonment is
            // disabled.
            .setLocalActivityMaxAbandonedExecutions(abandonEnabled ? 1 : 5);
    return builder.build();
  }

  @Rule
  public SDKTestWorkflowRule ruleAbandon =
      SDKTestWorkflowRule.newBuilder()
          .setWorkerOptions(workerOptions(true))
          .setActivityImplementations(
              new TestActivityImpl(
                  firstBlockStarted,
                  firstBlockReturn,
                  secondBlockStarted,
                  secondBlockReturn,
                  threadByInput,
                  retryBlockInvocations))
          .setWorkflowTypes(TestWorkflowImpl.class)
          .setUseTimeskipping(false)
          .build();

  @Rule
  public SDKTestWorkflowRule ruleNoAbandon =
      SDKTestWorkflowRule.newBuilder()
          .setWorkerOptions(workerOptions(false))
          .setActivityImplementations(
              new TestActivityImpl(
                  firstBlockStarted,
                  firstBlockReturn,
                  secondBlockStarted,
                  secondBlockReturn,
                  threadByInput,
                  retryBlockInvocations))
          .setWorkflowTypes(TestWorkflowImpl.class)
          .setUseTimeskipping(false)
          .build();

  @After
  public void releaseBlockedActivities() {
    firstBlockReturn.countDown();
    secondBlockReturn.countDown();
  }

  /**
   * The first activity times out while blocked, its permit is abandoned, and the second activity
   * executes on a spare executor thread while the first thread remains blocked. When the first
   * activity finally returns, no permit is released a second time.
   */
  @Test(timeout = 90_000)
  public void timedOutActivityAbandonsPermitAndSecondActivityExecutes() throws Exception {
    WorkflowClient client = ruleAbandon.getWorkflowClient();
    String taskQueue = ruleAbandon.getTaskQueue();

    // "tout-block" times out (startToClose=2s) while blocked on its latch and gets abandoned.
    WorkflowStub blocked = startWorkflow(client, taskQueue, "zombie-workflow", "tout-block");
    assertTrue(firstBlockStarted.await(10, TimeUnit.SECONDS));

    try {
      blocked.getResult(30, TimeUnit.SECONDS, String.class);
      fail("blocked workflow should have failed with a timeout");
    } catch (WorkflowFailedException e) {
      assertTrue("expected timeout but got: " + e, hasTimeoutFailure(e));
    }

    // The slot was abandoned; the second activity must dispatch on a *spare* thread while the
    // first user thread is still blocked.
    WorkflowStub victim = startWorkflow(client, taskQueue, "victim-workflow", "quick");
    assertEquals("quick", victim.getResult(30, TimeUnit.SECONDS, String.class));
    assertNotSame(
        "second activity must run on a different executor thread than the blocked first one",
        threadByInput.get("block"),
        threadByInput.get("quick"));
    assertNull("first activity must still be blocked", threadByInput.get("block-done"));

    // Release the blocked first activity; its late return must not release another permit.
    int releasesBefore = slotSupplier.releasedCount.get();
    firstBlockReturn.countDown();
    Thread.sleep(2_000);
    assertEquals(
        "late-returning abandoned activity released a permit again",
        releasesBefore,
        slotSupplier.releasedCount.get());
    assertEquals(slotSupplier.reservedCount.get(), slotSupplier.releasedCount.get());
  }

  /** With both the slot and the abandoned-execution allowance consumed, waiting is bounded. */
  @Test(timeout = 90_000)
  public void zombieCapacityIsBounded() throws Exception {
    WorkflowClient client = ruleAbandon.getWorkflowClient();
    String taskQueue = ruleAbandon.getTaskQueue();

    // Wedges thread 1, gets abandoned.
    startWorkflow(client, taskQueue, "z1", "tout-block");
    assertTrue(firstBlockStarted.await(10, TimeUnit.SECONDS));

    // Takes the freed slot on the spare thread and wedges it too.
    WorkflowStub second = startWorkflow(client, taskQueue, "z2", "tout-block2");
    assertTrue(secondBlockStarted.await(10, TimeUnit.SECONDS));
    try {
      second.getResult(30, TimeUnit.SECONDS, String.class);
      fail("second workflow should have failed with a timeout");
    } catch (WorkflowFailedException e) {
      assertTrue("expected timeout but got: " + e, hasTimeoutFailure(e));
    }

    // Normal slot + one abandoned thread are both consumed: the next activity must wait.
    WorkflowStub queued = startWorkflow(client, taskQueue, "z3", "quick2");
    try {
      queued.getResult(5, TimeUnit.SECONDS, String.class);
      fail("queued activity executed beyond the abandoned-execution bound");
    } catch (TimeoutException expected) {
    }
  }

  /** With abandonment disabled, a timed-out blocked activity holds its permit as before. */
  @Test(timeout = 90_000)
  public void abandonedDisabledPreservesExistingBehaviour() throws Exception {
    WorkflowClient client = ruleNoAbandon.getWorkflowClient();
    String taskQueue = ruleNoAbandon.getTaskQueue();

    WorkflowStub blocked = startWorkflow(client, taskQueue, "noab-block", "tout-block");
    assertTrue(firstBlockStarted.await(10, TimeUnit.SECONDS));
    try {
      blocked.getResult(30, TimeUnit.SECONDS, String.class);
      fail("blocked workflow should have failed with a timeout");
    } catch (WorkflowFailedException e) {
      assertTrue("expected timeout but got: " + e, hasTimeoutFailure(e));
    }

    // No abandonment: the permit stays held by the blocked thread, the next activity waits.
    // A configured maxAbandonedExecutions=5 must not expand executor capacity when disabled.
    WorkflowStub queued = startWorkflow(client, taskQueue, "noab-queue", "quick");
    try {
      queued.getResult(5, TimeUnit.SECONDS, String.class);
      fail("queued activity executed while permit was still held");
    } catch (TimeoutException expected) {
    }
  }

  /**
   * When a running attempt times out and is retried, the timed-out attempt's permit is abandoned
   * after scheduling the retry so the retry attempt can execute on a spare executor thread. The
   * stale first attempt then returns while attempt 2 still runs: it must not release attempt 2's
   * permit.
   */
  @Test(timeout = 90_000)
  public void retryPathAbandonsTimedOutAttemptPermit() throws Exception {
    WorkflowClient client = ruleAbandon.getWorkflowClient();
    String taskQueue = ruleAbandon.getTaskQueue();

    // "retryblock" attempt 1 blocks on the first latch, times out, and is retried. Attempt 2 gets
    // the abandoned permit's slot on a spare thread and blocks on the second latch.
    WorkflowStub workflow = startWorkflow(client, taskQueue, "retry-wf", "retryblock");
    assertTrue("retry attempt never started", secondBlockStarted.await(20, TimeUnit.SECONDS));
    assertEquals(2, retryBlockInvocations.get());

    // Let the abandoned first attempt return while attempt 2 is still running. Its stale permit
    // take must not release attempt 2's permit.
    int releasesBefore = slotSupplier.releasedCount.get();
    firstBlockReturn.countDown();
    Thread.sleep(800);
    assertEquals(
        "stale first attempt released the retry attempt's permit",
        releasesBefore,
        slotSupplier.releasedCount.get());

    // Attempt 2 completes; the retry succeeds and the workflow returns its result.
    secondBlockReturn.countDown();
    assertEquals("retryblock", workflow.getResult(30, TimeUnit.SECONDS, String.class));
    assertEquals(slotSupplier.reservedCount.get(), slotSupplier.releasedCount.get());
  }

  /**
   * A timeout that loses the terminal-completion race must not release a permit: an activity that
   * completes normally before scheduleToClose fires leaves accounting untouched.
   */
  @Test(timeout = 90_000)
  public void losingTimeoutCallbackDoesNotAbandon() throws Exception {
    WorkflowClient client = ruleAbandon.getWorkflowClient();
    String taskQueue = ruleAbandon.getTaskQueue();

    // "stc-quick" completes immediately; its 2s scheduleToClose timeout then fires but loses the
    // callback race and must not release anything.
    WorkflowStub workflow = startWorkflow(client, taskQueue, "stc-quick-wf", "stc-quick");
    assertEquals("stc-quick", workflow.getResult(30, TimeUnit.SECONDS, String.class));

    // Wait past the scheduleToClose deadline so the losing handler ran.
    Thread.sleep(3_000);
    assertEquals(1, slotSupplier.reservedCount.get());
    assertEquals(
        "a timeout that lost the completion released a permit",
        1,
        slotSupplier.releasedCount.get());
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

  private static boolean hasTimeoutFailure(Throwable e) {
    while (e != null) {
      if (e instanceof TimeoutFailure) {
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
    private final CountDownLatch firstStarted;
    private final CountDownLatch firstReturn;
    private final CountDownLatch secondStarted;
    private final CountDownLatch secondReturn;
    private final Map<String, String> threadByInput;
    private final AtomicInteger retryBlockInvocations;

    public TestActivityImpl(
        CountDownLatch firstStarted,
        CountDownLatch firstReturn,
        CountDownLatch secondStarted,
        CountDownLatch secondReturn,
        Map<String, String> threadByInput,
        AtomicInteger retryBlockInvocations) {
      this.firstStarted = firstStarted;
      this.firstReturn = firstReturn;
      this.secondStarted = secondStarted;
      this.secondReturn = secondReturn;
      this.threadByInput = threadByInput;
      this.retryBlockInvocations = retryBlockInvocations;
    }

    @Override
    public String execute(String input) {
      threadByInput.put(input, Thread.currentThread().getName());
      if ("block".equals(input)) {
        firstStarted.countDown();
        await(firstReturn);
        threadByInput.put("block-done", Thread.currentThread().getName());
      } else if ("block2".equals(input)) {
        secondStarted.countDown();
        await(secondReturn);
      } else if ("retryblock".equals(input)) {
        int invocation = retryBlockInvocations.incrementAndGet();
        if (invocation == 1) {
          firstStarted.countDown();
          await(firstReturn);
        } else {
          secondStarted.countDown();
          await(secondReturn);
        }
      }
      return input;
    }

    private void await(CountDownLatch latch) {
      try {
        // Bounded so the test can never leave a blocked activity behind on failure.
        latch.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
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
              .setStartToCloseTimeout(
                  input.startsWith("tout-") || "retryblock".equals(input)
                      ? Duration.ofSeconds(2)
                      : Duration.ofSeconds(30))
              .setRetryOptions(
                  "retryblock".equals(input)
                      ? RetryOptions.newBuilder()
                          .setMaximumAttempts(2)
                          .setInitialInterval(Duration.ofMillis(100))
                          .build()
                      : RetryOptions.newBuilder().setMaximumAttempts(1).build());
      if ("stc-quick".equals(input)) {
        options.setScheduleToCloseTimeout(Duration.ofSeconds(2));
      }
      TestActivity activity = Workflow.newLocalActivityStub(TestActivity.class, options.build());
      return activity.execute(input.startsWith("tout-") ? input.substring(5) : input);
    }
  }
}
