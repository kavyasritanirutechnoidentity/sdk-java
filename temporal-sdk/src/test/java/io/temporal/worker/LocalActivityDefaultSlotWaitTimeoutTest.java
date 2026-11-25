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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

/**
 * Coverage for {@code WorkerOptions.Builder#setDefaultLocalActivityScheduleToStartTimeout}, a
 * worker-wide fallback ScheduleToStart timeout for Local Activities that do not configure one.
 * Since the ScheduleToStart timer starts when the attempt is submitted and runs while the slot
 * reservation is pending, the default also bounds how long a Local Activity waits when the Local
 * Activity executor pool is exhausted.
 */
public class LocalActivityDefaultSlotWaitTimeoutTest {

  private static final Duration DEFAULT_SLOT_WAIT = Duration.ofSeconds(2);

  private final CountDownLatch blockerStarted = new CountDownLatch(1);
  private final CountDownLatch releaseBlocker = new CountDownLatch(1);

  private WorkerOptions.Builder workerOptions(boolean applyDefault) {
    WorkerOptions.Builder builder =
        WorkerOptions.newBuilder()
            .setWorkerTuner(
                new CompositeTuner(
                    new FixedSizeSlotSupplier<WorkflowSlotInfo>(10),
                    new FixedSizeSlotSupplier<ActivitySlotInfo>(10),
                    new CountingSlotSupplier<LocalActivitySlotInfo>(1),
                    new FixedSizeSlotSupplier<NexusSlotInfo>(10)));
    if (applyDefault) {
      builder.setDefaultLocalActivityScheduleToStartTimeout(DEFAULT_SLOT_WAIT);
    }
    return builder;
  }

  @Rule
  public SDKTestWorkflowRule ruleWithDefault =
      SDKTestWorkflowRule.newBuilder()
          .setWorkerOptions(workerOptions(true).build())
          .setActivityImplementations(new TestActivityImpl(blockerStarted, releaseBlocker))
          .setWorkflowTypes(TestWorkflowImpl.class)
          .setUseTimeskipping(false)
          .build();

  @Rule
  public SDKTestWorkflowRule ruleWithoutDefault =
      SDKTestWorkflowRule.newBuilder()
          .setWorkerOptions(workerOptions(false).build())
          .setActivityImplementations(new TestActivityImpl(blockerStarted, releaseBlocker))
          .setWorkflowTypes(TestWorkflowImpl.class)
          .setUseTimeskipping(false)
          .build();

  @After
  public void releaseBlockingActivity() {
    releaseBlocker.countDown();
  }

  /**
   * Original issue scenario: pool exhausted by a blocked activity, a Local Activity with no
   * ScheduleToStart configured must receive a terminal TIMEOUT_TYPE_SCHEDULE_TO_START result from
   * the worker default instead of waiting indefinitely.
   */
  @Test(timeout = 90_000)
  public void defaultTimeoutFailsActivityWaitingForSlot() throws Exception {
    WorkflowClient client = ruleWithDefault.getWorkflowClient();
    String taskQueue = ruleWithDefault.getTaskQueue();

    startWorkflow(client, taskQueue, "block-workflow", "block");
    assertTrue("blocking activity never started", blockerStarted.await(10, TimeUnit.SECONDS));

    // No scheduleToStart or scheduleToClose configured on the activity.
    WorkflowStub victim = startWorkflow(client, taskQueue, "victim-workflow", "victim");
    long start = System.nanoTime();
    try {
      victim.getResult(30, TimeUnit.SECONDS, String.class);
      fail("victim workflow should have failed with scheduleToStart timeout");
    } catch (WorkflowFailedException e) {
      assertTrue(
          "expected TIMEOUT_TYPE_SCHEDULE_TO_START but got: " + e,
          hasTimeoutType(e, TimeoutType.TIMEOUT_TYPE_SCHEDULE_TO_START));
      // Must have failed near the worker default, not hung indefinitely.
      assertTrue(
          "timeout took suspiciously long",
          TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < 20);
    }
  }

  /**
   * An explicitly configured scheduleToStart must take precedence over the worker default. With
   * explicit scheduleToStart=5s, scheduleToClose=3s and worker default=2s, the observed timeout
   * must be SCHEDULE_TO_CLOSE: the worker default must not preempt the activity's own options.
   */
  @Test(timeout = 90_000)
  public void explicitScheduleToStartOverridesWorkerDefault() throws Exception {
    WorkflowClient client = ruleWithDefault.getWorkflowClient();
    String taskQueue = ruleWithDefault.getTaskQueue();

    startWorkflow(client, taskQueue, "block-workflow-2", "block");
    assertTrue("blocking activity never started", blockerStarted.await(10, TimeUnit.SECONDS));

    WorkflowStub victim = startWorkflow(client, taskQueue, "explicit-victim", "explicit-victim");
    try {
      victim.getResult(30, TimeUnit.SECONDS, String.class);
      fail("victim workflow should have failed with scheduleToClose timeout");
    } catch (WorkflowFailedException e) {
      assertTrue(
          "expected TIMEOUT_TYPE_SCHEDULE_TO_CLOSE but got: " + e,
          hasTimeoutType(e, TimeoutType.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE));
    }
  }

  /**
   * With no worker default configured, an unconfigured Local Activity still waits indefinitely: the
   * option must be opt-in and not change existing behaviour.
   */
  @Test(timeout = 60_000)
  public void noDefaultKeepsUnlimitedWait() throws Exception {
    WorkflowClient client = ruleWithoutDefault.getWorkflowClient();
    String taskQueue = ruleWithoutDefault.getTaskQueue();

    startWorkflow(client, taskQueue, "block-workflow-3", "block");
    assertTrue("blocking activity never started", blockerStarted.await(10, TimeUnit.SECONDS));

    WorkflowStub victim = startWorkflow(client, taskQueue, "victim-no-default", "victim");
    try {
      victim.getResult(5, TimeUnit.SECONDS, String.class);
      fail("victim workflow completed while the pool was still exhausted");
    } catch (TimeoutException expected) {
      // Still running and still waiting for a slot, unchanged behaviour.
    }
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

  private static boolean hasTimeoutType(Throwable e, TimeoutType expected) {
    while (e != null) {
      if (e instanceof TimeoutFailure && ((TimeoutFailure) e).getTimeoutType() == expected) {
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
      if ("explicit-victim".equals(input)) {
        // Explicit scheduleToStart must win over the worker default; scheduleToClose then fires.
        options.setScheduleToStartTimeout(Duration.ofSeconds(5));
        options.setScheduleToCloseTimeout(Duration.ofSeconds(3));
      }
      TestActivity activity = Workflow.newLocalActivityStub(TestActivity.class, options.build());
      return activity.execute(input);
    }
  }
}
