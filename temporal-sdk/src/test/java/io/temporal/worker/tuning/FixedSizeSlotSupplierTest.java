package io.temporal.worker.tuning;

import static org.junit.Assert.*;

import com.uber.m3.tally.NoopScope;
import io.temporal.internal.worker.SlotReservationData;
import io.temporal.internal.worker.TrackingSlotSupplier;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class FixedSizeSlotSupplierTest {

  private static final SlotReservationData RESERVATION_DATA =
      new SlotReservationData("tq", "id", "bid");

  @Test
  public void ensureInterruptingReservationWorksWhenWaitingOnSemaphoreInQueue() throws Exception {
    FixedSizeSlotSupplier<WorkflowSlotInfo> supplier = new FixedSizeSlotSupplier<>(1);
    TrackingSlotSupplier<WorkflowSlotInfo> trackingSS =
        new TrackingSlotSupplier<>(supplier, new NoopScope());
    // Reserve one slot and don't release
    SlotPermit firstPermit = trackingSS.reserveSlot(RESERVATION_DATA).get();

    // Try to reserve another slot
    SlotSupplierFuture secondSlotFuture = trackingSS.reserveSlot(RESERVATION_DATA);
    // Try to reserve a third
    SlotSupplierFuture thirdSlotFuture = trackingSS.reserveSlot(RESERVATION_DATA);

    // Cancel second reservation & release first permit, which should allow third to be acquired
    SlotPermit maybePermit = secondSlotFuture.abortReservation();
    assertNull(maybePermit);
    trackingSS.releaseSlot(SlotReleaseReason.neverUsed(), firstPermit);

    SlotPermit p = thirdSlotFuture.get();
    assertNotNull(p);
  }

  /**
   * Races {@code abortReservation()} against a semaphore grant and asserts that no permit is ever
   * lost: whichever side wins, all accounting must return to zero and full semaphore capacity must
   * be restorable.
   *
   * <p>Note: the exact interleaving in which the grant lands between the wrapper completing
   * exceptionally and the abort handler cancelling the underlying waiter cannot be forced
   * deterministically from outside {@code SlotSupplierFuture}. Repeated concurrent iterations
   * provide stress coverage of the race, not guaranteed deterministic coverage of that specific
   * interleaving.
   */
  @Test(timeout = 120_000)
  public void abortRacingWithGrantNeverLosesAPermit() throws Exception {
    for (int i = 0; i < 200; i++) {
      FixedSizeSlotSupplier<WorkflowSlotInfo> supplier = new FixedSizeSlotSupplier<>(1);
      TrackingSlotSupplier<WorkflowSlotInfo> trackingSS =
          new TrackingSlotSupplier<>(supplier, new NoopScope());

      // Occupy the only slot, then start a reservation that stays pending.
      SlotPermit heldPermit = trackingSS.reserveSlot(RESERVATION_DATA).get();
      SlotSupplierFuture pending = trackingSS.reserveSlot(RESERVATION_DATA);

      // Release the held permit and abort the pending reservation as concurrently as possible.
      CyclicBarrier gate = new CyclicBarrier(2);
      AtomicReference<Throwable> releaserFailure = new AtomicReference<>();
      Thread releaser =
          new Thread(
              () -> {
                try {
                  gate.await(10, TimeUnit.SECONDS);
                  trackingSS.releaseSlot(SlotReleaseReason.taskComplete(), heldPermit);
                } catch (Throwable t) {
                  releaserFailure.set(t);
                }
              });
      releaser.start();
      gate.await(10, TimeUnit.SECONDS);
      SlotPermit abortedPermit = pending.abortReservation();
      releaser.join(10_000);
      assertFalse("releaser thread stuck on iteration " + i, releaser.isAlive());
      assertNull(releaserFailure.get());

      // The permit observed through the pending future — either via abortReservation or via the
      // wrapper completing normally — belongs to this thread and is released exactly once.
      if (abortedPermit != null) {
        trackingSS.releaseSlot(SlotReleaseReason.neverUsed(), abortedPermit);
      } else if (pending.isDone() && !pending.isCompletedExceptionally()) {
        trackingSS.releaseSlot(SlotReleaseReason.neverUsed(), pending.join());
      }

      assertEquals("issuedSlots drifted on iteration " + i, 0, trackingSS.getIssuedSlots());
      assertTrue(
          "semaphore capacity was lost on iteration " + i,
          trackingSS.tryReserveSlot(RESERVATION_DATA).isPresent());
    }
  }
}
