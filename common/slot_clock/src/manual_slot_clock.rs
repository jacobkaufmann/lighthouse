use super::{SlotClock, SlotDurationSchedule};
use parking_lot::RwLock;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
use types::{Epoch, Slot};

/// Determines the present slot based upon a manually-incremented UNIX timestamp.
pub struct ManualSlotClock {
    genesis_slot: Slot,
    /// Duration from UNIX epoch to genesis.
    genesis_duration: Duration,
    /// Duration from UNIX epoch to right now.
    current_time: Arc<RwLock<Duration>>,
    /// The number of slots per epoch.
    slots_per_epoch: u64,
    /// A schedule for the length of each slot.
    slot_duration_schedule: SlotDurationSchedule,
}

impl Clone for ManualSlotClock {
    fn clone(&self) -> Self {
        ManualSlotClock {
            genesis_slot: self.genesis_slot,
            genesis_duration: self.genesis_duration,
            current_time: Arc::clone(&self.current_time),
            slots_per_epoch: self.slots_per_epoch,
            slot_duration_schedule: self.slot_duration_schedule,
        }
    }
}

impl ManualSlotClock {
    pub fn set_slot(&self, slot: u64) {
        *self.current_time.write() = self
            .start_of(Slot::new(slot))
            .expect("slot must be post-genesis");
    }

    pub fn set_current_time(&self, duration: Duration) {
        *self.current_time.write() = duration;
    }

    pub fn advance_time(&self, duration: Duration) {
        let current_time = *self.current_time.read();
        *self.current_time.write() = current_time.add(duration);
    }

    pub fn advance_slot(&self) {
        self.set_slot(self.now().unwrap().as_u64() + 1)
    }

    pub fn genesis_duration(&self) -> &Duration {
        &self.genesis_duration
    }

    /// Returns the duration from `now` until the start of `slot`.
    ///
    /// Will return `None` if `now` is later than the start of `slot`.
    pub fn duration_to_slot(&self, slot: Slot, now: Duration) -> Option<Duration> {
        self.start_of(slot)?.checked_sub(now)
    }

    /// Returns the duration between `now` and the start of the next slot.
    pub fn duration_to_next_slot_from(&self, now: Duration) -> Option<Duration> {
        if now < self.genesis_duration {
            self.genesis_duration.checked_sub(now)
        } else {
            self.duration_to_slot(self.slot_of(now)? + 1, now)
        }
    }

    /// Returns the duration between `now` and the start of the next epoch.
    pub fn duration_to_next_epoch_from(&self, now: Duration) -> Option<Duration> {
        if now < self.genesis_duration {
            self.genesis_duration.checked_sub(now)
        } else {
            let next_epoch_start_slot = (self.slot_of(now)?.epoch(self.slots_per_epoch) + 1)
                .start_slot(self.slots_per_epoch);

            self.duration_to_slot(next_epoch_start_slot, now)
        }
    }
}

impl SlotClock for ManualSlotClock {
    fn new(
        genesis_slot: Slot,
        genesis_duration: Duration,
        slots_per_epoch: u64,
        slot_duration_schedule: SlotDurationSchedule,
    ) -> Self {
        if slot_duration_schedule.initial.as_millis() == 0 {
            panic!("ManualSlotClock cannot have a < 1ms slot duration");
        }
        if let Some((_e, d)) = slot_duration_schedule.breakpoint {
            if d.as_millis() == 0 {
                panic!("ManualSlotClock cannot have a < 1ms slot duration");
            }
        }

        Self {
            genesis_slot,
            current_time: Arc::new(RwLock::new(genesis_duration)),
            genesis_duration,
            slots_per_epoch,
            slot_duration_schedule,
        }
    }

    fn now(&self) -> Option<Slot> {
        self.slot_of(*self.current_time.read())
    }

    fn is_prior_to_genesis(&self) -> Option<bool> {
        Some(*self.current_time.read() < self.genesis_duration)
    }

    fn now_duration(&self) -> Option<Duration> {
        Some(*self.current_time.read())
    }

    fn slot_of(&self, now: Duration) -> Option<Slot> {
        self.slot_duration_schedule.slot_of(
            self.genesis_slot,
            self.genesis_duration,
            self.slots_per_epoch,
            now,
        )
    }

    fn duration_to_next_slot(&self) -> Option<Duration> {
        self.duration_to_next_slot_from(*self.current_time.read())
    }

    fn duration_to_next_epoch(&self) -> Option<Duration> {
        self.duration_to_next_epoch_from(*self.current_time.read())
    }

    fn slot_duration(&self, epoch: Epoch) -> Duration {
        self.slot_duration_schedule.slot_duration(epoch)
    }

    fn slot_duration_schedule(&self) -> SlotDurationSchedule {
        self.slot_duration_schedule
    }

    fn slots_per_epoch(&self) -> u64 {
        self.slots_per_epoch
    }

    fn duration_to_slot(&self, slot: Slot) -> Option<Duration> {
        self.duration_to_slot(slot, *self.current_time.read())
    }

    /// Returns the duration between UNIX epoch and the start of `slot`.
    fn start_of(&self, slot: Slot) -> Option<Duration> {
        let duration_from_genesis = self.slot_duration_schedule.duration_from_genesis_to_slot(
            self.genesis_slot,
            self.slots_per_epoch,
            slot,
        )?;
        self.genesis_duration.checked_add(duration_from_genesis)
    }

    fn genesis_slot(&self) -> Slot {
        self.genesis_slot
    }

    fn genesis_duration(&self) -> Duration {
        self.genesis_duration
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SLOTS_PER_EPOCH: u64 = 32;

    #[test]
    fn test_slot_now() {
        let clock = ManualSlotClock::new(
            Slot::new(10),
            Duration::from_secs(0),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(1), None),
        );
        assert_eq!(clock.now(), Some(Slot::new(10)));
        clock.set_slot(123);
        assert_eq!(clock.now(), Some(Slot::new(123)));
    }

    #[test]
    fn test_is_prior_to_genesis() {
        let genesis_secs = 1;

        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(genesis_secs),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(1), None),
        );

        *clock.current_time.write() = Duration::from_secs(genesis_secs - 1);
        assert!(clock.is_prior_to_genesis().unwrap(), "prior to genesis");

        *clock.current_time.write() = Duration::from_secs(genesis_secs);
        assert!(!clock.is_prior_to_genesis().unwrap(), "at genesis");

        *clock.current_time.write() = Duration::from_secs(genesis_secs + 1);
        assert!(!clock.is_prior_to_genesis().unwrap(), "after genesis");
    }

    #[test]
    fn start_of() {
        // Genesis slot and genesis duration 0.
        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(0),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(1), None),
        );
        assert_eq!(clock.start_of(Slot::new(0)), Some(Duration::from_secs(0)));
        assert_eq!(clock.start_of(Slot::new(1)), Some(Duration::from_secs(1)));
        assert_eq!(clock.start_of(Slot::new(2)), Some(Duration::from_secs(2)));

        // Genesis slot 1 and genesis duration 10.
        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(10),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(1), None),
        );
        assert_eq!(clock.start_of(Slot::new(0)), Some(Duration::from_secs(10)));
        assert_eq!(clock.start_of(Slot::new(1)), Some(Duration::from_secs(11)));
        assert_eq!(clock.start_of(Slot::new(2)), Some(Duration::from_secs(12)));

        // Genesis slot 1 and genesis duration 0.
        let clock = ManualSlotClock::new(
            Slot::new(1),
            Duration::from_secs(0),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(1), None),
        );
        assert_eq!(clock.start_of(Slot::new(0)), None);
        assert_eq!(clock.start_of(Slot::new(1)), Some(Duration::from_secs(0)));
        assert_eq!(clock.start_of(Slot::new(2)), Some(Duration::from_secs(1)));

        // Genesis slot 1 and genesis duration 10.
        let clock = ManualSlotClock::new(
            Slot::new(1),
            Duration::from_secs(10),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(1), None),
        );
        assert_eq!(clock.start_of(Slot::new(0)), None);
        assert_eq!(clock.start_of(Slot::new(1)), Some(Duration::from_secs(10)));
        assert_eq!(clock.start_of(Slot::new(2)), Some(Duration::from_secs(11)));
    }

    #[test]
    fn test_duration_to_next_slot() {
        let slot_duration_schedule = SlotDurationSchedule::new(Duration::from_secs(1), None);

        // Genesis time is now.
        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(0),
            SLOTS_PER_EPOCH,
            slot_duration_schedule,
        );
        *clock.current_time.write() = Duration::from_secs(0);
        assert_eq!(clock.duration_to_next_slot(), Some(Duration::from_secs(1)));

        // Genesis time is in the future.
        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(10),
            SLOTS_PER_EPOCH,
            slot_duration_schedule,
        );
        *clock.current_time.write() = Duration::from_secs(0);
        assert_eq!(clock.duration_to_next_slot(), Some(Duration::from_secs(10)));

        // Genesis time is in the past.
        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(0),
            SLOTS_PER_EPOCH,
            slot_duration_schedule,
        );
        *clock.current_time.write() = Duration::from_secs(10);
        assert_eq!(clock.duration_to_next_slot(), Some(Duration::from_secs(1)));
    }

    #[test]
    fn test_duration_to_next_epoch() {
        let slot_duration_schedule = SlotDurationSchedule::new(Duration::from_secs(1), None);

        // Genesis time is now.
        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(0),
            SLOTS_PER_EPOCH,
            slot_duration_schedule,
        );
        *clock.current_time.write() = Duration::from_secs(0);
        assert_eq!(
            clock.duration_to_next_epoch(),
            Some(Duration::from_secs(32))
        );

        // Genesis time is in the future.
        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(10),
            SLOTS_PER_EPOCH,
            slot_duration_schedule,
        );
        *clock.current_time.write() = Duration::from_secs(0);
        assert_eq!(
            clock.duration_to_next_epoch(),
            Some(Duration::from_secs(10))
        );

        // Genesis time is in the past.
        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(0),
            SLOTS_PER_EPOCH,
            slot_duration_schedule,
        );
        *clock.current_time.write() = Duration::from_secs(10);
        assert_eq!(
            clock.duration_to_next_epoch(),
            Some(Duration::from_secs(22))
        );

        // Genesis time is in the past.
        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(0),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(12), None),
        );
        *clock.current_time.write() = Duration::from_secs(72_333);
        assert!(clock.duration_to_next_epoch().is_some(),);
    }

    #[test]
    fn test_tolerance() {
        let clock = ManualSlotClock::new(
            Slot::new(0),
            Duration::from_secs(10),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(1), None),
        );

        // Set clock to the 0'th slot.
        *clock.current_time.write() = Duration::from_secs(10);
        assert_eq!(
            clock
                .now_with_future_tolerance(Duration::from_secs(0))
                .unwrap(),
            Slot::new(0),
            "future tolerance of zero should return current slot"
        );
        assert_eq!(
            clock
                .now_with_past_tolerance(Duration::from_secs(0))
                .unwrap(),
            Slot::new(0),
            "past tolerance of zero should return current slot"
        );
        assert_eq!(
            clock
                .now_with_future_tolerance(Duration::from_millis(10))
                .unwrap(),
            Slot::new(0),
            "insignificant future tolerance should return current slot"
        );
        assert_eq!(
            clock
                .now_with_past_tolerance(Duration::from_millis(10))
                .unwrap(),
            Slot::new(0),
            "past tolerance that precedes genesis should return genesis slot"
        );

        // Set clock to part-way through the 1st slot.
        *clock.current_time.write() = Duration::from_millis(11_200);
        assert_eq!(
            clock
                .now_with_future_tolerance(Duration::from_secs(0))
                .unwrap(),
            Slot::new(1),
            "future tolerance of zero should return current slot"
        );
        assert_eq!(
            clock
                .now_with_past_tolerance(Duration::from_secs(0))
                .unwrap(),
            Slot::new(1),
            "past tolerance of zero should return current slot"
        );
        assert_eq!(
            clock
                .now_with_future_tolerance(Duration::from_millis(800))
                .unwrap(),
            Slot::new(2),
            "significant future tolerance should return next slot"
        );
        assert_eq!(
            clock
                .now_with_past_tolerance(Duration::from_millis(201))
                .unwrap(),
            Slot::new(0),
            "significant past tolerance should return previous slot"
        );
    }
}
