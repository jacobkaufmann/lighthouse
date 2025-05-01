use super::{ManualSlotClock, SlotClock, SlotDurationSchedule};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use types::{Epoch, Slot};

/// Determines the present slot based upon the present system time.
#[derive(Clone)]
pub struct SystemTimeSlotClock {
    clock: ManualSlotClock,
}

impl SlotClock for SystemTimeSlotClock {
    fn new(
        genesis_slot: Slot,
        genesis_duration: Duration,
        slots_per_epoch: u64,
        slot_duration_schedule: SlotDurationSchedule,
    ) -> Self {
        Self {
            clock: ManualSlotClock::new(
                genesis_slot,
                genesis_duration,
                slots_per_epoch,
                slot_duration_schedule,
            ),
        }
    }

    fn now(&self) -> Option<Slot> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
        self.clock.slot_of(now)
    }

    fn is_prior_to_genesis(&self) -> Option<bool> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
        Some(now < *self.clock.genesis_duration())
    }

    fn now_duration(&self) -> Option<Duration> {
        SystemTime::now().duration_since(UNIX_EPOCH).ok()
    }

    fn slot_of(&self, now: Duration) -> Option<Slot> {
        self.clock.slot_of(now)
    }

    fn duration_to_next_slot(&self) -> Option<Duration> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
        self.clock.duration_to_next_slot_from(now)
    }

    fn duration_to_next_epoch(&self) -> Option<Duration> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
        self.clock.duration_to_next_epoch_from(now)
    }

    fn slot_duration(&self, epoch: Epoch) -> Duration {
        self.clock.slot_duration(epoch)
    }

    fn slot_duration_schedule(&self) -> SlotDurationSchedule {
        self.clock.slot_duration_schedule()
    }

    fn slots_per_epoch(&self) -> u64 {
        self.clock.slots_per_epoch()
    }

    fn duration_to_slot(&self, slot: Slot) -> Option<Duration> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
        self.clock.duration_to_slot(slot, now)
    }

    fn start_of(&self, slot: Slot) -> Option<Duration> {
        self.clock.start_of(slot)
    }

    fn genesis_slot(&self) -> Slot {
        self.clock.genesis_slot()
    }

    fn genesis_duration(&self) -> Duration {
        *self.clock.genesis_duration()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SLOTS_PER_EPOCH: u64 = 32;

    /*
     * Note: these tests are using actual system times and could fail if they are executed on a
     * very slow machine.
     */
    #[test]
    fn test_slot_now() {
        let genesis_slot = Slot::new(0);

        let prior_genesis = |milliseconds_prior: u64| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("should get system time")
                - Duration::from_millis(milliseconds_prior)
        };

        let clock = SystemTimeSlotClock::new(
            genesis_slot,
            prior_genesis(0),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(1), None),
        );
        assert_eq!(clock.now(), Some(Slot::new(0)));

        let clock = SystemTimeSlotClock::new(
            genesis_slot,
            prior_genesis(5_000),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(1), None),
        );
        assert_eq!(clock.now(), Some(Slot::new(5)));

        let clock = SystemTimeSlotClock::new(
            genesis_slot,
            prior_genesis(500),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(1), None),
        );
        assert_eq!(clock.now(), Some(Slot::new(0)));
        assert!(clock.duration_to_next_slot().unwrap() <= Duration::from_millis(500));

        let clock = SystemTimeSlotClock::new(
            genesis_slot,
            prior_genesis(1_500),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(1), None),
        );
        assert_eq!(clock.now(), Some(Slot::new(1)));
        assert!(clock.duration_to_next_slot().unwrap() <= Duration::from_millis(500));
    }

    #[test]
    #[should_panic]
    fn zero_seconds() {
        SystemTimeSlotClock::new(
            Slot::new(0),
            Duration::from_secs(0),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(0), None),
        );
    }

    #[test]
    #[should_panic]
    fn zero_millis() {
        SystemTimeSlotClock::new(
            Slot::new(0),
            Duration::from_secs(0),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_secs(0), None),
        );
    }

    #[test]
    #[should_panic]
    fn less_than_one_millis() {
        SystemTimeSlotClock::new(
            Slot::new(0),
            Duration::from_secs(0),
            SLOTS_PER_EPOCH,
            SlotDurationSchedule::new(Duration::from_nanos(999), None),
        );
    }
}
