mod manual_slot_clock;
mod metrics;
mod system_time_slot_clock;

use std::time::Duration;

pub use crate::manual_slot_clock::ManualSlotClock as TestingSlotClock;
pub use crate::manual_slot_clock::ManualSlotClock;
pub use crate::system_time_slot_clock::SystemTimeSlotClock;
pub use metrics::scrape_for_metrics;
use types::consts::bellatrix::INTERVALS_PER_SLOT;
pub use types::Slot;
use types::{ChainSpec, Epoch};

/// A schedule of the slot duration by epoch.
#[derive(Clone, Copy, Debug)]
pub struct SlotDurationSchedule {
    /// The initial slot duration.
    ///
    /// If `breakpoint` is `None`, then `initial` is the slot duration for all slots since genesis.
    initial: Duration,
    /// An optional breakpoint, such that the slot duration for all slots since the breakpoint epoch
    /// have the corresponding duration.
    breakpoint: Option<(Epoch, Duration)>,
}

impl SlotDurationSchedule {
    /// Creates a new slot duration schedule.
    ///
    /// NOTE: This method is intended for testing purposes only. For non-testing purposes, you
    /// should create a `SlotDurationSchedule` from a `ChainSpec`.
    pub const fn new(initial: Duration, breakpoint: Option<(Epoch, Duration)>) -> Self {
        Self {
            initial,
            breakpoint,
        }
    }

    /// Returns the duration of a slot within `epoch`.
    pub fn slot_duration(&self, epoch: Epoch) -> Duration {
        match self.breakpoint {
            Some((e, d)) => {
                if epoch >= e {
                    d
                } else {
                    self.initial
                }
            }
            None => self.initial,
        }
    }

    /// Returns the total duration of all slots from genesis up to but not including `slot`.
    pub fn duration_from_genesis_to_slot(
        &self,
        genesis_slot: Slot,
        slots_per_epoch: u64,
        slot: Slot,
    ) -> Option<Duration> {
        if slot < genesis_slot {
            return None;
        }

        let slot = slot.as_u64();
        let genesis_slot = genesis_slot.as_u64();

        // if there is no breakpoint, then all slots are before the breakpoint
        let mut slots_before_breakpoint = slot
            .checked_sub(genesis_slot)
            .expect("Control flow ensures slot is greater than or equal to genesis slot");

        let mut total = Duration::ZERO;
        if let Some((e, d)) = self.breakpoint {
            // SAFETY: assume `genesis_slot` is less than or equal to breakpoint start slot
            let start_slot = e.start_slot(slots_per_epoch).as_u64();
            slots_before_breakpoint = start_slot - genesis_slot;

            let slots_since_breakpoint = slot.saturating_sub(start_slot);
            let slots_since_breakpoint: u32 = slots_since_breakpoint
                .try_into()
                .expect("slot must fit within a u32");
            total += slots_since_breakpoint * d;
        }
        let slots_before_breakpoint: u32 = slots_before_breakpoint
            .try_into()
            .expect("slot must fit within a u32");
        total += slots_before_breakpoint * self.initial;

        Some(total)
    }

    /// Returns the slot of the given duration since the UNIX epoch.
    pub fn slot_of(
        &self,
        genesis_slot: Slot,
        genesis_duration: Duration,
        slots_per_epoch: u64,
        now: Duration,
    ) -> Option<Slot> {
        let genesis = genesis_duration;

        if now < genesis {
            return None;
        }

        let since_genesis = now
            .checked_sub(genesis)
            .expect("Control flow ensures now is greater than or equal to genesis");

        let mut breakpoint = Duration::MAX;
        let mut breakpoint_slot = Slot::max_value();
        let mut slot_duration_since_breakpoint = Duration::ZERO;
        if let Some((e, d)) = self.breakpoint {
            breakpoint_slot = e.start_slot(slots_per_epoch);

            // normalize breakpoint slot to compute duration since genesis. if `genesis_slot` is
            // greater than zero, then we must not account for slots `[0, genesis_slot)`.
            //
            // SAFETY: assume the start slot of `self.breakpoint` is greater than or equal to
            // `genesis_slot`
            let slots_before_breakpoint = u64::from(breakpoint_slot - genesis_slot);
            let slots_before_breakpoint: u32 = slots_before_breakpoint
                .try_into()
                .expect("slot must fit within a u32");

            breakpoint = genesis + (self.initial * slots_before_breakpoint);
            slot_duration_since_breakpoint = d;
        }

        let slot = if now < breakpoint {
            Slot::from((since_genesis.as_millis() / self.initial.as_millis()) as u64)
        } else {
            let since_breakpoint = now
                .checked_sub(breakpoint)
                .expect("Control flow ensures now is greater than or equal to breakpoint");
            Slot::from(
                (since_breakpoint.as_millis() / slot_duration_since_breakpoint.as_millis()) as u64,
            ) + breakpoint_slot
        };

        Some(slot + genesis_slot)
    }
}

impl From<&ChainSpec> for SlotDurationSchedule {
    fn from(spec: &ChainSpec) -> Self {
        let initial = Duration::from_secs(spec.seconds_per_slot);
        let breakpoint = match spec.electra_fork_epoch {
            Some(epoch) => {
                if epoch == Epoch::max_value() {
                    None
                } else {
                    Some((epoch, Duration::from_secs(spec.seconds_per_slot_electra)))
                }
            }
            None => None,
        };
        Self {
            initial,
            breakpoint,
        }
    }
}

/// A clock that reports the current slot.
///
/// The clock is not required to be monotonically increasing and may go backwards.
pub trait SlotClock: Send + Sync + Sized + Clone {
    /// Creates a new slot clock where the first slot is `genesis_slot`, genesis occurred
    /// `genesis_duration` after the `UNIX_EPOCH` and each slot is `slot_duration` apart.
    fn new(
        genesis_slot: Slot,
        genesis_duration: Duration,
        slots_per_epoch: u64,
        slot_duration_schedule: SlotDurationSchedule,
    ) -> Self;

    /// Returns the slot and epoch pair at this present time.
    fn now(&self) -> Option<Slot>;

    /// Returns the slot and epoch pair at this present time if genesis has happened. Otherwise,
    /// returns the genesis slot and epoch. Returns `None` if there is an error reading the clock.
    fn now_or_genesis(&self) -> Option<Slot> {
        if self.is_prior_to_genesis()? {
            Some(self.genesis_slot())
        } else {
            self.now()
        }
    }

    /// Indicates if the current time is prior to genesis time.
    ///
    /// Returns `None` if the system clock cannot be read.
    fn is_prior_to_genesis(&self) -> Option<bool>;

    /// Returns the present time as a duration since the UNIX epoch.
    ///
    /// Returns `None` if the present time is before the UNIX epoch (unlikely).
    fn now_duration(&self) -> Option<Duration>;

    /// Returns the slot of the given duration since the UNIX epoch.
    fn slot_of(&self, now: Duration) -> Option<Slot>;

    /// Returns the duration between slots within `epoch`.
    fn slot_duration(&self, epoch: Epoch) -> Duration;

    /// Returns the slot duration schedule for the slot clock.
    fn slot_duration_schedule(&self) -> SlotDurationSchedule;

    /// Returns the number of slots per epoch for the slot clock.
    fn slots_per_epoch(&self) -> u64;

    /// Returns the duration from now until `slot`.
    fn duration_to_slot(&self, slot: Slot) -> Option<Duration>;

    /// Returns the duration until the next slot.
    fn duration_to_next_slot(&self) -> Option<Duration>;

    /// Returns the duration until the first slot of the next epoch.
    fn duration_to_next_epoch(&self) -> Option<Duration>;

    /// Returns the start time of the slot, as a duration since `UNIX_EPOCH`.
    fn start_of(&self, slot: Slot) -> Option<Duration>;

    /// Returns the first slot to be returned at the genesis time.
    fn genesis_slot(&self) -> Slot;

    /// Returns the `Duration` from `UNIX_EPOCH` to the genesis time.
    fn genesis_duration(&self) -> Duration;

    /// Returns the slot if the internal clock were advanced by `duration`.
    fn now_with_future_tolerance(&self, tolerance: Duration) -> Option<Slot> {
        self.slot_of(self.now_duration()?.checked_add(tolerance)?)
    }

    /// Returns the slot if the internal clock were reversed by `duration`.
    fn now_with_past_tolerance(&self, tolerance: Duration) -> Option<Slot> {
        self.slot_of(self.now_duration()?.checked_sub(tolerance)?)
            .or_else(|| Some(self.genesis_slot()))
    }

    /// Returns the delay between the start of the slot and when unaggregated attestations should be
    /// produced.
    fn unagg_attestation_production_delay(&self, epoch: Epoch) -> Duration {
        self.slot_duration(epoch) / INTERVALS_PER_SLOT as u32
    }

    /// Returns the delay between the start of the slot and when sync committee messages should be
    /// produced.
    fn sync_committee_message_production_delay(&self, epoch: Epoch) -> Duration {
        self.slot_duration(epoch) / INTERVALS_PER_SLOT as u32
    }

    /// Returns the delay between the start of the slot and when aggregated attestations should be
    /// produced.
    fn agg_attestation_production_delay(&self, epoch: Epoch) -> Duration {
        self.slot_duration(epoch) * 2 / INTERVALS_PER_SLOT as u32
    }

    /// Returns the delay between the start of the slot and when partially aggregated `SyncCommitteeContribution` should be
    /// produced.
    fn sync_committee_contribution_production_delay(&self, epoch: Epoch) -> Duration {
        self.slot_duration(epoch) * 2 / INTERVALS_PER_SLOT as u32
    }

    /// Returns the `Duration` since the start of the current `Slot` at seconds precision. Useful in determining whether to apply proposer boosts.
    fn seconds_from_current_slot_start(&self) -> Option<Duration> {
        // TODO
        let slot = self.now()?;
        let epoch = slot.epoch(self.slots_per_epoch());
        self.now_duration()
            .and_then(|now| now.checked_sub(self.genesis_duration()))
            .map(|duration_into_slot| {
                Duration::from_secs(
                    duration_into_slot.as_secs() % self.slot_duration(epoch).as_secs(),
                )
            })
    }

    /// Returns the `Duration` since the start of the current `Slot` at milliseconds precision.
    fn millis_from_current_slot_start(&self) -> Option<Duration> {
        // TODO
        let slot = self.now()?;
        let epoch = slot.epoch(self.slots_per_epoch());
        self.now_duration()
            .and_then(|now| now.checked_sub(self.genesis_duration()))
            .map(|duration_into_slot| {
                Duration::from_millis(
                    (duration_into_slot.as_millis() % self.slot_duration(epoch).as_millis()) as u64,
                )
            })
    }

    /// Produces a *new* slot clock with the same configuration of `self`, except that clock is
    /// "frozen" at the `freeze_at` time.
    ///
    /// This is useful for observing the slot clock at arbitrary fixed points in time.
    fn freeze_at(&self, freeze_at: Duration) -> ManualSlotClock {
        let slot_clock = ManualSlotClock::new(
            self.genesis_slot(),
            self.genesis_duration(),
            self.slots_per_epoch(),
            self.slot_duration_schedule(),
        );
        slot_clock.set_current_time(freeze_at);
        slot_clock
    }

    /// Returns the delay between the start of the slot and when a request for block components
    /// missed over gossip in the current slot should be made via RPC.
    ///
    /// Currently set equal to 1/2 of the `unagg_attestation_production_delay`, but this may be
    /// changed in the future.
    fn single_lookup_delay(&self, epoch: Epoch) -> Duration {
        self.unagg_attestation_production_delay(epoch) / 2
    }
}
