use crate::*;
use serde::{Deserialize, Serialize};

#[derive(arbitrary::Arbitrary, Debug, PartialEq, Clone, Copy, Default, Serialize, Deserialize)]
pub struct InclusionListDuty {
    /// The slot during which the validator must produce an inclusion list.
    pub slot: Slot,
}
