use crate::test_utils::TestRandom;
use crate::{EthSpec, Hash256, Signature, SignedRoot, Slot, Transaction};

use derivative::Derivative;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use ssz_types::VariableList;
use test_random_derive::TestRandom;
use tree_hash::TreeHash;
use tree_hash_derive::TreeHash;

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Encode,
    Decode,
    TreeHash,
    TestRandom,
    Derivative,
    arbitrary::Arbitrary,
)]
#[serde(bound = "E: EthSpec")]
#[arbitrary(bound = "E: EthSpec")]
#[derivative(PartialEq, Eq, Hash(bound = "E: EthSpec"))]
pub struct InclusionList<E: EthSpec> {
    pub slot: Slot,
    #[serde(with = "serde_utils::quoted_u64")]
    pub validator_index: u64,
    pub inclusion_list_committee_root: Hash256,
    pub transactions:
        VariableList<Transaction<E::MaxBytesPerTransaction>, E::MaxTransactionsPerInclusionList>,
}

impl<E: EthSpec> SignedRoot for InclusionList<E> {}

#[derive(
    Debug, Clone, Serialize, Deserialize, Encode, Decode, TreeHash, Derivative, arbitrary::Arbitrary,
)]
#[serde(bound = "E: EthSpec")]
#[arbitrary(bound = "E: EthSpec")]
#[derivative(PartialEq, Eq, Hash(bound = "E: EthSpec"))]
pub struct SignedInclusionList<E: EthSpec> {
    pub message: InclusionList<E>,
    pub signature: Signature,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    ssz_and_tree_hash_tests!(InclusionList<MainnetEthSpec>);
}
