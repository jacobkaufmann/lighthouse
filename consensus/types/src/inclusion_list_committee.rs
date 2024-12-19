use crate::*;

pub type InclusionListCommittee<E> = FixedVector<u64, <E as EthSpec>::InclusionListCommitteeSize>;
