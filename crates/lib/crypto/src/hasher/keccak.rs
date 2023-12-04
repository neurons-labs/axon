use axon_basic_types::{web3::utils::keccak256, B256};

use crate::hasher::Hasher;

#[derive(Default, Clone, Debug)]
pub struct KeccakHasher;

impl Hasher for KeccakHasher {
    type Hash = B256;

    fn hash_bytes(&self, value: &[u8]) -> Self::Hash {
        keccak256(value)
    }

    fn compress(&self, lhs: &Self::Hash, rhs: &Self::Hash) -> Self::Hash {
        let mut bytes = [0_u8; 64];
        bytes[..32].copy_from_slice(&lhs.0);
        bytes[32..].copy_from_slice(&rhs.0);
        keccak256(bytes)
    }
}
