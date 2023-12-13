use alloy_primitives::B256;
use blake2::{Blake2s256, Digest};

use crate::hasher::Hasher;

#[derive(Default, Clone, Debug)]
pub struct Blake2Hasher;

impl Hasher for Blake2Hasher {
    type Hash = B256;

    fn hash_bytes(&self, value: &[u8]) -> Self::Hash {
        let mut hasher = Blake2s256::new();
        hasher.update(value);
        B256::new(hasher.finalize().into())
    }

    fn compress(&self, lhs: &Self::Hash, rhs: &Self::Hash) -> Self::Hash {
        let mut hasher = Blake2s256::new();
        hasher.update(lhs.as_slice());
        hasher.update(rhs.as_slice());
        B256::new(hasher.finalize().into())
    }
}
