use alloy_primitives::B256;
use sha2::{Digest, Sha256};

use crate::hasher::Hasher;

#[derive(Debug, Default, Clone, Copy)]
pub struct Sha256Hasher;

impl Hasher for Sha256Hasher {
    type Hash = B256;

    fn hash_bytes(&self, value: &[u8]) -> Self::Hash {
        let mut sha256 = Sha256::new();
        sha256.update(value);
        B256::new(sha256.finalize().into())
    }

    fn compress(&self, lhs: &Self::Hash, rhs: &Self::Hash) -> Self::Hash {
        let mut hasher = Sha256::new();
        hasher.update(lhs.as_slice());
        hasher.update(rhs.as_slice());
        B256::new(hasher.finalize().into())
    }
}
