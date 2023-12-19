use axon_primitives::{web3::utils::keccak256, B256, U256};

pub const fn ceil_div(a: u64, b: u64) -> u64 {
    if a == 0 { a } else { (a - 1) / b + 1 }
}

pub fn ceil_div_u256(a: U256, b: U256) -> U256 {
    (a + b - U256::from(1)) / b
}

pub fn concat_and_hash(hash1: B256, hash2: B256) -> B256 {
    let mut bytes = [0_u8; 64];
    bytes[..32].copy_from_slice(&hash1.0);
    bytes[32..].copy_from_slice(&hash2.0);
    keccak256(bytes)
}

pub fn expand_memory_contents(packed: &[(usize, U256)], memory_size_bytes: usize) -> Vec<u8> {
    let mut result: Vec<u8> = vec![0; memory_size_bytes];

    for (offset, value) in packed {
        let value_bytes: [u8; 32] = value.to_be_bytes();
        result[(offset * 32)..((offset + 1) * 32)].copy_from_slice(&value_bytes);
    }

    result
}

pub const U256ONE: U256 = U256::from_limbs([1, 0, 0, 0]);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ceil_div_u64_max() {
        assert_eq!(0, ceil_div(u64::MIN, u64::MAX));
        assert_eq!(1, ceil_div(u64::MAX, u64::MAX));
    }

    #[test]
    fn test_ceil_div_roundup_required() {
        assert_eq!(3, ceil_div(5, 2));
        assert_eq!(4, ceil_div(10, 3));
        assert_eq!(3, ceil_div(15, 7));
    }

    #[test]
    fn test_ceil_div_no_roundup_required() {
        assert_eq!(2, ceil_div(4, 2));
        assert_eq!(2, ceil_div(6, 3));
        assert_eq!(2, ceil_div(14, 7));
    }
}
