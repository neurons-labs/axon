use axon_primitives::{Address, B256, U256};
use bigdecimal::BigDecimal;
use num::{
    bigint::ToBigInt,
    rational::Ratio,
    traits::{sign::Signed, Pow},
    BigUint,
};

pub fn u256_to_big_decimal(value: U256) -> BigDecimal {
    let ratio = Ratio::new_raw(u256_to_biguint(value), BigUint::from(1u8));
    ratio_to_big_decimal(&ratio, 80)
}

pub fn ratio_to_big_decimal(num: &Ratio<BigUint>, precision: usize) -> BigDecimal {
    let bigint = round_precision_raw_no_div(num, precision)
        .to_bigint()
        .unwrap();
    BigDecimal::new(bigint, precision as i64)
}

pub fn ratio_to_big_decimal_normalized(
    num: &Ratio<BigUint>,
    precision: usize,
    min_precision: usize,
) -> BigDecimal {
    let normalized = ratio_to_big_decimal(num, precision).normalized();
    let min_scaled = normalized.with_scale(min_precision as i64);
    normalized.max(min_scaled)
}

pub fn big_decimal_to_ratio(num: &BigDecimal) -> Result<Ratio<BigUint>, anyhow::Error> {
    let (big_int, exp) = num.as_bigint_and_exponent();
    anyhow::ensure!(!big_int.is_negative(), "BigDecimal should be unsigned");
    let big_uint = big_int.to_biguint().unwrap();
    let ten_pow = BigUint::from(10_u32).pow(exp as u128);
    Ok(Ratio::new(big_uint, ten_pow))
}

fn round_precision_raw_no_div(num: &Ratio<BigUint>, precision: usize) -> BigUint {
    let ten_pow = BigUint::from(10u32).pow(precision);
    (num * ten_pow).round().to_integer()
}

/// Converts `U256` into the corresponding `BigUint` value.
fn u256_to_biguint(value: U256) -> BigUint {
    let bytes: [u8; 32] = value.to_le_bytes();
    BigUint::from_bytes_le(&bytes)
}

/// Converts `BigUint` value into the corresponding `U256` value.
fn biguint_to_u256(value: BigUint) -> U256 {
    let bytes = value.to_bytes_le();
    U256::from_le_slice(&bytes)
}

/// Converts `BigDecimal` value into the corresponding `U256` value.
pub fn bigdecimal_to_u256(value: BigDecimal) -> U256 {
    let bigint = value.with_scale(0).into_bigint_and_exponent().0;
    biguint_to_u256(bigint.to_biguint().unwrap())
}

fn ensure_chunkable(bytes: &[u8]) {
    assert!(
        bytes.len() % 32 == 0,
        "Bytes must be divisible by 32 to split into chunks"
    );
}

pub fn b256_to_u256(num: B256) -> U256 {
    U256::from_be_slice(num.as_slice())
}

pub fn address_to_b256(address: &Address) -> B256 {
    let mut buffer = [0u8; 32];
    buffer[12..].copy_from_slice(address.as_slice());
    B256::new(buffer)
}

pub fn address_to_u256(address: &Address) -> U256 {
    b256_to_u256(address_to_b256(address))
}

pub fn bytes_to_chunks(bytes: &[u8]) -> Vec<[u8; 32]> {
    ensure_chunkable(bytes);
    bytes
        .chunks(32)
        .map(|el| {
            let mut chunk = [0u8; 32];
            chunk.copy_from_slice(el);
            chunk
        })
        .collect()
}

pub fn be_chunks_to_h256_words(chunks: Vec<[u8; 32]>) -> Vec<B256> {
    chunks.into_iter().map(|el| B256::from_slice(&el)).collect()
}

pub fn bytes_to_be_words(vec: Vec<u8>) -> Vec<U256> {
    ensure_chunkable(&vec);
    vec.chunks(32).map(U256::from_be_slice).collect()
}

pub fn be_words_to_bytes(words: &[U256]) -> Vec<u8> {
    words.iter().flat_map(|w| w.to_be_bytes::<32>()).collect()
}

pub fn u256_to_b256(num: U256) -> B256 {
    let bytes: [u8; 32] = num.to_be_bytes();
    B256::from_slice(&bytes)
}

/// Converts `U256` value into the Address
pub fn u256_to_account_address(value: &U256) -> Address {
    let bytes: [u8; 32] = value.to_be_bytes();
    Address::from_slice(&bytes[12..])
}

/// Converts `B256` value into the Address
pub fn b256_to_account_address(value: &B256) -> Address {
    Address::from_slice(&value.as_slice()[12..])
}

pub fn be_bytes_to_safe_address(bytes: &[u8]) -> Option<Address> {
    if bytes.len() < 20 {
        return None;
    }

    let (zero_bytes, address_bytes) = bytes.split_at(bytes.len() - 20);

    if zero_bytes.iter().any(|b| *b != 0) {
        None
    } else {
        Some(Address::from_slice(address_bytes))
    }
}

/// Converts `b256` value as BE into the u32
pub fn b256_to_u32(value: B256) -> u32 {
    let be_u32_bytes: [u8; 4] = value[28..].try_into().unwrap();
    u32::from_be_bytes(be_u32_bytes)
}

/// Converts u32 into the B256 as BE bytes
pub fn u32_to_b256(value: u32) -> B256 {
    let mut result = [0u8; 32];
    result[28..].copy_from_slice(&value.to_be_bytes());
    B256::new(result)
}

/// Converts `U256` value into bytes array
pub fn u256_to_bytes_be(value: &U256) -> Vec<u8> {
    value.to_be_bytes_vec()
}

pub fn b256_from_low_u64_be(value: u64) -> B256 {
    let v = value.to_be_bytes();
    B256::left_padding_from(&v)
}

pub fn low_u64_of_u256(value: &U256) -> u64 {
    value.as_limbs()[0]
}

pub fn low_u128_of_u256(value: &U256) -> u128 {
    ((value.as_limbs()[1] as u128) << 64) + (value.as_limbs()[0]) as u128
}

pub fn u256_as_u128(value: &U256) -> u128 {
    let limbs = value.as_limbs();
    for limb in limbs.iter().take(4).skip(2) {
        if *limb != 0 {
            panic!("Integer overflow when casting to u128")
        }
    }
    low_u128_of_u256(value)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use num::BigInt;

    use super::*;

    #[test]
    fn test_ratio_to_big_decimal() {
        let ratio = Ratio::from_integer(BigUint::from(0u32));
        let dec = ratio_to_big_decimal(&ratio, 1);
        assert_eq!(dec.to_string(), "0.0");
        let ratio = Ratio::from_integer(BigUint::from(1234u32));
        let dec = ratio_to_big_decimal(&ratio, 7);
        assert_eq!(dec.to_string(), "1234.0000000");
        // 4 divided by 9 is 0.(4).
        let ratio = Ratio::new(BigUint::from(4u32), BigUint::from(9u32));
        let dec = ratio_to_big_decimal(&ratio, 12);
        assert_eq!(dec.to_string(), "0.444444444444");
        // First 7 decimal digits of pi.
        let ratio = Ratio::new(BigUint::from(52163u32), BigUint::from(16604u32));
        let dec = ratio_to_big_decimal(&ratio, 6);
        assert_eq!(dec.to_string(), "3.141592");
    }

    #[test]
    fn test_ratio_to_big_decimal_normalized() {
        let ratio = Ratio::from_integer(BigUint::from(10u32));
        let dec = ratio_to_big_decimal_normalized(&ratio, 100, 2);
        assert_eq!(dec.to_string(), "10.00");

        // First 7 decimal digits of pi.
        let ratio = Ratio::new(BigUint::from(52163u32), BigUint::from(16604u32));
        let dec = ratio_to_big_decimal_normalized(&ratio, 6, 2);
        assert_eq!(dec.to_string(), "3.141592");

        // 4 divided by 9 is 0.(4).
        let ratio = Ratio::new(BigUint::from(4u32), BigUint::from(9u32));
        let dec = ratio_to_big_decimal_normalized(&ratio, 12, 2);
        assert_eq!(dec.to_string(), "0.444444444444");
    }

    #[test]
    fn test_big_decimal_to_ratio() {
        // Expect unsigned number.
        let dec = BigDecimal::from(-1);
        assert!(big_decimal_to_ratio(&dec).is_err());
        let expected = Ratio::from_integer(BigUint::from(0u32));
        let dec = BigDecimal::from(0);
        let ratio = big_decimal_to_ratio(&dec).unwrap();
        assert_eq!(ratio, expected);
        let expected = Ratio::new(BigUint::from(1234567u32), BigUint::from(10000u32));
        let dec = BigDecimal::from_str("123.4567").unwrap();
        let ratio = big_decimal_to_ratio(&dec).unwrap();
        assert_eq!(ratio, expected);
    }

    #[test]
    fn test_bigdecimal_to_u256() {
        let value = BigDecimal::from(100u32);
        let expected = U256::from(100u32);
        assert_eq!(bigdecimal_to_u256(value), expected);

        let value = BigDecimal::new(BigInt::from(100), -2);
        let expected = U256::from(10000u32);
        assert_eq!(bigdecimal_to_u256(value), expected);
    }
}
