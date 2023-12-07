//! The network where the axon resides.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::L1ChainId;

/// Network to be used for the axon client.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Network {
    /// Cortex Mainnet.
    Mainnet,
    /// Self-hosted Cortex network.
    Localhost,
    /// Unknown network type.
    Unknown,
    /// Test network for testkit purposes
    Test,
}

impl FromStr for Network {
    type Err = String;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(match string {
            "mainnet" => Self::Mainnet,
            "localhost" => Self::Localhost,
            "test" => Self::Test,
            another => return Err(another.to_owned()),
        })
    }
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mainnet => write!(f, "mainnet"),
            Self::Localhost => write!(f, "localhost"),
            Self::Unknown => write!(f, "unknown"),
            Self::Test => write!(f, "test"),
        }
    }
}

impl Network {
    /// Returns the network chain ID on the Cortex side.
    pub fn from_chain_id(chain_id: L1ChainId) -> Self {
        match *chain_id {
            21 => Self::Mainnet,
            9 => Self::Localhost,
            _ => Self::Unknown,
        }
    }

    /// Returns the network chain ID on the Ethereum side.
    pub fn chain_id(self) -> L1ChainId {
        match self {
            Self::Mainnet => L1ChainId(21),
            Self::Localhost => L1ChainId(9),
            Self::Unknown => panic!("Unknown chain ID"),
            Self::Test => panic!("Test chain ID"),
        }
    }
}
