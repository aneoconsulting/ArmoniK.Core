// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Acknowledgement token: identifies one distribution of one message.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Token {
    pub epoch: u32,
    pub partition: u16,
    pub slot: u32,
    pub generation: u32,
}

impl Token {
    pub fn encode(&self) -> String {
        let mut b = [0u8; 16];
        b[0] = VERSION;
        b[2..4].copy_from_slice(&self.partition.to_le_bytes());
        b[4..8].copy_from_slice(&self.epoch.to_le_bytes());
        b[8..12].copy_from_slice(&self.slot.to_le_bytes());
        b[12..16].copy_from_slice(&self.generation.to_le_bytes());
        URL_SAFE_NO_PAD.encode(b)
    }

    /// `None` when the token is unreadable (the only case reported as an error).
    pub fn decode(s: &str) -> Option<Token> {
        let raw = URL_SAFE_NO_PAD.decode(s).ok()?;
        let b: [u8; 16] = raw.try_into().ok()?;
        if b[0] != VERSION || b[1] != 0 {
            return None;
        }
        let u32_at = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
        Some(Token {
            partition: u16::from_le_bytes([b[2], b[3]]),
            epoch: u32_at(4),
            slot: u32_at(8),
            generation: u32_at(12),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip(epoch: u32, partition: u16, slot: u32, generation: u32) {
            let t = Token { epoch, partition, slot, generation };
            prop_assert_eq!(Token::decode(&t.encode()), Some(t));
        }

        #[test]
        fn garbage_never_panics(s in ".*") {
            let _ = Token::decode(&s);
        }
    }
}
