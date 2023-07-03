use std::{
    io::{Read, Seek, Write},
    ops::BitOrAssign,
};

use cbor_data::codec::{ReadCbor, WriteCbor};
use libipld::{
    cbor::DagCborCodec,
    prelude::{Decode, Encode},
};

use crate::error::Result;

#[derive(Clone, Copy, Debug, PartialEq, ReadCbor, WriteCbor)]
pub struct Bitmap(u64);

impl Bitmap {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn set(&mut self, index: usize, value: bool) {
        let shifted = 1 << (63 - index);
        if value {
            self.0 |= shifted;
        } else {
            let mask = &0xffffffffffffffff_u64;
            self.0 &= mask - shifted;
        }
    }

    pub(crate) fn get(&self, index: usize) -> bool {
        let mask = 1 << (63 - index);
        self.0 & mask > 0
    }
}

impl BitOrAssign for Bitmap {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 = self.0 | rhs.0
    }
}

impl Encode<DagCborCodec> for Bitmap {
    fn encode<W: Write>(&self, c: DagCborCodec, w: &mut W) -> Result<()> {
        self.0.encode(c, w)
    }
}

impl Decode<DagCborCodec> for Bitmap {
    fn decode<R: Read + Seek>(c: DagCborCodec, r: &mut R) -> Result<Self> {
        Ok(Self(u64::decode(c, r)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() {
        assert_eq!(Bitmap::new().0, 0);
    }

    #[test]
    fn set() {
        let mut bm = Bitmap::new();
        bm.set(0, true);
        assert_eq!(
            bm.0,
            0b1000000000000000000000000000000000000000000000000000000000000000
        );
        bm.set(4, true);
        assert_eq!(
            bm.0,
            0b1000100000000000000000000000000000000000000000000000000000000000
        );
        bm.set(63, true);
        assert_eq!(
            bm.0,
            0b1000100000000000000000000000000000000000000000000000000000000001
        );

        bm.set(0, false);
        assert_eq!(
            bm.0,
            0b0000100000000000000000000000000000000000000000000000000000000001
        );
        bm.set(4, false);
        assert_eq!(
            bm.0,
            0b0000000000000000000000000000000000000000000000000000000000000001
        );
        bm.set(63, false);
        assert_eq!(
            bm.0,
            0b0000000000000000000000000000000000000000000000000000000000000000
        );
    }

    #[test]
    fn get() {
        let bm = Bitmap(0b1101101101101101101100000000000000000000000000000000000000000000);
        assert!(bm.get(0));
        assert!(bm.get(1));
        assert!(!bm.get(2));
        assert!(bm.get(3));
        assert!(bm.get(4));
        assert!(!bm.get(5));
        assert!(bm.get(6));
        assert!(bm.get(7));
        assert!(!bm.get(8));
        assert!(bm.get(9));
        assert!(bm.get(10));
    }
}
