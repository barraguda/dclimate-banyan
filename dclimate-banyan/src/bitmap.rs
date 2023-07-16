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

    pub fn get(&self, index: usize) -> bool {
        let mask = 1 << (63 - index);
        self.0 & mask > 0
    }

    pub fn rank(&self, index: usize) -> u32 {
        if index == 0 {
            return 0;
        }
        let shifted = self.0 >> (64 - index);

        shifted.count_ones()
    }
}

impl From<u64> for Bitmap {
    fn from(value: u64) -> Self {
        Bitmap(value)
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

    #[test]
    fn rank() {
        let bm = Bitmap(0b1101101101101101101100000000000000000000000000000000000000000000);
        assert_eq!(bm.rank(0), 0);
        assert_eq!(bm.rank(1), 1);
        assert_eq!(bm.rank(2), 2);
        assert_eq!(bm.rank(3), 2);
        assert_eq!(bm.rank(4), 3);
        assert_eq!(bm.rank(5), 4);
        assert_eq!(bm.rank(6), 4);
        assert_eq!(bm.rank(7), 5);
        assert_eq!(bm.rank(8), 6);
        assert_eq!(bm.rank(9), 6);
        assert_eq!(bm.rank(10), 7);
        assert_eq!(bm.rank(64), 14);
    }
}
