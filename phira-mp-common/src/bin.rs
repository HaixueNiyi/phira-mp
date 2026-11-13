use std::{collections::HashMap, hash::Hash};

use anyhow::{Result, anyhow, bail};
use byteorder::{ByteOrder, LittleEndian as LE};
use chrono::{DateTime, TimeZone, Utc};
use tap::TapFallible;
use uuid::Uuid;

pub trait BinaryData: Sized {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self>;
    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()>;
}

pub struct BinaryReader<'a>(&'a [u8], usize);

impl<'a> BinaryReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self(data, 0)
    }

    pub fn remaining(&self) -> usize {
        self.0.len().saturating_sub(self.1)
    }

    pub fn array<T: BinaryData>(&mut self) -> Result<Vec<T>> {
        let count = usize::try_from(self.uleb()?).map_err(|_| anyhow!("array too large"))?;
        let mut res = Vec::with_capacity(count.min(self.remaining()).min(1024));
        for idx in 0..count {
            let before = self.1;
            res.push(self.read()?);
            if idx == 0 && self.1 == before && count > 1024 {
                bail!("zero-sized array count too large");
            }
        }
        Ok(res)
    }

    pub fn byte(&mut self) -> Result<u8> {
        self.0
            .get(self.1)
            .ok_or_else(|| anyhow!("unexpected EOF"))
            .tap_ok(|_| self.1 += 1)
            .copied()
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .1
            .checked_add(n)
            .ok_or_else(|| anyhow!("unexpected EOF"))?;
        self.0
            .get(self.1..end)
            .ok_or_else(|| anyhow!("unexpected EOF"))
            .tap_ok(|_| self.1 = end)
    }

    pub fn read<T: BinaryData>(&mut self) -> Result<T> {
        T::read_binary(self)
    }

    pub fn uleb(&mut self) -> Result<u64> {
        let mut result = 0u64;
        let mut shift = 0u32;
        for _ in 0..10 {
            let byte = self.read::<u8>()?;
            if shift == 63 && byte > 1 {
                bail!("uleb overflow");
            }
            result |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
        bail!("uleb overflow");
    }
}

pub struct BinaryWriter<'a>(&'a mut Vec<u8>);

impl<'a> BinaryWriter<'a> {
    pub fn new(vec: &'a mut Vec<u8>) -> Self {
        Self(vec)
    }

    pub fn array<T: BinaryData>(&mut self, v: &[T]) -> Result<()> {
        self.uleb(v.len() as _)?;
        for element in v {
            element.write_binary(self)?;
        }
        Ok(())
    }

    #[inline]
    pub fn write<T: BinaryData>(&mut self, v: &T) -> Result<()> {
        v.write_binary(self)
    }

    #[inline]
    pub fn write_val<T: BinaryData>(&mut self, v: T) -> Result<()> {
        v.write_binary(self)
    }

    pub fn uleb(&mut self, mut v: u64) -> Result<()> {
        loop {
            let mut byte = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                byte |= 0x80;
            }
            self.write_val(byte)?;
            if v == 0 {
                break Ok(());
            }
        }
    }
}

impl BinaryData for () {
    fn read_binary(_r: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(())
    }

    fn write_binary(&self, _w: &mut BinaryWriter<'_>) -> Result<()> {
        Ok(())
    }
}

impl BinaryData for i8 {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(r.byte()? as i8)
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.0.push(*self as u8);
        Ok(())
    }
}

impl BinaryData for u8 {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        r.byte()
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.0.push(*self);
        Ok(())
    }
}

impl BinaryData for u16 {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(LE::read_u16(r.take(2)?))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.0.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl BinaryData for u32 {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(LE::read_u32(r.take(4)?))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.0.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl BinaryData for u64 {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(LE::read_u64(r.take(8)?))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.0.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl BinaryData for i32 {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(LE::read_i32(r.take(4)?))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.0.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl BinaryData for i64 {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(LE::read_i64(r.take(8)?))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.0.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl BinaryData for bool {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        match r.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            b => bail!("invalid bool: {b}"),
        }
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.write_val(*self as u8)
    }
}

impl BinaryData for f32 {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(LE::read_f32(r.take(4)?))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.0.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl BinaryData for String {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        let len = usize::try_from(r.uleb()?).map_err(|_| anyhow!("string too large"))?;
        Ok(std::str::from_utf8(r.take(len)?)
            .map_err(|_| anyhow!("invalid utf-8"))?
            .to_owned())
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.uleb(self.len() as _)?;
        w.0.extend_from_slice(self.as_bytes());
        Ok(())
    }
}

impl<A: BinaryData, B: BinaryData> BinaryData for (A, B) {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        Ok((r.read()?, r.read()?))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.write(&self.0)?;
        w.write(&self.1)?;
        Ok(())
    }
}

impl<T: BinaryData> BinaryData for Option<T> {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(if r.read::<bool>()? {
            Some(r.read()?)
        } else {
            None
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        match self {
            Some(val) => {
                w.write_val(true)?;
                w.write(val)?;
            }
            None => {
                w.write_val(false)?;
            }
        }
        Ok(())
    }
}

impl<A: BinaryData, B: BinaryData> BinaryData for Result<A, B> {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        Ok(if r.read::<bool>()? {
            Ok(r.read()?)
        } else {
            Err(r.read()?)
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        match self {
            Ok(val) => {
                w.write_val(true)?;
                w.write(val)?;
            }
            Err(err) => {
                w.write_val(false)?;
                w.write(err)?;
            }
        }
        Ok(())
    }
}

impl<T: BinaryData> BinaryData for Vec<T> {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        r.array()
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.array(self)
    }
}

impl<K: BinaryData + Eq + Hash, V: BinaryData> BinaryData for HashMap<K, V> {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        let count = usize::try_from(r.uleb()?).map_err(|_| anyhow!("map too large"))?;
        let mut res = HashMap::with_capacity(count.min(r.remaining()).min(1024));
        for idx in 0..count {
            let before = r.remaining();
            let (k, v) = r.read::<(K, V)>()?;
            if idx == 0 && r.remaining() == before && count > 1024 {
                bail!("zero-sized map count too large");
            }
            res.insert(k, v);
        }
        Ok(res)
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.uleb(self.len() as _)?;
        for (k, v) in self {
            k.write_binary(w)?;
            v.write_binary(w)?;
        }
        Ok(())
    }
}

impl BinaryData for Uuid {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        let low = r.read()?;
        let high = r.read()?;
        Ok(Self::from_u64_pair(high, low))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        let (high, low) = self.as_u64_pair();
        w.write_val(low)?;
        w.write_val(high)?;
        Ok(())
    }
}

impl BinaryData for DateTime<Utc> {
    fn read_binary(r: &mut BinaryReader<'_>) -> Result<Self> {
        Utc.timestamp_millis_opt(r.read::<i64>()?)
            .single()
            .ok_or_else(|| anyhow!("invalid timestamp"))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> Result<()> {
        w.write_val(self.timestamp_millis())
    }
}
