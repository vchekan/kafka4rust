//! While params are unsigned, encode it as signed to be compatible with kafka. The only case when
//! we need negative value is -1, which can be a constant.

pub fn zigzag_len(mut d: u64) -> u8 {
    if d == 0 {
        return 1;
    }

    d <<= 1;

    let mut i= 0;
    while d > 0 {
        d >>= 7;
        i += 1;
    }
    i
}

/// Return compressed number and length in bytes. Max len is 9 bytes
pub fn zigzag64(mut n: u64, buf: &mut [u8]) -> &[u8] {
    if n == 0 {
        buf[0] = 0;
        return &buf[..1];
    }

    n <<= 1;

    let mut res = 0;
    let mut i = 0;
    while n != 0 {
        buf[i] = n as u8 & 0b0111_1111 | 0b1000_0000;
        res |= n & 0b0111_1111;
        n >>= 7;
        res <<= 7;
        i += 1;
    };
    buf[i-1] &= 0b0111_1111;
    return &buf[..i];
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_len() {
        assert_eq!(zigzag_len(0b0000_0000), 1);
        assert_eq!(zigzag_len(0b0000_0001), 1);
        assert_eq!(zigzag_len(0b0111_1111), 2);
        assert_eq!(zigzag_len(0b1111_1111), 2);
        // TODO: finish tests
        /*assert_eq!(zigzag_len(0b1000_0000_0111_1111), 3);
        */
    }

    #[test]
    fn test_encoding() {
        let mut buf = vec![0,0,0,0,0,0,0,0];
        assert_eq!(zigzag64(0b0000_0000, &mut buf), vec![0].as_slice());
        assert_eq!(zigzag64(0b0000_0001, &mut buf), vec![2].as_slice());
        assert_eq!(zigzag64(0b0000_0011, &mut buf), vec![6].as_slice());
        /*assert_eq!(zigzag64(0b0111_1111, &mut buf), vec![127].as_slice());
        assert_eq!(zigzag64(0b1111_1111, &mut buf), vec![0b1111_1111, 0b0000_0001].as_slice());
        assert_eq!(zigzag64(300, &mut buf), vec![0b1010_1100, 0b0000_0010].as_slice());
        */

        /*
        let max_vec_encoded = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        assert_eq!(u64::decode_var_vec(&max_vec_encoded).0, u64::max_value());

        for i in 1 as u64..100 {
        assert_eq!(u64::decode_var_vec(&i.encode_var_vec()), (i, 1));
        }
        for i in 16400 as u64..16500 {
            assert_eq!(u64::decode_var_vec(&i.encode_var_vec()), (i, 3));
        }
        */
    }
}