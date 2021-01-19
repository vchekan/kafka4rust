/*
assert_eq!(Hash32A::hash(b"hello"), 259931098);
/// assert_eq!(Hash32A::hash_with_seed(b"hello", 123), 509510832);
/// assert_eq!(Hash32A::hash(b"helloworld"), 403945221);
*/

//uint32_t MurmurHash2A ( const void * key, int len, uint32_t seed )

/// Kafka's reimplementation of murmur32a.
/// Not the fastest implementation, because works with individual bytes, but should
/// be ok for intendend use: keys hash calculation.
pub fn hash32(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747b28c;
    const M: u32 = 0x5bd1e995;
    const R: u32 = 24;
    let len = data.len();

    let mut h = SEED ^ len as u32;

    for i in 0..len / 4 {
        let i4 = i * 4;
        let mut k = data[i4] as u32 + (data[i4 + 1] as u32)
            << 8 + (data[i4 + 2] as u32)
            << 16 + (data[i4 + 3] as u32)
            << 24;

        k *= M;
        k ^= k >> R;
        k *= M;
        h *= M;
        h ^= k;
    }

    // calculate remaining
    let remaining = len % 4;
    if remaining == 3 {
        h ^= (data[(len & !3) + 2] as u32) << 16;
    }
    if remaining >= 2 {
        h ^= (data[(len & !3) + 1] as u32) << 8;
    }
    if remaining >= 1 {
        h ^= data[(len & !3)] as u32;
        h *= M;
    }

    h ^= h >> 13;
    h *= M;
    h ^= h >> 15;

    h
}
