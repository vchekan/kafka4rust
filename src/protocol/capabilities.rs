use super::api::*;
use super::requests::*;

impl ApiKey for ProduceRequest0 {
    fn api_key() -> u16 {
        0
    }
}
impl ApiKey for ListOffsetsRequest0 {
    fn api_key() -> u16 {
        2
    }
}
impl ApiKey for MetadataRequest0 {
    fn api_key() -> u16 {
        3
    }
}
impl ApiKey for ListGroupRequest0 {
    fn api_key() -> u16 {
        16
    }
}
impl ApiKey for ApiVersionsRequest0 {
    fn api_key() -> u16 {
        18
    }
}
impl ApiKey for ApiVersionsRequest1 {
    fn api_key() -> u16 {
        18
    }
}

impl ApiVersion for ProduceRequest0 {
    fn api_version() -> u16 {
        0
    }
}
impl ApiVersion for ListOffsetsRequest0 {
    fn api_version() -> u16 {
        0
    }
}
impl ApiVersion for MetadataRequest0 {
    fn api_version() -> u16 {
        0
    }
}
impl ApiVersion for ListGroupRequest0 {
    fn api_version() -> u16 {
        0
    }
}
// one request struct, 2 response versions
impl ApiVersion for ApiVersionsRequest0 {
    fn api_version() -> u16 {
        0
    }
}
impl ApiVersion for ApiVersionsRequest1 {
    fn api_version() -> u16 {
        1
    }
}

/// (api_key, min_version, max_version)
pub(crate) fn supported_versions() -> Vec<(i16, i16, i16)> {
    vec![
        (2, 0, 0), // 2 ListOffset
        (3, 0, 0), // 3 MetadataRequest
                   //(18, 0, 1), // ApiVersionsRequest
    ]
}
