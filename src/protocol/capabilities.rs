use super::api::*;
use super::requests::*;

/*impl ApiKey for ProduceRequest0 {
    fn api_key() -> u16 {
        0
    }
}*/

impl HasApiKey for FetchRequest0 {
    fn api_key() -> ApiKey { ApiKey::Fetch }
}

impl HasApiKey for ListOffsetsRequest0 {
    fn api_key() -> ApiKey { ApiKey::ListOffsets }
}
impl HasApiKey for MetadataRequest0 {
    fn api_key() -> ApiKey { ApiKey::Metadata }
}
impl HasApiKey for ListGroupRequest0 {
    fn api_key() -> ApiKey { ApiKey::ListGroup }
}
impl HasApiKey for ApiVersionsRequest0 {
    fn api_key() -> ApiKey { ApiKey::ApiVersions }
}
impl HasApiKey for ApiVersionsRequest1 {
    fn api_key() -> ApiKey { ApiKey::ApiVersions }
}

impl HasApiVersion for FetchRequest0 {
    fn api_version() -> u16 { 5 }
}
impl HasApiVersion for ListOffsetsRequest0 {
    fn api_version() -> u16 {
        0
    }
}
impl HasApiVersion for MetadataRequest0 {
    fn api_version() -> u16 {
        0
    }
}
impl HasApiVersion for ListGroupRequest0 {
    fn api_version() -> u16 {
        0
    }
}
// one request struct, 2 response versions
impl HasApiVersion for ApiVersionsRequest0 {
    fn api_version() -> u16 {
        0
    }
}
impl HasApiVersion for ApiVersionsRequest1 {
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
