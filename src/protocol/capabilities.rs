use super::api::*;
use super::requests::*;

impl ApiKey for ListOffsetsRequest0 { fn api_key() -> u16 { 2 } }
impl ApiKey for MetadataRequest0 { fn api_key() -> u16 { 3 } }
impl ApiKey for ListGroupRequest0 { fn api_key() -> u16 { 16 } }
impl ApiKey for ApiVersionsRequest0 { fn api_key() -> u16 { 18 } }
impl ApiKey for ApiVersionsRequest1 { fn api_key() -> u16 { 18 } }

impl ApiVersion for ListOffsetsRequest0 { fn api_version() -> u16 { 0 } }
impl ApiVersion for MetadataRequest0 { fn api_version() -> u16 { 0 } }
impl ApiVersion for ListGroupRequest0 { fn api_version() -> u16 { 0 } }
// one request struct, 2 response versions
impl ApiVersion for ApiVersionsRequest0 { fn api_version() -> u16 { 0 } }
impl ApiVersion for ApiVersionsRequest1 { fn api_version() -> u16 { 1 } }
