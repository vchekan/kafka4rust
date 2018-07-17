use super::api::*;
use super::requests::*;

impl ApiKey for MetadataRequest0 { fn api_key() -> u16 { 3 } }
impl ApiKey for ListGroupRequest0 { fn api_key() -> u16 { 16 } }
impl ApiKey for ApiVersionsRequest { fn api_key() -> u16 { 18 } }

impl ApiVersion for MetadataRequest0 { fn api_version() -> u16 { 0 } }
impl ApiVersion for ListGroupRequest0 { fn api_version() -> u16 { 0 } }
// TODO: one struct, 2 versions
//impl ApiVersion for ApiVersionsRequest0 { fn api_version() -> u16 { 0 } }
impl ApiVersion for ApiVersionsRequest { fn api_version() -> u16 { 1 } }
