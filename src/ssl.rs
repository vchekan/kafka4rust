use clap::ValueEnum;

#[derive(Debug, Clone)]
pub struct SslOptions {
    pub security_protocol: SecurityProtocol,
    pub truststore_location: Option<String>,
    pub truststore_password: Option<String>,
    pub keystore_location: Option<String>,
    pub keystore_password: Option<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum SecurityProtocol {
    PLAINTEXT,
    SSL,

    // Not implemented
    // SASL_PLAINTEXT,
    // SASL_SSL,
}

impl Default for SslOptions {
    fn default() -> Self {
        SslOptions { 
            security_protocol: SecurityProtocol::PLAINTEXT, 
            truststore_location: None, 
            truststore_password: None, 
            keystore_location: None, 
            keystore_password: None 
        }
    }
}