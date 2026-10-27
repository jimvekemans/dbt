use crate::{AdapterConfig, Auth, AuthError, AuthWarningPrinter};
use dbt_adbc::{Backend, database};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

pub struct SingleStoreAuth {
    #[allow(dead_code)]
    pub warning_printer: Box<dyn AuthWarningPrinter>,
}

impl SingleStoreAuth {
    pub fn new(warning_printer: Box<dyn AuthWarningPrinter>) -> Self {
        Self { warning_printer }
    }
}

impl Auth for SingleStoreAuth {
    fn backend(&self) -> Backend {
        Backend::SingleStore
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let mut builder = database::Builder::new(self.backend());

        let user = config.require_string("user")?;
        let password = config.require_string("password")?;
        let host = config.require_string("host")?;
        let port = config.require_string("port")?;
        let dbname = config.require_string("database")?;

        let encoded_user = utf8_percent_encode(&user, NON_ALPHANUMERIC);
        let encoded_password = utf8_percent_encode(&password, NON_ALPHANUMERIC);
        let encoded_dbname = utf8_percent_encode(&dbname, NON_ALPHANUMERIC);

        let uri =
            format!("mysql://{encoded_user}:{encoded_password}@{host}:{port}/{encoded_dbname}");

        // TLS settings travel as named options, not URI query parameters: the
        // driver validates them and gives a clear error for unknown values.
        let ssl_mode = config
            .get_string("ssl_mode")
            .or_else(|| config.get_string("sslmode"));
        let ssl_ca = config
            .get_string("ssl_ca")
            .or_else(|| config.get_string("sslrootcert"));
        for (opt, value) in [
            ("ssl_mode", ssl_mode),
            ("ssl_ca", ssl_ca),
            ("ssl_cert", config.get_string("ssl_cert")),
            ("ssl_key", config.get_string("ssl_key")),
        ] {
            if let Some(value) = value {
                builder.with_named_option(opt, value.to_string())?;
            }
        }

        // Opt-in only: the driver defaults to false and rejects it without TLS.
        if let Some(allow_cleartext_plugin) = config
            .get_bool("allow_cleartext_plugin")
            .or_else(|| config.get_bool("cleartext_plugin"))
        {
            builder
                .with_named_option("allow_cleartext_plugin", allow_cleartext_plugin.to_string())?;
        }

        builder.with_parse_uri(uri)?;

        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoopAuthWarningPrinter;
    use adbc_core::options::{OptionDatabase, OptionValue};
    use dbt_yaml::Mapping;

    #[test]
    fn test_singlestore_auth_uri_construction() {
        let auth = SingleStoreAuth::new(Box::new(NoopAuthWarningPrinter));
        let config = Mapping::from_iter([
            ("user".into(), "user@domain".into()),
            ("password".into(), "p@ss:w/ord".into()),
            ("host".into(), "singlestore.internal".into()),
            ("port".into(), "3306".into()),
            ("database".into(), "analytics".into()),
            ("ssl_mode".into(), "Required".into()),
            ("ssl_ca".into(), "/etc/ssl/ca.pem".into()),
        ]);

        let builder = auth
            .configure(&AdapterConfig::new(config))
            .expect("configure failed");
        let uri = builder.uri.clone().expect("URI missing");
        let uri_str = uri.as_str();

        assert_eq!(
            uri_str,
            "mysql://user%40domain:p%40ss%3Aw%2Ford@singlestore.internal:3306/analytics"
        );

        let named = |name: &str| {
            builder.other.iter().find_map(|(k, v)| match (k, v) {
                (OptionDatabase::Other(k), OptionValue::String(v)) if k == name => Some(v.as_str()),
                _ => None,
            })
        };
        assert_eq!(named("ssl_mode"), Some("Required"));
        assert_eq!(named("ssl_ca"), Some("/etc/ssl/ca.pem"));
        assert_eq!(named("allow_cleartext_plugin"), None);
    }
}
