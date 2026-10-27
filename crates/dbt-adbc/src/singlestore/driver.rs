use std::sync::Arc;

use super::sqlx::mysql::{MySqlConnectOptions, MySqlSslMode};
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionDatabase, OptionValue};

use super::database::SingleStoreDatabase;
use crate::database::Database;
use crate::driver::Driver;
use crate::semaphore::Semaphore;

pub struct SingleStoreDriver {
    pub(crate) semaphore: Option<Arc<Semaphore>>,
}

impl SingleStoreDriver {
    pub fn new(semaphore: Option<Arc<Semaphore>>) -> Self {
        Self { semaphore }
    }
}

impl Driver for SingleStoreDriver {
    fn new_database(&mut self) -> Result<Box<dyn Database>> {
        self.new_database_with_opts(vec![])
    }

    fn new_database_with_opts(
        &mut self,
        opts: Vec<(OptionDatabase, OptionValue)>,
    ) -> Result<Box<dyn Database>> {
        let mut uri_str = None;
        let mut host = None;
        let mut port = None;
        let mut user = None;
        let mut password = None;
        let mut database = None;
        let mut ssl_mode = None;
        let mut ssl_ca = None;
        let mut ssl_cert = None;
        let mut ssl_key = None;
        // The cleartext auth plugin sends the password unhashed, so it is opt-in.
        let mut allow_cleartext_plugin = false;

        for (k, v) in opts {
            match k {
                OptionDatabase::Uri => {
                    if let OptionValue::String(u) = v {
                        uri_str = Some(u);
                    }
                }
                OptionDatabase::Other(key) => match key.to_ascii_lowercase().as_str() {
                    "host" => {
                        if let OptionValue::String(s) = v {
                            host = Some(s);
                        }
                    }
                    "port" => match v {
                        OptionValue::Int(p) => port = Some(p as u16),
                        OptionValue::String(s) => port = s.parse::<u16>().ok(),
                        _ => {}
                    },
                    "user" | "username" => {
                        if let OptionValue::String(s) = v {
                            user = Some(s);
                        }
                    }
                    "password" => {
                        if let OptionValue::String(s) = v {
                            password = Some(s);
                        }
                    }
                    "database" | "dbname" => {
                        if let OptionValue::String(s) = v {
                            database = Some(s);
                        }
                    }
                    "ssl_mode" | "sslmode" => {
                        if let OptionValue::String(s) = v {
                            ssl_mode = Some(s);
                        }
                    }
                    "ssl_ca" | "sslrootcert" => {
                        if let OptionValue::String(s) = v {
                            ssl_ca = Some(s);
                        }
                    }
                    "ssl_cert" => {
                        if let OptionValue::String(s) = v {
                            ssl_cert = Some(s);
                        }
                    }
                    "ssl_key" => {
                        if let OptionValue::String(s) = v {
                            ssl_key = Some(s);
                        }
                    }
                    "allow_cleartext_plugin" | "cleartext_plugin" => match v {
                        OptionValue::Int(i) => allow_cleartext_plugin = i != 0,
                        OptionValue::String(s) => {
                            allow_cleartext_plugin = !matches!(
                                s.to_ascii_lowercase().as_str(),
                                "false" | "0" | "no" | "off"
                            );
                        }
                        _ => {}
                    },
                    _ => {}
                },
                _ => {}
            }
        }

        let mut connect_options = if let Some(uri) = uri_str {
            uri.parse::<MySqlConnectOptions>().map_err(|e| {
                Error::with_message_and_status(
                    // The URI carries the password; never echo it back.
                    format!("Failed to parse SingleStore connection URI: {e}"),
                    Status::InvalidArguments,
                )
            })?
        } else {
            let mut opts = MySqlConnectOptions::new();
            if let Some(h) = host {
                opts = opts.host(&h);
            }
            if let Some(p) = port {
                opts = opts.port(p);
            }
            if let Some(u) = user {
                opts = opts.username(&u);
            }
            if let Some(pwd) = password {
                opts = opts.password(&pwd);
            }
            if let Some(db) = database {
                opts = opts.database(&db);
            }
            opts
        };

        // Without an explicit mode, sqlx keeps any `ssl-mode` from the URI and
        // otherwise uses `preferred`, like the MySQL/SingleStore CLI clients.
        let ssl_mode = ssl_mode.as_deref().map(parse_ssl_mode).transpose()?;
        if allow_cleartext_plugin
            && !matches!(
                ssl_mode,
                Some(
                    MySqlSslMode::Required | MySqlSslMode::VerifyCa | MySqlSslMode::VerifyIdentity
                )
            )
        {
            return Err(Error::with_message_and_status(
                "allow_cleartext_plugin requires ssl_mode 'required', 'verify_ca' or \
                 'verify_identity'; the cleartext plugin would otherwise send the \
                 password unencrypted"
                    .to_string(),
                Status::InvalidArguments,
            ));
        }
        if let Some(ssl_mode) = ssl_mode {
            connect_options = connect_options.ssl_mode(ssl_mode);
        }

        if let Some(ca) = ssl_ca {
            connect_options = connect_options.ssl_ca(std::path::Path::new(&ca));
        }

        if let Some(cert) = ssl_cert {
            connect_options = connect_options.ssl_client_cert(std::path::Path::new(&cert));
        }

        if let Some(key) = ssl_key {
            connect_options = connect_options.ssl_client_key(std::path::Path::new(&key));
        }

        connect_options = connect_options.enable_cleartext_plugin(allow_cleartext_plugin);

        Ok(Box::new(SingleStoreDatabase::new(
            connect_options,
            self.semaphore.clone(),
        )))
    }
}

fn parse_ssl_mode(mode: &str) -> Result<MySqlSslMode> {
    match mode.to_ascii_lowercase().as_str() {
        "disabled" => Ok(MySqlSslMode::Disabled),
        "preferred" => Ok(MySqlSslMode::Preferred),
        "required" => Ok(MySqlSslMode::Required),
        "verify_ca" | "verifyca" => Ok(MySqlSslMode::VerifyCa),
        "verify_identity" | "verifyidentity" => Ok(MySqlSslMode::VerifyIdentity),
        other => Err(Error::with_message_and_status(
            format!(
                "Invalid SingleStore ssl_mode '{other}': expected one of disabled, preferred, \
                 required, verify_ca, verify_identity"
            ),
            Status::InvalidArguments,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connect(opts: &[(&str, &str)]) -> Result<Box<dyn Database>> {
        let opts = opts
            .iter()
            .map(|(k, v)| {
                (
                    OptionDatabase::Other(k.to_string()),
                    OptionValue::String(v.to_string()),
                )
            })
            .collect();
        SingleStoreDriver::new(None).new_database_with_opts(opts)
    }

    fn err_message(result: Result<Box<dyn Database>>) -> String {
        match result {
            Ok(_) => panic!("expected an error"),
            Err(e) => e.message,
        }
    }

    #[test]
    fn ssl_mode_rejects_unknown_values() {
        assert!(matches!(
            parse_ssl_mode("VerifyCa"),
            Ok(MySqlSslMode::VerifyCa)
        ));
        let msg = err_message(connect(&[("ssl_mode", "requird")]));
        assert!(
            msg.contains("Invalid SingleStore ssl_mode 'requird'"),
            "{msg}"
        );
    }

    #[test]
    fn cleartext_plugin_requires_tls() {
        let msg = err_message(connect(&[("allow_cleartext_plugin", "true")]));
        assert!(
            msg.contains("allow_cleartext_plugin requires ssl_mode"),
            "{msg}"
        );
        assert!(connect(&[("allow_cleartext_plugin", "true"), ("ssl_mode", "required")]).is_ok());
    }

    #[test]
    fn uri_parse_error_does_not_echo_password() {
        let opts = vec![(
            OptionDatabase::Uri,
            OptionValue::String("mysql://user:s3cret@host:notaport/db".to_string()),
        )];
        let msg = err_message(SingleStoreDriver::new(None).new_database_with_opts(opts));
        assert!(!msg.contains("s3cret"), "{msg}");
    }
}
