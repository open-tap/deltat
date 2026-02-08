use std::fs::File;
use std::io::{self, BufReader, ErrorKind};
use std::sync::Arc;

use pgwire::tokio::tokio_rustls::rustls::ServerConfig;
use pgwire::tokio::TlsAcceptor;

pub fn load_tls_acceptor(
    cert_path: Option<&str>,
    key_path: Option<&str>,
) -> io::Result<Option<TlsAcceptor>> {
    let (cert_path, key_path) = match (cert_path, key_path) {
        (None, None) => return Ok(None),
        (Some(c), Some(k)) => (c, k),
        _ => {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "both DELTAT_TLS_CERT and DELTAT_TLS_KEY must be set, or neither",
            ));
        }
    };

    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(File::open(cert_path)?))
        .collect::<Result<_, _>>()?;

    let key = rustls_pemfile::private_key(&mut BufReader::new(File::open(key_path)?))?
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "no private key found in key file"))?;

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;

    config.alpn_protocols = vec![b"postgresql".to_vec()];

    Ok(Some(TlsAcceptor::from(Arc::new(config))))
}
