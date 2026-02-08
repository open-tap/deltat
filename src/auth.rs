use async_trait::async_trait;
use pgwire::api::auth::{AuthSource, LoginInfo, Password};
use pgwire::error::PgWireResult;

#[derive(Debug)]
pub struct DeltaTAuthSource {
    password: String,
}

impl DeltaTAuthSource {
    pub fn new(password: String) -> Self {
        Self { password }
    }
}

#[async_trait]
impl AuthSource for DeltaTAuthSource {
    async fn get_password(&self, _login: &LoginInfo) -> PgWireResult<Password> {
        Ok(Password::new(None, self.password.as_bytes().to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn auth_returns_configured_password() {
        let source = DeltaTAuthSource::new("my_secret".into());
        let login = LoginInfo::new(Some("testuser"), None, "127.0.0.1".to_string());
        let password = source.get_password(&login).await.unwrap();
        assert_eq!(password.password(), b"my_secret");
        assert!(password.salt().is_none());
    }

    #[tokio::test]
    async fn auth_ignores_username() {
        let source = DeltaTAuthSource::new("pass123".into());
        let login1 = LoginInfo::new(Some("alice"), None, "127.0.0.1".to_string());
        let login2 = LoginInfo::new(Some("bob"), None, "127.0.0.1".to_string());
        let p1 = source.get_password(&login1).await.unwrap();
        let p2 = source.get_password(&login2).await.unwrap();
        assert_eq!(p1.password(), p2.password());
    }
}
