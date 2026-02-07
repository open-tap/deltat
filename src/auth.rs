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
