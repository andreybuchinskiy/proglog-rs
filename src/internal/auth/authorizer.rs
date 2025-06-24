use anyhow::Result;
use casbin::Enforcer;
use casbin::prelude::*;
use std::sync::Arc;
use tonic::{Code, Status};

#[derive(Clone)]
pub struct Authorizer {
    enforcer: Arc<Enforcer>,
}

impl Authorizer {
    pub async fn new(model: &'static str, policy: &'static str) -> Result<Authorizer> {
        let enforcer = Enforcer::new(model, policy).await?;
        let authorizer = Authorizer {
            enforcer: Arc::new(enforcer),
        };
        Ok(authorizer)
    }

    pub fn authorize(
        &self,
        subject: String,
        object: String,
        action: String,
    ) -> Result<(), Box<Status>> {
        if !self
            .enforcer
            .enforce((subject, object, action))
            .map_err(|_| Status::new(Code::Unknown, "unknown enforcer error"))?
        {
            return Err(Box::new(Status::new(
                Code::PermissionDenied,
                "permission denied",
            )));
        }
        Ok(())
    }
}
