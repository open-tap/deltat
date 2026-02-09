use ulid::Ulid;

use crate::model::Span;

#[derive(Debug)]
pub enum EngineError {
    NotFound(Ulid),
    AlreadyExists(Ulid),
    Conflict(Ulid),
    NotCoveredByParent {
        rule_span: Span,
        uncovered: Vec<Span>,
    },
    CycleDetected(Ulid),
    HasChildren(Ulid),
    CapacityExceeded(u32),
    LimitExceeded(&'static str),
    WalError(String),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::NotFound(id) => write!(f, "not found: {id}"),
            EngineError::AlreadyExists(id) => write!(f, "already exists: {id}"),
            EngineError::Conflict(id) => write!(f, "conflict with allocation: {id}"),
            EngineError::NotCoveredByParent {
                rule_span,
                uncovered,
            } => {
                write!(
                    f,
                    "rule [{}, {}) not covered by parent availability; uncovered: {:?}",
                    rule_span.start, rule_span.end, uncovered
                )
            }
            EngineError::CycleDetected(id) => write!(f, "cycle detected at resource: {id}"),
            EngineError::HasChildren(id) => {
                write!(f, "cannot delete resource {id}: has children")
            }
            EngineError::CapacityExceeded(cap) => {
                write!(f, "capacity {cap} exceeded: all slots occupied")
            }
            EngineError::LimitExceeded(msg) => write!(f, "limit exceeded: {msg}"),
            EngineError::WalError(e) => write!(f, "WAL error: {e}"),
        }
    }
}

impl std::error::Error for EngineError {}
