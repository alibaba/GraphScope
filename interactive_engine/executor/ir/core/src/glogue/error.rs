use ir_common::error::ParsePbError;

use crate::glogue::PatternId;

/// Record the error for pattern match (via ExtendStrategy)
#[derive(Debug, Clone)]
pub enum IrPatternError {
    MissingPatternVertex(PatternId),
    MissingPatternEdge(PatternId),
    CanonicalLabelError(String),
    InvalidExtendPattern(String),
    ParsePbError(ParsePbError),
    Unsupported(String),
}

pub type IrPatternResult<T> = Result<T, IrPatternError>;

impl std::fmt::Display for IrPatternError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            IrPatternError::MissingPatternVertex(vid) => {
                write!(f, "missing vertex in pattern, vertex id is: {:?}", vid)
            }
            IrPatternError::MissingPatternEdge(eid) => {
                write!(f, "missing edge in pattern, edge id is: {:?}", eid)
            }
            IrPatternError::CanonicalLabelError(err) => {
                write!(f, "error in canonical labelling: {:?}", err)
            }
            IrPatternError::InvalidExtendPattern(err) => {
                write!(f, "invalid pattern with ExtendStrategy: {:?}", err)
            }
            IrPatternError::ParsePbError(err) => {
                write!(f, "parse pb error in pattern: {:?}", err)
            }
            IrPatternError::Unsupported(s) => {
                write!(f, "not supported in pattern: {:?}", s)
            }
        }
    }
}

impl From<ParsePbError> for IrPatternError {
    fn from(err: ParsePbError) -> Self {
        Self::ParsePbError(err)
    }
}
