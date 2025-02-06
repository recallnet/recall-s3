#![forbid(unsafe_code)]
#![deny(
clippy::all, //
clippy::cargo, //
)]
#![allow(
clippy::wildcard_imports,
clippy::missing_errors_doc, // TODO: docs
clippy::let_underscore_untyped,
clippy::module_name_repetitions,
clippy::multiple_crate_versions, // TODO: check later
)]

pub use self::error::*;
pub use self::recall::Recall;

#[macro_use]
mod error;

mod bucket;
mod recall;
mod s3;
mod utils;
