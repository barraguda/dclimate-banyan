use std::fmt::Debug;

pub type Result<T> = anyhow::Result<T>;

#[derive(Debug, thiserror::Error)]
#[error("Unable to convert {src} to {dst}")]
pub struct ConversionError {
    pub src: String,
    pub dst: String,
}

impl ConversionError {
    pub fn new<Src: Debug, Dst: Into<String>>(src: Src, dst: Dst) -> Self {
        let src = format!("{src:?}");
        let dst = dst.into();
        ConversionError { src, dst }
    }
}
