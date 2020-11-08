use crate::source::Source;

mod message;
mod publisher;
mod source;

pub fn local() -> Source {
    Source::Local
}
