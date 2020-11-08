use crate::source::Source;

mod source;
mod venta;

pub fn local() -> Source {
    Source::Local
}
