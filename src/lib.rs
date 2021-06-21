#![deny(warnings)]
#![deny(clippy::dbg_macro)]
pub use crate::producer::BackgroundProducer;

mod message;
#[cfg(feature = "metrics")]
mod metrics;
mod producer;

#[cfg(doctest)]
mod test_readme {
    macro_rules! external_doc_test {
        ($x:expr) => {
            #[doc = $x]
            extern "C" {}
        };
    }

    external_doc_test!(include_str!("../README.md"));
}
