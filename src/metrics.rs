use lazy_static::lazy_static;
use prometheus::{register_int_counter_vec, IntCounterVec};

lazy_static! {
    pub static ref NUM_MSGS_QUEUED: IntCounterVec = register_int_counter_vec!(
        "venta_num_messages_queued",
        "Number of messages queued",
        &["producer", "topic"]
    )
    .unwrap();
    pub static ref NUM_MSGS_SENT: IntCounterVec = register_int_counter_vec!(
        "venta_num_messages_sent",
        "Number of messages sent",
        &["producer", "topic"]
    )
    .unwrap();
}
