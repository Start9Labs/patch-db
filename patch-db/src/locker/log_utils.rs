use imbl::OrdSet;

use super::LockInfo;
use crate::handle::HandleId;

#[cfg(feature = "tracing")]
pub fn display_session_set(set: &OrdSet<HandleId>) -> String {
    use std::fmt::Write;

    let mut display = String::from("{");
    for session in set.iter() {
        write!(display, "{},", session.id).unwrap();
    }
    display.replace_range(display.len() - 1.., "}");
    display
}

#[cfg(feature = "tracing")]
pub(super) fn fmt_acquired(lock_info: &LockInfo) -> String {
    format!(
        "Acquired: session {} - {} lock on {}",
        lock_info.handle_id.id, lock_info.ty, lock_info.ptr,
    )
}

#[cfg(feature = "tracing")]
pub(super) fn fmt_deferred(deferred_lock_info: &LockInfo) -> String {
    format!(
        "Deferred: session {} - {} lock on {}",
        deferred_lock_info.handle_id.id, deferred_lock_info.ty, deferred_lock_info.ptr,
    )
}

#[cfg(feature = "tracing")]
pub(super) fn fmt_released(released_lock_info: &LockInfo) -> String {
    format!(
        "Released: session {} - {} lock on {}",
        released_lock_info.handle_id.id, released_lock_info.ty, released_lock_info.ptr
    )
}

#[cfg(feature = "tracing")]
pub(super) fn fmt_cancelled(cancelled_lock_info: &LockInfo) -> String {
    format!(
        "Canceled: session {} - {} lock on {}",
        cancelled_lock_info.handle_id.id, cancelled_lock_info.ty, cancelled_lock_info.ptr
    )
}
