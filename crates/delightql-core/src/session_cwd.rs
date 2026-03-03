// Per-session working directory for relative path resolution.
//
// Thread-local storage set by the server handler on ControlOp::Cwd,
// cleared on ControlOp::Reset. Read by mount!, consult!, consult_tree!,
// run!, etc. via resolve_path().
//
// Safety: one Unix socket = one thread = one session. The thread_local
// is never accessed across threads.

use std::cell::RefCell;
use std::path::{Path, PathBuf};

thread_local! {
    static SESSION_CWD: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Set the per-session CWD. Pass `None` to clear.
pub fn set(path: Option<String>) {
    SESSION_CWD.with(|cell| {
        *cell.borrow_mut() = path;
    });
}

/// Resolve a relative path against the session CWD.
///
/// - Absolute paths are returned unchanged.
/// - If no session CWD is set, the path is returned as-is (process CWD).
pub fn resolve_path(relative: &str) -> PathBuf {
    let path = Path::new(relative);
    if path.is_absolute() {
        return path.to_path_buf();
    }
    SESSION_CWD.with(|cell| {
        let borrowed = cell.borrow();
        match &*borrowed {
            Some(base) => Path::new(base).join(path),
            None => path.to_path_buf(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_path_unchanged() {
        set(Some("/tmp/isolate".into()));
        assert_eq!(resolve_path("/usr/bin/dql"), PathBuf::from("/usr/bin/dql"));
        set(None);
    }

    #[test]
    fn relative_path_with_cwd() {
        set(Some("/tmp/dql-isolate-abc123".into()));
        assert_eq!(
            resolve_path("manufacturing.db"),
            PathBuf::from("/tmp/dql-isolate-abc123/manufacturing.db")
        );
        set(None);
    }

    #[test]
    fn relative_path_without_cwd() {
        set(None);
        assert_eq!(
            resolve_path("manufacturing.db"),
            PathBuf::from("manufacturing.db")
        );
    }

    #[test]
    fn clear_restores_fallback() {
        set(Some("/tmp/isolate".into()));
        set(None);
        assert_eq!(resolve_path("foo.db"), PathBuf::from("foo.db"));
    }
}
