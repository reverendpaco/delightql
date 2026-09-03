// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Does a session reset clear the enlist set and the scratch namespaces? The
//! ball runner resets between cells and assumes yes.

use delightql_cli::exec_ng::run_dql_query;

const BLOCK: &str = "(~~ddl:\"scr\"\npair(*) :- _(a, b @ 1, \"x\")\n~~)\n";

#[test]
fn a_reset_session_forgets_the_enlist_set_and_scratch() {
    let mut handle = delightql_cli::connection::open_handle().expect("handle");
    {
        let mut session = handle.session().expect("session");
        run_dql_query(&format!("{BLOCK}enlist!(\"scr\")(*)"), &mut *session).expect("enlist scr");
        let ok = run_dql_query("pair(*)", &mut *session);
        assert!(ok.is_ok(), "enlisted scratch is bare in its own session: {ok:?}");
    }
    // The ordinary full reset a host uses (the ball runner's road too).
    handle.recover_session().expect("reset");
    let mut session = handle.session().expect("session after reset");
    let bare = run_dql_query("pair(*)", &mut *session);
    assert!(
        bare.is_err(),
        "the enlist set survived the reset: bare pair(*) still answers"
    );
    let redefined = run_dql_query(&format!("{BLOCK}pair(*)"), &mut *session);
    assert!(
        redefined.is_err(),
        "a named scratch block's names are not bare — unless the previous session's \
         enlist!(\"scr\") survived the reset: {redefined:?}"
    );
}

/// Definitions from an UNNAMED block land in `home`; a reset must forget
/// them, or the next session's bare names answer from a previous session.
#[test]
fn a_reset_session_forgets_home_scratch() {
    let mut handle = delightql_cli::connection::open_handle().expect("handle");
    {
        let mut session = handle.session().expect("session");
        run_dql_query("(~~ddl\nghost(*) :- _(a @ 1)\n~~)\nghost(*)", &mut *session)
            .expect("home scratch is bare in its own session");
    }
    handle.recover_session().expect("reset");
    let mut session = handle.session().expect("session after reset");
    let bare = run_dql_query("ghost(*)", &mut *session);
    assert!(bare.is_err(), "home scratch survived the reset: {bare:?}");
    let qualified = run_dql_query("home.ghost(*)", &mut *session);
    assert!(qualified.is_err(), "home scratch survived the reset (qualified): {qualified:?}");
}
