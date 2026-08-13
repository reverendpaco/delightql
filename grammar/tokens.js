// The tooling-visible token vocabulary.
//
// Every semantically meaningful keyword and sigil is declared here ONCE as a
// named production; grammar.js places it under role-specific parents. A token
// hidden behind an underscore is a token a Tree-sitter query cannot capture,
// so nothing carrying meaning to an editor is hidden here: highlight every `%`
// by matching `percent_sigil`, or tell its uses apart by matching the parent
// (`group` vs `distinct_mark` vs `fixpoint_badge` vs `unique_key_sigil`).
//
// A sigil with several meanings is defined once and disambiguated by its
// parent, never by a second spelling of the same characters. The exceptions
// are the anaphors: `@` and `_` instantiate per level as genuinely different
// carriers (TWO-ANAPHOR LAW), and naming them apart is what makes cross-level
// confusion unconstructible.
//
// `token.immediate` is glue, not decoration: it forbids whitespace before the
// sigil, which is what keeps `users!` an effect identifier while `users !(p)`
// stays a relation beside a negation.

module.exports = {
  // ---- keywords -----------------------------------------------------------
  // Spelled out rather than as a regex: only STRING tokens take part in
  // keyword extraction, and without it `as` needs a lexical precedence that
  // would then beat `asc` — a keyword whose prefix is another keyword.
  as_keyword: $ => choice('as', 'aS', 'As', 'AS'),
  // THE SQL-STYLE SPELLING IS THE SAME KEYWORD. DelightQL borrows SQL's
  // connective vocabulary, and SQL writes it upper: both cases reach one
  // token, and normalization never sees the difference. The keywords that
  // are NOT SQL's (`of`, `asc`/`desc` beside their long forms) keep one
  // spelling — there is no other language to agree with.
  and_keyword: $ => choice('and', 'AND'),
  or_keyword: $ => choice('or', 'OR'),
  not_keyword: $ => choice('not', 'NOT'),
  in_keyword: $ => choice('in', 'IN'),
  of_keyword: $ => 'of',
  // Two spellings each; the CST keeps the authored bytes and normalization
  // drops the distinction.
  asc_keyword: $ => choice('asc', 'ascending'),
  desc_keyword: $ => choice('desc', 'descending'),

  // ---- pipes and stage boundaries ----------------------------------------
  pipe_operator: $ => '|>',
  unwrap_pipe_operator: $ => '!>',
  materialize: $ => '|*>',
  function_pipe_first: $ => '/->',
  function_pipe_last: $ => '/->>',

  // ---- reduction, iteration, arrows --------------------------------------
  reduction_sigil: $ => '~>',
  // ':~>' is ONE token; interior whitespace is allowed.
  metadata_sigil: $ => token(seq(':', /[ \t\r\n]*/, '~>')),
  destructure_sigil: $ => '~=',
  arrow: $ => '->',
  window_sigil: $ => '<~',

  // ---- the overloaded sigils ---------------------------------------------
  // '*' has four homes: qualify (postfix), rename head, reposition head, glob.
  // '%' has four: operator head, inner-distinct prefix, fixpoint badge, and
  // the DDL unique key. The token after the sigil decides, never content.
  percent_sigil: $ => '%',
  double_percent_sigil: $ => '%%',
  star_sigil: $ => '*',
  effect_marker: $ => token.immediate('!'),
  mutation_marker: $ => token.immediate('!!'),
  outer_marker: $ => token.immediate('?'),
  sparse_mark: $ => '?',
  meta_sigil: $ => '^',
  signed_witness_sigil: $ => '+-',

  // ---- chain connectives --------------------------------------------------
  comma_sigil: $ => ',',
  positional_union_sigil: $ => '||',
  smart_union_sigil: $ => '|;|',
  corresponding_union_sigil: $ => ';',
  minus_sigil: $ => '-',
  edge_sigil: $ => '&',
  transitive_edge_sigil: $ => '&&',
  // THE LIFT'S COST — the same character in a different construct and a
  // different position: inside a call's argument row, bounding arguments.
  lift_sigil: $ => '&',

  // ---- polarity and bounds ------------------------------------------------
  // polarity is DATA, one carrier — never a variant pair.
  polarity: $ => choice('+', token(seq('\\', '+'))),
  // The pair is one carrier; the lexical layer has always let whitespace
  // stand between the '#' and its direction, so the token is not glued.
  bound_op: $ => seq('#', choice('<', '>')),
  // The literal 1 of a bound-to-one. A String token, so where both it and
  // `number` are admitted the lexer prefers this one; where only `number` is
  // admitted — every ordinary bound — nothing changes.
  one: $ => '1',

  // ---- necks and goals ----------------------------------------------------
  definition_neck: $ => choice(':-', ':='),
  goal_marker: $ => '?-',

  // ---- separators ---------------------------------------------------------
  // ONE shape for every tabular interior: anon bodies, fact bodies, and
  // fact-function arms all take '@' or three-or-more dashes.
  separator: $ => token(choice('@', /---+/)),

  // ---- anaphors -----------------------------------------------------------
  // `@` always names what flows in; `_` always names the disregarded. Each
  // instantiates per level as a DIFFERENT carrier, so a relational landing can
  // never be mistaken for a value-level composition input.
  disregarded: $ => '_',
  composition_input: $ => '@',
  landing: $ => '@',
  skipped: $ => '_',
  deictic_stage: $ => '_',
  // The companion sub-language's column self-reference is the SAME bytes in the
  // same position as a value-level hole, so it enters through
  // `composition_input` and the parse ROOT supplies the category — which is
  // what ddl-grammar.md FN.3 asks for: no reader classifies a cell by content.

  // ---- terminal sorts -----------------------------------------------------
  // Spelling belongs to the lexical layer, deferred deliberately by the
  // semantic grammars. These follow the language as it is spelled today.
  number: $ => token(choice(
    /0[xX][0-9a-fA-F]+/,
    /0[oO][0-7]+/,
    /-?[0-9][0-9_]*(\.[0-9][0-9_]*)?/,
  )),
  boolean: $ => choice('true', 'false'),
  null: $ => 'null',
  string: $ => token(choice(
    /"""([^"]|"[^"]|""[^"])*"""/,
    /"[^"]*"/,
  )),
  blob: $ => token(seq('b64:', choice(
    /"""([^"]|"[^"]|""[^"])*"""/,
    /"[A-Za-z0-9+\/=]*"/,
  ))),
  // The light mention spelling, written as a strict prefix of the future
  // type-term grammar: ::name, ::people(*), ::varchar(20). The token carries
  // two nesting levels; anything deeper takes the delimited spelling, which
  // subparses properly.
  symbol: $ => token(seq('::', /[a-zA-Z_][a-zA-Z0-9_]*/, optional(/\(([^()]|\([^()]*\))*\)/))),
  delimited_mention: $ => token(seq(':', '`', /[^`]*/, '`')),
  // Stropping is spelling: a strop is a reference, never a value, and the
  // engine-facing bytes are never stripped.
  stropped_form: $ => token(seq('`', /[^`]+/, '`')),
  regex: $ => token(seq('/', /[^\/\r\n]+/, '/')),
  comment: $ => token(prec(10, /\/\/[^\r\n]*/)),
};
