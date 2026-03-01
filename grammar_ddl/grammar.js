// DelightQL DDL Grammar — Sigil Sub-language
// Extends the DQL grammar with constraint/default expression rules.
// Used to parse c:"..." (constraint) and d:"..." (default) sigil strings
// from companion table definitions.
//
// Grammar rules:
//   constraint_expression → primary_key_decl | unique_key_decl | domain_expression
//   default_expression   → domain_expression
//   primary_key_decl     → %% [( col, ... )]
//   unique_key_decl      → %  [( col, ... )]
//   column_self_ref      → @   (refers to the column being defined)

const dqlGrammar = require('../grammar_dql/grammar');

function sep1(separator, rule) {
  return seq(rule, repeat(seq(separator, rule)));
}

module.exports = grammar(dqlGrammar, {
  name: 'delightql_ddl',

  conflicts: ($, previous) => previous.concat([
    // domain_expression is valid as both constraint_expression and default_expression
    [$.constraint_expression, $.default_expression],
  ]),

  rules: {
    // Override DQL root to accept sigil expressions
    source_file: $ => choice(
      $.constraint_expression,
      $.default_expression,
    ),

    constraint_expression: $ => choice(
      $.primary_key_decl,
      $.unique_key_decl,
      $.domain_expression,
    ),

    default_expression: $ => $.domain_expression,

    primary_key_decl: $ => seq('%%', optional(seq('(', sep1(',', $.identifier), ')'))),
    unique_key_decl: $ => seq('%', optional(seq('(', sep1(',', $.identifier), ')'))),

    column_self_ref: $ => '@',
  },
});
