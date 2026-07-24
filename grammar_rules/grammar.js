// DelightQL DDL Grammar — Definition Language
// Extends the DQL grammar with definition head/neck rules.
// Body expressions use inherited DQL rules (no duplication).
//
// This eliminates ~1900 lines of duplicated DQL expression rules that had
// drifted out of sync (missing meta-ize, wrong outer-join syntax, missing
// SPI hooks, etc.). Bodies are re-parsed by the DQL parser anyway
// (via body_parser.rs), so the DDL grammar only needs to parse definition
// heads and necks correctly.

const dqlGrammar = require('../grammar_dql/grammar');

function sep1(separator, rule) {
  return seq(rule, repeat(seq(separator, rule)));
}

module.exports = grammar(dqlGrammar, {
  name: 'delightql_rules',

  conflicts: ($, previous) => previous.concat([
    // er_edge_definition: `name(*) &(::ctx) …` shares its prefix with a
    // view head `name(*) :-` and with expression glob/qualify reads —
    // GLR forks; the token after ')' decides ('&(' vs neck vs operator).
    [$.qualify_operator, $.glob, $.view_definition],
    [$.column_spec_item, $.view_head_item],
    [$.lvar, $.column_spec_item],
    [$.lvar, $.column_spec_item, $.view_head_item],
    [$.lvar, $.column_header_item, $.column_spec_item, $.ho_param, $.view_head_item, $.sigma_definition],
    [$.lvar, $.column_spec_item, $.ho_param, $.view_head_item, $.sigma_definition],
    [$.domain_expression, $.column_header_item, $.column_spec_item],
    [$.domain_expression, $.column_spec_item],
    // liminal_relation_statement (Phase 1A): insert!(audit_log(*)) shares its
    // prefix with an effect-rule head's ho_param (insert!(audit_log(*)) :- …).
    // GLR forks; the presence or absence of a following neck decides.
    [$.qualify_operator, $.ho_param],
    [$.column_spec_item, $.ho_param],

    // Ground membership probe in anon headers vs DDL head params: a
    // number after identifier( could be any of these. GLR forks.
    [$.ground_header, $.ho_param, $.literal, $.view_head_item],
    [$.ground_header, $.literal, $.view_head_item],

    // DDL definition rules create new ambiguities with inherited DQL rules

    // Effect-rule heads (EFFECT-ALGEBRA §1): name!(*) — the * after name!( could
    // be the effect head's glob or a glob argument of a pseudo_predicate_call
    // (liminal directive / query expression). GLR forks; the token after ')'
    // resolves (neck ':-' => effect rule, anything else => call).
    [$.glob, $.effect_rule_definition],

    // 3.1b interiority: pseudo_predicate_call parens now also accept a
    // relational_continuation, so `name!( * • )` forks three ways — effect
    // head glob (`name!(*) :-`), glob ARGUMENT (call form), or qualify
    // continuation. GLR forks; the token after ')' resolves (':-' => effect
    // rule) and the arguments branch's prec(2) keeps `s!(*)` an argument.
    [$.qualify_operator, $.glob, $.effect_rule_definition],

    // HO view: name(params)(*) conflicts with column_spec_item and tvf_argument
    [$.ho_view_definition, $.column_spec_item, $.tvf_argument],

    // Sigma: name(params) conflicts with table_access name(columns)
    [$.sigma_definition, $.table_access],
    [$.sigma_definition, $.ho_view_definition, $.table_access],
    [$.sigma_definition, $.ho_view_definition, $.column_spec_item, $.tvf_argument],

    // Fact: name(data) conflicts with sigma/table/ho_view (no neck disambiguator)
    [$.fact_definition, $.sigma_definition, $.table_access],
    [$.fact_definition, $.sigma_definition, $.ho_view_definition, $.table_access],
    [$.fact_definition, $.sigma_definition, $.ho_view_definition, $.column_spec_item, $.tvf_argument],

    // HO view/sigma: identifier after name( could be param, header, or data lvar
    [$.ho_view_definition, $.sigma_definition, $.lvar, $.column_header_item],
    [$.ho_view_definition, $.sigma_definition, $.lvar],
    [$.ho_view_definition, $.sigma_definition, $.column_header_item],

    // ho_param bare identifier vs sigma params vs lvar vs view_head_item
    [$.ho_param, $.lvar, $.sigma_definition],
    [$.ho_param, $.lvar],
    [$.ho_param, $.sigma_definition],
    [$.ho_param, $.lvar, $.column_header_item],
    [$.ho_param, $.lvar, $.sigma_definition, $.column_header_item],
    [$.column_header_item, $.sigma_definition],
    // view_head_item identifier conflicts with lvar, ho_param, sigma, column_header_item
    [$.view_head_item, $.lvar, $.sigma_definition],
    [$.view_head_item, $.lvar],
    [$.view_head_item, $.sigma_definition],
    [$.view_head_item, $.lvar, $.column_header_item],
    [$.view_head_item, $.lvar, $.sigma_definition, $.column_header_item],
    [$.view_head_item, $.ho_param, $.lvar, $.sigma_definition],
    [$.view_head_item, $.ho_param, $.lvar],
    [$.view_head_item, $.ho_param, $.sigma_definition],
    [$.view_head_item, $.ho_param, $.lvar, $.column_header_item],
    [$.view_head_item, $.ho_param, $.lvar, $.sigma_definition, $.column_header_item],
    [$.view_head_item, $.column_header_item, $.sigma_definition],

    // View: name(*) conflicts with table_access and glob_spec
    [$.view_definition, $.table_access],
    [$.view_definition, $.glob_spec],
    [$.fact_definition, $.view_definition, $.table_access],
    [$.fact_definition, $.view_definition, $.glob_spec],
    [$.view_definition, $.glob],

    // Argumentative view: name(items) :- query conflicts with sigma, fact, ho_view, table_access
    [$.argumentative_view_definition, $.sigma_definition, $.table_access],
    [$.argumentative_view_definition, $.sigma_definition, $.ho_view_definition, $.table_access],
    [$.argumentative_view_definition, $.sigma_definition, $.ho_view_definition, $.column_spec_item, $.tvf_argument],
    [$.argumentative_view_definition, $.fact_definition, $.sigma_definition, $.table_access],
    [$.argumentative_view_definition, $.fact_definition, $.sigma_definition, $.ho_view_definition, $.table_access],
    [$.argumentative_view_definition, $.fact_definition, $.sigma_definition, $.ho_view_definition, $.column_spec_item, $.tvf_argument],
    // Argumentative view head_item identifier vs sigma/lvar/column_header_item
    [$.argumentative_view_definition, $.ho_view_definition, $.sigma_definition, $.lvar, $.column_header_item],
    [$.argumentative_view_definition, $.ho_view_definition, $.sigma_definition, $.lvar],
    [$.argumentative_view_definition, $.ho_view_definition, $.sigma_definition, $.column_header_item],
    [$.argumentative_view_definition, $.sigma_definition, $.lvar, $.column_header_item],
    [$.argumentative_view_definition, $.sigma_definition, $.lvar],
    [$.argumentative_view_definition, $.sigma_definition, $.column_header_item],
    // ho_param with argumentative view
    [$.ho_param, $.lvar, $.sigma_definition, $.argumentative_view_definition],
    [$.ho_param, $.lvar, $.sigma_definition, $.argumentative_view_definition, $.column_header_item],
    // Argumentative view vs view (both have query body, differ in head: (*) vs (items))
    [$.argumentative_view_definition, $.view_definition, $.table_access],
    [$.argumentative_view_definition, $.view_definition, $.glob_spec],
    [$.argumentative_view_definition, $.view_definition, $.glob],
    [$.argumentative_view_definition, $.fact_definition, $.view_definition, $.table_access],
    [$.argumentative_view_definition, $.fact_definition, $.view_definition, $.glob_spec],
    // Function: name:(params) conflicts with CFE definition
    [$.function_definition, $.cfe_definition],
    [$.function_definition, $.cfe_first_param_list],
    [$.function_param, $.cfe_first_param_list],
    [$.function_param, $.cfe_definition],

    // view_head_item and ho_param ground literals conflict with literal in data rows
    [$.literal, $.ho_param, $.view_head_item],
    [$.literal, $.ho_param],
    [$.literal, $.view_head_item],
    [$.ho_param, $.view_head_item],

    // HO fact sugar: name(ho_params)(data) shares prefix with ho_view/fact/sigma/table
    [$.ho_fact_definition, $.ho_view_definition, $.column_spec_item, $.tvf_argument],
    [$.ho_fact_definition, $.sigma_definition, $.table_access],
    [$.ho_fact_definition, $.sigma_definition, $.ho_view_definition, $.table_access],
    [$.ho_fact_definition, $.sigma_definition, $.ho_view_definition, $.column_spec_item, $.tvf_argument],
    [$.ho_fact_definition, $.fact_definition, $.sigma_definition, $.table_access],
    [$.ho_fact_definition, $.fact_definition, $.sigma_definition, $.ho_view_definition, $.table_access],
    [$.ho_fact_definition, $.fact_definition, $.sigma_definition, $.ho_view_definition, $.column_spec_item, $.tvf_argument],
    [$.ho_fact_definition, $.ho_view_definition, $.sigma_definition, $.lvar, $.column_header_item],
    [$.ho_fact_definition, $.ho_view_definition, $.sigma_definition, $.lvar],
    [$.ho_fact_definition, $.ho_view_definition, $.sigma_definition, $.column_header_item],
    // ho_param/view_head_item inside first parens of ho_fact
    [$.ho_param, $.lvar, $.sigma_definition, $.ho_fact_definition],
    [$.ho_param, $.sigma_definition, $.ho_fact_definition],
    [$.ho_param, $.lvar, $.sigma_definition, $.column_header_item, $.ho_fact_definition],
    [$.view_head_item, $.ho_param, $.lvar, $.sigma_definition, $.ho_fact_definition],
    [$.view_head_item, $.ho_param, $.sigma_definition, $.ho_fact_definition],
    [$.view_head_item, $.ho_param, $.lvar, $.column_header_item, $.ho_fact_definition],
    [$.view_head_item, $.ho_param, $.lvar, $.sigma_definition, $.column_header_item, $.ho_fact_definition],
    [$.view_head_item, $.lvar, $.sigma_definition, $.ho_fact_definition],
    [$.view_head_item, $.sigma_definition, $.ho_fact_definition],
    [$.view_head_item, $.lvar, $.column_header_item, $.ho_fact_definition],
    [$.view_head_item, $.lvar, $.sigma_definition, $.column_header_item, $.ho_fact_definition],
    [$.view_head_item, $.column_header_item, $.sigma_definition, $.ho_fact_definition],
    // * in second parens: glob (data) vs ho_view_definition (output head)
    [$.glob, $.ho_view_definition],
    // Argumentative view combinations with ho_fact
    [$.argumentative_view_definition, $.ho_fact_definition, $.sigma_definition, $.table_access],
    [$.argumentative_view_definition, $.ho_fact_definition, $.sigma_definition, $.ho_view_definition, $.table_access],
    [$.argumentative_view_definition, $.ho_fact_definition, $.sigma_definition, $.ho_view_definition, $.column_spec_item, $.tvf_argument],
    [$.argumentative_view_definition, $.ho_fact_definition, $.fact_definition, $.sigma_definition, $.table_access],
    [$.argumentative_view_definition, $.ho_fact_definition, $.fact_definition, $.sigma_definition, $.ho_view_definition, $.table_access],
    [$.argumentative_view_definition, $.ho_fact_definition, $.fact_definition, $.sigma_definition, $.ho_view_definition, $.column_spec_item, $.tvf_argument],
    [$.argumentative_view_definition, $.ho_fact_definition, $.ho_view_definition, $.sigma_definition, $.lvar, $.column_header_item],
    [$.argumentative_view_definition, $.ho_fact_definition, $.ho_view_definition, $.sigma_definition, $.lvar],
    [$.argumentative_view_definition, $.ho_fact_definition, $.ho_view_definition, $.sigma_definition, $.column_header_item],
    [$.argumentative_view_definition, $.ho_fact_definition, $.sigma_definition, $.lvar, $.column_header_item],
    [$.argumentative_view_definition, $.ho_fact_definition, $.sigma_definition, $.lvar],
    [$.argumentative_view_definition, $.ho_fact_definition, $.sigma_definition, $.column_header_item],
    [$.argumentative_view_definition, $.ho_fact_definition, $.fact_definition, $.view_definition, $.table_access],
    [$.argumentative_view_definition, $.ho_fact_definition, $.fact_definition, $.view_definition, $.glob_spec],
    [$.argumentative_view_definition, $.ho_fact_definition, $.view_definition, $.table_access],
    [$.argumentative_view_definition, $.ho_fact_definition, $.view_definition, $.glob_spec],
    [$.argumentative_view_definition, $.ho_fact_definition, $.view_definition, $.glob],
    // ho_param with argumentative + ho_fact
    [$.ho_param, $.lvar, $.sigma_definition, $.argumentative_view_definition, $.ho_fact_definition],
    [$.ho_param, $.lvar, $.sigma_definition, $.argumentative_view_definition, $.column_header_item, $.ho_fact_definition],
    // Fact + view + ho_fact
    [$.ho_fact_definition, $.fact_definition, $.view_definition, $.table_access],
    [$.ho_fact_definition, $.fact_definition, $.view_definition, $.glob_spec],
    [$.ho_fact_definition, $.view_definition, $.table_access],
    [$.ho_fact_definition, $.view_definition, $.glob_spec],
    [$.ho_fact_definition, $.view_definition, $.glob],
    // Literal conflicts with ho_fact
    [$.literal, $.ho_param, $.view_head_item, $.ho_fact_definition],
    [$.literal, $.ho_param, $.ho_fact_definition],
    [$.ho_param, $.view_head_item, $.ho_fact_definition],
  ]),

  rules: {
    // Override DQL root to accept definitions + query statements.
    // The Rust extraction code (extract_ddl_file) iterates root.children()
    // matching on "definition", "function_definition", "query_statement", etc.
    // — all preserved by this rule.
    source_file: $ => repeat1(choice($.definition, $.query_statement, $.ddl_annotation, $.liminal_directive)),

    // Liminal directive: a bare directive call statement at the top of the
    // file (EFFECT-ALGEBRA §8). Since Phase 1A of the directive convergence
    // (complete-form extraction), this grammar IS the segmentation authority:
    // extract_embedded_directives walks these nodes and lifts exactly the
    // session directives; every other name refuses with the eligibility
    // message. Reuses the inherited DQL pseudo_predicate_call shape
    // (name!(args)) plus a relation-argument statement shape so DML
    // spellings like insert!(audit_log(*)) parse cleanly and can be refused
    // BY NAME rather than dying as a garbled recovery parse.
    liminal_directive: $ => choice(
      $.pseudo_predicate_call,
      $.liminal_relation_statement,
    ),

    // A directive-shaped whole statement whose arguments are relation
    // applications (e.g. insert!(audit_log(*))). Never liminally
    // executable — it exists so the extraction layer can issue the §8
    // eligibility refusal with the directive's name (pinned by
    // effects/liminal--41_dml_not_eligible).
    liminal_relation_statement: $ => prec.dynamic(-1, seq(
      field('name', $.identifier),
      token.immediate('!'),
      '(',
      field('relation_args', sep1(',', $.table_access)),
      ')',
    )),

    // === DDL-specific rules (the only reason this grammar exists) ===

    definition: $ => choice(
      $.effect_rule_definition, // name!(*) :- body — the ! disambiguates; first for priority
      $.function_definition,
      $.constant_definition,  // nl :- char:(10) — zero-arity function without parens
      $.named_case_definition, // name(in -> out ---- arms) — before fact; the `->` head disambiguates
      $.ho_fact_definition,   // name(ho_params)(data) — before fact for priority
      $.fact_definition,
      $.sigma_definition,
      $.ho_view_definition,
      $.argumentative_view_definition,
      $.view_definition,
      $.er_edge_definition,   // table_access & ( — before er_rule (identifier &)
      $.er_rule_definition,
    ),

    // Constant definition: name neck body (no parens)
    // Sugar for zero-arity function: nl :- char:(10) === nl:() :- char:(10)
    // Disambiguated from other definitions because all others follow identifier
    // with '(' or ':(' or '&', while constants follow with ':-' or ':='.
    constant_definition: $ => seq(
      field('name', $.identifier),
      field('neck', $.definition_neck),
      optional(field('doc', $.annotation_body)),
      field('body', $.domain_expression),
    ),

    // Function definition: name:(params) neck [docs] domain_expression
    // Supports context-aware functions: name:(.., params) or name:(..{ctx}, params)
    function_definition: $ => seq(
      field('name', $.identifier),
      token.immediate(':('),
      optional(choice(
        // Context-aware: .., params  OR  ..{ctx}, params  OR just ..
        seq(
          field('context_marker', $.context_marker),
          optional(seq(',', field('params', sep1(',', $.function_param))))
        ),
        // Regular: params only
        field('params', sep1(',', $.function_param))
      )),
      ')',
      field('neck', $.definition_neck),
      optional(field('doc', $.annotation_body)),
      field('body', $.domain_expression),
    ),

    // CCAFE context marker: .. (implicit) or ..{list} (explicit)
    context_marker: $ => choice(
      // Implicit context: .. (auto-discover from body)
      '..',
      // Explicit context: ..{id1, id2, ...} (declared context params)
      seq('..', '{', optional(field('context_params', sep1(',', $.identifier))), '}')
    ),

    // Function parameter: identifier, guarded, or callable (higher-order)
    function_param: $ => choice(
      // Callable function param: f:() — marks this param as a function reference
      seq(
        field('param_name', $.identifier),
        token.immediate(':('),
        ')',
      ),
      // Guarded param: name | guard_expr
      seq(
        field('param_name', $.identifier),
        '|',
        field('guard', $.domain_expression),
      ),
      // Regular scalar param
      $.identifier,
    ),

    // Higher-order view definition: name(params)(output) neck [docs] query
    // Params can be: T(*) glob functor, T(x,y) argumentative functor, bare n scalar, or ground literal
    // Output can be: (*) glob or (items) argumentative with optional ground terms
    ho_view_definition: $ => seq(
      field('name', $.identifier),
      '(',
      field('ho_params', sep1($._comma, $.ho_param)),
      ')',
      '(',
      choice(
        '*',
        field('output_head', sep1($._comma, $.view_head_item)),
      ),
      ')',
      field('neck', $.definition_neck),
      optional(field('doc', $.annotation_body)),
      field('body', $.query),
    ),

    // HO fact-table sugar: name(ho_params)(data) — inline data, no neck/body
    // Combines HO params (first parens) with fact data (second parens).
    // Desugars to ho_view_definition head + anonymous table body.
    ho_fact_definition: $ => seq(
      field('name', $.identifier),
      '(',
      field('ho_params', sep1($._comma, $.ho_param)),
      ')',
      '(',
      choice(
        seq($.column_headers, $.anonymous_table_separator, $.data_rows),
        $.data_rows,
      ),
      ')',
    ),

    // HO parameter declaration: T(*), T(x, y), bare n, or ground literal ("x", 42)
    ho_param: $ => choice(
      // Inner glob functor: T(*)
      seq(field('param_name', $.identifier), '(', '*', ')'),
      // Inner argumentative functor: T(x, y)
      seq(field('param_name', $.identifier), '(',
          field('columns', sep1($._comma, $.identifier)), ')'),
      // Scalar parameter (or legacy bare table name): n
      field('param_name', $.identifier),
      // Ground scalar literal: "value" or 42 — or a mention, which
      // grounds on its canonical encoding: ::fast, :`people(*)`
      field('ground_value', choice($.string_literal, $.number_literal, $.symbol, $.delimited_mention)),
    ),

    // Argumentative view definition: name(items) neck [docs] query
    // Items can be identifiers (free variables → projection) or literals (ground terms → constants)
    // Disambiguated from sigma_definition by body type (query vs domain_expression)
    argumentative_view_definition: $ => seq(
      field('name', $.identifier),
      '(',
      field('head_items', sep1($._comma, $.view_head_item)),
      ')',
      field('neck', $.definition_neck),
      optional(field('doc', $.annotation_body)),
      field('body', $.query),
    ),

    // View head item: free variable (identifier) or ground term (literal),
    // optionally labeled with `as name` (defining-head naming/conformance).
    // In a DEFINING head, `as` means "left side supplies, right side labels":
    //   `nation as country` — plumb the lvar, offer `country` as the position's name
    //   `"VIP" as tag`      — supply the constant, offer `tag` as the position's name
    // See book/design/clause-head-catechism.md §II (remedy 2).
    view_head_item: $ => choice(
      seq(
        field('supply', choice($.identifier, $.string_literal, $.number_literal, $.symbol, $.delimited_mention)),
        $._as,
        field('label', $.identifier),
      ),
      $.identifier,
      $.string_literal,
      $.number_literal,
      // Mention grounds a head item by its canonical encoding.
      $.symbol,
      $.delimited_mention,
    ),

    // Sigma predicate definition: name(params) neck [docs] domain_expression
    sigma_definition: $ => seq(
      field('name', $.identifier),
      '(',
      field('params', sep1($._comma, $.identifier)),
      ')',
      field('neck', $.definition_neck),
      optional(field('doc', $.annotation_body)),
      field('body', $.domain_expression),
    ),

    // Effect-rule definition (EFFECT-ALGEBRA §1): a rule defining a user
    // directive. The head name carries a `!` (token.immediate — no space).
    // Forms:
    //   glob head:  do_something!(*) :- body      (route!, main!, ...)
    //   HO head:    quarantine!(Bad(*))(*) :- body (params via HO machinery, §1/F4)
    // Multi-clause rules (R5) are simply repeated effect_rule_definitions with
    // the same head name; the grammar does not group clauses.
    // prec.dynamic(2) so that when a GLR fork survives against a top-level
    // liminal_directive reading (`name!(*)` is also a valid pseudo-predicate
    // call until the neck token appears), the definition reading wins.
    // NECK: `:-` ONLY — the shared definition_neck's `:=` is STRICKEN for
    // effect rules (IMPLEMENTATION-PLAN §3.0, ruled 2026-07-11): a directive
    // rule is a session definition, never a temporary table. Hard removal,
    // no curated message — `name!(*) := body` is a plain parse error. Pinned
    // by effect_rule_walrus_neck_refuses_at_grammar (parser/mod.rs) and
    // effects-ball rules--46_walrus_neck_refused.
    effect_rule_definition: $ => prec.dynamic(2, seq(
      field('name', $.identifier),
      token.immediate('!'),
      choice(
        // Glob head: name!(*)
        seq('(', '*', ')'),
        // HO head: name!(ho_params)(* | output items)
        seq(
          '(',
          field('ho_params', sep1($._comma, $.ho_param)),
          ')',
          '(',
          choice(
            '*',
            field('output_head', sep1($._comma, $.view_head_item)),
          ),
          ')',
        ),
      ),
      field('neck', alias(token(':-'), $.session_neck)),
      optional(field('doc', $.annotation_body)),
      field('body', $.query),
    )),

    // View definition: name(*) neck [docs] query
    view_definition: $ => seq(
      field('name', $.identifier),
      '(', '*', ')',
      field('neck', $.definition_neck),
      optional(field('doc', $.annotation_body)),
      field('body', $.query),
    ),

    // Named case function definition (Prolog-mode case predicate):
    //   name( input -> output ---- "a" -> "x"; "b" -> "y"; _ -> "z" )
    // The `input -> output` head is a functional-dependency adornment: `input`
    // is the function parameter the caller binds (`name:(input)`); `output` is
    // documentation only and never names the result column. The `/---+/` (3+
    // hyphens) divides the head from the arms. The arms are the inherited
    // grammar_dql case arms (value -> result; _ -> default). Desugars in the
    // builder to the already-working case-bodied function
    //   name:(input) :- _:( input @ <arms> )
    // so registration/inlining/lowering are all reused (see case1_reusable).
    named_case_definition: $ => seq(
      field('name', $.identifier),
      '(',
      field('input', $.identifier),
      '->',
      field('output', $.identifier),
      /---+/,
      $.case_arm,
      repeat(seq(';', $.case_arm)),
      optional(seq(';', $.case_default)),
      ')'
    ),

    // Fact definition: name(data) — inline data literal, no neck required
    fact_definition: $ => seq(
      field('name', $.identifier),
      '(',
      choice(
        seq($.column_headers, $.anonymous_table_separator, $.data_rows),
        $.data_rows,
      ),
      ')',
    ),

    // Definition necks: :- (rule/view) and := (data/table)
    definition_neck: $ => choice(
      alias(token(':-'), $.session_neck),
      alias(token(':='), $.temporary_table_neck),
    ),

    // Override annotation_body from DQL grammar: use ddl_body_content for generic
    // annotations so that *, /, ! etc. are allowed inside (~~docs ... ~~) blocks.
    // The DQL grammar's comment_content can't handle these because tree-sitter's
    // lexer matches * as a keyword token. ddl_body_content works because it's
    // already used in ddl_annotation where the same issue exists.
    annotation_body: $ => choice(
      $.assertion_annotation,
      $.error_annotation,
      $.emit_annotation,
      $.danger_annotation,
      $.option_annotation,
      seq(
        '(~~',
        field('hook_name', $.identifier),
        optional(field('hook_data', $.ddl_body_content)),
        '~~)'
      )
    ),

    // ER-rule definition: left_table & right_table(*) within context neck body
    // REMOVED dialect — still parses so the builder can refuse with the
    // teaching naming the edge-declaration form below.
    er_rule_definition: $ => seq(
      field('left_table', $.identifier),
      '&',
      field('right_table', $.identifier),
      '(', '*', ')',
      'within',
      field('context', $.identifier),
      field('neck', $.definition_neck),
      optional(field('doc', $.annotation_body)),
      field('body', $.query),
    ),

    // ER EDGE declaration (GROUNDING-AND-MENTION.md): a ground-instance
    // clause wearing operator fixity — both terms and the context ground
    // on mentions the surface form inserts:
    //   people(*) &(::normal) orders(*) :- BODY
    // token.immediate('(') after '&': the context form is spelled `&(`
    // with no space, leaving `& ` to the removed dialect's teaching.
    er_edge_definition: $ => seq(
      field('left_term', $.table_access),
      '&',
      token.immediate('('),
      field('context', $.symbol),
      ')',
      field('right_term', $.table_access),
      field('neck', $.definition_neck),
      optional(field('doc', $.annotation_body)),
      field('body', $.query),
    ),

    // Query statement: ?- query
    query_statement: $ => seq(
      '?-',
      field('query', $.query),
    ),
  },
});
