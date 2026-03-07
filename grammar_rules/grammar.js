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
    // DDL definition rules create new ambiguities with inherited DQL rules

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
    source_file: $ => repeat1(choice($.definition, $.query_statement, $.ddl_annotation)),

    // === DDL-specific rules (the only reason this grammar exists) ===

    definition: $ => choice(
      $.function_definition,
      $.constant_definition,  // nl :- char:(10) — zero-arity function without parens
      $.ho_fact_definition,   // name(ho_params)(data) — before fact for priority
      $.fact_definition,
      $.sigma_definition,
      $.ho_view_definition,
      $.argumentative_view_definition,
      $.view_definition,
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

    // Function parameter: identifier with optional guard expression
    function_param: $ => choice(
      $.identifier,
      seq(
        field('param_name', $.identifier),
        '|',
        field('guard', $.domain_expression),
      ),
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
      // Ground scalar literal: "value" or 42
      field('ground_value', choice($.string_literal, $.number_literal)),
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

    // View head item: free variable (identifier) or ground term (literal)
    view_head_item: $ => choice(
      $.identifier,
      $.string_literal,
      $.number_literal,
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

    // View definition: name(*) neck [docs] query
    view_definition: $ => seq(
      field('name', $.identifier),
      '(', '*', ')',
      field('neck', $.definition_neck),
      optional(field('doc', $.annotation_body)),
      field('body', $.query),
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
    // After the first identifier, '&' uniquely identifies ER-rule
    // (all other definitions expect '(' or ':(').
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

    // Query statement: ?- query
    query_statement: $ => seq(
      '?-',
      field('query', $.query),
    ),
  },
});
