// DelightQL Tree-sitter Grammar - Minimal Semantic Version
// Starting with just: db.users(*) |> [users.id].

// Helper function for comma-separated lists with at least one element
function sep1(separator, rule) {
  return seq(
    rule,
    repeat(seq(separator, rule))
  );
}

module.exports = grammar({
  name: 'delightql_v2',
  
  // TEST MARKER: GRAMMAR VERSION ABC
  
  extras: $ => [
    /\s/,           // whitespace
    $.comment,      // line comments
  ],

  conflicts: $ => [
    // CTE binding conflict: When identifier: appears, could be lvar (end of expr) or function_call (start)
    // This is the item-level conflict similar to TVF fix
    [$.lvar, $.function_call],
    // The real conflict is between the items within the lists
    [$.column_spec_item, $.tvf_argument],
    [$.literal, $.tvf_argument],
    // (removed: ho_view_definition - definitions belong in grammar_rules)
    // Conflict between column ordinal/range and filter pipe in function arguments
    [$.column_ordinal, $.column_range, $.lvar],
    // EPOCH 7: Allow GLR to fork when identifier could be header or data
    // Similar to TVF bug - need conflict at item level, not top level
    [$.lvar, $.column_header_item],
    // Conflict between parenthesized predicates and tuple expressions
    [$.paren_predicate, $.domain_expression],
    // EPOCH 7.1: Full domain expressions in data rows can conflict with headers
    [$.domain_expression, $.column_header_item],
    // Window functions: + in frame_bound conflicts with + in exists_marker
    [$.frame_bound, $.exists_marker],
    // (removed: view_definition conflicts - definitions belong in grammar_rules)
    // STAR-AS-SCOPE-ASSIGNER: bare * in CTE head vs qualify_operator in body
    [$.cte_definition, $.qualify_operator],
    [$.glob, $.qualify_operator],
    // (removed: glob_spec vs qualify_operator - no longer overlap after functor paren uniformity)
    // (removed: function_definition conflicts - definitions belong in grammar_rules)
    // Namespace paths: identifier could be lvar or start of namespace_path
    [$.lvar, $.namespace_path],
    // Passthrough/divide ambiguity: identifier followed by / could be lvar (for binary divide)
    // or scalar_subquery namespace (for passthrough separator). GLR forks and prunes.
    [$.scalar_subquery, $.lvar, $.table_access],
    [$.scalar_subquery, $.lvar],
    // Grounded namespace: identifier could start namespace_path or grounded_namespace
    [$.grounded_namespace, $.namespace_path],
    // Grounded namespace vs lvar: identifier at start could be either
    [$.lvar, $.namespace_path, $.grounded_namespace],
    // EPOCH 5: No longer needed - unified parenthesized and tuple into single rule
    // CTE ambiguity: table_access could be part of inline CTE or start of definition CTE
    // GLR needs to fork at identifier(column_spec) when followed by ':'
    [$.cte_definition, $.table_access],
    // PATH FIRST-CLASS: Root path '.' can conflict with path literal starting with identifier
    // When seeing '. identifier', could be: (.) followed by identifier, or (.identifier)
    [$.path_literal],
    // Pseudo-predicates: identifier! conflicts with identifier followed by NOT expression
    // GLR should fork when seeing identifier followed by ! to try both interpretations
    [$.pseudo_predicate_call, $.not_expression],
    // Bang pipe operation: inside first paren, identifier could be table name (DML)
    // or lvar/function in domain_expression (directive). Tree-sitter resolves
    // deterministically but declaration kept for documentation.
    // [$.table_access, $.pseudo_predicate_argument_list],  // unnecessary per tree-sitter
    // Anonymous table in DML pipe: data_rows vs sparse_fill inside anonymous_table
    [$.data_rows, $.sparse_fill],
    // Namespace-qualified function call vs scalar subquery: both share ns.identifier:( prefix
    // Disambiguation happens at first token inside parens (domain expr vs relational continuation)
    [$.function_call, $.scalar_subquery],
    // HO argument list: ho_argument_row has same structure as argument_list (comma-separated tvf_args)
    [$.argument_list, $.ho_argument_row],
    // Catalog functor: identifier could start catalog_functor (main::) or table_access (main. or main/) or CTE
    [$.catalog_functor, $.table_access],
    [$.catalog_functor, $.table_access, $.cte_definition],
    // Stropped identifier: `foo` could be catalog_functor catalog_name or identifier (via stropped_identifier)
    [$.catalog_functor, $.identifier],
    // Catalog functor: namespace_path repeat could continue or stop before trailing ::
    [$.namespace_path],
    // DDL-only input: ddl_annotation could be standalone (source_file) or query preamble
    [$.source_file, $.query],
  ],

  word: $ => $._bare_identifier,

  rules: {
    // Main entry point - queries and REPL commands only.
    // Definitions (:-) belong in DDL consult files (grammar_rules/).
    source_file: $ => choice(
      $.repl_command,            // REPL commands
      repeat1($.ddl_annotation), // DDL-only input (no query)
      repeat1($.query)           // One or more queries (handles single query too)
    ),

    // Alternative entry point for non-REPL contexts (future use)
    query_only: $ => $.query,

    // Definition: top-level choice between function, HO view, and view definitions
    // Let the grammar do the work - each has its own node type
    // DDL definitions (view_definition, ho_view_definition, function_definition,
    // definition_neck, query_statement) belong in grammar_rules/, not here.
    // The DQL grammar handles queries and REPL commands only.

    // Comment rule - high precedence to match before divide operator
    comment: $ => token(prec(10, /\/\/[^\r\n]*/)),

    // A query has optional ER-context directive, optional CTEs/CFEs/DDL annotations, then a relational expression
    query: $ => seq(
      optional($.er_context_directive),
      repeat(choice($.cte_inline, $.cte_definition, $.cfe_definition, $.ddl_annotation)),     // Zero or more CTE/CFE/DDL definitions (intermixed)
      $.relational_expression    // The main query
    ),

    // ER-context directive: under context_name:
    // Scopes the query to use ER-rules from the named context
    er_context_directive: $ => seq(
      'under',
      field('context', $.er_context_path),
      ':'
    ),

    // ER-context path: bare name or namespace-qualified
    er_context_path: $ => choice(
      seq(field('namespace', choice($.grounded_namespace, $.namespace_path, $.identifier)),
          '.', field('name', $.identifier)),   // ns.context or ns::sub.context
      field('name', $.identifier),              // bare context (after engage)
    ),
    
    // REPL-only commands for creating temporary tables/views
    // Parsed by source_file but only processed when using parse_repl_input() builder
    repl_command: $ => choice(
      // query -: name (creates temporary view)
      seq(
        $.query,
        '-:',
        field('temp_view_name', $.identifier)
      ),
      // query =: name (creates temporary table)
      seq(
        $.query,
        '=:',
        field('temp_table_name', $.identifier)
      )
    ),
    
    // CTE inline-style: expression : name
    // Example: users(*), age > 30 : adults
    cte_inline: $ => prec.dynamic(1, seq(
      $.relational_expression,
      ':',
      field('name', $.identifier)
    )),

    // CTE definition-style: name(columns): expression
    // Example: adults(*): users(*), age > 30
    // Head accepts either * (all columns) or explicit column names
    cte_definition: $ => prec.dynamic(0, seq(
      field('name', $.identifier),
      '(',
      choice('*', field('columns', $.column_spec)),
      ')',
      ':',
      $.relational_expression
    )),

    // CFE (Common Function Expression) definition: name:(param1, param2) : expression
    // Defines reusable parameterized expressions
    // Example: full_name:(first_name, last_name) : (first_name || ' ' || last_name)
    //
    // HIGHER-ORDER CFE: name:(curried_params)(regular_params) : expression
    // Curried params are functions/lambdas used as "code" templates
    // Example: apply_transform:(transform)(value) : value /-> transform:()
    cfe_definition: $ => seq(
      field('name', $.identifier),
      token.immediate(':('),  // Compound token - requires both : and ( with no space
      optional(field('first_params', $.cfe_first_param_list)),
      ')',
      optional(seq(
        '(',
        optional(field('second_params', $.cfe_parameter_list)),
        ')'
      )),
      ':',
      field('body', $.domain_expression)
    ),

    // First parameter list: can be curried (callable) or regular (identifiers)
    // When second parens present: these are curried params (must be callable)
    // When second parens absent: these are regular params (identifiers)
    // CCAFE: Can optionally start with context marker (..) or (..{list})
    cfe_first_param_list: $ => choice(
      // Context marker followed by params: .., param1, param2
      seq(
        field('context_marker', $.context_marker),
        optional(seq(',', field('params_after_context', sep1(',', choice($.callable_param, $.identifier)))))
      ),
      // Just params without context marker: param1, param2
      sep1(',', choice($.callable_param, $.identifier))
    ),

    // CCAFE context marker: .. (implicit) or ..{list} (explicit)
    context_marker: $ => choice(
      // Implicit context: .. (auto-discover from body)
      '..',
      // Explicit context: ..{id1, id2, ...} (declared context params, can be empty)
      seq('..', '{', optional(field('context_params', sep1(',', $.identifier))), '}')
    ),

    // Callable parameter: function call or lambda (anonymous function in pipe)
    callable_param: $ => choice(
      $.function_call,
      $.piped_expression  // Lambdas appear as piped expressions: :(@ + 1)
    ),

    // CFE parameter list: comma-separated identifiers (for regular/second params)
    // In HOCFEs, second params can also have context marker: (x):(..{ctx}, y):
    cfe_parameter_list: $ => choice(
      // Context marker followed by params: .., param1, param2 OR ..{ctx1, ctx2}, param1
      seq(
        field('context_marker', $.context_marker),
        optional(seq(',', sep1(',', $.identifier)))
      ),
      // Just params without context marker: param1, param2
      sep1(',', $.identifier)
    ),
    
    // RECURSIVE GRAMMAR: Core relational expression
    relational_expression: $ => prec.left(seq(
      $.base_expression,
      optional($.relational_continuation)
    )),
    
    // Base expressions - the atomic units (tables only, no predicates!)
    // Predicates can only appear after commas in continuations
    base_expression: $ => choice(
      prec(3, $.pseudo_predicate_call),  // Highest precedence for pseudo-predicates (! suffix is unambiguous)
      prec(2, $.catalog_functor),        // Catalog functor: ns::(*)
      prec(2, $.table_access),           // Higher precedence for table access (passthrough via / separator)
      prec(1, $.tvf_call),               // Lower precedence for TVF calls
      $.anonymous_table
      // NO predicates here - they can only appear after commas
    ),
    
    // Continuation after base expression
    relational_continuation: $ => choice(
      seq(
        repeat($.annotation),  // Annotations are siblings, builder will skip them
        choice(
          $.binary_operator_expression,
          $.unary_operator_expression
        )
      ),
      repeat1($.annotation)  // Trailing annotations at end of expression
    ),
    
    // Annotations that can appear at valid query points
    annotation: $ => choice(
      $.smart_comment,    // (/* ... */) - inline documentation
      $.stop_point,       // (!) or (/! ... !/) - execution stops here
      $.annotation_body,         // (~~identifier ... ~~) - annotations for extensions
      $.debug_point,      // >>> - debugging breakpoint
    ),
    
    // Smart comments carry documentation through the pipeline
    smart_comment: $ => seq(
      '(/*',
      optional($.comment_content),
      '*/)'
    ),
    
    // Stop points mark where execution should terminate
    stop_point: $ => choice(
      '(!)',                           // Simple stop
      seq('(/!', $.stop_reason_content, '!/)') // Stop with reason
    ),
    
    // Annotation body: dispatches to specific annotation types
    annotation_body: $ => choice(
      // Assertion annotation: body is structured DQL
      $.assertion_annotation,
      // Error annotation: expected compilation error with URI
      $.error_annotation,
      // Emit annotation: fan-out to named sink
      $.emit_annotation,
      // Danger annotation: named safety gate control
      $.danger_annotation,
      // Option annotation: strategy/preference selection
      $.option_annotation,
      // Generic annotation: body is raw text
      seq(
        '(~~',
        field('hook_name', $.identifier),  // The hook identifier
        optional(field('hook_data', $.comment_content)),  // Optional data for the hook
        '~~)'
      )
    ),

    // Assertion annotation: (~~assert <continuation> ~~)
    // Optionally named: (~~assert:"name" <continuation> ~~)
    // The body is parsed as structured DQL (not raw text), giving full
    // CST support for syntax highlighting, error detection, and tooling.
    // The body ends with an assertion view (exists(*), forall(*), etc.)
    // reached via a normal |> pipe inside the relational_continuation.
    assertion_annotation: $ => seq(
      '(~~assert',
      optional(seq(':', field('assertion_name', $.string_literal))),
      field('assertion_body', $.relational_continuation),
      '~~)'
    ),

    // Error annotation: (~~error ~~) or (~~error://uri/path ~~)
    // Asserts that the query should fail compilation with an error
    // matching the URI prefix. No body — just the URI.
    error_annotation: $ => seq(
      '(~~error',
      optional(seq(
        token.immediate('://'),
        field('error_uri', $.error_uri_path)
      )),
      '~~)'
    ),

    // Emit annotation: (~~emit:name ~~) or (~~emit:name , predicate ~~)
    // Fan-out: the relation at this point is forked and optionally
    // filtered by the body continuation. The host routes the resulting
    // rows to a named sink (file, socket, stderr).
    // Inline URI form: (~~emit://file/a/csv/out.csv ~~) — same ://path convention
    // as error/danger/option annotations.
    // prec(1) ensures this wins over the generic annotation when the name is "emit".
    emit_annotation: $ => prec(1, seq(
      '(~~emit',
      optional(choice(
        seq(token.immediate('://'), field('emit_uri', $.emit_uri_path)),
        seq(':', field('emit_name', $.identifier)),
      )),
      optional(field('emit_body', $.relational_continuation)),
      '~~)'
    )),
    
    // Danger annotation: (~~danger://uri/path STATE~~)
    // Opens or closes a named safety gate for the current query.
    // STATE is required (ON, OFF, ALLOW, or 1-9). Bare form is a parse error.
    // prec(1) ensures this wins over the generic annotation.
    danger_annotation: $ => prec(1, seq(
      '(~~danger',
      token.immediate('://'),
      field('danger_uri', $.error_uri_path),
      field('danger_state', $.danger_toggle),
      '~~)'
    )),

    // Option annotation: (~~option://uri/path STATE~~)
    // Strategy/preference selection for the current query.
    // Same toggle syntax as danger gates: ON, OFF, ALLOW, or 1-9.
    // prec(1) ensures this wins over the generic annotation.
    option_annotation: $ => prec(1, seq(
      '(~~option',
      token.immediate('://'),
      field('option_uri', $.error_uri_path),
      field('option_state', $.danger_toggle),
      '~~)'
    )),

    // Inline DDL annotation: (~~ddl ... ~~) or (~~ddl:"namespace" ... ~~)
    // DDL annotations are preambles — they define entities before the query runs.
    // They belong alongside CTEs and CFEs in the query rule, not inside
    // relational_continuation (where other annotations live).
    ddl_annotation: $ => choice(
      prec(1, seq('(~~ddl:', field('ddl_namespace', $.string_literal), field('ddl_body', $.ddl_body_content), '~~)')),
      prec(1, seq('(~~ddl', field('ddl_body', $.ddl_body_content), '~~)')),
    ),

    // Matches everything up to ~~) — the regex cannot consume ~~) itself.
    // ([^~]|~[^~]|~~[^)]) covers: non-tilde, tilde+non-tilde, two-tildes+non-paren.
    ddl_body_content: $ => /([^~]|~[^~]|~~[^)])+/,

    // Toggle value for danger/option gates
    danger_toggle: $ => choice(
      'ON', 'OFF', 'ALLOW',
      /[1-9]/
    ),

    // Debug points for breakpoint/logging
    debug_point: $ => '>>>',

    // URI path for error hooks: slash-separated identifiers
    error_uri_path: $ => seq(
      $.identifier,
      repeat(seq('/', $.identifier))
    ),

    // URI path for emit destinations: allows dots, hyphens, underscores, slashes
    // e.g. file/a/csv/out.csv or file/w/jsonl//tmp/abs.jsonl or stderr/jsonl
    emit_uri_path: $ => /[a-zA-Z0-9_.\/\-]+/,
    
    // Opaque text for smart comments: (/* ... */) — stops before */)
    comment_content: $ => /([^*]|\*[^\/]|\*\/[^)])+/,

    // Opaque text for stop-with-reason: (/! ... !/) — stops before !/)
    stop_reason_content: $ => /([^!]|![^\/]|!\/[^)])+/,

    
    // Binary operator (comma) continues with another relational expression
    // Binary operators - different rules for different operators
    binary_operator_expression: $ => choice(
      // Comma can have any continuation (predicates, tables, etc.)
      seq(
        field('operator', $.comma_operator),
        $.continuation_expression
      ),
      // ER-join operators: & (direct) and && (transitive)
      seq(
        field('operator', $.er_transitive_join_operator),  // && before & (greedy match)
        $.base_expression,
        optional($.relational_continuation)
      ),
      seq(
        field('operator', $.er_join_operator),
        $.base_expression,
        optional($.relational_continuation)
      ),
      // Set operators must have table expressions
      seq(
        field('operator', choice(
          $.union_all_positional,
          $.union_corresponding,
          $.smart_union_all,
          $.minus_corresponding,
        )),
        $.base_expression,  // Must be a table/base expression
        optional($.relational_continuation)  // Can continue after
      )
    ),

    // ER-join operators
    er_join_operator: $ => '&',
    er_transitive_join_operator: $ => '&&',
    
    // Comma operator for natural join/cross product
    comma_operator: $ => $._comma,

    // Set operators with principled semantics
    union_all_positional: $ => '||',      // Pure positional union all
    union_corresponding: $ => ';',         // Union by name with NULL padding
    smart_union_all: $ => '|;|',          // Same names, different order
    minus_corresponding: $ => '-',         // Minus/except by name
    
    // After a comma, we allow predicates, limits, using clauses, or more tables
    continuation_expression: $ => seq(
      $.continuation_base,
      optional($.relational_continuation)
    ),
    
    // Base expressions that can appear after a comma
    // Order matters! Check special cases before predicates
    continuation_base: $ => choice(
      $.ordering,                 // ORDER BY: #(...) - must come before limit_offset
      $.limit_offset,             // Check limit/offset (starts with #)
      $.table_access,             // Check tables (have specific syntax, passthrough via / separator)
      $.tvf_call,                 // TVFs can appear after comma
      $.anonymous_table,          // Anonymous tables start with _
      $.tuple_expression,         // EPOCH 5: Tuples (for multi-column IN, etc)
      // $.parenthesized_expression, // REMOVED - parenthesized exprs only appear INSIDE other exprs
      $.predicate                 // Predicates last - includes AND with comma
    ),
    
    // Unary operators (pipes) transform the relation
    unary_operator_expression: $ => choice(
      seq($.pipe_operator, $.unary_operator, optional($.relational_continuation)),
      seq($.aggregate_pipe_operator, $.aggregate_function, optional($.relational_continuation)),  // Allow continuation for CPR
      seq($.materialize_pipe_operator, optional($.relational_continuation)),
      prec.right(1, seq($.meta_ize_operator, optional($.relational_continuation))),  // ^ or ^^ for schema reification
      prec.right(1, seq($.qualify_operator, optional($.relational_continuation))),  // * for qualification (no pipe needed)
      prec.right(1, seq($.using_operator, optional($.relational_continuation))),    // .(cols) for USING semantics
      prec.right(1, seq($.drill_operator, optional($.relational_continuation))),    // .col(*) for interior drill-down
    ),

    // Meta-ize: reify schema as relation
    // ^ returns (colname, colposition, coltype)
    // ^^ returns full DDL metadata (colname, colposition, coltype, nullable, default, pk, fk_table, fk_column)
    meta_ize_operator: $ => choice('^^', '^', '+', '$'),  // ^^ first to avoid prefix match; + = constraint, $ = default

    // Qualify: marks columns as qualified (table-prefixed)
    // Unqualified names from () unify; qualified names from * don't
    qualify_operator: $ => '*',

    // USING operator: .(cols) - leftward search, unify, dedupe
    // Replaces *{cols} syntax to avoid conflict with tree group {}
    using_operator: $ => seq(
      '.',
      '(',
      $.using_column_list,
      ')'
    ),

    using_column_list: $ => seq(
      $.identifier,
      repeat(seq($._comma, $.identifier))
    ),

    // Interior drill-down: .column_name(*) or .column_name(col1, col2)
    // Explodes an interior relation (tree group) column into rows.
    // Disambiguated from using_operator by the identifier between . and (
    drill_operator: $ => seq(
      '.',
      field('column', $.identifier),
      '(',
      choice(
        field('glob', $.glob_spec),
        field('columns', $.column_spec),
      ),
      ')',
    ),

    // Narrowing destructure: .column_name{.field1, .field2}
    // Iterates a JSON array column and extracts named fields from each element.
    // Replaces the entire row with just the extracted fields (no context carry-forward).
    // Disambiguated from drill_operator by { vs ( after .identifier.
    narrowing_destructure: $ => seq(
      '.',
      field('column', $.identifier),
      '{',
      field('members', $.narrowing_member_list),
      '}',
    ),

    narrowing_member_list: $ => seq(
      choice($.path_literal, $.identifier),
      repeat(seq(',', choice($.path_literal, $.identifier))),
    ),

    // Semantic nodes for pipe operators
    pipe_operator: $ => '|>',
    aggregate_pipe_operator: $ => '~>',
    materialize_pipe_operator: $ => '|*>',
    
    // What follows |>
    unary_operator: $ => $.pipe_operation,

    // Metadata-oriented tree group: column:~> {...}
    // Data values become JSON object keys (aggregate context only)
    // Can be nested: country:~> status:~> {...}
    metadata_tree_group: $ => seq(
      field('key', $.lvar),           // The column that becomes keys
      choice(
        token.immediate(':~>'),         // No space before :~>
        token.immediate('::')
      ),
      choice(
        $.curly_function,             // Most common: country:~> {...}
        $.bracket_function,           // Also valid: country:~> [...]
        $.array_destructure_pattern,  // Array destructuring: country:~> [.0, .1]
        $.metadata_tree_group,        // Nested: country:~> status:~> {...}
        $.placeholder                 // Keys only (no explosion): country:~> _
      )
    ),

    // What follows |~> (aggregate functions or piped expressions, optionally with alias)
    // A piped expression ending in a function IS semantically a function
    // Note: piped_expression can start with a column identifier, so we need to handle
    // the ambiguity where an identifier could be either:
    // 1. The start of a function_call (e.g., sum:(total))
    // 2. The start of a piped_expression (e.g., total /-> :(@ / 100) /-> sum:())
    aggregate_function: $ => seq(
      choice(
        $.piped_expression,  // Try piped_expression first (includes column /-> ... patterns)
        $.function_call,     // Then try function_call (includes count_star)
        $.metadata_tree_group  // Metadata-oriented tree groups
      ),
      optional(seq($._as, field('alias', $.identifier)))
    ),

    // REMOVED: comma_sequence - now handled by recursive structure
    // Old comma_continuation logic is now part of the recursive grammar:
    // - If comma followed by table → Join  
    // - If comma followed by predicate → Sigma
    // - limit_offset and using_clause will need special handling
    
    // Limit and offset: #<N and #>N (integers only!)  
    limit_offset: $ => seq(
      '#',
      field('operator', choice('<', '>')),
      field('value', choice($.integer_literal, $.identifier))
    ),
    
    // REMOVED: Old USING clause syntax ={...} - replaced with .(cols) operator
    
    // Domain expression - all value-producing expressions
    domain_expression: $ => seq(
      choice(
        $.literal,
        $.lvar,  // Was column_ref
        $.function_call,
        $.glob,
        $.binary_expression,
        $.predicate,  // Better name - predicates produce booleans
        $.tuple_expression,  // EPOCH 5: Handles both (expr) and (expr, expr, ...)
        $.bracket_function,  // Bracket functions like [1] or [1, 2]
        $.column_ordinal,  // Column ordinal reference: |1|, |-1|, users|2|
        $.column_range,  // Column range reference: |1:3|, |2:|, |:-1|, users|1:5|
        $.value_placeholder,  // @ placeholder for value in transforms/lambdas
        $.pattern_literal,  // Pattern for column matching: /_name/
        $.string_template,  // String template with interpolation: :"Hello {name}"
        $.piped_expression,  // Value-level functional pipe: (expr /-> func /-> func)
        $.case_expression,  // CASE expression: _:(cond -> result; ...)
        $.scalar_subquery,  // Scalar subquery: relation:(inner-cpr)
        $.metadata_tree_group,  // Metadata tree group: column:~> {...}
        $.pivot_expression,     // Pivot: score of subject
        $.sparse_fill,          // Sparse fill: _(col @ val) in sparse anonymous table rows
        $.citation              // Citation: :nl, :tab (zero-arity call via :name)
        // PATH FIRST-CLASS: path_literal removed from domain_expression to avoid conflict
        // Paths only available via specific rules: curly_function_member, function arguments
      ),
      optional(seq($._as, field('alias', $.lvar)))
    ),

    // Distinct expression for inner-distinct in aggregates: %column or %(expression)
    // High precedence (10) to parse before binary expressions
    distinct_expression: $ => prec(10, seq(
      '%',
      choice(
        $.lvar,  // Simple case: %column_name
        seq($._lparen, $.domain_expression, $._rparen)  // Complex case: %(expression)
      )
    )),

    // Non-binary domain expression - domain expression without top-level binary_expression
    // Used in binary/comparison operands to enforce parenthesization (Pony rule)
    non_binary_domain_expression: $ => prec.left(2, seq(
      choice(
        $.literal,
        $.lvar,
        $.function_call,
        $.glob,
        // NOTE: binary_expression excluded here - must be parenthesized
        // NOTE: piped_expression excluded here - PONY rule enforcement
        //       Use parentheses: age * (col /-> f:()) instead of age * col /-> f:()
        $.predicate,  // Predicates are allowed (they're not binary expressions)
        $.tuple_expression,  // EPOCH 5: Now handles both parenthesized and tuple
        $.bracket_function,
        $.column_ordinal,  // Ordinals allowed in comparisons
        $.column_range,
        $.value_placeholder,  // @ placeholder for value in transforms/lambdas
        $.pattern_literal,  // Pattern for column matching: /_name/
        $.string_template,  // String template allowed in operands
        $.case_expression,  // CASE expression allowed in operands
        $.scalar_subquery,  // EPOCH 7: Scalar subqueries allowed (e.g., in IN lists)
        $.metadata_tree_group,  // EPOCH 4: Metadata tree groups allowed in destructuring
        $.array_destructure_pattern,  // ARRAY DESTRUCTURING: Allow [.0, .1, .2] after ~=
        $.citation                    // Citation: :nl, :tab (zero-arity call via :name)
        // PATH FIRST-CLASS: path_literal removed to avoid conflict with curly_function_member
      ),
      optional(seq($._as, field('alias', $.identifier)))
    )),


    // Glob - the star (can be qualified like o.*)
    glob: $ => choice(
      '*',  // Simple glob
      seq(
        field('qualifier', $.identifier), 
        token.immediate('.'),  // No space before dot
        '*'
      )  // Qualified glob like o.*
    ),
    
    // Simple expressions for binary/comparison operands (no recursion to enforce Pony rule)
    simple_expression: $ => choice(
      $.literal,
      $.lvar,
      $.function_call,
      $.glob,
      $.pattern_literal,
      $.parenthesized_expression
    ),
    
    // Binary arithmetic - no chaining without parens (Pony rule)
    binary_expression: $ => prec.left(2, seq(
      field('left', $.non_binary_domain_expression),
      field('operator', $.binary_operator),
      field('right', $.non_binary_domain_expression)
    )),
    
    // Binary operators with semantic node names
    // Note: token.immediate('/') added as divide alternative because the lexer
    // may emit it instead of plain '/' when no whitespace precedes the slash
    // (passthrough_separator poisoning). Both forms map to $.divide.
    binary_operator: $ => choice(
      alias('+', $.add),
      alias('-', $.subtract),
      alias('*', $.multiply),
      alias('/', $.divide),
      alias(token.immediate('/'), $.divide),
      alias('%', $.modulo),
      alias('++', $.concat)  // String concatenation (changed from ||)
    ),
    
    // Parentheses allow full recursion (keep visible - they're semantic!)
    parenthesized_expression: $ => seq(
      '(',  // Parentheses for grouping/precedence only
      $.domain_expression,
      ')'
    ),

    // EPOCH 5: Parenthesized or tuple expression
    // (expr) - single expression in parens
    // (expr1, expr2, ...) - tuple with multiple expressions
    // This unifies both patterns to avoid ambiguity
    tuple_expression: $ => prec(1, seq(
      '(',
      $.domain_expression,
      repeat(seq($._comma, $.domain_expression)),  // Zero or more additional elements
      ')'
    )),

    // Bracket function: [1] or [1, 2, 3] as a domain expression
    bracket_function: $ => seq(
      '[',
      $.domain_expression_list,
      ']'
    ),

    // Curly function (INTERIOR-RECORD): {name, "key": value, "nested": ~> {...}}
    curly_function: $ => seq(
      '{',
      optional($.curly_function_members),
      '}'
    ),

    curly_function_members: $ => seq(
      $.curly_function_member,
      repeat(seq(',', $.curly_function_member))
    ),

    curly_function_member: $ => choice(
      // PATH FIRST-CLASS: Path literal with optional alias
      // Examples: {.scripts.dev} or {.name_info.last_name as ln}
      // MUST BE FIRST with very high precedence (10) and left assoc
      prec.left(10, seq(
        field('path', $.path_literal),
        optional(seq(
          token.immediate('as'),
          field('alias', $.identifier)
        ))
      )),

      // Explicit key-value pair with group inducer (metadata tree group)
      prec(2, seq(
        field('key', choice(
          $.string_literal,  // "FirstName"
          $.identifier       // country (becomes "country")
        )),
        ':',
        '~>',
        field('value', $.group_inducer)
      )),

      // Aggregate TVar capture (destructor-only): "key": ~> identifier
      // Only valid in destructuring context, not in construction
      // Note: This looks like regular KeyValue with ~>, but value is lvar not group_inducer
      // Parser will detect this pattern and handle appropriately
      prec(2, seq(
        field('key', $.string_literal),
        ':',
        '~>',
        field('value', $.lvar)  // Plain identifier, not group_inducer (self-documenting)
      )),

      // Explicit key-value pair (regular)
      // ONLY string literals allowed as keys (identifiers only valid with ~> for metadata tree groups)
      prec(2, seq(
        field('key', $.string_literal),  // "FirstName" - no unquoted identifiers!
        ':',
        field('value', $.domain_expression)  // ANY domain expression
      )),

      // Shorthand with predicate: {country="USA", age > 18}
      // Grammar accepts any comparison, resolver must validate one side is an lvar in scope
      prec(1, $.comparison),

      // NEW: Ergonomic inductors (TG-ERGONOMIC-INDUCTOR)
      $.glob_spec,        // {*} - all columns
      $.pattern_literal,  // {/pattern/} - pattern-matched columns
      $.column_range,     // {|1:4|} - ordinal range
      $.placeholder,      // {_} - wildcard (explode but don't extract fields)

      // Shorthand: {name, email, t.column} → {"name": name, "email": email, "column": t.column}
      // IMPORTANT: Only lvars (simple or qualified columns), NOT arbitrary expressions
      $.lvar
    ),

    // ARRAY DESTRUCTURING: Array destructure pattern [.0, .1, .2]
    // Used with ~= operator: coords ~= [.0 as x, .1 as y, .2 as z]
    array_destructure_pattern: $ => seq(
      '[',
      optional(seq(
        $.array_destructure_member,
        repeat(seq(',', $.array_destructure_member)),
        optional(',')  // Trailing comma allowed
      )),
      ']'
    ),

    // Array destructure member: .0, .1 as x, .2 as y
    array_destructure_member: $ => seq(
      field('index', $.path_literal),  // Must be path literal like .0, .1, .2
      optional(seq(
        token.immediate('as'),
        field('alias', $.identifier)
      ))
    ),

    // Group inducer wraps a tree group function to indicate it needs inductive evaluation
    // Used in: "key": ~> {columns...} or "key": ~> [.0, .1] or "key": ~> column:~> {...}
    group_inducer: $ => choice(
      $.curly_function,
      $.bracket_function,
      $.array_destructure_pattern,  // Allow array destructuring patterns: ~> [.0, .1]
      $.metadata_tree_group  // Allow metadata tree groups inside regular tree groups
    ),

    // Column ordinal reference: |1|, |-1|, or qualified like users|2|, db.users|-1|
    // Also accepts |identifier| to produce a clear semantic error instead of a parse error.
    column_ordinal: $ => choice(
      // Simple ordinal: |1|, |-1|, or |identifier| (rejected semantically)
      seq(
        '|',
        field('position', choice(
          $.integer_literal,
          seq('-', $.integer_literal),  // Negative indexing
          $.identifier                  // Caught as semantic error in builder
        )),
        '|'
      ),
      // Qualified ordinal: users|2|, db.users|-1|, catalog.db.users|3|
      // Also supports glob ordinal: users|*| (all columns by position)
      seq(
        field('qualifier', seq(
          $.identifier,
          repeat(seq(
            token.immediate('.'),
            $.identifier
          ))
        )),
        '|',
        field('position', choice(
          $.integer_literal,
          seq('-', $.integer_literal),  // Negative indexing
          '*'                           // Glob ordinal (all columns)
        )),
        '|'
      )
    ),
    
    // Column range reference: |1:3|, |2:|, |:-1|, or qualified like users|1:5|
    // Note: At least one of start or end must be present (no |:|)
    column_range: $ => choice(
      // Simple range: |1:3|, |2:|, |:5| (but NOT |:|)
      seq(
        '|',
        choice(
          // Has start, maybe end: |2:| or |2:5|
          seq(
            field('start', choice(
              $.integer_literal,
              seq('-', $.integer_literal)  // Negative start
            )),
            ':',
            optional(field('end', choice(
              $.integer_literal,
              seq('-', $.integer_literal)  // Negative end
            )))
          ),
          // No start, has end: |:5|
          seq(
            ':',
            field('end', choice(
              $.integer_literal,
              seq('-', $.integer_literal)  // Negative end
            ))
          )
        ),
        '|'
      ),
      // Qualified range: users|1:3|, db.users|:-1|
      seq(
        field('qualifier', seq(
          $.identifier,
          repeat(seq(
            token.immediate('.'),
            $.identifier
          ))
        )),
        '|',
        choice(
          // Has start, maybe end: users|2:| or users|2:5|
          seq(
            field('start', choice(
              $.integer_literal,
              seq('-', $.integer_literal)  // Negative start
            )),
            ':',
            optional(field('end', choice(
              $.integer_literal,
              seq('-', $.integer_literal)  // Negative end
            )))
          ),
          // No start, has end: users|:5|
          seq(
            ':',
            field('end', choice(
              $.integer_literal,
              seq('-', $.integer_literal)  // Negative end
            ))
          )
        ),
        '|'
      )
    ),
    
    // Predicates - expressions that produce boolean values
    // Now supports AND/OR combinations
    // NOTE: Semicolon (;) as OR requires parentheses
    predicate: $ => choice(
      $.or_expression,     // Lowest precedence (no semicolon at top level)
      $.and_expression,    // Higher precedence than OR
      $.atomic_predicate   // Highest precedence (base cases)
    ),

    // OR expression - precedence 3 (higher than non_binary_domain_expression)
    // NOTE: Semicolon NOT allowed at top level - must use OR/or, or wrap in parens
    or_expression: $ => prec.left(3, seq(
      field('left', $.predicate),
      field('operator', choice('OR', 'or')),  // SQL-style only at top level
      field('right', $.predicate)
    )),

    // AND expression - precedence 4 (higher than OR, and higher than non_binary_domain_expression)
    // SQL-style only - comma handled contextually in builder
    and_expression: $ => prec.left(4, seq(
      field('left', $.predicate),
      field('operator', choice('AND', 'and')),  // SQL style only
      field('right', $.predicate)
    )),

    // Atomic predicates - cannot be further decomposed
    atomic_predicate: $ => choice(
      $.comparison,
      $.in_relational_predicate,  // col [not] in table(|> ...) — before in_predicate to prefer relational form
      $.in_predicate,
      $.inner_exists,  // Functor-style EXISTS/NOT EXISTS
      $.sigma_call,    // Sigma predicate call: +like(arg1, arg2)
      $.not_expression,  // Unary NOT with required parentheses
      prec(5, $.boolean_literal),  // Support true/false as predicates (lower precedence than literal usage)
      prec(10, $.paren_predicate)  // Parenthesized predicates with semicolon support
    ),

    // Parenthesized predicate - allows semicolon as OR inside parens
    paren_predicate: $ => seq(
      '(',
      choice(
        $.or_expression_with_semicolon,  // Can use semicolon as OR
        $.predicate                        // Or regular predicate (will recurse)
      ),
      ')'
    ),

    // OR expression with semicolon support (only inside parentheses)
    or_expression_with_semicolon: $ => prec.left(3, seq(
      field('left', $.predicate),
      field('operator', ';'),              // Semicolon only!
      field('right', $.predicate)
    )),

    // NOT expression - requires parenthesized predicate
    // Syntax: !(predicate)
    not_expression: $ => prec(8, seq(
      '!',
      '(',
      field('expr', $.predicate),
      ')'
    )),
    
    // New functor-style EXISTS with continuation inside parentheses
    // Supports namespace paths: +sys.orders(...) or +myapp::services.orders(...)
    // Also supports passthrough: +main/orders(...) via / separator
    inner_exists: $ => prec.right(10, seq(
      field('operator', $.exists_marker),  // + or \+
      optional(seq(
        field('namespace_path', choice(
          $.grounded_namespace,  // Grounded: data::test^lib::math
          $.namespace_path,      // Multi-level: a::b::c
          $.identifier           // Single-level: sys
        )),
        choice(
          token.immediate('.'),
          alias(token.immediate('/'), $.passthrough_separator)
        )
      )),
      field('table', $.identifier),
      '(',
      $.relational_continuation,  // Required continuation inside
      ')',
      optional($.table_alias)
    )),

    // Sigma predicate call - constraint predicates with arguments
    // Syntax: +like(arg1, arg2) or \+like(arg1, arg2)
    // Supports namespace paths: +std.like(...) or +std::predicates.like(...)
    sigma_call: $ => prec.right(10, seq(
      field('operator', $.exists_marker),  // + or \+
      optional(seq(
        field('namespace_path', choice(
          $.grounded_namespace,  // Grounded: data::test^lib::predicates
          $.namespace_path,      // Multi-level: a::b::c
          $.identifier           // Single-level: std
        )),
        token.immediate('.')
      )),
      field('functor', $.identifier),
      '(',
      optional(field('arguments', $.sigma_argument_list)),
      ')'
    )),

    // Argument list for sigma predicates (comma-separated domain expressions)
    sigma_argument_list: $ => seq(
      $.domain_expression,
      repeat(seq(',', $.domain_expression))
    ),

    // Scalar subquery - returns single value from subquery
    // Syntax: table:(inner-cpr) or sys.table:(inner-cpr) or namespace::path.table:(inner-cpr)
    // Also supports passthrough: main/orders:(inner-cpr) via / separator
    // prec.dynamic(10) instead of prec(10): allows GLR to fork when identifier/
    // could be passthrough (scalar_subquery) or divide (binary_expression).
    // Static prec(10) would deterministically pick scalar_subquery and prevent
    // binary_expression from being tried — causing age/2 to fail.
    scalar_subquery: $ => prec.dynamic(10, seq(
      optional(seq(
        field('namespace_path', choice(
          $.grounded_namespace,  // Grounded: data::test^lib::math
          $.namespace_path,      // Multi-level: a::b::c
          $.identifier           // Single-level: sys
        )),
        choice(
          token.immediate('.'),
          alias(token.immediate('/'), $.passthrough_separator)
        )
      )),
      field('table', $.identifier),
      token.immediate(':('),  // Compound token - requires both : and ( with no space
      $.relational_continuation,
      ')'
    )),

    // Comparison predicate - no chaining without parens (Pony rule)
    comparison: $ => prec.left(1, seq(
      field('left', $.non_binary_domain_expression),
      field('operator', $.comparison_operator),
      field('right', $.non_binary_domain_expression)
    )),

    // Comparison operators with semantic node names
    comparison_operator: $ => choice(
      alias(seq(token('~='), '~>'), $.destructure_aggregate_op),        // Aggregate destructure (LEFT JOIN): json_col ~= ~> {pattern} (MUST BE FIRST)
      alias(token('~='), $.destructure_scalar_op),                     // Scalar destructure: json_col ~= {pattern}
      alias('=', $.null_safe_eq),                                      // NULL-safe equality (IS NOT DISTINCT FROM)
      alias('==', $.traditional_eq),                                   // Traditional SQL equality
      alias('!=', $.null_safe_ne),                                     // NULL-safe inequality (IS DISTINCT FROM)
      alias('!==', $.traditional_ne),                                  // Traditional SQL inequality
      alias('<', $.less_than),
      alias('>', $.greater_than),
      alias('<=', $.less_than_eq),
      alias('>=', $.greater_than_eq),
      // '<>' deprecated - use !== instead
    ),

    // IN predicate: col in (val1; val2) or (col1, col2) in (v1, v2; v3, v4)
    // Also supports NOT IN: col not in (val1; val2)
    // Semicolon separates rows, comma separates values within tuple
    in_predicate: $ => prec.left(1, seq(
      field('value', $.non_binary_domain_expression),
      field('operator', choice(
        alias(seq(choice('not','NOT'), choice('in','IN')), $.not_in_op),
        alias(choice('in', 'IN'), $.in_op)
      )),
      $._lparen,
      field('set', $.in_value_list),
      $._rparen
    )),

    // IN relational predicate: col in table(|> (col)) or col not in table(|> (col))
    // RHS is a relation with a required continuation (subquery)
    in_relational_predicate: $ => prec.left(2, seq(
      field('value', $.non_binary_domain_expression),
      field('operator', choice(
        alias(seq(choice('not','NOT'), choice('in','IN')), $.not_in_op),
        alias(choice('in', 'IN'), $.in_op)
      )),
      optional(seq(
        field('namespace_path', choice(
          $.grounded_namespace,
          $.namespace_path,
          $.identifier
        )),
        token.immediate('.')
      )),
      field('table', $.identifier),
      '(',
      field('continuation', $.relational_continuation),
      ')'
    )),

    // Values in IN list: val1; val2; val3 OR val1, val2; val3, val4 for tuple IN
    // EPOCH 5: Support both single values and multi-column rows
    // Semicolon separates rows, comma separates values within row
    in_value_list: $ => seq(
      $.in_value_row,
      repeat(seq(';', $.in_value_row))
    ),

    // Single row in IN list - can have multiple comma-separated values
    // For single-column: just one value
    // For tuple IN: multiple values separated by commas
    in_value_row: $ => seq(
      $.non_binary_domain_expression,
      repeat(seq($._comma, $.non_binary_domain_expression))
    ),
    
    // Function call: func:(args) or func:() for no args
    // Or lambda: :(expression) where expression contains @
    // Higher precedence to disambiguate from CTE bindings
    // TESTING: Changed all $._colon to token.immediate(':') to eliminate CTE binding conflicts
    function_call: $ => prec(3, choice(
      // SQL STANDARD SPECIAL FORM: count:(*)
      //
      // In the SQL standard, count(*) is the ONLY function that accepts * as an
      // argument. No other scalar or aggregate function does. The * in count(*)
      // is not a column glob — it's a special token meaning "count rows."
      //
      // name:(*) is genuinely ambiguous in our grammar:
      //   - function_call:  name is a function, * is a glob (domain_expression)
      //   - scalar_subquery: name is a table, * is qualify_operator (continuation)
      //
      // For every other identifier, scalar_subquery is the correct parse (prec 10
      // beats function_call's prec 3). But count:(*) must parse as a function call.
      // By placing count_star HERE inside function_call (at prec 11), it wins the
      // GLR ambiguity everywhere function_call appears — domain_expression,
      // non_binary_domain_expression, column_spec_item, aggregate_function, etc.
      // One rule, one place, all contexts covered.
      $.count_star,

      // Enclyph functions (compound data constructors)
      $.bracket_function,     // [a, b, c] - INTERIOR-TUPLE
      $.curly_function,       // {a, "key": b} - INTERIOR-RECORD

      // JSON path extraction: x:{path} or x:[path]
      $.json_path,

      // Higher-order CFE call: name:(curried_args)(regular_args)
      seq(
        optional(seq(
          field('namespace_path', choice(
            $.grounded_namespace,
            $.namespace_path,
            $.identifier
          )),
          token.immediate('.')
        )),
        field('name', $.identifier),
        token.immediate(':('),  // Compound token
        optional(field('curried_arguments', $.function_arguments)),
        $._rparen,
        $._lparen,
        optional(field('regular_arguments', $.function_arguments)),
        $._rparen
      ),
      // Window function: name:(args <~ partition, order, frame)
      // The <~ operator must be present to disambiguate from regular calls
      seq(
        optional(seq(
          field('namespace_path', choice(
            $.grounded_namespace,
            $.namespace_path,
            $.identifier
          )),
          token.immediate('.')
        )),
        field('name', $.identifier),
        token.immediate(':('),  // Compound token
        optional(field('arguments', $.function_arguments)),
        field('window_context', $.window_context),  // Required to disambiguate
        $._rparen
      ),
      // Regular function call: name:(args)
      // TESTING: Use token.immediate(':(') to require both : and ( together
      seq(
        optional(seq(
          field('namespace_path', choice(
            $.grounded_namespace,
            $.namespace_path,
            $.identifier
          )),
          token.immediate('.')
        )),
        field('name', $.identifier),
        token.immediate(':('),  // Compound token - requires both : and ( with no space
        optional(field('arguments', $.function_arguments)),
        $._rparen
      ),
      // Lambda function: :(lambda_body)
      // NOTE: Lambda uses plain ':' because there's no identifier before it
      seq(
        ':',  // Keep as plain ':' - no identifier to bind to
        $._lparen,
        field('lambda_body', $.domain_expression),
        $._rparen
      )
    )),

    // count:(*) — the one SQL aggregate that accepts *.
    // Lives inside function_call so it's available in every expression context.
    // prec(11) beats scalar_subquery's prec(10).
    count_star: $ => prec(11, seq(
      field('name', $.identifier),
      token.immediate(':('),
      '*',
      ')'
    )),

    function_arguments: $ => choice(
      // CCAFE context-aware call: .., arg1, arg2
      // Higher precedence to match this first
      prec(1, seq(
        field('context_call_marker', '..'),
        optional(seq(',', sep1(',', choice(
          $.domain_expression,
          $.path_literal,        // PATH FIRST-CLASS: Allow paths as CFE arguments
          $.distinct_expression
        )))),
        // Optional filter condition with pipe
        optional(seq(
          '|',
          field('filter_condition', $.predicate)
        ))
      )),
      // Regular call: arg1, arg2
      seq(
        choice(
          $.domain_expression,
          $.path_literal,          // PATH FIRST-CLASS: Allow paths as CFE arguments
          $.distinct_expression  // Allow %column or %(expression) in function arguments
        ),
        repeat(seq($._comma, choice(
          $.domain_expression,
          $.path_literal,          // PATH FIRST-CLASS: Allow paths as CFE arguments
          $.distinct_expression
        ))),
        // Optional filter condition with pipe
        optional(seq(
          '|',
          field('filter_condition', $.predicate)
        ))
      )
    ),

    // Window context for window functions: <~ partition, order, frame
    window_context: $ => seq(
      $.window_operator,
      optional(seq(field('partition', $.window_partition), optional($._comma))),
      optional(seq(field('ordering', $.window_ordering), optional($._comma))),
      optional(field('frame', $.window_frame))
    ),

    // Window operator token
    window_operator: $ => token('<~'),

    // Window partition: %(expr, ...)
    window_partition: $ => seq(
      '%',
      $._lparen,
      optional(sep1(',', $.domain_expression)),
      $._rparen
    ),

    // Window ordering: #(expr [asc|desc], ...)
    window_ordering: $ => seq(
      '#',
      $._lparen,
      optional(sep1(',', $.window_order_item)),
      $._rparen
    ),

    // Window order item - allows any domain expression (including @)
    window_order_item: $ => seq(
      field('column', $.domain_expression),
      optional(field('direction', choice(
        'ascending',
        'descending',
        'asc',
        'desc'
      )))
    ),

    // Window frame: groups(...) | rows(...) | range(...)
    window_frame: $ => choice(
      seq('groups', $._lparen, $.frame_bound, $._comma, $.frame_bound, $._rparen),
      seq('rows', $._lparen, $.frame_bound, $._comma, $.frame_bound, $._rparen),
      seq('range', $._lparen, $.frame_bound, $._comma, $.frame_bound, $._rparen)
    ),

    // Frame bound: _ | . | -expr | +expr | expr
    frame_bound: $ => choice(
      '_',  // unbounded
      '.',  // current row
      seq('-', $.domain_expression),  // preceding
      seq('+', $.domain_expression),  // following (explicit)
      $.domain_expression  // following (implicit)
    ),

    // JSON path extraction: source:{path} or source:[path]
    // Maps to SQLite json_extract(source, '$path')
    // IMPORTANT: Must use token.immediate for both : and {/[ to avoid conflicts with CTE binding (age: a)
    // PATH FIRST-CLASS: Accepts path_literal, identifier, or string_literal
    // - path_literal (.name): compile-time validated path
    // - identifier (p): CFE parameter or column containing path string
    // - string_literal ("$.name"): explicit string path
    // Builder validates context: identifiers only allowed in CFE bodies
    json_path: $ => choice(
      // Object path: x:{.name} or x:{p} or x:{"$.name"}
      seq(
        field('source', choice($.identifier, $.qualified_column)),
        token.immediate(':{'),  // Combined token to avoid CTE binding conflicts
        field('path', choice(
          $.path_literal,    // .name, .scripts.dev
          $.identifier,      // p, path_var (validated by builder)
          $.string_literal   // "$.name"
        )),
        '}'
      ),
      // Array path with path literal: x:{.0.name}
      // Note: Array paths now use unified dot syntax, not brackets
      // Keep bracket syntax for backwards compatibility during migration
      seq(
        field('source', choice($.identifier, $.qualified_column)),
        token.immediate(':['),  // Combined token to avoid CTE binding conflicts
        field('path', $.array_path_syntax),
        ']'
      )
    ),

    // PATH FIRST-CLASS: Path literal as standalone expression
    // Leading dot distinguishes paths from columns/parameters
    // Examples: . (root), .name, .scripts.dev, .config.server.port
    // Very high precedence (15) to prefer matching as path over frame_bound '.'
    // Dynamic precedence: prefer longer matches (.identifier over .)
    path_literal: $ => choice(
      // Object path: .name or .scripts.dev
      // Higher dynamic precedence (1) to prefer this over root path when identifier follows
      prec.dynamic(1, prec(15, seq(
        '.',
        field('first', choice($.identifier, $.quoted_identifier)),
        repeat(seq('.', field('segment', $.path_segment)))
      ))),
      // Array path: .0 or .0.name (unified dot syntax)
      // Higher dynamic precedence (1) to prefer this over root path when number follows
      prec.dynamic(1, prec(15, seq(
        '.',
        field('first', $.integer_literal),
        repeat(seq('.', field('segment', $.path_segment)))
      ))),
      // Root path: . (extracts entire JSON value)
      // Lower dynamic precedence (0) - only match if nothing else works
      prec.dynamic(0, prec(15, '.'))
    ),

    // Object path must start with identifier (or quoted identifier)
    object_path_syntax: $ => seq(
      field('first', choice($.identifier, $.quoted_identifier)),
      repeat(seq('.', field('segment', $.path_segment)))
    ),

    // Array path must start with integer
    array_path_syntax: $ => seq(
      field('first', $.integer_literal),
      repeat(seq('.', field('segment', $.path_segment)))
    ),

    // Path segments can be identifiers, quoted identifiers, or integers
    path_segment: $ => choice(
      $.identifier,
      $.quoted_identifier,
      $.integer_literal
    ),

    // Quoted identifier for special characters in JSON keys
    // Examples: "@types/react", "server.production", "1"
    quoted_identifier: $ => seq(
      '"',
      field('value', /[^"]+/),
      '"'
    ),

    // Local variable (column reference - simple or qualified)
    lvar: $ => choice(
      $.qualified_column,
      $.identifier
    ),
    
    literal: $ => choice(
      $.hex_literal,      // Try hex first (most specific with 0x prefix)
      $.octal_literal,    // Then octal (with 0o prefix)
      $.integer_literal,  // Then integer (more specific than decimal)
      $.number_literal,   // Then general numbers
      $.string_literal,
      $.boolean_literal,
      $.null_literal
    ),

    // Hexadecimal literal: 0x0A or 0x0a (case insensitive)
    hex_literal: $ => /0[xX][0-9a-fA-F]+/,

    // Octal literal: 0o12 or 0O12 (case insensitive)
    octal_literal: $ => /0[oO][0-7]+/,

    number_literal: $ => /-?[0-9]+(\.[0-9]+)?/,
    integer_literal: $ => /-?[0-9]+/,  // Integers only (no decimals)
    string_literal: $ => choice(
      /"""([^"]|"[^"]|""[^"])*"""/,  // Triple double-quotes (longest match wins)
      seq('"', /[^"]*/, '"'),  // Double quotes
      seq('b64:', choice(
        /"""([^"]|"[^"]|""[^"])*"""/,      // b64:"""..."""
        seq('"', /[A-Za-z0-9+/=]*/, '"')   // b64:"..." (base64 charset only)
      ))
    ),
    boolean_literal: $ => choice(
      'true',
      'false'
    ),
    null_literal: $ => 'null',
    
    // Namespace path: x::y::z for multi-level namespace hierarchies
    // Used in: table access, scalar subqueries, inner exists
    // Example: myapp::services::orders (namespace path)
    // NOTE: Must have at least one :: to be a namespace_path
    namespace_path: $ => seq(
      $.identifier,
      repeat1(seq(
        token.immediate('::'),  // No space before/after ::
        $.identifier
      ))
    ),

    // Grounded namespace: data_ns^lib_ns for binding data to groundable definitions
    // Example: data::test^lib::math  (data namespace grounded with lib namespace)
    // Multiple groundings: data::test^lib::math^lib::extra
    // The ^ must be immediate (no whitespace) to disambiguate from meta-ize operator ^
    grounded_namespace: $ => seq(
      choice($.namespace_path, $.identifier),
      repeat1(seq(
        token.immediate('^'),
        choice($.namespace_path, $.identifier)
      ))
    ),

    // Table access: namespace::path.table(columns) or table(columns), with optional alias
    // Can have ? (outer join) postfix on table name: table?(...) or ns.table?(...)
    // Supports both column_spec (e.g., users(*)) and relational_continuation (e.g., users(|> (id)))
    // Namespace syntax:
    //   - sys.namespaces(*) - single-level namespace
    //   - myapp::services.orders(*) - multi-level namespace (:: for levels, . for table)
    table_access: $ => seq(
      optional(seq(
        field('namespace_path', choice(
          $.grounded_namespace,  // Grounded: data::test^lib::math
          $.namespace_path,      // Multi-level: a::b::c
          $.identifier           // Single-level: sys
        )),
        choice(
          token.immediate('.'),
          alias(token.immediate('/'), $.passthrough_separator)
        )
      )),
      field('table', $.identifier),
      optional(field('outer', token.immediate('?'))),  // Postfix ? for outer join: table?(*)
      optional(field('mutation_target', token.immediate('!!'))),  // Postfix !! for DML mutation target
      '(',
      optional(choice(                                   // CHANGED: optional() wrapper for empty parens
        prec(2, field('columns', $.column_spec)),         // Higher precedence so * matches glob, not qualify_operator
        field('continuation', $.relational_continuation)  // INNER-RELATION support
      )),
      ')',
      optional($.table_alias)
    ),
    
// Catalog functor: ns::(*)  or  `ns::`(*)
    // Queries namespace metadata. The trailing :: signals "list contents of this namespace".
    // Bare form: main::(*) or sys::entities::(*) — namespace_path/identifier + trailing ::
    // Stropped form: `main::`(*) or `sys::entities::`(*) — everything in backticks
    catalog_functor: $ => seq(
      field('catalog_name', choice(
        $.stropped_identifier,    // `main::` or `sys::entities::`
        seq(                      // bare: namespace_path + trailing ::
          choice(
            $.namespace_path,     // multi-level: sys::entities
            $.identifier          // single-level: main
          ),
          token.immediate('::')   // trailing :: — signals catalog functor
        )
      )),
      '(',
      optional(choice(
        prec(2, field('columns', $.column_spec)),
        field('continuation', $.relational_continuation)
      )),
      ')',
      optional($.table_alias)
    ),

    // Backtick-quoted identifier for catalog functors
    stropped_identifier: $ => token(seq(
      '`',
      /[^`]+/,
      '`'
    )),

    // Table alias: as name
    table_alias: $ => seq(
      $._as,  // Hidden 'as'
      field('name', $.identifier)
    ),
    
    // TVF (Table-Valued Function) calls - two syntaxes
    // Supports namespace-qualified TVFs: sys.pragma_table_info(...) or myapp::db.func(...)
    tvf_call: $ => choice(
      // Double parentheses syntax: func(args)(columns) or namespace::path.func(args)(columns)
      // The ')(' sequence disambiguates from table_access
      seq(
        optional(seq(
          field('namespace_path', choice(
            $.grounded_namespace,  // Grounded: data::test^lib::math
            $.namespace_path,      // Multi-level: a::b::c
            $.identifier           // Single-level: sys
          )),
          choice(
            token.immediate('.'),
            alias(token.immediate('/'), $.passthrough_separator)
          )
        )),
        field('function', $.identifier),
        '(',
        field('arguments', $.ho_argument_list),
        ')',
        '(',
        choice(
          prec(2, field('columns', $.column_spec)),
          field('continuation', $.relational_continuation)
        ),
        ')',
        optional($.table_alias)
      ),

      // Ampersand syntax: func(args & columns) or namespace::path.func(args & columns)
      // The '&' disambiguates from table_access
      seq(
        optional(seq(
          field('namespace_path', choice(
            $.grounded_namespace,  // Grounded: data::test^lib::math
            $.namespace_path,      // Multi-level: a::b::c
            $.identifier           // Single-level: sys
          )),
          choice(
            token.immediate('.'),
            alias(token.immediate('/'), $.passthrough_separator)
          )
        )),
        field('function', $.identifier),
        '(',
        field('arguments', $.argument_list),
        '&',
        choice(
          prec(2, field('columns', $.column_spec)),
          field('continuation', $.relational_continuation)
        ),
        ')',
        optional($.table_alias)
      )
    ),
    
    // Arguments for TVF calls
    argument_list: $ => seq(
      $.tvf_argument,
      repeat(seq(',', $.tvf_argument))
    ),

    tvf_argument: $ => choice(
      $.string_literal,
      $.number_literal,          // Numeric arguments (e.g., HO view: above_balance(1000)(*))
      $.table_access,            // Functor args: users(*), ns.table(*)
      $.identifier,              // Scalar args: bare name, literal value
      $.qualified_column         // Support table.column references
    ),

    // HO argument list: supports & (parameter group separator) and ; (row separator)
    // Used for HO view calls: ho_view(table_arg & 1, 2; 3, 4)(*)
    //   - & separates parameter groups (one per declared param)
    //   - ; separates rows within an argumentative functor group
    //   - , separates values within a row
    ho_argument_list: $ => sep1('&', $.ho_argument_group),

    // A parameter group: one or more rows of values separated by ;
    ho_argument_group: $ => sep1(';', $.ho_argument_row),

    // A single row of values: comma-separated tvf_arguments
    ho_argument_row: $ => seq(
      $.tvf_argument,
      repeat(seq(',', $.tvf_argument))
    ),

    // Pseudo-predicate call: mount!("path", "namespace"), engage!("ns"), part!("ns")
    // These are state-mutating relations with the ! suffix
    // Syntax: identifier!(arguments) with optional alias
    // Examples:
    //   mount!("nba.db", "nba")
    //   engage!("std::string") as str
    // Strategy: Use seq with token.immediate to ensure ! follows identifier without space
    // The prec.dynamic gives it priority when there's ambiguity with table_access
    pseudo_predicate_call: $ => prec.dynamic(10, seq(
      field('name', $.identifier),
      token.immediate('!'),  // ! must follow immediately (no whitespace)
      '(',
      optional(field('arguments', $.pseudo_predicate_argument_list)),
      ')',
      optional($.table_alias)
    )),

    // Argument list for pseudo-predicates
    pseudo_predicate_argument_list: $ => seq(
      $.domain_expression,
      repeat(seq(',', $.domain_expression))
    ),

    anonymous_table_separator: $ => choice('@', /---+/),  // @ or multiple dashes
    
    // Anonymous table: _(data) or _(headers @ data) or _(headers --- data)
    // Can have ? (outer join) or + (exists) prefix
    // IMPORTANT: Use compound tokens (?_( , +_( , \+_() to avoid ambiguity with outer-join ? prefix
    anonymous_table: $ => seq(
      choice(
        alias(token('?_('), $.outer_anon_open),        // Outer join anonymous: ?_(...)
        alias(token('+_('), $.exists_anon_open),       // Exists anonymous: +_(...)
        alias(token('\\+_('), $.not_exists_anon_open), // Not exists anonymous: \+_(...)
        field('unifiying', token('_('))                // Regular anonymous: _(...)
      ),
      choice(
        // Just data
        $.data_rows,
        // Headers @ data or headers --- data
        seq(
          $.column_headers,
          $.anonymous_table_separator,
          $.data_rows
        )
      ),
      ')',
      optional(seq('qua', field('qua_target', $.identifier))),  // Schema conformance
      optional($.table_alias)  // Anonymous tables can have aliases too!
    ),
    
    column_headers: $ => seq(
      $.column_header_item,
      repeat(seq($._comma, $.column_header_item))
    ),

    // A column header optionally marked as sparse with ?
    column_header_item: $ => seq(
      choice($.identifier, $.qualified_column, $.function_call),
      optional(alias('?', $.sparse_marker)),
    ),
    
    data_rows: $ => seq(
      $.data_row,
      repeat(seq(';', $.data_row))
    ),
    
    // EPOCH 7: Allow domain expressions in data rows for melt/unpivot
    // Updated for Epoch 7.1: Full domain_expression support (including binary ops)
    data_row: $ => seq(
      $.domain_expression,
      repeat(seq($._comma, $.domain_expression))
    ),
    
    // Sparse fill: _(col @ val) or _(col1, col2 @ val1, val2)
    // Used in data rows of sparse anonymous tables to fill named ? columns
    sparse_fill: $ => seq(
      token('_('),
      $.column_headers,
      $.anonymous_table_separator,
      $.data_row,
      ')'
    ),

    // Column specification
    // NOTE: * in column_spec is for piped_invocation like forall(*), exists(*)
    // column_spec is for explicit positional column binding only.
    // * (glob) is a relational continuation, not a column spec.
    column_spec: $ => $.column_list,

    // glob_spec is used in tree group inductors {*}
    glob_spec: $ => '*',
    
    column_list: $ => seq(
      $.column_spec_item,
      repeat(seq(',', $.column_spec_item))
    ),
    
    column_spec_item: $ => seq(
      choice(
        $.identifier,
        $.placeholder,
        $.literal,  // Allow literals (numbers, strings)
        $.function_call,  // Allow function calls
        $.scalar_subquery,  // Allow scalar subqueries in column list
        $.parenthesized_expression,  // Allow (expr) - this handles complex expressions safely
        // Note: Using parenthesized_expression instead of binary_expression to avoid precedence issues
        // For simple expressions like 4-2, users must write (4-2) to be explicit
        // Note: not including glob to avoid conflict with table(*)
      ),
      optional(seq($._as, field('alias', $.identifier)))
    ),

    placeholder: $ => '_',

    // Value placeholder @ for transforms and lambdas
    value_placeholder: $ => '@',

    // Pattern literal for column matching: /_name/, /^id$/
    pattern_literal: $ => seq(
      '/',
      field('pattern', /[^\/]+/),  // Any characters except /
      '/'
    ),

    // Citation: :name (sugar for name:())
    // A citation is a zero-arity call written :name — you define it, then cite it.
    // Uses plain ':' so the GLR parser can disambiguate on the next token:
    // - ':' + identifier → citation
    // - ':' + '"' → string_template
    // - ':' + '(' → lambda (inside function_call)
    // CTE inline also uses ':' + identifier, but prec.dynamic(1) on cte_inline
    // resolves that ambiguity (CTE wins at the relational level; citation
    // only appears inside domain expressions where CTE binding isn't valid).
    citation: $ => seq(
      ':',
      field('name', $.identifier),
    ),

    // String template with interpolation: :"Hello {name}" or :"""multi "line" template"""
    string_template: $ => choice(
      // Triple-quoted template (must be first / higher precedence)
      prec(2, seq(':', '"""', repeat(choice($.triple_template_text, $.template_interpolation)), '"""')),
      // Regular template
      seq(':', '"', repeat(choice($.template_text, $.template_interpolation)), '"'),
    ),

    // Literal text in triple-quoted template (allows " and "" but not """)
    triple_template_text: $ => /([^{"]|"[^{"]|""[^{"])+/,

    // Literal text in string template
    template_text: $ => /[^{"]+/,  // Any text except { or "

    // Interpolation in string template: {expression}
    template_interpolation: $ => seq(
      '{',
      field('expression', $.domain_expression),  // Full expressions, NO nested string_template
      '}'
    ),

    // Functional pipe operator for value-level transformations
    functional_pipe_operator: $ => token('/->'),

    // Piped expression: value flowing through transformations
    // E.g., col /-> upper:() /-> trim:()
    // The value can be any domain expression except binary_expression or piped_expression
    // (both must be parenthesized to enforce PONY rule - no implicit precedence)
    // Lower precedence to bind after arithmetic
    piped_expression: $ => prec.left(-1, seq(
      field('value', choice(
        $.literal,
        $.lvar,
        $.function_call,
        $.glob,
        // NOTE: binary_expression excluded - PONY rule enforcement
        // Use parentheses: (a ++ b) /-> f:() instead of a ++ b /-> f:()
        $.predicate,
        $.parenthesized_expression,  // Can contain binary_expression or piped_expression inside parens
        $.bracket_function,
        $.column_ordinal,
        $.column_range,
        $.value_placeholder,
        $.case_expression,  // Allow case as initial value for pipes
        $.citation          // Citation as pipe value: :nl /-> f:()
        // Note: piped_expression excluded to prevent direct nesting
        // Use parentheses for nested pipes: ((a /-> b) /-> c)
      )),
      repeat1(seq(
        $.functional_pipe_operator,
        field('transform', choice(
          $.function_call,
          $.string_template,
          $.case_expression  // Allow curried CASE in value pipe
        ))
      ))
    )),

    // CASE expression: _:(cond -> result; ...) with optional default
    case_expression: $ => seq(
      token('_:('),
      optional(seq(
        $.case_arm,
        repeat(seq(';', $.case_arm)),
        optional(seq(';', $.case_default))
      )),
      ')'
    ),

    // Case arm - can be:
    // 1. Simple case first arm: expr @ value -> result
    // 2. Simple case continuation: value -> result (just a literal)
    // 3. Searched case: condition -> result (non-literal expression)
    case_arm: $ => choice(
      // Curried simple case: @ value -> result (for use with lambdas)
      seq(
        '@',
        field('value', $.literal),
        '->',
        field('result', $.domain_expression)
      ),
      // Simple case first arm: expr @ value -> result
      seq(
        field('test_expr', $.domain_expression),
        '@',
        field('value', $.literal),
        '->',
        field('result', $.domain_expression)
      ),
      // Simple case continuation OR searched case
      // Parser will determine based on whether 'condition' is a literal
      seq(
        field('condition', $.case_condition),
        '->',
        field('result', $.domain_expression)
      )
    ),

    // Case condition - can be comma-separated predicates (treated as AND)
    case_condition: $ => seq(
      $.domain_expression,
      repeat(seq(',', $.domain_expression))
    ),

    // Default case: _ -> result
    case_default: $ => seq(
      '_',
      '->',
      field('result', $.domain_expression)
    ),

    // Hidden operators (don't create nodes)
    _comma: $ => ',',
    _as: $ => /[aA][sS]/,
    _minus: $ => '-',  // Only for project_out
    _colon: $ => ':',
    _lparen: $ => '(',
    _rparen: $ => ')',
    _lbracket: $ => '[',
    _rbracket: $ => ']',
    
    // REMOVED: Old pipe expressions - now handled by unary_operator_expression in recursive grammar
    
    // Pipe operations
    pipe_operation: $ => choice(
      $.generalized_projection,
      $.grouping,
      $.project_out,
      $.rename_cover,
      $.embed_cover,
      $.embed_map_cover,  // New combined operator
      $.map_cover,
      $.transform,
      $.ordering,
      $.reposition,
      $.piped_invocation,
      $.narrowing_destructure,
      $.bang_pipe_operation,
    ),

    // Bang pipe operation: unified rule for both DML and directive pipes.
    // DML: name!(table)(columns) — two paren groups, target is table_access or anonymous_table
    // Directive: name!(args) — one paren group, args are domain expressions
    // prec.dynamic(20) on DML ensures it wins over directive (prec.dynamic(10))
    // when both paths parse successfully (e.g., delete!(users(*))(*)).
    bang_pipe_operation: $ => choice(
      // DML with named table target: delete!(users(*))(*)
      prec.dynamic(20, seq(
        field('operation', $.identifier),
        token.immediate('!'),
        '(',
        field('target', $.table_access),
        ')',
        '(',
        choice(
          prec(2, field('columns', $.column_spec)),
          field('continuation', $.relational_continuation)
        ),
        ')',
      )),
      // DML with anonymous table target: delete!(_(*))(*)
      // Separate alternative because token('_(') compound token prevents
      // _ from being lexed as identifier when anonymous_table is reachable.
      prec.dynamic(20, seq(
        field('operation', $.identifier),
        token.immediate('!'),
        '(',
        field('anon_target', $.anonymous_table),
        ')',
        '(',
        choice(
          prec(2, field('columns', $.column_spec)),
          field('continuation', $.relational_continuation)
        ),
        ')',
      )),
      // Directive pipe terminal: enlist!(*), consult!("file", "ns") |> enlist!(*)
      prec.dynamic(10, seq(
        field('name', $.identifier),
        token.immediate('!'),
        '(',
        optional(field('arguments', $.pseudo_predicate_argument_list)),
        ')',
      )),
    ),

    // Unicode assertion view symbols: ∃ ∄ ∀ ≡
    assertion_view_symbol: $ => choice('∃', '∄', '∀', '≡'),

    // Piped higher-order view invocation: source |> ho_view(cols) or source |> ho_view(args)(cols)
    // Optionally followed by reverse pipe <| for binary operators (equals, except, contains).
    // Function name may be an identifier or a Unicode assertion view symbol (∃, ∄, ∀, ≡).
    piped_invocation: $ => choice(
      // name(args)(columns|continuation) — extra args provided, pipe supplies first param
      seq(
        optional(seq(
          field('namespace_path', choice($.grounded_namespace, $.namespace_path, $.identifier)),
          choice(token.immediate('.'), alias(token.immediate('/'), $.passthrough_separator))
        )),
        field('function', choice($.identifier, $.assertion_view_symbol)),
        '(',
        field('arguments', $.ho_argument_list),
        ')',
        '(',
        choice(
          prec(2, field('columns', $.column_spec)),
          field('continuation', $.relational_continuation)
        ),
        ')',
        optional(seq('<|', field('right_operand', $.base_expression)))
      ),
      // name(columns|continuation) — zero extra args, pipe supplies the table param
      seq(
        optional(seq(
          field('namespace_path', choice($.grounded_namespace, $.namespace_path, $.identifier)),
          choice(token.immediate('.'), alias(token.immediate('/'), $.passthrough_separator))
        )),
        field('function', choice($.identifier, $.assertion_view_symbol)),
        '(',
        choice(
          prec(2, field('columns', $.column_spec)),
          field('continuation', $.relational_continuation)
        ),
        ')',
        optional(seq('<|', field('right_operand', $.base_expression)))
      )
    ),
    
    // Grouping: %(fields) with semantic node types
    grouping: $ => seq(
      $.grouping_operator,
      $.grouping_paren
    ),

    // Grouping operator (same as distinct but different context)
    grouping_operator: $ => '%',

    grouping_paren: $ => seq(
      $._lparen,
      $._grouping_content,
      $._rparen
    ),
    
    _grouping_content: $ => choice(
      // GROUP BY with aggregates: %(country ~> sum:(total))
      seq(
        field('reducing_by', $.domain_expression_list),
        $.aggregation_arrow,
        field('reducing_on', $.domain_expression_list),
        // Optional arbitrary expressions: %(country ~> sum:(total) ~? name, email)
        optional(seq(
          $.arbitrary_separator,
          field('arbitrary', $.domain_expression_list)
        ))
      ),
      // GROUP BY without aggregates but with arbitrary: %(country ~? last_name)
      seq(
        field('reducing_by', $.domain_expression_list),
        $.arbitrary_separator,
        field('arbitrary', $.domain_expression_list)
      ),
      // Simple distinct/GROUP BY: %(country)
      field('reducing_by', $.domain_expression_list),
      // Whole-table aggregation: %(~> count:(*))
      seq(
        $.aggregation_arrow,
        field('reducing_on', $.domain_expression_list),
        // Optional arbitrary expressions: %(~> count:(*) ~? name)
        optional(seq(
          $.arbitrary_separator,
          field('arbitrary', $.domain_expression_list)
        ))
      )
    ),
    
    domain_expression_list: $ => seq(
      $.domain_expression,
      repeat(seq($._comma, $.domain_expression))
    ),
    
    // Project-out: -(columns) - removes columns
    project_out: $ => seq(
      $._minus,  // Hidden - we know it's project_out from the node type
      seq($._lparen, $.domain_expression_list, $._rparen)
    ),
    
    // Reusable list of lvars
    lvar_list: $ => seq(
      $.lvar,
      repeat(seq($._comma, $.lvar))
    ),

    // List that allows lvars or literals (for positional references)
    lvar_or_literal_list: $ => seq(
      choice($.lvar, $.literal),
      repeat(seq($._comma, choice($.lvar, $.literal)))
    ),
    
    // Rename-cover: *(old as new, ...) - renames columns
    rename_cover: $ => seq(
      '*',
      $._lparen,
      $.rename_list,
      $._rparen
    ),
    
    // Embed-cover: +(expr as name, ...) - embeds computed expressions
    embed_cover: $ => seq(
      '+',
      $._lparen,
      $.embed_list,
      $._rparen
    ),
    
    // Embed + Map-cover combined: +$(func)(columns) with optional column name template
    embed_map_cover: $ => prec(2, seq(
      '+$',
      $._lparen,
      choice(
        $.function_call,  // Includes :(...) lambda syntax
        $.string_template,
        $.case_expression  // Allow curried CASE expressions
      ),
      optional(seq(
        $._as,
        field('alias_template', $.column_name_template)
      )),
      $._rparen,
      $.embed_map_cover_paren
    )),

    embed_map_cover_paren: $ => seq(
      $._lparen,
      $.column_selector,
      $._rparen
    ),

    // Column selector for map operations - supports various selection patterns
    column_selector: $ => prec(1, choice(
      $.glob,                        // (*) - reuse existing glob rule
      $.column_selector_regex,      // (/pattern/)
      $.column_selector_multi_regex, // (/pattern1/, /pattern2/)
      $.column_selector_positional, // (|2:5|)
      $.domain_expression_list      // (col1, col2) - explicit columns
    )),

    column_selector_regex: $ => $.regex_pattern,

    column_selector_multi_regex: $ => seq(
      $.regex_pattern,
      repeat1(seq($._comma, $.regex_pattern))
    ),

    column_selector_positional: $ => seq(
      '|',
      field('start', /[0-9]+/),
      ':',
      field('end', /[0-9]+/),
      '|'
    ),

    // Regex pattern for column matching: (/pattern/)
    regex_pattern: $ => prec(2, seq(
      '(',
      '/',
      field('pattern', $.regex_body),
      '/',
      ')'
    )),

    regex_body: $ => prec(2, /[^\/]+/),

    // Column name template for compile-time name generation: :"{@}_suffix"
    column_name_template: $ => seq(
      ':',
      '"',
      repeat1(choice(
        $.column_template_placeholder,
        $.column_template_text
      )),
      '"'
    ),

    column_template_placeholder: $ => choice('{@}', '{#}'),
    column_template_text: $ => /[^{}"]+/,

    // Map-cover: $(func:())(columns) or $(func:())[columns] or $(string_template)(columns)
    map_cover: $ => prec(1, seq(
      '$',
      $._lparen,
      choice(
        $.function_call,  // Includes :(...) lambda syntax
        $.string_template,
        $.case_expression  // Allow curried CASE expressions in map cover
      ),
      $._rparen,
      $.map_cover_paren
    )),

    map_cover_paren: $ => seq(
      $._lparen,
      $.domain_expression_list,
      optional(seq('|', field('filter_condition', $.predicate))),
      $._rparen
    ),
    
    // Transform: $$(expr as alias, expr as alias, ...)
    transform: $ => seq(
      '$$',
      $._lparen,
      $.transform_list,
      $._rparen
    ),
    
    transform_list: $ => seq(
      $.transform_item,
      repeat(seq($._comma, $.transform_item)),
      optional(seq('|', field('filter_condition', $.predicate)))
    ),
    
    transform_item: $ => $.domain_expression,
    
    // Ordering: #(field, field descending, ...)
    ordering: $ => seq(
      '#',
      $._lparen,
      $.order_list,
      $._rparen
    ),
    
    order_list: $ => seq(
      $.order_item,
      repeat(seq($._comma, $.order_item))
    ),
    
    order_item: $ => seq(
      field('column', choice(
        $.lvar,
        $.column_ordinal  // Support ordinal references like |1|, |-1|
      )),
      optional(field('direction', choice(
        'ascending',
        'descending',
        'asc',   // Short form
        'desc'   // Short form
      )))
    ),
    
    // Reposition: *[column as position, ...] - moves columns to specific positions
    reposition: $ => seq(
      '*',
      $._lbracket,
      $.reposition_list,
      $._rbracket
    ),

    reposition_list: $ => seq(
      $.reposition_item,
      repeat(seq($._comma, $.reposition_item))
    ),

    reposition_item: $ => seq(
      field('column', choice(
        $.lvar,              // Column name
        $.integer_literal    // Bare integer: 1, -1 (replaces |1|, |-1|)
      )),
      $._as,
      field('position', $.integer_literal)  // Target position (always integer)
    ),
    
    embed_list: $ => seq(
      $.embed_item,
      repeat(seq($._comma, $.embed_item))
    ),
    
    embed_item: $ => field('expression', $.domain_expression),
    
    rename_list: $ => seq(
      $.rename_item,
      repeat(seq($._comma, $.rename_item))
    ),
    
    rename_item: $ => seq(
      field('old_name', choice(
        $.lvar,
        $.column_ordinal,
        $.pattern_literal,
        $.glob
      )),
      $._as,
      field('new_name', choice(
        $.identifier,
        $.column_name_template
      ))
    ),
    
    // Generalized projection: (items) with semantic node types
    generalized_projection: $ => $.generalized_projection_paren,

    generalized_projection_paren: $ => seq(
      $._lparen,
      $.domain_expression_list,
      $._rparen
    ),
    
    // Qualified column: table.column or schema.table.column
    // Note: Ambiguity with trailing period is a known language design issue
    // IMPORTANT: No spaces allowed around dots in qualified names
    qualified_column: $ => choice(
      // Three-part: schema.table.column (highest precedence)
      prec(3, seq(
        field('schema', $.identifier),
        token.immediate('.'),
        field('table', $.identifier),
        token.immediate('.'),
        field('column', $.identifier)
      )),
      // Two-part: table.column  
      prec(2, seq(
        field('table', $.identifier),
        token.immediate('.'),
        field('column', $.identifier)
      )),
      // CPR reference: _.column (underscore for current piped relation)
      prec(2, seq(
        field('table', $.cpr_reference),
        token.immediate('.'),
        field('column', $.identifier)
      ))
    ),
    
    // Special CPR reference marker
    cpr_reference: $ => '_',
    
    // Basic identifier
    _bare_identifier: $ => /[a-zA-Z_][a-zA-Z0-9_]*/,

    identifier: $ => choice(
      $._bare_identifier,
      $.stropped_identifier
    ),
    

    // Table prefixes
    // Note: outer_marker removed - now using compound tokens ?_( in anonymous_table
    exists_marker: $ => choice(
      '+',           // EXISTS
      alias(seq('\\', '+'), $.not_exists)  // NOT EXISTS (backslash plus)
    ),
    
    // Pivot expression: value_col of pivot_key
    // Used inside %() to rotate row values into columns
    // Example: score of subject, sum:(total) of status
    pivot_expression: $ => prec.left(1, seq(
      field('value_column', $.non_binary_domain_expression),
      'of',
      field('pivot_key', $.non_binary_domain_expression)
    )),

    // Aggregation arrow for grouping
    aggregation_arrow: $ => '~>',

    // Arbitrary column separator (for GROUP BY)
    arbitrary_separator: $ => '~?'
  }
});
