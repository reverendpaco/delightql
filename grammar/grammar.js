// The consolidated DelightQL grammar.
//
// ONE grammar for the whole language. The productions below
// follow SEMANTICS/ — top-grammar.md for the relational and definitional line,
// domain-expressions-grammar.md for value and truth position,
// pipe-operators-grammar.md for spec position, ddl-grammar.md for the
// companion sigil sub-language. Where a name differs from the semantic
// grammar's, a comment says why.
//
// The semantic grammars are LOGICAL: `x*` there means "a Vec of x", not
// "juxtaposed with no separator". Concrete separators — commas in argument and
// item lists, semicolons between rows and arms — belong to this layer and are
// spelled here.
//
// This grammar describes what the author WROTE: spans, alternate spellings and
// concrete structure all survive. Deciding which spelling distinctions
// disappear belongs to CST-to-AST normalization, not here — citations,
// composition parens, zero-interpolation templates and implicit landings are
// all still visible in this tree. Over-admission is deliberate where a law
// names a BUILD judgment (hole counting, arity, groundness, order
// consumption): the parser must not pre-empt a judgment the builder owns.
//
// The token vocabulary lives in tokens.js so that a sigil with several
// meanings is spelled once and told apart by its parent.

const tokens = require('./tokens');
const conflicts = require('./conflicts');

function sep1(separator, rule) {
  return seq(rule, repeat(seq(separator, rule)));
}

// A comma-separated list whose separators stay visible as nodes.
function commaSep1($, rule) {
  return seq(rule, repeat(seq($.comma_sigil, rule)));
}

// The same list with a field on each ITEM. Naming the whole sequence instead
// would put the separators under the field, so `inputs()` would answer commas.
function commaSep1Field($, name, rule) {
  return seq(field(name, rule), repeat(seq($.comma_sigil, field(name, rule))));
}

module.exports = grammar({
  name: 'delightql',

  extras: $ => [
    /\s/,
    $.comment,
    // Session tools: surface without semantics. They attach by position at
    // build, like annotations, but unlike annotations their admitted
    // positions are not law — spelling them at every continuation anchor
    // would multiply the grammar without adding a distinction.
    $.smart_comment,
    $.stop_point,
    $.debug_point,
  ],

  word: $ => $._classic_ident,

  conflicts: conflicts,

  // Supertypes are the exhaustiveness contract. Each becomes one enum in the
  // generated typed CST, so a consumer matching on it cannot silently miss a
  // member: adding an alternative here becomes a compile error downstream.
  supertypes: $ => [
    $.entity_definition,
    $.rule_form,
    $.fact_like,
    $.grelex,
    $.named_grelex,
    $.continuation,
    $.operator_continuation,
    $.binary_continuation,
    $.union_like_continuation,
    $.grelex_like_member,
    $.cte,
    $.effect_cte,
    $.direct_effrelex,
    $.dml_form,
    $.post_pipe_form,
    $.pipe_operation,
    $.pipe_structural,
    $.postfix_operator,
    $.reduction_item,
    $.selector_item,
    $.domain_expression,
    $.function_application,
    $.non_infix_application,
    $.ground,
    $.literal,
    $.mention,
    $.reference,
    $.callable,
    $.functor_like,
    $.relation_like,
    $.enclyph_like,
    $.spread,
    $.record_member,
    $.pattern_member,
    $.tree_pattern,
    $.compression,
    $.compile_time_integer,
    $.frame_bound,
    $.truth_expression,
    $.slot,
    $.constraint_term,
    $.ho_argument,
    $.relation_hole,
    $.domain_hole,
    $.open_expression,
    $.annotation,
    $.definition_annotation,
    $.companion_cell,
    $.constraint_cell,
    $.operand,
    $.probe,
    $.meta_target,
    $.name_target,
    $.rename_source,
    $.group_key,
    $.heading_reference,
    $.cfe_param,
    $.function_param,
    $.ho_param,
    $.rule_mode_param,
    $.datum,
    $.out_item,
    $.out_value,
    $.argument,
    $.path_key,
    $.template_part,
    $.qualifier,
  ],

  rules: {
    // =====================================================================
    // Roots
    //
    // One entry point. The DEFAULT branch is the definition file — the
    // canonical language form, where every query begins with `?-` and a naked
    // one is refused. Every other root is reached through a host-supplied
    // SELECTOR, never by classifying the text, because the branches overlap:
    // `f(1, 2)` is a fact in the canonical form and an argumentative query in
    // the utility one, with identical bytes. A parser that guessed would
    // sometimes elaborate a query as a stored fact.
    //
    // The selectors are the `?-` prompt wrap's cousins: text the HOST writes
    // to name a category it already knows — the utility execution mode, or a
    // companion COLUMN (ddl-grammar.md FN.3). They are never authored
    // DelightQL, and the façade maps every span back past them so no caller
    // sees a coordinate it did not write.
    // =====================================================================
    // A canonical file declaring nothing declares nothing: `consult_file` is a
    // Kleene star, so blank and comments-only sources stand. Emptiness lives
    // on the start rule because that is the one place Tree-sitter admits it;
    // an unmarked source with no forms IS the canonical branch, having simply
    // nothing to show.
    source_file: $ => optional(choice(
      $.definition_file,
      $.query_sequence_root,
      $.companion_cell_root,
    )),

    // A SUBORDINATE BLOCK BELONGS TO ITS FILE. `ddl_annotation` is the one
    // annotation admitted here, and it is not decorating a form: it declares a
    // DDL block subordinate to the file, processed in the file's own
    // consultation namespace or — named — in that namespace's child, which is
    // the sanctioned road to a reserved `_` child. The other definition
    // annotations state something ABOUT a form, and at file scope there is no
    // form for them to state it about.
    definition_file: $ => repeat1(choice(
      $.entity_definition,
      $.top_level_goal,
      $.ddl_annotation,
    )),

    // THE UTILITY FILE DECLARES ITSELF. A host that already knows the category
    // states it by injecting the header; a raw consumer of this language — an
    // editor loading the parser, a highlighter — has no host to ask, so the
    // file says which world it is in. The sequence is optional for the same
    // reason the canonical file is a Kleene star: a file declaring nothing
    // declares nothing.
    query_sequence_root: $ => seq($.query_sequence_header, optional($.query_sequence)),

    // A READER DIRECTIVE, not DelightQL. It carries no meaning into the AST
    // and promises nothing about the file being executable. Its placement —
    // first nonblank line, column zero, once — is not expressible here:
    // extras are skipped before any token, so no production can say that
    // nothing precedes this one. The façade holds that law and teaches it.
    query_sequence_header: $ => '#!dql query-sequence',

    query_sequence: $ => repeat1(choice($.relex, $.effrelex)),

    // The sole top-level-goal spelling; a let block is admitted — the REPL
    // wrap depends on it.
    top_level_goal: $ => seq(
      $.goal_marker,
      field('goal', choice($.relex, $.effrelex)),
    ),

    // =====================================================================
    // Basics — names
    // =====================================================================

    // THE ENGINE'S CATALOG IS THE ENGINE'S: the dot is DQL's catalog, the
    // slash is the engine's. An engine reference heads a pure relation access
    // and is excluded by construction from effect names and DML targets.
    //
    // WHEREVER A RELATION IS ACCESSED, and nowhere else. This is the head of
    // every pure relation access — the functor family, the outer-marked one,
    // existence, relational membership, the inner form, the piped invocation.
    // A CALLABLE's name is `callee`, which stays narrow: `main/upper:(x)`
    // names no function, so the exclusion is by construction there too. The
    // narrower spelling in the relational positions was not: a `main/orders`
    // in value position then read as `main` divided by a scalar subquery, and
    // resolution reported a missing column named `main`.
    relation_name: $ => choice($.predicate_identifier, $.engine_reference),

    predicate_identifier: $ => seq(
      optional(field('namespace', $.namespace_qual)),
      field('name', $.identifier),
    ),

    namespace_qual: $ => seq($.namespace, token.immediate('.')),

    namespace: $ => seq(
      $.identifier,
      repeat(seq(token.immediate('::'), $.identifier)),
    ),

    // THE SLASH RIDES THE NAME, and it has to. A `/` of its own wins the
    // lexer race against division on length, so `x/2.2` — which no engine
    // reference can derive, `2.2` being no identifier — would have no reading
    // at all. Matching the whole `/name` lets longest-match decide: where a
    // name follows, this is the reference; where a number does, the `/` is
    // division, as the law says it is.
    engine_reference: $ => seq(
      field('engine', $.identifier),
      field('name', $.engine_name),
    ),

    // ONE token, slash included. Split into `/` plus a name, the immediate
    // slash would still win on preference before the name was ever seen.
    engine_name: $ => token.immediate(seq(
      '/',
      choice(/[a-zA-Z_][a-zA-Z0-9_]*/, seq('`', /[^`\n]*/, '`')),
    )),

    identifier: $ => choice($._classic_ident, $.stropped_form),

    _classic_ident: $ => /[a-zA-Z_][a-zA-Z0-9_]*/,

    // =====================================================================
    // Relex
    // =====================================================================

    relex: $ => seq(optional(field('let_block', $.let_block)), field('body', $.let_free_relex)),

    // `grelex | let_free_relex continuation+` unrolls to a flat chain. The
    // CHAIN is the relex with its continuations; nesting it left-recursively
    // would bury the second link one level deeper than the first for no
    // semantic reason. An annotation rides here because it stands at any
    // continuation anchor — it decorates a position and is not a continuation.
    //
    // A LEADING OUTER WAITS FOR ITS PEER. `a?(*), b(*)` is the right-outer
    // orientation and `a?(*), b?(*)` is full outer, but `?` is not an
    // independently runnable relation: the second alternative requires the
    // completing comma member in the same production, so a terminal
    // outer-marked access has no derivation. Position changes orientation,
    // never the meaning of the marker.
    let_free_relex: $ => choice(
      seq(
        field('grelex', $.grelex),
        repeat(choice($.continuation, $.annotation)),
      ),
      seq(
        field('leading_outer', $.leading_outer_grelex),
        // NAMING IT IS NOT RUNNING IT. The alias belongs to the access it
        // stands on, and the peer still completes the join in this same
        // production — which is what keeps a terminal outer-marked access
        // underivable. A join written `a?(*) as x, b(*) as y` names both
        // sides, and only the left one had nowhere to put its name.
        optional(field('leading_outer_name', $.stage_name)),
        field('peer', $.outer_peer),
        repeat(choice($.continuation, $.annotation)),
      ),
    ),

    leading_outer_grelex: $ => choice($.outer_grelex, $.outer_anon_grelex),

    // The member that completes the join. It is a comma continuation like any
    // other — the same carrier the chain uses everywhere — restricted to the
    // RELATIONAL members, because a predicate or a bound completes nothing.
    outer_peer: $ => seq($.comma_sigil, field('member', $.grelex_like_member)),

    grelex: $ => choice($.named_grelex, $.anon_grelex),

    named_grelex: $ => choice(
      $.inchoate_functor,
      $.argumentative_functor,
      $.interior_functor,
      $.catalog_functor,
    ),

    inchoate_functor: $ => seq(
      field('relation', $.relation_name),
      '(',
      ')',
    ),

    // A POSITIONAL caller pattern — one slot per column, judged TOTAL against
    // the relation's degree at resolution.
    argumentative_functor: $ => seq(
      field('relation', $.relation_name),
      optional(field('ho_part', $.ho_part)),
      '(',
      field('arguments', $.argumentative_form),
      ')',
    ),

    // THE IMPLICIT STAR: an interior continuation always starts realised, so
    // `p(C) ≡ p(*) C` and the `*` in `users(*)` is the qualify postfix, not a
    // second glob carrier. ONE authority for `*`-in-an-interior; the head
    // positions (`glob_head`, `ho_param`) keep their own literal glob because
    // no continuation is admitted there.
    interior_functor: $ => seq(
      field('relation', $.relation_name),
      optional(field('ho_part', $.ho_part)),
      '(',
      field('interior', $.interior),
      ')',
    ),

    // THE CATALOG ANSWERS AS DATA. The trailing '::' belongs to this
    // production, not to `namespace`.
    catalog_functor: $ => seq(
      field('catalog', $.namespace),
      token.immediate('::'),
      '(',
      optional(field('interior', choice($.argumentative_form, $.interior))),
      ')',
    ),

    anon_grelex: $ => seq('_(', $.anon_body, ')'),

    interior: $ => repeat1($.continuation),

    // =====================================================================
    // Continuations
    // =====================================================================

    continuation: $ => choice(
      $.operator_continuation,
      $.binary_continuation,
    ),

    operator_continuation: $ => choice(
      $.pipe_continuation,
      $.postfix_operator,
      $.stage_name,
      $.argumentative_stage,
      $.singleton_reduction,
    ),

    // In PURE position the pipe admits pure_invocation only; the effect
    // terminal enters solely through effect_chain's own pipe alternatives.
    pipe_continuation: $ => seq($.pipe_operator, $.post_pipe_form),

    binary_continuation: $ => choice(
      $.comma_continuation,
      $.union_like_continuation,
      $.minus_continuation,
      $.edge_continuation,
    ),

    comma_continuation: $ => seq(
      $.comma_sigil,
      field('member', choice(
        $.grelex_like_member,
        $.truth_expression,
        $.destructure_relex,
        $.ordering,
        $.row_bound,
      )),
    ),

    grelex_like_member: $ => choice(
      $.grelex,
      $.outer_grelex,
      $.outer_anon_grelex,
      $.exists_anon_grelex,
    ),

    union_like_continuation: $ => choice(
      $.positional_union_continuation,
      $.smart_union_continuation,
      $.corresponding_union_continuation,
    ),

    positional_union_continuation: $ => seq($.positional_union_sigil, $.grelex),
    smart_union_continuation: $ => seq($.smart_union_sigil, $.grelex),
    corresponding_union_continuation: $ => seq($.corresponding_union_sigil, $.grelex),
    minus_continuation: $ => seq($.minus_sigil, $.grelex),

    // '&' holds only declared edges and selects by the term's exact canonical
    // spelling; '&&' composes edge relations. The context is a light mention.
    // The peer takes the outer mark: `users_t(*) &(::normal) orders_t?(*)`
    // keeps every left row.
    edge_continuation: $ => seq(
      field('operator', choice($.transitive_edge_sigil, $.edge_sigil)),
      optional(field('context', $.edge_context)),
      field('term', choice($.named_grelex, $.outer_grelex)),
    ),

    edge_context: $ => seq('(', $.symbol, ')'),

    // A destructure occupies predicate position but is not a predicate: it
    // EXPANDS. Its tree_pattern is a static heading witness — declared, never
    // evaluated.
    destructure_relex: $ => seq(
      field('source', $.domain_expression),
      field('mode', $.destructure_mode),
      // The pattern is a member LIST; braces belong to the NESTING form, so
      // a metadata binding standing alone at the top of a destructure is the
      // whole pattern and needs none.
      field('pattern', choice($.tree_pattern, $.metadata_binding)),
    ),

    destructure_mode: $ => seq(
      $.destructure_sigil,
      optional($.reduction_sigil),
    ),

    // ONE name; '?' marks the access as outer.
    outer_grelex: $ => seq(
      field('relation', $.relation_name),
      $.outer_marker,
      optional(field('ho_part', $.ho_part)),
      '(',
      field('interior', choice($.argumentative_form, $.interior)),
      ')',
    ),

    outer_anon_grelex: $ => seq('?_(', $.anon_body, ')'),

    // THE ANON HEADER IS A SLOT ROW, and an existence-marked one IS the
    // inverted membership: `+_("MA" @ bst; dst)` derives what
    // `"MA" in (bst; dst)` derives. The opener is one compound token, like
    // `?_(`, because the marker and the `_(` cannot be told apart once
    // whitespace stands between them. Polarity is DATA: one production, and
    // the token says which.
    exists_anon_grelex: $ => seq($.exists_anon_open, $.anon_body, ')'),

    exists_anon_open: $ => choice('+_(', token(seq('\\', '+_('))),

    // =====================================================================
    // Argument rows
    // =====================================================================

    // THE LIFT'S COST: '&' BOUNDS the arguments and ';' separates lifted ROWS,
    // so `f(users(*) & 1, 2; 10, 20)(*)` ≡ `f(users(*), _(1, 2; 10, 20))(*)`.
    // Both glyphs are CST-only and dissolve at build into ordinary arguments.
    //
    // WITH NO '&' THERE IS NOTHING TO BOUND, so every row lifts:
    // `f("a"; "b")(*)` ≡ `f(_("a"; "b"))(*)`. The bounded shape cannot absorb
    // the first row into its argument list — one written row-set split across
    // two roles passes a one-row relation where the author wrote several, and
    // the surplus rows vanish with no diagnostic. The ';' is what tells the
    // two shapes apart, and it arrives after the shared prefix.
    ho_part: $ => seq(
      '(',
      choice(
        // `;`-rows left of `&` are one lifted relation, rows right of it
        // another: `f(1, 100; 2, 200 & 1, "x"; 2, "y")(*)` supplies two.
        // Disambiguating exactly that is why `&` was adopted (FN.9).
        seq(
          field('lifted', $.data_row),
          repeat1(seq(';', field('lifted', $.data_row))),
          optional(seq(
            $.lift_sigil,
            field('second', $.data_row),
            repeat(seq(';', field('second', $.data_row))),
          )),
        ),
        seq(
          commaSep1($, $.ho_argument),
          optional(seq(
            $.lift_sigil,
            field('lifted', $.data_row),
            repeat(seq(';', field('lifted', $.data_row))),
          )),
        ),
      ),
      ')',
    ),

    // ONE relation carrier among ho_arguments: whether a grelex binds a
    // relation parameter or stands in a scalar slot is judged at build against
    // the callee's descriptor, never at parse.
    //
    // A BARE GLOB IS NOT AN ARGUMENT. `f(*)` is the ordinary one-group access,
    // so admitting `*` here would make the left group of `f(*)(*)` a silently
    // discarded open-parameter naming instead of the refusal it is. A relation
    // is supplied as a relation — `f(users(*))(*)` — or through the relation
    // hole at a landing site.
    ho_argument: $ => choice(
      $.residual_designator,
      $.grelex,
      $.ho_argument_reference,
      $.ground,
      $.relation_hole,
    ),

    // In a rule-value position, one argument group is a configured prefix,
    // not a relational access. Keeping this production inside `ho_argument`
    // makes the enclosing formal the authority that decides the role. The
    // same bytes in a relation formal remain a whole relation actual.
    residual_designator: $ => seq(
      field('relation', $.relation_name),
      field('ho_part', $.ho_part),
    ),

    // AN ARGUMENT THAT ADDRESSES A COLUMN REACHES AS FAR AS ANY REFERENCE:
    // `json_each(d.data)(*)` says which live scope holds the column being
    // passed, and `json_each(|1|)(*)` reaches the same column by position.
    ho_argument_reference: $ => $.reference,

    relation_hole: $ => choice($.landing, $.skipped),

    argumentative_form: $ => commaSep1($, $.slot),

    // A slot never holds a PREDICATE: a truth standing here is a crossed
    // VALUE the column unifies with, reached through the ordinary value
    // grammar. The predicate reading takes the comma road.
    slot: $ => choice(
      $.named_reference,
      $.disregarded,
      $.constraint_term,
      $.renamed_slot,
    ),

    // THE WRITTEN NAME IS THE NAMING (FN.12), so a positional slot has nothing
    // for `as` to rename. Recognized here so the ruled teaching can name the
    // alias the author wrote and point at the projection that does rename,
    // rather than a generic syntax error at the `as`. Nothing normalizes it:
    // it exists to be refused.
    renamed_slot: $ => seq(
      field('slot', choice($.named_reference, $.disregarded, $.constraint_term)),
      $.as_keyword,
      field('alias', $.identifier),
    ),

    constraint_term: $ => $.function_application,

    // =====================================================================
    // Anonymous tables
    // =====================================================================

    anon_body: $ => seq(
      optional(seq(field('header', $.header_row), $.separator)),
      sep1(';', $.data_row),
    ),

    // THE ANON HEADER IS A SLOT ROW — the caller-pattern slot law, verbatim.
    header_row: $ => commaSep1($, $.header_item),

    header_item: $ => seq($.slot, optional($.sparse_mark)),

    data_row: $ => commaSep1($, $.datum),

    datum: $ => choice($.domain_expression, $.sparse_fill),

    // ONE SHAPE FOR EVERY TABULAR INTERIOR: a fill names the columns it
    // supplies and the constants it supplies them with, separated the way a
    // header is separated from its rows. A single-column fill is the one-item
    // case of that, not a different production.
    sparse_fill: $ => seq(
      '_(',
      commaSep1Field($, 'column', $.identifier),
      $.separator,
      commaSep1Field($, 'value', $.ground),
      ')',
    ),

    // =====================================================================
    // Let blocks
    // =====================================================================

    // ONE let block. A pure let block and an effect one would accept the same
    // items until an effect CTE's '!' appeared, so two repeat nonterminals over
    // the same alternatives fork at EVERY item and the stacks multiply — five
    // preamble bindings was enough to exhaust them. The relex/effrelex split is
    // carried by the BODY, which is where it is decidable; whether an effect
    // CTE may stand in a pure query is a judgment over the built block.
    let_block: $ => repeat1(choice($.cte, $.cfe, $.effect_cte, $.ddl_annotation)),

    cte: $ => choice($.standard_cte, $.label_cte, $.ho_cte),

    // A query-scoped label is a BARE name — no namespace. It outranks the
    // head-first reading: `users(*) : adults` is the labelling shorthand, not
    // a definition of `users` with body `adults`.
    //
    // `!!` IS EVIDENCE ABOUT THE RELATION, so it travels with it. A label is
    // presentation — naming a result changes what it is called and not which
    // rows it holds — so a marked source may be named, and the name carries
    // the mark to the terminal that consumes it.
    label_cte: $ => prec.dynamic(1, seq(
      field('body', choice($.let_free_relex, $.mutation_source)),
      ':',
      field('name', $.identifier),
      optional($.fixpoint_badge),
    )),

    standard_cte: $ => seq(
      field('name', $.predicate_identifier),
      field('head', choice($.argumentative_heading, $.glob_heading)),
      ':',
      field('body', $.let_free_relex),
    ),

    // The COMMON HIGHER-ORDER EXPRESSION: the consulted `ho_rule`'s head with
    // the SHADOW-NECK — a parameterized rule that lives for the query. The
    // parameter group and the head group are the rule's own (`ho_param`,
    // `head_term`), so a formal admitted in a file is admitted here; the
    // body is the query's own text and stays a `let_free_relex` like every
    // other binding's. No badge position: a query-scoped parameterized
    // fixpoint has no ruling to stand on.
    ho_cte: $ => seq(
      field('name', $.predicate_identifier),
      '(',
      commaSep1($, $.ho_param),
      ')',
      '(',
      field('head', choice(commaSep1($, $.head_term), $.glob)),
      ')',
      ':',
      field('body', $.let_free_relex),
    ),

    // The deduplicating fixpoint; legal only on a recursive target.
    fixpoint_badge: $ => $.percent_sigil,

    // The heading PAYLOAD, without the subject. Named `…_heading` rather than
    // the semantic grammar's `…_head` because the subject sits on the PARENT:
    // every named form spells `name: (predicate_identifier)` itself, so a typed
    // consumer and a highlighting query address the subject one way whether the
    // form is a rule or a query-scoped binding.
    argumentative_heading: $ => seq(
      optional($.fixpoint_badge),
      '(',
      commaSep1($, $.head_term),
      ')',
    ),

    // A ground term SUPPLIES a constant — SUPPLY IS ELABORATION; one law for
    // the ':' and ':-' necks.
    head_term: $ => seq(
      choice($.identifier, $.ground),
      optional(seq($.as_keyword, field('alias', $.identifier))),
    ),

    glob_heading: $ => seq(
      optional($.fixpoint_badge),
      '(',
      $.glob,
      ')',
    ),

    // One list: a query-scoped function. Two: HO-CFE — the first list holds
    // the curried (function-valued) params.
    cfe: $ => seq(
      field('name', $.identifier),
      ':(',
      optional(field('first_params', $.cfe_params)),
      ')',
      optional(seq('(', optional(field('second_params', $.cfe_params)), ')')),
      ':',
      field('body', $.domain_expression),
    ),

    cfe_params: $ => commaSep1($, $.cfe_param),

    cfe_param: $ => choice($.context_marker, $.callable_param, $.plain_param),

    callable_param: $ => seq(field('name', $.identifier), ':(', ')'),
    plain_param: $ => $.identifier,

    // CCAFE — the context-capturing CFE: '..' captures enclosing-row columns,
    // implicit or declared. The brace group is its own production because
    // `..{}` declares an EMPTY capture and `..` declares an implicit one:
    // both have zero identifiers, so only the group's presence tells them
    // apart.
    context_marker: $ => seq('..', optional($.context_capture)),

    context_capture: $ => seq('{', optional(commaSep1($, $.identifier)), '}'),

    // =====================================================================
    // Destructure patterns — a static heading witness
    //
    // MIRROR LAW: the pattern grammar mirrors the constructor grammar member
    // for member. Constructor and pattern curlies are DISTINCT node kinds
    // generated from one shared shape, so a side-illegal member is a parse
    // error rather than a builder check: diff `record_member` against
    // `pattern_member` and every difference must be a licensed exception.
    // =====================================================================

    // The braces belong to the pattern itself: `j ~= {a, b}`, `"k": {…}` and
    // `|> .t{…}` all reach the SAME braced carrier, so there is no second
    // "pattern braces" kind wrapping it.
    tree_pattern: $ => choice($.record_pattern, $.array_pattern),

    record_pattern: $ => seq('{', commaSep1($, $.pattern_member), '}'),

    pattern_member: $ => choice(
      $.binder,
      $.keyed_binding,
      $.nested_pattern,
      $.path_binding,
      $.metadata_binding,
      $.disregarded,
    ),

    binder: $ => $.identifier,

    // Rename; nested structure kept as-is, not iterated.
    keyed_binding: $ => seq($.key, field('name', $.identifier)),

    // "k": {…} nests; "k": ~> {…} iterates.
    nested_pattern: $ => seq($.key, choice($.tree_pattern, $.iteration)),

    // FN.22 (amended): a metadata group may stand as an induced member's
    // body — `"by_country": ~> country:~> {sales}` iterates into the keyed
    // group.
    iteration: $ => seq($.reduction_sigil, choice($.tree_pattern, $.metadata_binding)),

    // Reach without matching. A path binding publishes the underscore-
    // flattened spelling; `as` renames.
    path_binding: $ => seq(
      $.path,
      optional(seq($.as_keyword, field('alias', $.identifier))),
    ),

    // KEYS become column values; `country: ~> _` binds keys and disregards
    // contents.
    //
    // MIRROR LAW: a metadata group CHAINS on the construction side
    // (`meta_target = enclyph_like | metadata_group`), so it chains here.
    // `g:~> k:~> {v}` and its pattern are the same shape read in two
    // directions, and a level that had to be braced on one side only would
    // not be a mirror.
    metadata_binding: $ => seq(
      field('key_column', $.key_column),
      $.metadata_sigil,
      choice($.tree_pattern, $.metadata_binding, $.disregarded),
    ),

    array_pattern: $ => seq('[', commaSep1($, $.indexed_binding), ']'),

    // A pattern, never a domain expression. The `.` opens a member wherever
    // one may stand — an immediate `.` would have admitted only the member
    // written against the bracket, and every later one is written after a
    // comma and a space.
    indexed_binding: $ => seq(
      '.',
      field('index', $.number),
      // THE INDEX DIRECTS, THE REACH FOLLOWS. An array's member is selected
      // by position and then read into, which is what a record member's path
      // binding does one level down.
      optional(field('reach', $.path)),
      optional(seq($.as_keyword, field('alias', $.identifier))),
    ),

    // =====================================================================
    // Annotations — THE SET IS CLOSED
    // =====================================================================

    annotation: $ => choice(
      $.definition_annotation,
      $.reserved_annotation,
    ),

    // The closed annotation family carries declarations, not executable
    // relation checks. Runtime assertions are ordinary `assert!` effects.
    definition_annotation: $ => choice(
      $.error_annotation,
      $.danger_annotation,
      $.config_annotation,
      $.ddl_annotation,
    ),

    error_annotation: $ => seq(
      '(~~error',
      optional(field('uri', $.annotation_uri)),
      '~~)',
    ),

    // A DANGER GATE is a named, refused-by-default behavior; this annotation
    // acknowledges it beside the query whose meaning or limits it changes.
    danger_annotation: $ => seq(
      '(~~danger',
      field('uri', $.annotation_uri),
      '~~)',
    ),

    // The option hook — a setting that changes tool behavior, never meaning.
    config_annotation: $ => seq(
      '(~~config',
      field('uri', $.annotation_uri),
      optional(field('value', $.ground)),
      '~~)',
    ),

    ddl_annotation: $ => seq(
      '(~~ddl',
      // IMMEDIATE: the namespace colon stands against the marker — whitespace
      // between `(~~ddl` and `:` has no derivation. A body never opens with a
      // bare `:` against the marker; it opens with a definition head.
      optional(seq(token.immediate(':'), field('namespace', $.string))),
      optional(field('body', $.ddl_content)),
      '~~)',
    ),

    // THE BODY IS DEFINITION CONTENT, parsed with its enclosing submission —
    // never inline text. Stated, not borrowed from `definition_file`: the
    // file production also admits `top_level_goal`, and a block runs no
    // goals. Nested blocks are recursive typed children; `repeat1` because
    // Tree-sitter refuses a nullable named rule, so emptiness lives at the
    // use site — an absent body IS the lawful empty block.
    ddl_content: $ => repeat1(choice(
      $.entity_definition,
      $.ddl_annotation,
    )),

    // Reserved room, recognized so the refusal can teach rather than read as a
    // typo. It refuses toward the effect algebra. A generic `(~~name …~~)` is
    // not admitted at all — that is what THE SET IS CLOSED means here.
    reserved_annotation: $ => seq(
      '(~~emit',
      optional(field('uri', $.annotation_uri)),
      optional(field('body', $.reserved_text)),
      '~~)',
    ),

    annotation_uri: $ => seq(
      token.immediate('://'),
      sep1('/', $.uri_segment),
    ),

    uri_segment: $ => /[a-zA-Z0-9_][a-zA-Z0-9_.-]*/,

    // The reserved room's body is opaque: it is consumed by the parsed
    // `reserved_annotation` production but never interpreted as DelightQL
    // content — the block refuses whole, teaching toward the effect algebra.
    reserved_text: $ => repeat1($._opaque_annotation_text),

    // Everything up to a closing `~~)`, one `(` at a time. Owned by the two
    // positions whose bodies are prose rather than DelightQL — the reserved
    // `(~~emit …~~)` room and `(~~docs …~~)` documentation. A DDL body is
    // definition content and never lexes through here.
    _opaque_annotation_text: $ => token(choice(
      /([^~(]|~[^~]|~~[^)])+/,
      /\(/,
    )),

    smart_comment: $ => seq('(/*', optional($._opaque_comment_text), '*/)'),
    _opaque_comment_text: $ => /([^*]|\*[^\/]|\*\/[^)])+/,

    stop_point: $ => choice(
      '(!)',
      seq('(/!', $._opaque_stop_text, '!/)'),
    ),
    _opaque_stop_text: $ => /([^!]|![^\/]|!\/[^)])+/,

    debug_point: $ => '>>>',

    // =====================================================================
    // Effects
    // =====================================================================

    effect_identifier: $ => seq($.predicate_identifier, $.effect_marker),

    effrelex: $ => seq(optional(field('let_block', $.let_block)), field('chain', $.effect_chain)),

    // Every effect chain contains at least one effect call with no counting
    // rule needed: the first alternative IS one, and the pipe and connective
    // alternatives ADD one. Ordinary continuations are pure by construction,
    // so the continuation alternative is how pure material attaches.
    //
    // An annotation stands at any CONTINUATION ANCHOR — it decorates a
    // position and is not a continuation — and an effect chain has the same
    // anchors every other chain has. Without this an error hook could not
    // stand beside the statement whose outcome it judges.
    effect_chain: $ => choice(
      $.direct_effrelex,
      seq($.effect_chain, $.continuation),
      // Left, so the annotation binds to the chain in hand: a `(~~ddl …~~)`
      // after a chain decorates THAT statement rather than opening the next
      // one's let block.
      prec.left(1, seq($.effect_chain, $.annotation)),
      // Substitution, not combination: the piped source becomes the call's
      // first parameter. dml_form appears ONLY here — a mutation source exists
      // solely to be fed to its terminal, never to be joined.
      seq(
        field('source', choice($.let_free_relex, $.dml_form, $.effect_chain)),
        $.pipe_operator,
        field('terminal', $.post_pipe_effrelex),
      ),
      // THE UNWRAP PIPE: Q !> S ≡ Q |> S |> .returned(*) — a pipe form, never
      // a boundary. Any right side whose result carries a `returned` payload
      // works, effectful or not; a right side without one gets the error the
      // equivalence itself produces.
      seq(
        field('source', choice($.let_free_relex, $.mutation_source, $.effect_chain)),
        $.unwrap_pipe_operator,
        field('terminal', choice($.post_pipe_effrelex, $.pure_invocation)),
      ),
      // Genuine peer joins: two relations meet and a combined relation
      // results.
      seq(
        field('left', choice($.let_free_relex, $.effect_chain)),
        field('connective', $.binary_connective),
        field('right', $.direct_effrelex),
      ),
    ),

    binary_connective: $ => choice(
      $.comma_sigil,
      $.corresponding_union_sigil,
      $.smart_union_sigil,
      $.positional_union_sigil,
      $.minus_sigil,
    ),

    direct_effrelex: $ => choice(
      $.effrelex_argumentative_functor,
      $.effrelex_interior_functor,
      $.lower_order_effrelex,
    ),

    // An empty ho_part is deliberately unspellable: the `()` in `f!()(access)`
    // is a surface marker that normalizes to an omitted ho_part and never
    // constructs an empty HoPart.
    effect_argument_part: $ => choice($.ho_part, $.empty_effect_arguments),

    empty_effect_arguments: $ => seq('(', ')'),

    effrelex_argumentative_functor: $ => seq(
      field('name', $.effect_identifier),
      field('arguments', $.effect_argument_part),
      '(',
      field('access', $.argumentative_form),
      ')',
    ),

    effrelex_interior_functor: $ => seq(
      field('name', $.effect_identifier),
      field('arguments', $.effect_argument_part),
      '(',
      field('access', $.interior),
      ')',
    ),

    // A lower-order ground call has ONE group — receipt access. Order is
    // judged after parse, never during it.
    lower_order_effrelex: $ => seq(
      field('name', $.effect_identifier),
      '(',
      field('access', choice($.argumentative_form, $.interior)),
      ')',
    ),

    // On the right of a pipe, one group is always receipt access and two
    // groups are always (parameters)(receipt access). Read by position;
    // neither group contents nor a callee descriptor participates in parsing.
    post_pipe_effrelex: $ => seq(
      field('name', $.effect_identifier),
      optional(field('arguments', $.effect_argument_part)),
      '(',
      field('access', choice($.argumentative_form, $.interior)),
      ')',
    ),

    dml_form: $ => choice($.insert_source, $.mutation_source),

    insert_source: $ => $.relex,

    // ONE production — the consuming terminal (update!/delete!) classifies; a
    // per-terminal pair would be byte-identical definitions.
    // An annotation stands at any CONTINUATION ANCHOR, and a mutation source
    // is a chain like any other — there is no anchor here the law exempts.
    // The continuations are optional: `emp!!(*) |> delete!(emp(*))(*)` reaches
    // every row, and a chain that restricts nothing is still a chain.
    mutation_source: $ => seq(
      $.marked_target,
      repeat(choice($.continuation, $.annotation)),
    ),

    marked_target: $ => seq(
      field('name', $.predicate_identifier),
      $.mutation_marker,
      '(',
      $.glob,
      ')',
    ),

    effect_cte: $ => choice($.effect_standard_cte, $.effect_label_cte, $.effect_ho_cte),

    effect_label_cte: $ => prec.dynamic(1, seq(
      field('body', choice($.let_free_relex, $.effect_chain)),
      ':',
      field('name', $.identifier),
      $.effect_marker,
    )),

    effect_standard_cte: $ => seq(
      field('head', choice($.effect_argumentative_head, $.effect_glob_head)),
      ':',
      field('body', choice($.let_free_relex, $.effect_chain)),
    ),

    // The effect mirror of `ho_cte`: the consulted `effect_rule`'s head with
    // the SHADOW-NECK. Its head is a glob, as every effect rule's is.
    effect_ho_cte: $ => seq(
      field('name', $.effect_identifier),
      '(',
      commaSep1($, $.ho_param),
      ')',
      '(',
      $.glob,
      ')',
      ':',
      field('body', choice($.let_free_relex, $.effect_chain)),
    ),

    effect_argumentative_head: $ => seq(
      field('name', $.effect_identifier),
      '(',
      commaSep1($, $.head_term),
      ')',
    ),

    effect_glob_head: $ => seq(
      field('name', $.effect_identifier),
      '(',
      $.glob,
      ')',
    ),

    // =====================================================================
    // Definitions
    // =====================================================================

    entity_definition: $ => choice(
      $.rule_form,
      $.fact_like,
      $.fact_function,
      $.edge_declaration,
    ),

    // AN EDGE DECLARATION IS A GROUND HEAD: sugar for
    // `&(:a-term, :b-term, ::ctx)(*) :- body`, all three params ground on
    // mentions; fixity is surface. Only '&' declares; '&&' walks and is never
    // a head.
    edge_declaration: $ => seq(
      field('left', $.edge_term),
      $.edge_sigil,
      optional(field('context', $.edge_context)),
      field('right', $.edge_term),
      $.definition_neck,
      optional($.doc_slot),
      field('body', $.relex),
    ),

    // The declared spelling — its exact CANONICAL bytes are the edge's
    // identity.
    edge_term: $ => $.named_grelex,

    // Rule bodies come in exactly three positions: RELATIONAL (fo_rule,
    // ho_rule), VALUE (function_rule; constant_rule its nullary), TRUTH
    // (sigma_rule).
    rule_form: $ => choice(
      $.effect_rule,
      $.ho_rule,
      $.fo_rule,
      $.function_rule,
      $.constant_rule,
      $.sigma_rule,
    ),

    // The NULLARY function rule, paren-less; the citation `:pi` is its
    // consumer.
    constant_rule: $ => seq(
      field('name', $.identifier),
      $.definition_neck,
      optional($.doc_slot),
      field('body', $.domain_expression),
    ),

    // The PREDICATE rule — truth category. A CATEGORY constraint: the body is
    // TRUTH material, never a domain expression, so `p(x) :- users` is a
    // parse-level category error rather than a builder check.
    sigma_rule: $ => seq(
      field('name', $.predicate_identifier),
      '(',
      commaSep1($, $.identifier),
      ')',
      $.definition_neck,
      optional($.doc_slot),
      field('body', $.sigma_body),
    ),

    sigma_body: $ => $.truth_expression,

    // A relational clause body is a complete `relex`: its optional let block
    // holds clause-local CTEs and CFEs. A definition does not narrow its body
    // to `let_free_relex` — a rule that wants a local binding writes one where
    // any other query would.
    fo_rule: $ => seq(
      field('name', $.predicate_identifier),
      field('head', choice($.argumentative_heading, $.glob_heading)),
      $.definition_neck,
      optional($.doc_slot),
      field('body', $.relex),
    ),

    ho_rule: $ => seq(
      field('name', $.predicate_identifier),
      '(',
      commaSep1($, $.ho_param),
      ')',
      '(',
      field('head', choice(commaSep1($, $.head_term), $.glob)),
      ')',
      $.definition_neck,
      optional($.doc_slot),
      field('body', $.relex),
    ),

    // A ground constant is the clause's member of the enumeration domain: a
    // PURE-GROUND position is one where every clause supplies a constant, and
    // those constants are the domain a free call-site argument ranges over.
    ho_param: $ => choice(
      $.rule_param,
      $.open_relation_param,
      $.declared_relation_param,
      $.scalar_param,
      $.ground,
    ),

    // A CLOSED RESIDUAL CONTRACT. `...` existentially hides the complete
    // left prefix already sealed into the value; the rest of this row is the
    // exact ordered mode still required. The final group is the heading the
    // residual publishes. These are signature members, not binders of the
    // enclosing rule, so they have their own productions instead of reusing
    // `ho_param` recursively.
    rule_param: $ => seq(
      field('name', $.identifier),
      '(',
      '...',
      optional($.comma_sigil),
      commaSep1($, $.rule_mode_param),
      ')',
      '(',
      field('head', choice(commaSep1($, $.identifier), $.glob)),
      ')',
    ),

    rule_mode_param: $ => choice(
      $.open_relation_param,
      $.declared_relation_param,
      $.scalar_param,
    ),

    open_relation_param: $ => seq(field('name', $.identifier), '(', $.glob, ')'),
    declared_relation_param: $ => seq(
      field('name', $.identifier),
      '(', commaSep1($, $.identifier), ')',
    ),
    scalar_param: $ => $.identifier,

    function_rule: $ => seq(
      field('name', $.predicate_identifier),
      ':(',
      commaSep1($, $.function_param),
      ')',
      $.definition_neck,
      optional($.doc_slot),
      field('body', $.domain_expression),
    ),

    // CCAFE — THE CONTEXT-CAPTURING FUNCTION, at definition scope. `..`
    // captures enclosing-row columns exactly as it does in a query-scoped CFE;
    // a consulted function and a query-scoped one are the same kind of thing,
    // so the parameter list is the same list. (FN.38)
    function_param: $ => choice(
      $.context_marker,
      $.callable_param,
      $.guarded_param,
      $.plain_param,
    ),

    guarded_param: $ => seq(field('name', $.identifier), $.guard),

    // A CLAUSE BODY IS A COMPLETE EXPRESSION OF ITS CATEGORY. The relational
    // rule's body is a `relex` — a let block and the chain it feeds — and the
    // effect rule's is the effectual twin, `effrelex`. Narrowing it to the
    // bare chain made a labelled CTE unspellable in an effect rule while the
    // same block is spellable in every other effect position, which is a
    // shape accident and not a distinction the effect algebra draws.
    //
    // The block is ONE production carrying pure and effectual bindings alike,
    // and the body after it is still an `effect_chain`: an effect rule's body
    // effectuates, a pure one does not, and neither becomes a mixture.
    effect_rule: $ => seq(
      field('name', $.effect_identifier),
      optional(seq('(', commaSep1($, $.ho_param), ')')),
      '(',
      $.glob,
      ')',
      ':-',
      optional($.doc_slot),
      field('body', $.effrelex),
    ),

    fact_like: $ => choice($.fact_form, $.ho_fact_form),

    // The PARAMETERIZED fact: a fact body behind an ho_part. Sugar for the
    // necked ho_rule-with-fact-body; ONE elaboration.
    ho_fact_form: $ => seq(
      field('name', $.predicate_identifier),
      '(',
      commaSep1($, $.ho_param),
      ')',
      '(',
      field('body', $.fact_body),
      ')',
    ),

    // The FACT-FUNCTION: a fact whose '->' declares the functional mode
    // (inputs -> outputs). It is always callable. Without a default its arms
    // also form a finite relation; a default makes the family callable-only.
    fact_function: $ => seq(
      field('name', $.predicate_identifier),
      '(',
      commaSep1Field($, 'inputs', $.identifier),
      $.arrow,
      commaSep1Field($, 'outputs', $.identifier),
      $.separator,
      sep1(';', $.fact_arm),
      optional(seq(';', $.fact_default)),
      ')',
    ),

    // Fielded for the same reason match_arm is: a ground input and a domain
    // output are the same kind on both sides of the arrow.
    fact_arm: $ => seq(
      commaSep1Field($, 'inputs', $.ground),
      $.arrow,
      commaSep1Field($, 'outputs', $.domain_expression),
    ),

    fact_default: $ => seq(
      $.disregarded,
      $.arrow,
      commaSep1Field($, 'outputs', $.domain_expression),
    ),

    fact_form: $ => seq(
      field('name', $.predicate_identifier),
      '(',
      field('body', $.fact_body),
      ')',
    ),

    // FACT ELABORATION: a fact is not a distinct body kind — it elaborates
    // ONCE, at definition, into an ordinary ground relational clause body.
    // header_row and separator are anon_body's: ONE shape for every tabular
    // interior.
    fact_body: $ => seq(
      optional(seq(field('header', $.header_row), $.separator)),
      sep1(';', $.fact_row),
    ),

    fact_row: $ => commaSep1($, $.fact_datum),

    // 'as': a heading offer. A SPARSE FILL IS A DATUM WHEREVER A HEADER MAY
    // DECLARE ONE SPARSE: `fact_body` reuses `header_row`, so a fact declares
    // sparse columns exactly as an anonymous table does, and the same fill
    // supplies them. The fill's values are `ground`, so admitting it here
    // leaves every fact datum a constant.
    fact_datum: $ => choice(
      seq(
        $.ground,
        optional(seq($.as_keyword, field('alias', $.identifier))),
      ),
      $.sparse_fill,
    ),

    // The doc slot after a definition neck.
    //
    // ONE definition document, and it is the grammar that says so: a second
    // has no derivation, rather than a builder counting them and a consumer
    // being able to forget to. The annotations beside it are the ones that
    // need no relation.
    doc_slot: $ => choice(
      repeat1($.definition_annotation),
      seq(
        repeat($.definition_annotation),
        $.definition_doc,
        repeat($.definition_annotation),
      ),
    ),

    // A DEFINITION'S OWN DOCUMENTATION, and nowhere else.
    //
    // Not an annotation: THE SET IS CLOSED speaks about the forms that decorate
    // a POSITION in a chain, and this decorates a DEFINITION — it stands in the
    // doc slot after a neck and has no derivation at a continuation anchor. Its
    // body is opaque text that travels to the entity's `doc`.
    //
    // `(/* … */)` is the other documentation form and stays distinct: a smart
    // comment attaches by position anywhere, this belongs to one clause.
    definition_doc: $ => seq(
      '(~~docs',
      optional(field('body', $.doc_text)),
      '~~)',
    ),

    // The same opaque-text token the reserved room uses: everything up to
    // the closing marker, `*` and `/` and `!` included, because
    // documentation is prose and not DelightQL.
    doc_text: $ => repeat1($._opaque_annotation_text),

    // =====================================================================
    // Spec position — the operator layer
    // =====================================================================

    // SORTING LAW — everything after `|>` and every pipe-less postfix form is
    // exactly one of: a call (substitution; never survives as a node), an
    // operator (anonymous, spec-directed), or chain structure.
    post_pipe_form: $ => choice(
      $.pure_invocation,
      $.pipe_operation,
      $.pipe_structural,
    ),

    // Named `pipe_operation` because `pipe_operator` names the `|>` token the
    // tooling vocabulary prescribes; the semantic grammar's production of that
    // name is this one.
    pipe_operation: $ => choice(
      $.project,
      $.project_out,
      $.rename,
      $.embed,
      $.map_cover,
      $.embed_map_cover,
      $.transform,
      $.group,
    ),

    // Payload-only narrows are post-pipe ONLY: RETENTION DECIDES POSITION.
    pipe_structural: $ => choice(
      $.ordering,
      $.reposition,
      $.narrowing_access,
      $.narrowing_destructure,
    ),

    // Context-keepers are postfix — a postfix form extends the complete
    // expression to its left. ONE OPERAND; no postfix precedences.
    postfix_operator: $ => choice(
      $.meta,
      $.witness,
      $.signed_witness,
      $.domain_activate,
      $.using,
      $.drill,
    ),

    // THE SINGLETON PIPE — sugar: `R ~> f:(*)` normalizes to the zero-key
    // group `R |> %( ~> f:(*))`.
    singleton_reduction: $ => seq(
      $.reduction_sigil,
      choice($.out_item, $.metadata_group),
    ),

    project: $ => seq('(', commaSep1($, $.out_item), ')'),

    // A SUBTRACTION ENUMERATES. The columns to remove are addressed the way
    // a cover's selector addresses the columns to apply to — a reference, or
    // a spread that expands to the several it covers. THE SPREAD IS A
    // MULTI-DOMEX admitted in enumerating positions, and this is one.
    project_out: $ => seq('-(', commaSep1($, $.selector_item), ')'),
    rename: $ => seq($.star_sigil, token.immediate('('), commaSep1($, $.rename_pair), ')'),

    rename_pair: $ => seq(
      field('source', $.rename_source),
      $.as_keyword,
      field('target', $.name_target),
    ),

    rename_source: $ => choice($.reference, $.regex, $.glob),
    name_target: $ => choice($.identifier, $.as_name_template),

    embed: $ => seq('+(', commaSep1($, $.out_item), ')'),

    // THE GUARD IS PER-CELL: conditional application judged per row — where it
    // holds the cell is redefined, where it fails the column's own value rides
    // through. Never a row filter.
    map_cover: $ => seq(
      '$(', field('cover', $.callable), ')',
      '(', field('selector', $.selector), optional($.guard), ')',
    ),

    // No guard: a guarded application needs a fallback value, and an embedded
    // column has none.
    // NAMING IS ONE ACT: the embedded column is named with the same `as` the
    // rename cover spells before its name template. A template standing bare
    // after the callable (`+$(f:() :"…")`) has no derivation.
    embed_map_cover: $ => seq(
      '+$(',
      field('cover', $.callable),
      optional(seq($.as_keyword, field('naming', $.as_name_template))),
      ')',
      '(', field('selector', $.selector), ')',
    ),

    // Naming is MANDATORY and addresses an EXISTING column: a transform
    // REDEFINES in place; extension is embed's job.
    transform: $ => seq(
      '$$(',
      commaSep1($, $.transform_item),
      optional($.guard),
      ')',
    ),

    // A transform's naming ADDRESSES an existing column, so it reaches exactly
    // as far as a reference does and a live scope may qualify it. A
    // projection's `as` BAPTISES, and a baptism publishes what was written, so
    // that one stays an identifier.
    transform_item: $ => seq($.out_value, $.transform_naming),

    transform_naming: $ => seq($.as_keyword, field('name', $.named_reference)),

    // Read '~>' as AND: the keys on the left are DISTINCTED ON; the right is
    // reduced per group. Both halves empty is underivable by construction.
    group: $ => seq(
      $.percent_sigil,
      token.immediate('('),
      choice(
        commaSep1($, $.group_key),
        seq(
          optional(commaSep1($, $.group_key)),
          $.reduction_sigil,
          commaSep1($, $.reduction_item),
        ),
      ),
      ')',
    ),

    // A KEY PUBLISHES, and it publishes exactly what an out item publishes:
    // one column per key, named or minted. So it READS as an out item too —
    // the crossing's out-item admission is the same admission here, and
    // `%(age > 30 ~> count:(*))` groups by the predicate's value.
    group_key: $ => choice($.named_group_key, $.spread),

    named_group_key: $ => seq($.out_value, optional($.naming)),

    reduction_item: $ => choice(
      $.out_item,
      $.pivot,
      $.group_delegate,
      $.metadata_group,
    ),

    // THE IN IS THE HEADING WITNESS: a pivot requires an authored membership
    // predicate on its key in the same chain. Reduction position ONLY; both
    // operands are non-infix.
    pivot: $ => seq(
      field('value', $.operand),
      $.of_keyword,
      field('key', $.operand),
    ),

    // DISTINCT ON semantics: ordered consumption of the group's rows — NOT a
    // window.
    group_delegate: $ => seq(
      '(', commaSep1($, $.out_item), ')',
      $.window_sigil,
      optional($.ordering),
    ),

    out_item: $ => choice($.named_out_item, $.spread),

    // Naming on a spread refuses — it expands to many; bulk renames are the
    // rename cover's job.
    named_out_item: $ => seq($.out_value, optional($.naming)),

    out_value: $ => $.domain_expression,

    // ONE spelling; rename versus baptism is a classification by operand kind,
    // not two syntaxes.
    naming: $ => seq($.as_keyword, field('name', $.identifier)),

    selector: $ => commaSep1($, $.selector_item),
    selector_item: $ => choice($.reference, $.spread),

    // Spec-level — a NAME template, never the value template.
    as_name_template: $ => seq(
      ':"',
      repeat1(choice($.name_template_text, $.name_template_placeholder)),
      token.immediate('"'),
    ),
    name_template_placeholder: $ => choice('{@}', '{#}'),
    name_template_text: $ => token.immediate(/[^{}"]+/),

    // ho_part's rows are top-grammar's ho_arguments; landing and skipped
    // normalize exactly once, at build. There is NO reverse operand.
    pure_invocation: $ => seq(
      field('callee', $.relation_name),
      optional(field('ho_part', $.ho_part)),
      field('access', $.access),
    ),

    access: $ => seq('(', choice($.argumentative_form, $.interior), ')'),

    ordering: $ => seq('#(', commaSep1($, $.order_item), ')'),

    order_item: $ => seq(
      $.domain_expression,
      optional(field('direction', $.order_direction)),
    ),

    order_direction: $ => choice($.asc_keyword, $.desc_keyword),

    reposition: $ => seq($.star_sigil, token.immediate('['), commaSep1($, $.reposition_pair), ']'),

    // A negative number counts from the end.
    reposition_pair: $ => seq(
      field('source', choice($.reference, $.number)),
      $.as_keyword,
      field('position', $.number),
    ),

    narrowing_access: $ => seq('.', $.reference, $.access),

    // THE NEST NAME IS A REFERENCE, as it is on the access side: which live
    // scope holds the nested column is the qualifier's question.
    narrowing_destructure: $ => seq(
      '.', field('column', $.reference),
      field('pattern', $.record_pattern),
    ),

    // ONE application; iteration is ordinary postfix stacking — `^^` is two
    // adjacent applications, never a token.
    meta: $ => $.meta_sigil,

    witness: $ => $.polarity,
    signed_witness: $ => $.signed_witness_sigil,
    domain_activate: $ => $.star_sigil,

    using: $ => choice(
      seq('.(', commaSep1($, $.reference), ')'),
      seq('.', token.immediate('*')),
    ),

    // drill vs narrow is THE licensed same-shape pair: context kept beside
    // payload versus payload only. Position classifies; no descriptor or
    // content ever does.
    drill: $ => seq('.', $.reference, $.access),

    // ONE home — the comma member. Postfix and pipe spellings refuse.
    row_bound: $ => seq($.bound_op, $.compile_time_integer),

    // `as f` on a stage names its output and removes it from `_`'s deictic
    // domain.
    stage_name: $ => seq($.as_keyword, field('name', $.identifier)),

    // `as f(slots)` — THE ARGUMENTATIVE STAGE: names the occurrence standing
    // here and applies a total slot row to it in one act. The row is the
    // same argumentative form a caller pattern writes, and it attaches to
    // exactly the occurrence plain `as f` would name.
    argumentative_stage: $ => seq(
      $.as_keyword,
      field('name', $.identifier),
      '(',
      field('slots', $.argumentative_form),
      ')',
    ),

    // =====================================================================
    // Value position
    // =====================================================================

    domain_expression: $ => choice($.reference, $.function_application),

    // Binder interiors ONLY: lambda body, function-pipe step, sparse fill,
    // slot list.
    open_expression: $ => choice($.domain_expression, $.domain_hole),

    domain_hole: $ => choice($.disregarded, $.composition_input),

    reference: $ => choice($.named_reference, $.positional_reference),

    named_reference: $ => choice(
      // THE REFUSAL WITNESS. A column reference carries at most one qualifier,
      // and everything left of the last one was never read — which is the
      // argument against the form. Recognized here so the ruled teaching can
      // name what it found, rather than a generic syntax error where the
      // author cannot see which segment was ignored.
      seq(
        field('refused_segment', $.qualifier_name),
        token.immediate('.'),
        field('qualifier', $.qualifier),
        token.immediate('.'),
        field('name', $.identifier),
      ),
      seq(
        optional(seq(field('qualifier', $.qualifier), token.immediate('.'))),
        field('name', $.identifier),
      ),
    ),

    // The deictic `_` names a RELATION — the unnamed stage — and disregards
    // nothing. Position distinguishes it from the anaphor.
    qualifier: $ => choice($.qualifier_name, $.deictic_stage),
    qualifier_name: $ => $.identifier,

    // Authored-only: resolution answers a position; nothing survives.
    //
    // A QUALIFIER IS AN ADDRESS, and position is an addressing route like any
    // other: `u|1|` reaches position one of the scope `u`, exactly as `u.id`
    // reaches its named column. The pipe is glued to the qualifier so a guard
    // (`f:(x | x > 1)`) and an addressed position (`f:(x|1|)`) are told apart
    // by whitespace rather than by what follows.
    positional_reference: $ => $.ordinal,
    ordinal: $ => choice(
      seq(
        field('qualifier', $.qualifier),
        token.immediate('|'),
        $.compile_time_integer,
        '|',
      ),
      seq('|', $.compile_time_integer, '|'),
    ),

    // A SCALAR PARAMETER IS CODE, NOT DATA. The reference arm names a
    // definition parameter whose value is substituted to an integer before the
    // ordinary resolved query exists — it never becomes a dynamic heading or a
    // bind parameter. Which names qualify is a resolution judgment; that the
    // term is a bare name and nothing row-dependent is this layer's, and it is
    // why no column expression, application or literal-bearing operand
    // derives here.
    compile_time_integer: $ => choice($.number, $.scalar_parameter_reference),

    // A lone `_` is the disregarded anaphor, not a name, so it cannot stand
    // here: the parameter spelling admits every identifier EXCEPT that one.
    scalar_parameter_reference: $ => choice(
      alias($._parameter_name, $.identifier),
      $.stropped_form,
    ),

    _parameter_name: $ => token(/(_[a-zA-Z0-9_]+|[a-zA-Z][a-zA-Z0-9_]*)/),

    // THE SPREAD IS A MULTI-DOMEX — an authored multi-reference that EXPANDS
    // at resolution into the columns it addresses, admitted in ENUMERATING
    // positions only. Never a value; nothing of it survives resolution.
    spread: $ => choice($.glob, $.regex, $.positional_span),

    // Fielded ends: `|3:|` and `|:3|` both carry one number, so an unfielded
    // list would leave which end it bounds to be read off the colon's place.
    // The qualifier addresses the same way an ordinal's does.
    positional_span: $ => choice(
      seq(
        field('qualifier', $.qualifier),
        token.immediate('|'),
        $._span_body,
        '|',
      ),
      seq('|', $._span_body, '|'),
    ),

    _span_body: $ => choice(
      seq(field('start', $.number), ':', optional(field('end', $.number))),
      seq(':', field('end', $.number)),
    ),

    // Bare '*' stands only ALONE; qualified globs mix freely.
    glob: $ => seq(
      optional(seq(field('qualifier', $.qualifier), token.immediate('.'))),
      $.star_sigil,
    ),

    function_application: $ => choice(
      $.infix_operator,
      // THE CROSSING, whole-value stratum: an infix truth is a value the way
      // `a + b` is one — open on a side, so never an operand. An admission
      // stratum, not a second carrier: the node kind is crossed_truth.
      alias($._infix_crossing, $.crossed_truth),
      $.non_infix_application,
    ),

    _infix_crossing: $ => $._infix_truth,

    // An admission stratum, not a second carrier — the AST kind is
    // function_application. Parens are admission, not meaning: ONE carrier
    // for them, reachable both as an operand and as a complete expression
    // (`f:(x) :- (x * 2)`).
    non_infix_application: $ => choice(
      // THE CROSSING, operand stratum: a truth read as a value is an
      // ordinary value, so it stands where one stands — an operand included.
      // Only truth's NON-INFIX forms stand here, for the reason `a + b` is
      // not an operand: an operand derives no infix form.
      $.crossed_truth,
      $.parenthesized_operand,
      // The flowing value stands wherever a value does, at any depth: the CST
      // admits zero or many holes and the BUILDER judges the count once
      // (`x /-> upper:(trim:(@))` needs the inner one). Refusing a second hole
      // here would put that judgment in two places.
      $.composition_input,
      $.ground,
      $.template,
      $.functor_like,
      $.function_pipe,
      $.case_like,
      $.relation_like,
      $.enclyph_like,
      $.json_access,
    ),

    // Every callable has exactly ONE open slot — a BUILD JUDGMENT over one CST
    // carrier, not a syntactic guarantee: this admits zero or many holes and
    // the builder judges the count once. NO CASE IS A CALLABLE.
    // A COVER APPLIES A FUNCTION PER COLUMN, and a window function is a
    // function: its spec stands INSIDE the open functor's parens exactly as it
    // stands inside a window application, and the cover's own naming follows
    // the closed callable.
    callable: $ => choice($.open_functor, $.open_window_functor, $.template, $.lambda),

    // THE SPEC IS ENCLOSED BY THE CALL IT WINDOWS. `f:(x <~ %(a))` is one
    // closed value; a spec after the closing paren (`f:(x) <~ %(a)`) has no
    // derivation, so composing windowed calls never needs added parentheses.
    open_window_functor: $ => seq(
      field('callee', $.callee),
      ':(',
      optional(commaSep1($, $.open_expression)),
      optional($.guard),
      field('window', $.window_spec),
      ')',
    ),

    // Zero holes: the flowing value lands at the row's final place, which
    // is why `x /-> upper:(y)` means `upper(y, x)`.
    open_functor: $ => seq(
      field('callee', $.callee),
      ':(',
      optional(commaSep1($, $.open_expression)),
      optional($.guard),
      ')',
    ),

    // The self-denoting nullaries. A ground term is semantically a nullary
    // functor — which is why every ground position is a future
    // mention-grounding position.
    ground: $ => choice($.literal, $.mention),

    literal: $ => choice($.number, $.string, $.blob, $.boolean, $.null),

    // Structured quotation; the type-term grammar grows here.
    mention: $ => choice($.symbol, $.delimited_mention),

    functor_like: $ => choice($.standard_application, $.window_application, $.citation),

    standard_application: $ => seq(
      field('callee', $.callee),
      ':(',
      optional(commaSep1($, $.argument)),
      optional($.guard),
      ')',
    ),

    // The nullary consumer — normalizes to the zero-argument application and
    // is never ground.
    citation: $ => seq(':', field('callee', $.callee)),

    // `<~` is one glyph, two carriers: the group delegate and this window
    // context. Related by lowering, never merged in meaning.
    //
    // THE SPEC IS ENCLOSED BY THE CALL IT WINDOWS: it follows the argument row
    // inside the call's own parens, so a windowed call is a closed value and
    // `(f:(x <~ %(a)) + 2) / 3` reads with no added parentheses. A spec after
    // the closing paren (`f:(x) <~ %(a)`) has no derivation. The delegate's
    // sigil is bare because a column is a function of itself — there is no
    // call for it to sit inside.
    window_application: $ => seq(
      field('callee', $.callee),
      ':(',
      optional(commaSep1($, $.argument)),
      optional($.guard),
      field('window', $.window_spec),
      ')',
    ),

    // Partition, ordering, frame — each optional, in that order, and a comma
    // stands only BETWEEN two written items. The closing paren ends the spec,
    // so no item list to the right can be confused for a spec item.
    window_spec: $ => seq(
      $.window_sigil,
      optional(choice(
        seq(
          field('partition', $.partition),
          optional(seq(optional($.comma_sigil), field('order', $.ordering))),
          optional(seq(optional($.comma_sigil), field('frame', $.frame))),
        ),
        seq(
          field('order', $.ordering),
          optional(seq(optional($.comma_sigil), field('frame', $.frame))),
        ),
        field('frame', $.frame),
      )),
    ),

    partition: $ => seq(
      $.percent_sigil, token.immediate('('),
      commaSep1($, $.domain_expression),
      ')',
    ),

    frame: $ => seq(
      field('kind', $.frame_kind),
      '(', $.frame_bound, ',', $.frame_bound, ')',
    ),

    frame_kind: $ => choice('rows', 'range', 'groups'),

    // Frame bounds are PUNCTUATION, not anaphors: '_' unbounded, '.' current
    // row. Four SEMANTIC carriers, so four productions: two of them carry no
    // child at all, and a normalizer that had to tell `_` from `.` would have
    // nothing but the bytes to do it with.
    frame_bound: $ => choice(
      $.frame_unbounded,
      $.frame_current_row,
      $.frame_preceding,
      $.frame_following,
    ),

    frame_unbounded: $ => '_',
    frame_current_row: $ => '.',
    frame_preceding: $ => seq('-', $.domain_expression),
    frame_following: $ => seq('+', $.domain_expression),

    // Namespace-capable; one Ref. The `:f` citation normalizes here.
    callee: $ => $.predicate_identifier,

    // A CALLABLE STANDS WHERE THE POSITION SUPPLIES ITS SLOT, and a curried
    // parameter is such a position: `apply_twice:(:(@ * 2), age)` hands the
    // declared `f:()` its function. Only the anonymous spelling needs saying
    // here — a named callable (`substr:()`) and an open string are already
    // applications, and which arguments a callee curries is the descriptor's
    // judgment at build, never this layer's.
    argument: $ => choice(
      $.value_argument,
      $.spread,
      $.context_marker,
      $.lambda,
    ),

    value_argument: $ => seq(optional($.distinct_mark), $.domain_expression),

    // The inner-distinct prefix: the argument's values dedupe before the
    // function sees them.
    distinct_mark: $ => $.percent_sigil,

    guard: $ => seq('|', $.truth_expression),

    // NO PEMDAS, structurally: an operand derives no infix form; nesting
    // re-enters only through parens.
    infix_operator: $ => seq($.operand, $.binary_op, $.operand),

    operand: $ => choice(
      $.reference,
      $.non_infix_application,
    ),

    // CST-only: parens are admission, not meaning.
    parenthesized_operand: $ => seq('(', $.domain_expression, ')'),

    // `++` concatenates; `||` is SQL's spelling and is not DelightQL's.
    binary_op: $ => choice('+', '-', '*', '/', '%', '++'),
    // `=` and `!=` are DelightQL's null-safe equality pair; the target's own
    // three-valued comparison is a prelude sigma predicate (`+sql_eq(l, r)`,
    // `+sql_ne(l, r)`), never an infix glyph. Null-safety is semantic, not
    // syntactic — the lowering says what `=` means on the target. `==` and
    // `!==` are not tokens: a retired spelling is diagnosed after the parse
    // fails, never derived as an operator.
    cmp_op: $ => choice('=', '!=', '<', '<=', '>', '>='),

    // THE SUBSTITUTION LAW at value level: the flowing value lands at the
    // row's FINAL place, or at a written composition_input. TWO CARRIERS,
    // ONE LAW — one glyph per composition family, no landing to choose.
    function_pipe: $ => prec.left(seq(
      $.domain_expression,
      repeat1($.function_pipe_step),
    )),

    function_pipe_step: $ => seq($.function_pipe_operator, $.callable),

    // THE HEADER CLASSIFIES: an '@' header present means anchored — every arm
    // a ground match term; absent means searched — every arm a condition.
    // Decided at parse; arm content never reclassifies. The `_:(` enclosure is
    // the case's ONE surface, discriminated from the sourceless inner form by
    // the third character.
    case_like: $ => seq(
      '_:', token.immediate('('),
      choice($.anchored_case, $.searched_case),
      ')',
    ),

    // The operand is any domain expression — the separator classifies, not the
    // operand's kind.
    anchored_case: $ => seq(
      field('anchor', $.domain_expression),
      $.separator,
      sep1(';', $.match_arm),
      optional(seq(';', $.default_arm)),
    ),

    // Matching is NULL-SAFE equality: a `null` match arm MATCHES a null
    // anchor.
    //
    // FIELDED because the two positions OVERLAP as kinds: every ground IS a
    // domain expression, so `null -> null` has one node kind in both slots
    // and only the field says which is the match term.
    match_arm: $ => seq(
      field('value', $.ground),
      $.arrow,
      field('result', $.domain_expression),
    ),

    searched_case: $ => seq(
      sep1(';', $.searched_arm),
      optional(seq(';', $.default_arm)),
    ),

    // `,` as `and` is scoped here: FN.30 admits the comma spelling of
    // conjunction in case-arm conditions only.
    searched_arm: $ => seq(
      field('condition', $.arm_condition),
      $.arrow,
      field('result', $.domain_expression),
    ),

    arm_condition: $ => commaSep1($, $.truth_expression),

    default_arm: $ => seq($.disregarded, $.arrow, field('result', $.domain_expression)),

    relation_like: $ => choice(
      $.scalar_subquery,
      $.anon_scalar_subquery,
      $.field_select,
    ),

    // THE MODE IS THE COMPRESSION — a column pick on a call that is one row by
    // declared functional dependency, so no authored compression is needed.
    field_select: $ => seq(
      field('call', $.standard_application),
      token.immediate('.'),
      field('column', $.identifier),
    ),

    // CARDINALITY IS AUTHORED, DEGREE IS JUDGED: a relation enters value
    // position only through an inner form ending in a declared compression.
    // COLON-FIRST (FN.41, reversed by ruling): `foo:(, continuation)` when
    // the callee takes no arguments, `foo:(args)(, continuation)` when it
    // does. A continuation begins with its connective and an argument row
    // never does, so the two groups cannot be confused.
    scalar_subquery: $ => seq(
      field('callee', $.relation_name),
      ':(',
      choice(
        seq(field('interior', $.compressed_interior), ')'),
        seq(
          field('arguments', $.inner_argument_row),
          ')',
          '(',
          field('interior', $.compressed_interior),
          ')',
        ),
      ),
    ),

    inner_argument_row: $ => commaSep1($, $.ho_argument),

    // THE SOURCELESS INNER FORM: no base relation; the body resolves against
    // the ENCLOSING row only, and the leading comma is the no-op base made
    // visible — it also discriminates the token from the case sigil at parse.
    anon_scalar_subquery: $ => seq(
      '_:', token.immediate('('),
      field('interior', $.compressed_interior),
      ')',
    ),

    // THE COMPRESSION CLOSES THE INTERIOR. The one-row guarantee is authored,
    // and the placement is part of the surface: nothing may follow the
    // compression and reopen the relational interior. An uncompressed inner
    // form has no derivation, so the refusal is structural rather than a
    // builder check a consumer could forget to run.
    // The parens belong to the two callers because `:(` is one token at this
    // layer; what this carries is what stands BETWEEN them, and it cannot be
    // empty — the compression is mandatory and last.
    compressed_interior: $ => seq(
      repeat(field('continuation', $.continuation)),
      field('compression', $.compression),
    ),

    compression: $ => choice($.singleton_reduction, $.bound_to_one),

    // The bound's ONE home is still the comma member; this is that member,
    // pinned to exactly one row. `#>` and any other count are not
    // compressions, and cannot be spelled as one. The operator keeps its
    // tooling name so a query capturing `bound_op` finds this one too.
    bound_to_one: $ => seq($.comma_sigil, alias($._at_most, $.bound_op), $.one),

    _at_most: $ => seq('#', '<'),

    interior_continuation: $ => seq('(', $.interior, ')'),

    // ONE TEMPLATE PARSE — the CST has one template form; the build classifies
    // it ONCE by content into ground string, template, or open_string. No
    // downstream consumer rescans the parts.
    template: $ => choice(
      seq(':"""', repeat($.triple_template_part), token.immediate('"""')),
      seq(':"', repeat($.template_part), token.immediate('"')),
    ),

    // A TEMPLATE'S TEXT IS TEXT. The session tools are extras — legal
    // between any two tokens — and `>>>` is one of them, so without a
    // lexical precedence a template ending `…{@}>>>` loses its tail to a
    // debug point. Nothing inside a template is a session tool.
    template_part: $ => choice($.template_text, $.interpolation),
    template_text: $ => token.immediate(prec(1, /[^{"]+/)),
    triple_template_part: $ => choice($.triple_template_text, $.interpolation),
    triple_template_text: $ => token.immediate(prec(1, /([^{"]|"[^{"]|""[^{"])+/)),
    interpolation: $ => seq('{', $.domain_expression, '}'),

    // Value position: ONE nested value; reduction position: an interior table.
    // There is no tree_group kind — a tree group IS an enclyph whose position
    // compresses it.
    enclyph_like: $ => choice($.record, $.tuple),

    // THE one accessor — exactly ONE path, a scalar reach. No path variables,
    // no raw "$…" strings, no bracket variant.
    json_access: $ => seq($.named_reference, $.json_accessor),

    json_accessor: $ => seq(':{', $.path, '}'),

    path: $ => repeat1(seq('.', $.path_key)),

    // "quoted" keys reach special characters.
    path_key: $ => choice($.path_name, $.string, $.number),
    path_name: $ => $.identifier,

    // THE CROSSING — truth read as a value. It stands wherever a value
    // stands; the stratum that admits it says which truth forms may stand
    // there, exactly as the value grammar says for its own infix form. This
    // is the operand stratum; function_application admits the infix one.
    crossed_truth: $ => $._non_infix_truth,

    // THE BINDER NAMES THE FLOW: the name IS the flowing value, usable any
    // number of times. `|x|` is position-discriminated, never an ordinal.
    lambda: $ => seq(
      ':(',
      optional($.lambda_binder),
      $.open_expression,
      ')',
    ),

    lambda_binder: $ => seq('|', $.identifier, '|'),

    // =====================================================================
    // Compound constructs
    // =====================================================================

    record: $ => seq('{', commaSep1($, $.record_member), '}'),
    // A tuple position takes the record's spread spellings (FN.28),
    // expanding as FN.35 states.
    tuple: $ => seq('[', commaSep1($, choice($.domain_expression, $.spread)), ']'),

    record_member: $ => choice(
      $.keyed_value,
      $.induced_member,
      $.keyed_metadata,
      $.spread,
      $.self_keyed_reference,
    ),

    // FN.22 (amended): a metadata group may stand as an induced member's
    // body, under a fixed key: `"by_order_status": o.status:~> {…}`. The
    // key is the name, so the group's own naming has no place here.
    keyed_metadata: $ => seq(
      $.key,
      alias($._nested_metadata_group, $.metadata_group),
    ),

    keyed_value: $ => seq($.key, $.domain_expression),
    key: $ => seq($.string, ':'),

    // A nested level, re-entering reduction in the parent's group; the
    // induction IS the marker plus position.
    //
    // A METADATA GROUP IS NOT A CONSTRUCTION MEMBER. It is reached from
    // reduction position and from a `meta_target` chain, and nowhere else —
    // so a bare one under an inducing key has no derivation. THE MIRROR LAW
    // holds through the ENCLYPH: the pattern side reaches its metadata
    // binding inside the braces an iteration opens, and so does this side.
    induced_member: $ => seq(
      $.key,
      $.reduction_sigil,
      $.enclyph_like,
    ),

    // A reference donates its own unqualified name as the key; only references
    // qualify — nothing else has a name to donate.
    self_keyed_reference: $ => $.named_reference,

    // Data values become the KEYS; one metadata key per level; yields an
    // interior RECORD, not a table. Reduction position only — never a domain
    // expression.
    // A metadata group publishes one column, so it takes a name like every
    // other thing that publishes one.
    // THE NAME BELONGS TO WHAT THE REDUCTION PUBLISHES, so only the OUTERMOST
    // level of a chain takes one: `g:~> k:~> {v} as n` publishes one column
    // and names it. A nested level has nothing of its own to publish, and a
    // grammar that let it take the naming anyway left both readings derivable
    // — the name then landed on the interior and the group published a mint
    // nobody had written.
    metadata_group: $ => seq(
      field('key_column', $.key_column),
      $.metadata_sigil,
      $.meta_target,
      optional($.naming),
    ),

    // The nested level is the SAME node kind — one carrier, read one way —
    // minus the naming it cannot carry.
    meta_target: $ => choice(
      $.enclyph_like,
      alias($._nested_metadata_group, $.metadata_group),
    ),

    _nested_metadata_group: $ => seq(
      field('key_column', $.key_column),
      $.metadata_sigil,
      $.meta_target,
    ),

    // The metadata key — a bare reference; its VALUES become the record's
    // keys.
    key_column: $ => $.named_reference,

    // =====================================================================
    // Truth position
    // =====================================================================

    // TWO STRATA, ONE SUPERTYPE. The strata are hidden so the supertype keeps
    // all its members; the crossing admits them at different value tiers.
    truth_expression: $ => choice($._infix_truth, $._non_infix_truth),

    // The forms open on at least one side. Not operands: they cross into
    // value position only as a whole value, and re-enter operand position
    // through parentheses — NO PEMDAS holds for truth as for arithmetic.
    _infix_truth: $ => choice(
      $.comparison,
      $.heading_correlation,
      $.conjunction_expression,
      $.disjunction_expression,
      $.membership,
      $.relational_membership,
    ),

    // The forms a closing token ends. These are operands.
    _non_infix_truth: $ => choice(
      $.negation,
      $.existence,
      $.sigma_application,
      $.parenthesized_truth,
    ),

    // CST-only, like parenthesized_operand: `(age > 18) as adult` and
    // `(x > 0) = true` are the same parens — grouping, never meaning.
    parenthesized_truth: $ => seq('(', $.truth_expression, ')'),

    // `=` null-safety is semantic, not syntactic.
    comparison: $ => seq($.operand, $.cmp_op, $.operand),

    // THE WHOLE HEADING CORRELATES. A spread is never an operand — it is not
    // a value — so the whole-heading comparison is its OWN truth form, in the
    // two modes a step aligns by: NAMES (`x.* = y.*`, every name both arms
    // publish) and POSITIONS (`x|*| = y|*|`, every position). The name mode
    // reuses `glob` rather than respelling it; that a correlation operand
    // must name a stage is the build's judgment, not a second spelling.
    heading_correlation: $ => seq(
      field('left', $.heading_reference),
      field('operator', $.cmp_op),
      field('right', $.heading_reference),
    ),

    heading_reference: $ => choice($.glob, $.positional_heading),

    positional_heading: $ => seq(
      field('qualifier', $.qualifier),
      token.immediate('|'),
      $.star_sigil,
      token.immediate('|'),
    ),

    // n-ary Vec carriers — associativity makes nesting meaningless.
    conjunction_expression: $ => prec.left(seq(
      $.truth_expression,
      repeat1(seq($.and_keyword, $.truth_expression)),
    )),

    disjunction_expression: $ => choice(
      prec.left(seq($.truth_expression, repeat1(seq($.or_keyword, $.truth_expression)))),
      seq('(', $.truth_expression, repeat1(seq($.corresponding_union_sigil, $.truth_expression)), ')'),
    ),

    // The parens are part of the form.
    negation: $ => seq('!(', $.truth_expression, ')'),

    // Membership negates with the keyword `not`; the sigils and the keyword
    // never trade places.
    membership: $ => seq(
      field('probe', $.probe),
      optional($.not_keyword),
      $.in_keyword,
      '(', sep1(';', $.value_row), ')',
    ),

    probe: $ => choice($.domain_expression, $.probe_row),

    // ONE element is a parenthesized operand; the COMMA makes the row.
    probe_row: $ => seq(
      '(', $.domain_expression, repeat1(seq($.comma_sigil, $.domain_expression)), ')',
    ),

    value_row: $ => commaSep1($, $.domain_expression),

    // The inner form again; probe arity = the relation's published width,
    // judged at resolution.
    relational_membership: $ => seq(
      field('probe', $.probe),
      optional($.not_keyword),
      $.in_keyword,
      field('callee', $.relation_name),
      field('interior', $.interior_continuation),
    ),

    // The interior continuation is decisive. The same bytes can also begin a
    // sigma application, but later syntax must not reinterpret the completed
    // atom.
    existence: $ => prec.dynamic(1, seq(
      $.polarity,
      field('callee', $.relation_name),
      optional(field('ho_part', $.ho_part)),
      field('interior', $.interior_continuation),
    )),

    // Colon-less: polarity is truth position's reinterpretation mark, as ':'
    // is value position's. ONE application carrier after build.
    sigma_application: $ => seq(
      $.polarity,
      field('callee', $.callee),
      '(', optional(commaSep1($, $.argument)), ')',
    ),

    // =====================================================================
    // Companion sigil sub-language (ddl-grammar.md)
    //
    // A companion cell is DATA: a fact standing in the catalog whose cells
    // must survive storage, transport and imprinting. This grammar is the
    // single reader of those strings, and nothing else may quote semantics.
    //
    // Tree-sitter has one start rule, so the host's choice of parse root —
    // constraint cell or default cell, selected by companion COLUMN and never
    // by the cell's content — is carried by an explicit marker rather than
    // inferred. The marker is a host-side root selector, the `?-` prepend's
    // cousin, and is never authored DelightQL.
    //
    // The column self-reference `@` reaches this sub-language through
    // `operand`'s composition_input: the bytes and the position are identical
    // to a value-level hole, so the CST records one node and the ROOT supplies
    // the category — which is exactly what FN.3 asks for.
    // =====================================================================

    companion_cell_root: $ => seq($.companion_root_marker, $.companion_cell),

    companion_root_marker: $ => choice('@constraint-cell', '@default-cell'),

    companion_cell: $ => choice($.constraint_cell, $.default_cell),

    constraint_cell: $ => choice(
      $.primary_key_sigil,
      $.unique_key_sigil,
      $.constraint_truth,
    ),

    constraint_truth: $ => $.truth_expression,

    default_cell: $ => $.domain_expression,

    // Bare: the carrying column is the key. With parens: the composite key,
    // spelled from a table-level row.
    primary_key_sigil: $ => seq(
      $.double_percent_sigil,
      optional(seq('(', commaSep1($, $.identifier), ')')),
    ),

    unique_key_sigil: $ => seq(
      $.percent_sigil,
      optional(seq('(', commaSep1($, $.identifier), ')')),
    ),

    ...tokens,
  },
});
