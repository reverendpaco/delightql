;; DEFINITIONS
(function_rule name:(predicate_identifier) @markup.strong @function.definition )
(fact_function name:(predicate_identifier) @markup.strong @function.definition )
(cfe name:(identifier) @function.definition )

(fo_rule  name:(predicate_identifier name:(identifier) @markup.strong @type.definition.fo ))
(fact_form name:(predicate_identifier) @markup.strong @type.definition.fact  )
(ho_rule  name:(predicate_identifier name:(identifier) @type.definition.ho))
(induced_member (reduction_sigil)@module.builtin)

(effect_identifier (predicate_identifier) @keyword.directive)

(induced_member
    (tuple "[" @module.builtin (#set! "priority" 120)"]" @module.builtin(#set! "priority" 120)))
(induced_member
    (record ("{") @module.builtin (#set! "priority" 120)("}") @module.builtin(#set! "priority" 120)))
(effect_rule name:(effect_identifier) @markup.strong @keyword.directive.defin )

(anon_grelex ("_(") @module.anon (")")@module.anon )

(standard_cte name:(predicate_identifier) @module.common)
(label_cte name:(identifier) @module.common)

(label_cte (":") @label)
(standard_cte (":") @label)
(definition_neck) @label
(cfe (":") @label)

;; Params
(head_term (identifier)@variable.parameter)
(plain_param (identifier)@variable.parameter)
(ho_rule (scalar_param)@variable.parameter.input)
(fact_function inputs:(identifier) @variable.parameter)
(fact_function outputs:(identifier) @variable.parameter)
(record "{" @function.call "}" @function.call)
(tuple "[" @function.call "]" @function.call)
(naming (identifier)@variable.parameter)
(bound_op ("#")@variable.parameter)
(binder (identifier)@variable.parameter)
(composition_input)@variable.parameter
(lambda_binder (identifier)@variable.parameter)
(named_reference name:(identifier ) @variable.parameter)
(anchored_case (named_reference name:(identifier ) @variable.parameter.case_input))

(metadata_group
  (key_column
    (named_reference
      name:(identifier)@module.builtin ))
  (metadata_sigil)@module.builtin)

(metadata_group
  (record ("{") @module.builtin ("}") @module.builtin))
(metadata_group
  (tuple ("[") @module.builtin ("]") @module.builtin))

;;;;;;;;;;;;;;;;;;;;;;;

(exists_as_column (relation_name (predicate_identifier) @function.call @markup.quote))
(existence (relation_name (predicate_identifier) @markup.quote))

(open_relation_param (identifier) @markup.quote @module.table)
(declared_relation_param name:(identifier) @markup.quote @module.table (identifier)@variable.parameter)

;; Function calls
(scalar_subquery (relation_name (predicate_identifier) @markup.quote))
(callee (predicate_identifier name:(identifier) @function.call))
(case_like ("_:") @function.call)
(lambda (":(") @function.call (")") @function.call)
(scalar_subquery (":(") @function.call (")") @function.call)
(template) @function
(json_accessor ":{" @function.call "}" @function.call)

; (predicate_identifier name:(identifier) @type.definiton.fo)
(stage_name name:(identifier)@module)
(qualifier_name (identifier)@module.reference)
(relation_name (predicate_identifier (identifier)@module))
(drill (named_reference (identifier)@module))
(narrowing_access (named_reference (identifier)@module))
(narrowing_destructure  column:(identifier)@module)
(record_pattern  "{" @module "}" @module)
;(outer_grelex (predicate_identifier (identifier)@module))


(effrelex_interior_functor name:(_) @keyword)
(lower_order_effrelex name:(_) @keyword)

(interior) @scope.inner
(anon_body) @scope.inner.anon

; KEYWORDS
; (as_keyword) @keyword.modifier

(namespace (identifier)@module)

; SIGILS
(outer_marker) @character.special
(pipe_operator) @punctuation.composition
(domain_activate (star_sigil) @variable.parameter)
(glob (star_sigil) @variable.parameter)
; (separator)@punctuation.delimiter


; (top_level_goal (goal_marker)@keyword.builtin)


; String LIKE
(string) @string
(template (template_text) @string)
(definition_doc) @string
(definition_doc ("(~~docs") @label )
(definition_doc ("~~)") @label )
(regex) @string.regexp
(path) @string.special.path


(symbol) @string.special.symbol
(citation) @function

(positional_reference) @variable.parameter.ordinal
(positional_span) @variable.parameter.ordinal

; GROUND LIKE
(number) @number

(comment) @comment
 (query_sequence_header) @comment

(ERROR) @markup.underline @comment.error


