; Highlighting for the consolidated DelightQL language.
;
; This file exists as much to PROVE the token vocabulary as to colour code: if
; a semantically meaningful sigil were hidden behind an underscore, or spelled
; twice for two meanings, the patterns below could not be written. Every
; capture here addresses a named token from tokens.js, and every overloaded
; sigil is reached either uniformly or through its parent — never by a second
; spelling of the same characters.

; ---- keywords ---------------------------------------------------------------
(as_keyword) @keyword
(and_keyword) @keyword.operator
(or_keyword) @keyword.operator
(not_keyword) @keyword.operator
(in_keyword) @keyword.operator
(of_keyword) @keyword.operator
(asc_keyword) @keyword
(desc_keyword) @keyword
(frame_kind) @keyword

; ---- pipes, necks and goals -------------------------------------------------
(pipe_operator) @operator
(unwrap_pipe_operator) @operator
(function_pipe_operator) @operator
(definition_neck) @operator
(goal_marker) @keyword.directive
; The utility file's own header — a reader directive, not DelightQL, and the
; one thing in a source that tells an editor which world the file is in.
(query_sequence_header) @keyword.directive
(arrow) @operator
(reduction_sigil) @operator
(destructure_sigil) @operator
(metadata_sigil) @operator
(window_sigil) @operator

; ---- the overloaded sigils --------------------------------------------------
; Uniformly: every '%' in the file, whatever it means.
(percent_sigil) @operator
(double_percent_sigil) @operator
; By role: the same token, told apart by its parent alone.
(group (percent_sigil) @punctuation.special)
(distinct_mark (percent_sigil) @operator.modifier)
(fixpoint_badge (percent_sigil) @keyword.modifier)
(unique_key_sigil (percent_sigil) @keyword.storage)

; '*' has four homes; the parent names each one.
(star_sigil) @operator
(domain_activate (star_sigil) @operator.modifier)
(glob (star_sigil) @punctuation.special)
(rename (star_sigil) @punctuation.special)
(reposition (star_sigil) @punctuation.special)

; A definition-owned scalar reference: the sigil and the formal it reads.
(parameter_sigil) @punctuation.special
(parameter_reference name: (_) @variable.parameter)

(effect_marker) @operator.dangerous
(mutation_marker) @operator.dangerous
(outer_marker) @operator.modifier
(sparse_mark) @operator.modifier
(meta_sigil) @operator
(signed_witness_sigil) @operator
(polarity) @operator
(bound_op) @operator

; ---- connectives ------------------------------------------------------------
(comma_sigil) @punctuation.delimiter
(positional_union_sigil) @operator
(smart_union_sigil) @operator
(corresponding_union_sigil) @operator
(minus_sigil) @operator
(edge_sigil) @operator
(transitive_edge_sigil) @operator
(lift_sigil) @operator
(separator) @punctuation.special
(singleton_sigil) @punctuation.special
(binary_op) @operator
(cmp_op) @operator

; ---- anaphors ---------------------------------------------------------------
; Each is its own node kind because each is its own carrier; the glyph never
; classifies.
(disregarded) @variable.builtin
(skipped) @variable.builtin
(deictic_stage) @variable.builtin
(composition_input) @variable.builtin
(landing) @variable.builtin

; ---- literals and names -----------------------------------------------------
(number) @number
(string) @string
(blob) @string.special
(boolean) @boolean
(null) @constant.builtin
(symbol) @constant
(delimited_mention) @constant
(regex) @string.regex
(stropped_form) @variable
(comment) @comment
(smart_comment) @comment.documentation

(template_text) @string
(triple_template_text) @string
(name_template_text) @string
(name_template_placeholder) @punctuation.special

(identifier) @variable
(namespace (identifier) @namespace)
(callee (predicate_identifier name: (identifier) @function))
(citation name: (identifier) @function)

; A definition's SUBJECT. The subject stands ON the form — never buried in a
; heading — so every `rule_form` member is addressed the same way, by its own
; `name` field, one pattern per member. Four members name a predicate; an
; effect's name carries its mark; a constant is named by a bare identifier.
;
; The supertype is not a road to the field. In a query a supertype stands
; for its member SET: the children written under it must be members, and a
; field cannot be reached through it, so `(rule_form name: …)` is refused
; when the query compiles. What keeps a seventh member from going silently
; unhighlighted is `definition_names.rs`, which reads the grammar's own
; member list and each member's name kind, requires this file to carry that
; member's pattern, and measures that the pattern captures the subject and
; nothing else — a body call is never a definition.
(fo_rule name: (predicate_identifier name: (identifier) @function.definition))
(ho_rule name: (predicate_identifier name: (identifier) @function.definition))
(function_rule name: (predicate_identifier name: (identifier) @function.definition))
(sigma_rule name: (predicate_identifier name: (identifier) @function.definition))
(effect_rule name: (effect_identifier) @function.definition)
(constant_rule name: (identifier) @function.definition)

(naming name: (identifier) @variable.parameter)
(stage_name name: (identifier) @label)
(key_column) @property
(key) @property

; ---- annotations ------------------------------------------------------------
(annotation) @attribute
(uri_segment) @string.special.url
