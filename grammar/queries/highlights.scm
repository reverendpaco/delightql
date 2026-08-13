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
(materialize) @operator
(function_pipe_first) @operator
(function_pipe_last) @operator
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
(stop_point) @comment.warning
(debug_point) @comment.warning

(template_text) @string
(triple_template_text) @string
(name_template_text) @string
(name_template_placeholder) @punctuation.special

(identifier) @variable
(namespace (identifier) @namespace)
(callee (predicate_identifier name: (identifier) @function))

; A definition's SUBJECT. The subject stands ON the form — never buried in a
; heading — so ONE pattern through the `rule_form` supertype reaches every
; form that declares the same KIND of name, and four do.
;
; The other two declare their own kind: an effect's name carries its mark, and
; a constant is named by a bare identifier. Their patterns name the form
; because the supertype spelling does not RESOLVE for them — measured:
; `(rule_form name: (identifier) @d)` captures every identifier in the file,
; body calls included. A pattern that highlights `users` in `adults(*) :-
; users(*)` as a definition is worse than one that names a form.
;
; What keeps a seventh form from going silently unhighlighted is not the
; supertype but `definition_names.rs`, which reads the grammar's own member
; list and requires each member's subject to be captured and nothing else.
(rule_form name: (predicate_identifier name: (identifier) @function.definition))
(effect_rule name: (effect_identifier) @function.definition)
(constant_rule name: (identifier) @function.definition)

(naming name: (identifier) @variable.parameter)
(stage_name name: (identifier) @label)
(key_column) @property
(key) @property

; ---- annotations ------------------------------------------------------------
(annotation) @attribute
(uri_segment) @string.special.url
