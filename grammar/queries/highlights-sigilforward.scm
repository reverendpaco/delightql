
(star_sigil) @variable.parameter
(comma_sigil)@punctuation.delimiter
(ho_rule "("@keyword")"@keyword )
(definition_neck) @punctuation.delimiter
(metadata_sigil)@punctuation.special
(effect_marker)@punctuation.special
(symbol) @string.special.symbol
(pipe_operator) @punctuation.special
(separator) @punctuation.delimiter
(comma_continuation (comma_sigil)@punctuation.special)
(arrow) @punctuation.special
(function_pipe_first) @punctuation.special
(goal_marker) @punctuation.special.directive
(reduction_sigil) @punctuation.special
(corresponding_union_sigil) @punctuation.special
(smart_union_sigil) @punctuation.special
(positional_union_sigil) @punctuation.special
(string)@string
(number)@number
(cmp_op) @function.method
(binary_op) @function.method
(path) @string.special.path

(existence (polarity)@keyword.special)
(outer_marker)@keyword.special
(exists_as_column (polarity)@keyword.special)
(record_pattern ("{") @module.builtin (#set! "priority" 120)("}") @module.builtin(#set! "priority" 120))
(induced_member
    (tuple "[" @module.builtin (#set! "priority" 120)"]" @module.builtin(#set! "priority" 120)))
(induced_member
    (record ("{") @module.builtin (#set! "priority" 120)("}") @module.builtin(#set! "priority" 120)))
(metadata_group
  (record ("{") @module.builtin ("}") @module.builtin))
(metadata_group
  (tuple ("[") @module.builtin ("]") @module.builtin))
(map_cover ("$(") @keyword ")"@keyword "("@keyword)
(embed_map_cover ("+$(") @keyword ")"@keyword "("@keyword)
(embed ("+(") @keyword ")"@keyword)
(project_out ("-(") @keyword ")"@keyword)
(group (percent_sigil)@keyword)
(rename (star_sigil)@keyword  "("@keyword")"@keyword )
(project "("@keyword")"@keyword )
(reposition (star_sigil)@keyword  "["@keyword"]"@keyword )

(anon_grelex ("_(") @module.anon (")")@module.anon )

(record "{" @function.call "}" @function.call)
(tuple "[" @function.call "]" @function.call)

(standard_application ":("@function.call ")"@function.call)
(open_functor ":("@function.call ")"@function.call)
(case_like ("_:") @function.call)
(lambda (":(") @function.call (")") @function.call)
(scalar_subquery (":(") @function.call (")") @function.call)
(template) @function
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
(comment) @comment


