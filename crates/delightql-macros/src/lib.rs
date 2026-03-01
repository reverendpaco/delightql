use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Lit};

/// Derive macro for the ToLispy trait
/// 
/// # Basic usage:
/// ```
/// #[derive(ToLispy)]
/// pub enum Query {
///     Relational(RelationalExpression),
/// }
/// ```
/// 
/// # With custom names:
/// ```
/// #[derive(ToLispy)]
/// pub enum AndExpression {
///     #[lispy("and:join")]
///     Join { left: RelationalExpression, right: RelationalExpression },
///     
///     #[lispy("and:sigma")]
///     Sigma { relation: RelationalExpression, condition: SigmaCondition },
/// }
/// ```
#[proc_macro_derive(ToLispy, attributes(lispy))]
pub fn derive_to_lispy(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    
    // Extract generics to support types like RelationalExpression<Phase>
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    
    let implementation = match &input.data {
        Data::Enum(data_enum) => {
            // Generate match arms for each variant
            let match_arms = data_enum.variants.iter().map(|variant| {
                let variant_name = &variant.ident;
                
                // Check for #[lispy("custom:name")] attribute
                let lispy_name = get_lispy_name(&variant.attrs)
                    .unwrap_or_else(|| {
                        // Default: lowercase enum name + variant
                        let enum_name = camel_to_snake(&name.to_string());
                        let variant_snake = camel_to_snake(&variant_name.to_string());
                        format!("{}:{}", enum_name, variant_snake)
                    });
                
                match &variant.fields {
                    Fields::Named(fields) => {
                        // Struct-like variant: AndExpression::Join { left, right }
                        let field_names: Vec<_> = fields.named.iter()
                            .map(|f| &f.ident)
                            .collect();
                        
                        if field_names.is_empty() {
                            quote! {
                                Self::#variant_name {} => format!("({})", #lispy_name),
                            }
                        } else {
                            // Generate (field_name value) pairs for each field
                            quote! {
                                Self::#variant_name { #(#field_names),* } => {
                                    let mut lispy_parts = vec![#lispy_name.to_string()];
                                    #(
                                        lispy_parts.push(format!("({} {})", stringify!(#field_names), #field_names.to_lispy()));
                                    )*
                                    // Always keep fields on same line as parent
                                    format!("({})", lispy_parts.join(" "))
                                },
                            }
                        }
                    }
                    Fields::Unnamed(fields) => {
                        // Tuple-like variant: Query::Relational(expr)
                        if fields.unnamed.len() == 1 {
                            // Single field - common case
                            quote! {
                                Self::#variant_name(inner) => {
                                    format!("({} {})", #lispy_name, inner.to_lispy())
                                },
                            }
                        } else {
                            // Multiple fields
                            let field_names: Vec<_> = (0..fields.unnamed.len())
                                .map(|i| syn::Ident::new(&format!("field_{}", i), proc_macro2::Span::call_site()))
                                .collect();
                            let format_str = format!("({} {})", 
                                lispy_name, 
                                vec!["{}"; fields.unnamed.len()].join(" ")
                            );
                            
                            quote! {
                                Self::#variant_name(#(#field_names),*) => {
                                    format!(#format_str, #(#field_names.to_lispy()),*)
                                },
                            }
                        }
                    }
                    Fields::Unit => {
                        // Unit variant: Option::None
                        quote! {
                            Self::#variant_name => format!("({})", #lispy_name),
                        }
                    }
                }
            });
            
            quote! {
                impl #impl_generics ToLispy for #name #ty_generics #where_clause {
                    fn to_lispy(&self) -> String {
                        match self {
                            #(#match_arms)*
                        }
                    }
                }
            }
        }
        Data::Struct(data_struct) => {
            // Get struct's lispy name
            let lispy_name = get_lispy_name(&input.attrs)
                .unwrap_or_else(|| camel_to_snake(&name.to_string()));
            
            match &data_struct.fields {
                Fields::Named(fields) => {
                    // Regular struct with named fields
                    let field_names: Vec<_> = fields.named.iter()
                        .map(|f| &f.ident)
                        .collect();
                    
                    if field_names.is_empty() {
                        quote! {
                            impl #impl_generics ToLispy for #name #ty_generics #where_clause {
                                fn to_lispy(&self) -> String {
                                    format!("({})", #lispy_name)
                                }
                            }
                        }
                    } else {
                        // Format each field as (field_name value)
                        quote! {
                            impl #impl_generics ToLispy for #name #ty_generics #where_clause {
                                fn to_lispy(&self) -> String {
                                    let mut lispy_parts = vec![#lispy_name.to_string()];
                                    #(
                                        lispy_parts.push(format!("({} {})", stringify!(#field_names), self.#field_names.to_lispy()));
                                    )*
                                    // Always keep fields on same line as parent
                                    format!("({})", lispy_parts.join(" "))
                                }
                            }
                        }
                    }
                }
                Fields::Unnamed(_) => {
                    // Tuple struct - not common in ASTs
                    quote! {
                        impl #impl_generics ToLispy for #name #ty_generics #where_clause {
                            fn to_lispy(&self) -> String {
                                format!("({} {})", #lispy_name, self.0.to_lispy())
                            }
                        }
                    }
                }
                Fields::Unit => {
                    // Unit struct
                    quote! {
                        impl #impl_generics ToLispy for #name #ty_generics #where_clause {
                            fn to_lispy(&self) -> String {
                                format!("({})", #lispy_name)
                            }
                        }
                    }
                }
            }
        }
        Data::Union(_) => {
            // Unions are rare in Rust, skip for now
            panic!("ToLispy does not support unions")
        }
    };
    
    TokenStream::from(implementation)
}

/// Extract the lispy name from #[lispy("name")] attribute
fn get_lispy_name(attrs: &[syn::Attribute]) -> Option<String> {
    attrs.iter()
        .find(|attr| attr.path().is_ident("lispy"))
        .and_then(|attr| {
            attr.parse_args::<Lit>().ok().and_then(|lit| {
                if let Lit::Str(lit_str) = lit {
                    Some(lit_str.value())
                } else {
                    None
                }
            })
        })
}

/// Convert CamelCase to snake_case
fn camel_to_snake(s: &str) -> String {
    let mut result = String::new();
    let mut prev_upper = false;
    
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 && !prev_upper {
            result.push('_');
        }
        result.push(ch.to_lowercase().next().unwrap());
        prev_upper = ch.is_uppercase();
    }
    
    result
}

// ============================================================================
// PhaseConvert derive macro
// ============================================================================

/// Derive macro that generates `From` impls for AST phase conversions.
///
/// By default generates two impls:
/// - `From<T<Resolved>> for T<Refined>`
/// - `From<T<Refined>> for T<Addressed>`
///
/// Convention: fields whose type mentions the `Phase` generic parameter are
/// converted via `.into()`; all others pass through unchanged.
///
/// # Attributes
///
/// **Type-level:**
/// ```ignore
/// #[phase_convert(only(Refined => Addressed))]  // generate only one transition
/// ```
///
/// **Variant-level (enums):**
/// ```ignore
/// #[phase_convert(unreachable)]                  // panic on ALL transitions
/// #[phase_convert(unreachable_after(Refined))]   // panic only on Refined → Addressed
/// #[phase_convert(phantom, unreachable_after(Refined))]  // phantom for earlier, panic from Refined
/// ```
///
/// **Field-level:**
/// ```ignore
/// #[phase_convert(phantom)]  // generates PhaseBox::phantom()
/// ```
#[proc_macro_derive(PhaseConvert, attributes(phase_convert))]
pub fn derive_phase_convert(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Verify the type has a `Phase` generic parameter
    let has_phase = input.generics.params.iter().any(|p| {
        if let syn::GenericParam::Type(tp) = p {
            tp.ident == "Phase"
        } else {
            false
        }
    });
    if !has_phase {
        return syn::Error::new_spanned(
            &input.ident,
            "PhaseConvert requires a generic parameter named `Phase`",
        )
        .to_compile_error()
        .into();
    }

    // Parse type-level #[phase_convert(only(A => B))] attribute
    let transitions = parse_transitions(&input.attrs, name);

    let mut impls = TokenStream2::new();
    for (source, target) in &transitions {
        let impl_body = match &input.data {
            Data::Struct(data) => generate_struct_conversion(name, source, target, data),
            Data::Enum(data) => generate_enum_conversion(name, source, target, data),
            Data::Union(_) => {
                return syn::Error::new_spanned(name, "PhaseConvert does not support unions")
                    .to_compile_error()
                    .into();
            }
        };
        impls.extend(impl_body);
    }

    TokenStream::from(impls)
}

/// Parse `#[phase_convert(only(A => B))]` or return default transitions.
fn parse_transitions(
    attrs: &[syn::Attribute],
    _name: &syn::Ident,
) -> Vec<(TokenStream2, TokenStream2)> {
    for attr in attrs {
        if !attr.path().is_ident("phase_convert") {
            continue;
        }
        // Parse: phase_convert(only(Resolved => Refined))
        let mut result = Vec::new();
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("only") {
                let content;
                syn::parenthesized!(content in meta.input);
                let source: syn::Ident = content.parse()?;
                let _arrow: syn::Token![=>] = content.parse()?;
                let target: syn::Ident = content.parse()?;
                result.push((quote! { #source }, quote! { #target }));
                // Check for comma and another transition
                while content.peek(syn::Token![,]) {
                    let _comma: syn::Token![,] = content.parse()?;
                    let source: syn::Ident = content.parse()?;
                    let _arrow: syn::Token![=>] = content.parse()?;
                    let target: syn::Ident = content.parse()?;
                    result.push((quote! { #source }, quote! { #target }));
                }
                Ok(())
            } else {
                Err(meta.error("expected `only`"))
            }
        });
        if !result.is_empty() {
            return result;
        }
    }
    // Default: both transitions
    vec![
        (quote! { Resolved }, quote! { Refined }),
        (quote! { Refined }, quote! { Addressed }),
    ]
}

/// Check if a field has `#[phase_convert(phantom)]`.
fn has_phantom_attr(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if !attr.path().is_ident("phase_convert") {
            return false;
        }
        let mut found = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("phantom") {
                found = true;
            }
            Ok(())
        });
        found
    })
}

/// Check if a variant has `#[phase_convert(unreachable)]`.
fn has_unreachable_attr(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if !attr.path().is_ident("phase_convert") {
            return false;
        }
        let mut found = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("unreachable") {
                found = true;
            }
            Ok(())
        });
        found
    })
}

/// Check if a variant has `#[phase_convert(unreachable_after(Phase))]` and return the phase name.
///
/// `unreachable_after(Refined)` means: for transitions FROM Refined onward, generate panic!().
/// For earlier transitions (e.g. Resolved → Refined), fall through to other attributes or
/// normal conversion.
fn get_unreachable_after_phase(attrs: &[syn::Attribute]) -> Option<String> {
    for attr in attrs {
        if !attr.path().is_ident("phase_convert") {
            continue;
        }
        let mut phase = None;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("unreachable_after") {
                let content;
                syn::parenthesized!(content in meta.input);
                let ident: syn::Ident = content.parse()?;
                phase = Some(ident.to_string());
            }
            Ok(())
        });
        if phase.is_some() {
            return phase;
        }
    }
    None
}

/// Check whether a syn::Type mentions the identifier `Phase` anywhere in its tokens.
fn type_mentions_phase(ty: &syn::Type) -> bool {
    let tokens = quote! { #ty }.to_string();
    // Look for Phase as a word boundary — not substring of another identifier
    tokens.split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|word| word == "Phase")
}

/// Generate field conversion expression for a single field.
fn convert_field_expr(
    field_access: &TokenStream2,
    ty: &syn::Type,
    attrs: &[syn::Attribute],
) -> TokenStream2 {
    if has_phantom_attr(attrs) {
        return quote! { PhaseBox::phantom() };
    }

    if !type_mentions_phase(ty) {
        // No Phase in type — passthrough
        return quote! { #field_access };
    }

    // Determine container pattern and generate conversion
    convert_phased_type(field_access, ty)
}

/// Generate conversion code for a type that mentions Phase.
fn convert_phased_type(val: &TokenStream2, ty: &syn::Type) -> TokenStream2 {
    match ty {
        syn::Type::Path(type_path) => {
            let seg = type_path.path.segments.last().unwrap();
            let ident_str = seg.ident.to_string();

            match ident_str.as_str() {
                "Vec" => convert_vec(val, seg),
                "Option" => convert_option(val, seg),
                "Box" => convert_box(val, seg),
                "StackSafe" => convert_stacksafe(val, seg),
                _ => {
                    // Direct Phase-containing type: T<Phase> or PhaseBox<T, Phase>
                    quote! { #val.into() }
                }
            }
        }
        _ => quote! { #val.into() },
    }
}

/// Convert Vec<T> where T mentions Phase.
fn convert_vec(val: &TokenStream2, seg: &syn::PathSegment) -> TokenStream2 {
    if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
        if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
            // Check if inner type is a tuple: Vec<(A, B, ...)>
            if let syn::Type::Tuple(tuple) = inner_ty {
                return convert_vec_of_tuple(val, tuple);
            }
            // Vec<T<Phase>> → val.into_iter().map(Into::into).collect()
            return quote! { #val.into_iter().map(Into::into).collect() };
        }
    }
    quote! { #val.into_iter().map(Into::into).collect() }
}

/// Convert Vec<(A, B, ...)> where some tuple elements mention Phase.
fn convert_vec_of_tuple(val: &TokenStream2, tuple: &syn::TypeTuple) -> TokenStream2 {
    let n = tuple.elems.len();
    let field_names: Vec<_> = (0..n)
        .map(|i| syn::Ident::new(&format!("f{}", i), proc_macro2::Span::call_site()))
        .collect();

    let conversions: Vec<_> = tuple
        .elems
        .iter()
        .enumerate()
        .map(|(i, elem_ty)| {
            let name = &field_names[i];
            if type_mentions_phase(elem_ty) {
                quote! { #name.into() }
            } else {
                quote! { #name }
            }
        })
        .collect();

    let pat = quote! { (#(#field_names),*) };
    let body = quote! { (#(#conversions),*) };
    quote! { #val.into_iter().map(|#pat| #body).collect() }
}

/// Convert Option<T> where T mentions Phase.
fn convert_option(val: &TokenStream2, seg: &syn::PathSegment) -> TokenStream2 {
    if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
        if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
            if let syn::Type::Path(inner_path) = inner_ty {
                if let Some(inner_seg) = inner_path.path.segments.last() {
                    // Option<Box<T<Phase>>>
                    if inner_seg.ident == "Box" {
                        return quote! { #val.map(|b| Box::new((*b).into())) };
                    }
                    // Option<Vec<T<Phase>>>
                    if inner_seg.ident == "Vec" {
                        return quote! { #val.map(|v| v.into_iter().map(Into::into).collect()) };
                    }
                }
            }
            // Option<T<Phase>> → val.map(Into::into)
            return quote! { #val.map(Into::into) };
        }
    }
    quote! { #val.map(Into::into) }
}

/// Convert Box<T<Phase>>. Recurses into inner type to handle Box<StackSafe<T<Phase>>>.
fn convert_box(val: &TokenStream2, seg: &syn::PathSegment) -> TokenStream2 {
    if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
        if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
            let inner_val = quote! { (*#val) };
            let inner_expr = convert_phased_type(&inner_val, inner_ty);
            return quote! { Box::new(#inner_expr) };
        }
    }
    quote! { Box::new((*#val).into()) }
}

/// Convert StackSafe<T<Phase>>: unwrap, convert inner, rewrap.
fn convert_stacksafe(val: &TokenStream2, seg: &syn::PathSegment) -> TokenStream2 {
    if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
        if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
            let inner_val = quote! { #val.into_inner() };
            let inner_expr = convert_phased_type(&inner_val, inner_ty);
            return quote! { stacksafe::StackSafe::new(#inner_expr) };
        }
    }
    quote! { stacksafe::StackSafe::new(#val.into_inner().into()) }
}

/// Generate From impl for a struct.
fn generate_struct_conversion(
    name: &syn::Ident,
    source: &TokenStream2,
    target: &TokenStream2,
    data: &syn::DataStruct,
) -> TokenStream2 {
    match &data.fields {
        Fields::Named(fields) => {
            let field_names: Vec<_> = fields
                .named
                .iter()
                .map(|f| f.ident.as_ref().unwrap())
                .collect();
            let conversions: Vec<_> = fields
                .named
                .iter()
                .map(|f| {
                    let fname = f.ident.as_ref().unwrap();
                    let access = quote! { val.#fname };
                    let expr = convert_field_expr(&access, &f.ty, &f.attrs);
                    quote! { #fname: #expr }
                })
                .collect();

            quote! {
                impl From<#name<#source>> for #name<#target> {
                    #[stacksafe::stacksafe]
                    fn from(val: #name<#source>) -> #name<#target> {
                        let _ = &[#(stringify!(#field_names)),*]; // suppress unused warnings
                        #name {
                            #(#conversions),*
                        }
                    }
                }
            }
        }
        Fields::Unnamed(fields) => {
            if fields.unnamed.len() == 1 {
                let f = fields.unnamed.first().unwrap();
                let access = quote! { val.0 };
                let expr = convert_field_expr(&access, &f.ty, &f.attrs);
                quote! {
                    impl From<#name<#source>> for #name<#target> {
                        #[stacksafe::stacksafe]
                        fn from(val: #name<#source>) -> #name<#target> {
                            #name(#expr)
                        }
                    }
                }
            } else {
                let conversions: Vec<_> = fields
                    .unnamed
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        let idx = syn::Index::from(i);
                        let access = quote! { val.#idx };
                        convert_field_expr(&access, &f.ty, &f.attrs)
                    })
                    .collect();
                quote! {
                    impl From<#name<#source>> for #name<#target> {
                        #[stacksafe::stacksafe]
                        fn from(val: #name<#source>) -> #name<#target> {
                            #name(#(#conversions),*)
                        }
                    }
                }
            }
        }
        Fields::Unit => {
            quote! {
                impl From<#name<#source>> for #name<#target> {

                    fn from(_val: #name<#source>) -> #name<#target> {
                        #name
                    }
                }
            }
        }
    }
}

/// Generate a panic!() match arm for an unreachable variant.
fn generate_panic_arm(
    name: &syn::Ident,
    vname: &syn::Ident,
    source: &TokenStream2,
    target: &TokenStream2,
    fields: &Fields,
) -> TokenStream2 {
    match fields {
        Fields::Named(_) => quote! {
            #name::#vname { .. } => {
                panic!(
                    concat!(
                        "INTERNAL ERROR: ",
                        stringify!(#name),
                        "::",
                        stringify!(#vname),
                        " must be consumed before ",
                        stringify!(#source),
                        " → ",
                        stringify!(#target),
                        " conversion"
                    )
                )
            }
        },
        Fields::Unnamed(_) => quote! {
            #name::#vname(..) => {
                panic!(
                    concat!(
                        "INTERNAL ERROR: ",
                        stringify!(#name),
                        "::",
                        stringify!(#vname),
                        " must be consumed before ",
                        stringify!(#source),
                        " → ",
                        stringify!(#target),
                        " conversion"
                    )
                )
            }
        },
        Fields::Unit => quote! {
            #name::#vname => {
                panic!(
                    concat!(
                        "INTERNAL ERROR: ",
                        stringify!(#name),
                        "::",
                        stringify!(#vname),
                        " must be consumed before ",
                        stringify!(#source),
                        " → ",
                        stringify!(#target),
                        " conversion"
                    )
                )
            }
        },
    }
}

/// Generate From impl for an enum.
fn generate_enum_conversion(
    name: &syn::Ident,
    source: &TokenStream2,
    target: &TokenStream2,
    data: &syn::DataEnum,
) -> TokenStream2 {
    let match_arms: Vec<_> = data
        .variants
        .iter()
        .map(|variant| {
            let vname = &variant.ident;

            // Check transition-specific unreachable_after(Phase) FIRST.
            // If the source phase matches, this transition panics.
            // Otherwise, fall through to phantom/normal handling.
            if let Some(after_phase) = get_unreachable_after_phase(&variant.attrs) {
                if source.to_string() == after_phase {
                    return generate_panic_arm(name, vname, source, target, &variant.fields);
                }
                // Fall through: this transition is before the unreachable boundary
            }

            // Variant-level phantom: discard input, construct PhaseBox::phantom() for each field
            if has_phantom_attr(&variant.attrs) {
                return match &variant.fields {
                    Fields::Unnamed(fields) => {
                        let phantoms: Vec<_> = (0..fields.unnamed.len())
                            .map(|_| quote! { PhaseBox::phantom() })
                            .collect();
                        quote! {
                            #name::#vname(..) => #name::#vname(#(#phantoms),*)
                        }
                    }
                    Fields::Named(fields) => {
                        let field_names: Vec<_> = fields
                            .named
                            .iter()
                            .map(|f| f.ident.as_ref().unwrap())
                            .collect();
                        let phantoms: Vec<_> = field_names
                            .iter()
                            .map(|fname| quote! { #fname: PhaseBox::phantom() })
                            .collect();
                        quote! {
                            #name::#vname { .. } => #name::#vname { #(#phantoms),* }
                        }
                    }
                    Fields::Unit => quote! {
                        #name::#vname => #name::#vname
                    },
                };
            }

            if has_unreachable_attr(&variant.attrs) {
                return generate_panic_arm(name, vname, source, target, &variant.fields);
            }

            match &variant.fields {
                Fields::Named(fields) => {
                    let field_names: Vec<_> = fields
                        .named
                        .iter()
                        .map(|f| f.ident.as_ref().unwrap())
                        .collect();
                    let conversions: Vec<_> = fields
                        .named
                        .iter()
                        .map(|f| {
                            let fname = f.ident.as_ref().unwrap();
                            let access = quote! { #fname };
                            let expr = convert_field_expr(&access, &f.ty, &f.attrs);
                            quote! { #fname: #expr }
                        })
                        .collect();

                    quote! {
                        #name::#vname { #(#field_names),* } => #name::#vname {
                            #(#conversions),*
                        }
                    }
                }
                Fields::Unnamed(fields) => {
                    let field_names: Vec<_> = (0..fields.unnamed.len())
                        .map(|i| {
                            syn::Ident::new(&format!("f{}", i), proc_macro2::Span::call_site())
                        })
                        .collect();
                    let conversions: Vec<_> = fields
                        .unnamed
                        .iter()
                        .enumerate()
                        .map(|(i, f)| {
                            let fname = &field_names[i];
                            let access = quote! { #fname };
                            convert_field_expr(&access, &f.ty, &f.attrs)
                        })
                        .collect();

                    quote! {
                        #name::#vname(#(#field_names),*) => #name::#vname(#(#conversions),*)
                    }
                }
                Fields::Unit => {
                    quote! {
                        #name::#vname => #name::#vname
                    }
                }
            }
        })
        .collect();

    quote! {
        impl From<#name<#source>> for #name<#target> {
            #[stacksafe::stacksafe]
            fn from(val: #name<#source>) -> #name<#target> {
                match val {
                    #(#match_arms),*
                }
            }
        }
    }
}
