// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use proc_macro::TokenStream;
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
