// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `#[derive(Taxon)]` — the repetitive machinery around a hand-authored
//! diagnostic hierarchy.
//!
//! The hierarchy is ordinary nested Rust enums. This derive reads three
//! variant attributes and writes what would otherwise be copied by hand:
//!
//! - `#[family("segment", summary = "…")] Variant(Child)` — a URI family
//!   whose descendants are the child enum's leaves. The doc comment is the
//!   family's explanation.
//! - `#[leaf("segment", class = Syntax, summary = "…")] Variant { … }` — an
//!   emitted identity. The segment may carry several slashes
//!   (`"fact_function/relational_face"`) and may be empty, in which case the
//!   leaf IS the family's own emitted identity. The doc comment is the
//!   explanation `dql explain` shows.
//! - `#[external(summary = "…")] Variant(Provider)` — a provider-owned open
//!   tail under this family; `Provider: ExternalTerminal` supplies the
//!   validated dynamic segments and the class.
//!
//! An enum-level `#[taxon(lineage(Root::A, A::B))]` states the containment
//! chain above the enum so `From<Self>` exists for every proper ancestor. The
//! direct parent's `From` is written by the parent's own derive.
//!
//! What the derive never does: read a segment from a string at run time,
//! choose a class from prose, or accept an identity beside a payload. A
//! variant without exactly one of the three attributes is a compile error, so
//! an identity cannot be omitted; a segment is a literal in the declaration,
//! so an identity cannot be invented elsewhere.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    spanned::Spanned,
    Data, DeriveInput, Fields, Ident, LitStr, Path, Token,
};

enum Kind {
    Family {
        segment: String,
        summary: String,
    },
    Leaf {
        segments: Vec<String>,
        class: Ident,
        summary: String,
    },
    External {
        summary: String,
    },
    /// `#[carrier]`: holds an occurrence whose identity the tree already
    /// declared; contributes no node of its own.
    Carrier,
}

struct Variant<'a> {
    ident: &'a Ident,
    fields: &'a Fields,
    kind: Kind,
    explanation: String,
}

/// `"segment", summary = "…"` or `"segment", class = Syntax, summary = "…"`.
struct Args {
    segment: Option<String>,
    class: Option<Ident>,
    summary: Option<String>,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = Args {
            segment: None,
            class: None,
            summary: None,
        };
        if input.peek(LitStr) {
            args.segment = Some(input.parse::<LitStr>()?.value());
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "summary" => args.summary = Some(input.parse::<LitStr>()?.value()),
                "class" => args.class = Some(input.parse::<Ident>()?),
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown taxon argument `{other}`"),
                    ))
                }
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(args)
    }
}

/// `lineage(Root::A, A::B)` inside `#[taxon(…)]`.
struct Lineage(Vec<Path>);

impl Parse for Lineage {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let key: Ident = input.parse()?;
        if key != "lineage" {
            return Err(syn::Error::new(key.span(), "expected `lineage(…)`"));
        }
        let inner;
        syn::parenthesized!(inner in input);
        let paths = Punctuated::<Path, Token![,]>::parse_terminated(&inner)?;
        Ok(Lineage(paths.into_iter().collect()))
    }
}

fn doc_text(attrs: &[syn::Attribute]) -> String {
    let mut lines: Vec<String> = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("doc") {
            continue;
        }
        if let syn::Meta::NameValue(nv) = &attr.meta {
            if let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            }) = &nv.value
            {
                lines.push(s.value().trim().to_string());
            }
        }
    }
    lines
        .iter()
        .filter(|l| !l.is_empty())
        .cloned()
        .collect::<Vec<_>>()
        .join(" ")
}

fn read_variant(v: &syn::Variant) -> syn::Result<Variant<'_>> {
    let mut kind: Option<Kind> = None;
    for attr in &v.attrs {
        let name = match attr.path().get_ident() {
            Some(id) => id.to_string(),
            None => continue,
        };
        let parsed: Option<Kind> = match name.as_str() {
            "family" => {
                let args: Args = attr.parse_args()?;
                let segment = args.segment.ok_or_else(|| {
                    syn::Error::new(attr.span(), "#[family] needs its segment literal")
                })?;
                if segment.is_empty() || segment.contains('/') {
                    return Err(syn::Error::new(
                        attr.span(),
                        "a family segment is one non-empty path segment",
                    ));
                }
                let summary = args.summary.ok_or_else(|| {
                    syn::Error::new(attr.span(), "#[family] needs `summary = \"…\"`")
                })?;
                Some(Kind::Family { segment, summary })
            }
            "leaf" => {
                let args: Args = attr.parse_args()?;
                let segment = args.segment.ok_or_else(|| {
                    syn::Error::new(attr.span(), "#[leaf] needs its segment literal")
                })?;
                let segments: Vec<String> = if segment.is_empty() {
                    Vec::new()
                } else {
                    segment.split('/').map(|s| s.to_string()).collect()
                };
                if segments.iter().any(|s| s.is_empty()) {
                    return Err(syn::Error::new(attr.span(), "empty path segment"));
                }
                let class = args.class.ok_or_else(|| {
                    syn::Error::new(attr.span(), "#[leaf] needs `class = <DiagnosticClass>`")
                })?;
                let summary = args.summary.ok_or_else(|| {
                    syn::Error::new(attr.span(), "#[leaf] needs `summary = \"…\"`")
                })?;
                Some(Kind::Leaf {
                    segments,
                    class,
                    summary,
                })
            }
            "carrier" => {
                if !matches!(attr.meta, syn::Meta::Path(_)) {
                    return Err(syn::Error::new(
                        attr.span(),
                        "#[carrier] takes no arguments: the carried occurrence owns its identity",
                    ));
                }
                Some(Kind::Carrier)
            }
            "external" => {
                let args: Args = attr.parse_args()?;
                if args.segment.is_some() || args.class.is_some() {
                    return Err(syn::Error::new(
                        attr.span(),
                        "#[external] takes only `summary`: the provider owns segments and class",
                    ));
                }
                let summary = args.summary.ok_or_else(|| {
                    syn::Error::new(attr.span(), "#[external] needs `summary = \"…\"`")
                })?;
                Some(Kind::External { summary })
            }
            _ => None,
        };
        if let Some(k) = parsed {
            if kind.is_some() {
                return Err(syn::Error::new(
                    attr.span(),
                    "a variant is exactly one of #[family], #[leaf], #[external], #[carrier]",
                ));
            }
            kind = Some(k);
        }
    }
    let kind = kind.ok_or_else(|| {
        syn::Error::new(
            v.span(),
            "every variant of a Taxon enum states its identity: #[family], #[leaf], #[external] or #[carrier]",
        )
    })?;
    let explanation = doc_text(&v.attrs);
    if explanation.is_empty() {
        return Err(syn::Error::new(
            v.span(),
            "a diagnostic explains itself: give this variant a doc comment",
        ));
    }
    if matches!(
        kind,
        Kind::Family { .. } | Kind::External { .. } | Kind::Carrier
    ) {
        let ok = matches!(&v.fields, Fields::Unnamed(f) if f.unnamed.len() == 1);
        if !ok {
            return Err(syn::Error::new(
                v.span(),
                "#[family], #[external] and #[carrier] variants hold exactly one unnamed child",
            ));
        }
    }
    Ok(Variant {
        ident: &v.ident,
        fields: &v.fields,
        kind,
        explanation,
    })
}

/// A pattern that binds nothing: `V { .. }`, `V(..)` or `V`.
fn ignore_pattern(v: &Variant<'_>) -> TokenStream {
    let ident = v.ident;
    match v.fields {
        Fields::Named(_) => quote!(Self::#ident { .. }),
        Fields::Unnamed(_) => quote!(Self::#ident(..)),
        Fields::Unit => quote!(Self::#ident),
    }
}

fn child_type<'a>(v: &'a Variant<'a>) -> &'a syn::Type {
    match v.fields {
        Fields::Unnamed(f) => &f.unnamed[0].ty,
        _ => unreachable!("checked at read_variant"),
    }
}

pub fn derive(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let data = match &input.data {
        Data::Enum(e) => e,
        _ => {
            return Err(syn::Error::new(
                input.span(),
                "Taxon is derived on the nested enums of the diagnostic hierarchy",
            ))
        }
    };
    let mut lineage: Vec<Path> = Vec::new();
    for attr in &input.attrs {
        if attr.path().is_ident("taxon") {
            let Lineage(paths) = attr.parse_args()?;
            lineage = paths;
        }
    }
    let variants: Vec<Variant<'_>> = data
        .variants
        .iter()
        .map(read_variant)
        .collect::<syn::Result<_>>()?;

    let externals = variants
        .iter()
        .filter(|v| matches!(v.kind, Kind::External { .. }))
        .count();
    if externals > 1 {
        return Err(syn::Error::new(
            input.span(),
            "a family has at most one provider-owned open tail",
        ));
    }

    let mut statics = Vec::new();
    let mut children = Vec::new();
    let mut segment_arms = Vec::new();
    let mut tail_arms = Vec::new();
    let mut leaf_arms = Vec::new();
    let mut class_arms = Vec::new();
    let mut from_impls = Vec::new();
    let mut external_const = quote!(None);

    for v in &variants {
        let vident = v.ident;
        let explanation = &v.explanation;
        let ignore = ignore_pattern(v);
        match &v.kind {
            Kind::Family { segment, summary } => {
                let child = child_type(v);
                let family_static = format_ident!("__TAXON_FAMILY_{}_{}", name, vident);
                statics.push(quote! {
                    #[allow(non_upper_case_globals)]
                    static #family_static: crate::taxon::FamilyDescriptor =
                        crate::taxon::FamilyDescriptor {
                            segment: #segment,
                            summary: #summary,
                            explanation: #explanation,
                            children: <#child as crate::taxon::Taxon>::CHILDREN,
                            external: <#child as crate::taxon::Taxon>::EXTERNAL,
                        };
                });
                children.push(quote!(crate::taxon::Node::Family(&#family_static)));
                segment_arms.push(quote! {
                    Self::#vident(inner) => {
                        out.push(#segment);
                        crate::taxon::Taxon::push_segments(inner, out);
                    }
                });
                tail_arms.push(quote!(Self::#vident(inner) => crate::taxon::Taxon::tail(inner)));
                leaf_arms.push(quote!(Self::#vident(inner) => crate::taxon::Taxon::leaf(inner)));
                class_arms.push(quote!(Self::#vident(inner) => crate::taxon::Taxon::class(inner)));
                from_impls.push(quote! {
                    #[automatically_derived]
                    impl ::core::convert::From<#child> for #name {
                        fn from(inner: #child) -> Self {
                            Self::#vident(inner)
                        }
                    }
                });
            }
            Kind::Leaf {
                segments,
                class,
                summary,
            } => {
                let leaf_static = format_ident!("__TAXON_LEAF_{}_{}", name, vident);
                let segs = segments.iter().map(|s| quote!(#s));
                statics.push(quote! {
                    #[allow(non_upper_case_globals)]
                    static #leaf_static: crate::taxon::LeafDescriptor =
                        crate::taxon::LeafDescriptor {
                            segments: &[#(#segs),*],
                            class: ::core::option::Option::Some(
                                crate::taxon::DiagnosticClass::#class
                            ),
                            summary: #summary,
                            explanation: #explanation,
                        };
                });
                children.push(quote!(crate::taxon::Node::Leaf(&#leaf_static)));
                segment_arms.push(quote! {
                    #ignore => out.extend_from_slice(#leaf_static.segments),
                });
                tail_arms.push(quote!(#ignore => ::std::vec::Vec::new()));
                leaf_arms.push(quote!(#ignore => &#leaf_static));
                class_arms.push(quote!(#ignore => crate::taxon::DiagnosticClass::#class));
            }
            Kind::External { summary } => {
                let child = child_type(v);
                let leaf_static = format_ident!("__TAXON_LEAF_{}_{}", name, vident);
                let ext_static = format_ident!("__TAXON_EXTERNAL_{}", name);
                statics.push(quote! {
                    #[allow(non_upper_case_globals)]
                    static #leaf_static: crate::taxon::LeafDescriptor =
                        crate::taxon::LeafDescriptor {
                            segments: &[],
                            class: ::core::option::Option::None,
                            summary: #summary,
                            explanation: #explanation,
                        };
                    #[allow(non_upper_case_globals)]
                    static #ext_static: crate::taxon::ExternalTail =
                        crate::taxon::ExternalTail {
                            validate: <#child as crate::taxon::ExternalTerminal>::validate_tail,
                            class_of_tail:
                                <#child as crate::taxon::ExternalTerminal>::class_of_tail,
                            descriptor: &#leaf_static,
                            summary: #summary,
                            explanation: #explanation,
                        };
                });
                external_const = quote!(::core::option::Option::Some(&#ext_static));
                segment_arms.push(quote!(Self::#vident(..) => {}));
                tail_arms.push(quote! {
                    Self::#vident(inner) => crate::taxon::ExternalTerminal::tail(inner)
                });
                leaf_arms.push(quote!(Self::#vident(..) => &#leaf_static));
                class_arms.push(quote! {
                    Self::#vident(inner) => crate::taxon::ExternalTerminal::class(inner)
                });
                from_impls.push(quote! {
                    #[automatically_derived]
                    impl ::core::convert::From<#child> for #name {
                        fn from(inner: #child) -> Self {
                            Self::#vident(inner)
                        }
                    }
                });
            }
            Kind::Carrier => {
                let child = child_type(v);
                segment_arms.push(quote! {
                    Self::#vident(inner) => crate::taxon::Carried::push_segments(inner, out),
                });
                tail_arms.push(quote!(Self::#vident(inner) => crate::taxon::Carried::tail(inner)));
                leaf_arms.push(quote!(Self::#vident(inner) => crate::taxon::Carried::leaf(inner)));
                class_arms
                    .push(quote!(Self::#vident(inner) => crate::taxon::Carried::class(inner)));
                from_impls.push(quote! {
                    #[automatically_derived]
                    impl ::core::convert::From<#child> for #name {
                        fn from(inner: #child) -> Self {
                            Self::#vident(inner)
                        }
                    }
                });
            }
        }
    }

    // Lineage: `From<Self>` for every proper ancestor above the direct parent.
    // `lineage(Root::A, A::B)` means Root::A(A::B(self)).
    if !lineage.is_empty() {
        for depth in 0..lineage.len() {
            // The ancestor at `depth` wraps the chain from depth..end.
            let ancestor_path = &lineage[depth];
            let ancestor_ty = ancestor_type(ancestor_path)?;
            if depth == lineage.len() - 1 {
                // The direct parent writes this one itself.
                continue;
            }
            let mut expr = quote!(inner);
            for p in lineage[depth..].iter().rev() {
                expr = quote!(#p(#expr));
            }
            from_impls.push(quote! {
                #[automatically_derived]
                impl ::core::convert::From<#name> for #ancestor_ty {
                    fn from(inner: #name) -> Self {
                        #expr
                    }
                }
            });
        }
    }

    Ok(quote! {
        #(#statics)*
        #[automatically_derived]
        impl crate::taxon::Taxon for #name {
            const CHILDREN: &'static [crate::taxon::Node] = &[#(#children),*];
            const EXTERNAL: ::core::option::Option<&'static crate::taxon::ExternalTail> =
                #external_const;
            fn push_segments(&self, out: &mut ::std::vec::Vec<&'static str>) {
                match self {
                    #(#segment_arms)*
                }
            }
            fn tail(&self) -> ::std::vec::Vec<::std::string::String> {
                match self {
                    #(#tail_arms),*
                }
            }
            fn leaf(&self) -> &'static crate::taxon::LeafDescriptor {
                match self {
                    #(#leaf_arms),*
                }
            }
            fn class(&self) -> crate::taxon::DiagnosticClass {
                match self {
                    #(#class_arms),*
                }
            }
        }
        #(#from_impls)*
    })
}

/// `Root::A` names the type `Root`.
fn ancestor_type(path: &Path) -> syn::Result<Path> {
    if path.segments.len() < 2 {
        return Err(syn::Error::new(
            path.span(),
            "a lineage entry is `Ancestor::Variant`",
        ));
    }
    let mut ty = path.clone();
    ty.segments.pop();
    // pop leaves a trailing `::`; rebuild without it
    let segs: Vec<_> = path
        .segments
        .iter()
        .take(path.segments.len() - 1)
        .cloned()
        .collect();
    ty.segments = segs.into_iter().collect();
    Ok(ty)
}
