use std::collections::BTreeMap;

use heck::*;
use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::parse::ParseStream;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::token::{Comma, Paren, Pub};
use syn::{
    Attribute, Data, DataEnum, DataStruct, DeriveInput, Error, Fields, Ident, Lit, LitInt, LitStr,
    MetaNameValue, Path, Type, VisRestricted, Visibility,
};

pub fn build_model(item: &DeriveInput) -> TokenStream {
    let res = match &item.data {
        Data::Struct(struct_ast) => build_model_struct(item, struct_ast),
        Data::Enum(enum_ast) => build_model_enum(item, enum_ast),
        _ => panic!("Models can only be created for Structs and Enums"),
    };
    if let Some(dbg) = item.attrs.iter().find(|a| a.path.is_ident("macro_debug")) {
        return Error::new_spanned(dbg, format!("{}", res)).to_compile_error();
    } else {
        res
    }
}

fn get_accessor(serde_rename_all: &Option<String>, attrs: &[Attribute], ident: &Ident) -> LitStr {
    if let Some(serde_rename) = attrs
        .iter()
        .filter(|attr| attr.path.is_ident("serde"))
        .filter_map(|attr| syn::parse2::<MetaNameValue>(attr.tokens.clone()).ok())
        .filter(|nv| nv.path.is_ident("rename"))
        .find_map(|nv| match nv.lit {
            Lit::Str(s) => Some(s),
            _ => None,
        })
    {
        return serde_rename;
    }
    let ident_string = ident.to_string();
    let ident_str = ident_string.as_str();
    match serde_rename_all.as_deref() {
        Some("lowercase") => LitStr::new(
            &ident_str.to_lower_camel_case().to_lowercase(),
            ident.span(),
        ),
        Some("UPPERCASE") => LitStr::new(
            &ident_str.to_lower_camel_case().to_uppercase(),
            ident.span(),
        ),
        Some("PascalCase") => LitStr::new(&ident_str.to_pascal_case(), ident.span()),
        Some("camelCase") => LitStr::new(&ident_str.to_lower_camel_case(), ident.span()),
        Some("SCREAMING_SNAKE_CASE") => {
            LitStr::new(&ident_str.to_shouty_snake_case(), ident.span())
        }
        Some("kebab-case") => LitStr::new(&ident_str.to_kebab_case(), ident.span()),
        Some("SCREAMING-KEBAB-CASE") => {
            LitStr::new(&ident_str.to_shouty_kebab_case(), ident.span())
        }
        _ => LitStr::new(&ident.to_string(), ident.span()),
    }
}

struct ChildInfo {
    vis: Visibility,
    name: Ident,
    accessor: Option<Lit>,
    ty: Type,
}
impl ChildInfo {
    fn from_fields(serde_rename_all: &Option<String>, fields: &Fields) -> Vec<Self> {
        let mut children = Vec::new();
        match fields {
            Fields::Named(f) => {
                for field in &f.named {
                    let ident = field.ident.clone().unwrap();
                    let ty = field.ty.clone();
                    let accessor = if field
                        .attrs
                        .iter()
                        .filter(|attr| attr.path.is_ident("serde"))
                        .filter_map(|attr| syn::parse2::<Path>(attr.tokens.clone()).ok())
                        .any(|path| path.is_ident("flatten"))
                    {
                        None
                    } else {
                        Some(Lit::Str(get_accessor(
                            serde_rename_all,
                            &field.attrs,
                            field.ident.as_ref().unwrap(),
                        )))
                    };
                    children.push(ChildInfo {
                        vis: field.vis.clone(),
                        name: ident,
                        accessor,
                        ty,
                    })
                }
            }
            Fields::Unnamed(f) => {
                for (i, field) in f.unnamed.iter().enumerate() {
                    let ident = Ident::new(&format!("idx_{i}"), field.span());
                    let ty = field.ty.clone();
                    let accessor = if f.unnamed.len() > 1 {
                        Some(Lit::Int(LitInt::new(
                            &format!("{}", i),
                            proc_macro2::Span::call_site(),
                        )))
                    } else {
                        None // newtype wrapper
                    };
                    children.push(ChildInfo {
                        vis: field.vis.clone(),
                        name: ident,
                        accessor,
                        ty,
                    })
                }
            }
            Fields::Unit => (),
        }
        children
    }
}

struct Fns {
    impl_fns: TokenStream,
    impl_mut_fns: TokenStream,
}

fn impl_fns(children: &[ChildInfo]) -> Fns {
    let mut impl_fns = TokenStream::new();
    let mut impl_mut_fns = TokenStream::new();
    for ChildInfo {
        vis,
        name,
        accessor,
        ty,
    } in children
    {
        let name_owned = Ident::new(&format!("into_{name}"), name.span());
        let name_mut = Ident::new(&format!("as_{name}_mut"), name.span());
        let vis = match vis {
            Visibility::Inherited => Visibility::Restricted(VisRestricted {
                pub_token: Pub::default(),
                paren_token: Paren::default(),
                in_token: None,
                path: Box::new(Path::from(Ident::new("super", Span::call_site()))),
            }),
            Visibility::Restricted(VisRestricted {
                path: orig_path, ..
            }) if orig_path
                .segments
                .first()
                .map(|s| s.ident == "super")
                .unwrap_or(false) =>
            {
                Visibility::Restricted(VisRestricted {
                    pub_token: Pub::default(),
                    paren_token: Paren::default(),
                    in_token: None,
                    path: Box::new({
                        let mut path = Path::from(Ident::new("super", Span::call_site()));
                        path.segments.extend(orig_path.segments.iter().cloned());
                        path
                    }),
                })
            }
            a => a.clone(),
        };
        let accessor_owned = if let Some(accessor) = accessor {
            quote! {
                {
                    #[allow(unused_imports)]
                    use patch_db::value::index::Index;
                    #accessor.index_into_owned(v).unwrap_or_default()
                }
            }
        } else {
            quote! { v }
        };
        let accessor_mut = if let Some(accessor) = accessor {
            quote! {
                {
                    #[allow(unused_imports)]
                    use patch_db::value::index::Index;
                    #accessor.index_or_insert(v)
                }
            }
        } else {
            quote! { v }
        };
        impl_fns.extend(quote_spanned! { name.span() =>
            #vis fn #name_owned (self) -> patch_db::Model::<#ty> {
                self.transmute(|v| #accessor_owned)
            }
        });
        impl_mut_fns.extend(quote_spanned! { name.span() =>
            #vis fn #name_mut (&mut self) -> &mut patch_db::Model::<#ty> {
                self.transmute_mut(|v| #accessor_mut)
            }
        });
    }
    Fns {
        impl_fns,
        impl_mut_fns,
    }
}

fn build_model_struct(base: &DeriveInput, ast: &DataStruct) -> TokenStream {
    let serde_rename_all = base
        .attrs
        .iter()
        .filter(|attr| attr.path.is_ident("serde"))
        .filter_map(|attr| attr.parse_args::<MetaNameValue>().ok())
        .filter(|nv| nv.path.is_ident("rename_all"))
        .find_map(|nv| match nv.lit {
            Lit::Str(s) => Some(s.value()),
            _ => None,
        });
    let children = ChildInfo::from_fields(&serde_rename_all, &ast.fields);
    let name = &base.ident;
    let Fns {
        impl_fns,
        impl_mut_fns,
    } = impl_fns(&children);
    quote! {
        impl patch_db::HasModel for #name {}
        impl patch_db::Model<#name> {
            #impl_fns
            #impl_mut_fns
        }
    }
}

fn build_model_enum(base: &DeriveInput, ast: &DataEnum) -> TokenStream {
    let mut match_name = None;
    let mut match_name_mut = None;
    for arg in base
        .attrs
        .iter()
        .filter(|attr| attr.path.is_ident("model"))
        .map(|attr| attr.parse_args::<MetaNameValue>().unwrap())
    {
        match arg {
            MetaNameValue {
                path,
                lit: Lit::Str(s),
                ..
            } if path.is_ident("match") => match_name = Some(s.parse().unwrap()),
            MetaNameValue {
                path,
                lit: Lit::Str(s),
                ..
            } if path.is_ident("match_mut") => match_name_mut = Some(s.parse().unwrap()),
            _ => (),
        }
    }
    let match_name = match_name
        .unwrap_or_else(|| Ident::new(&format!("{}MatchModel", base.ident), Span::call_site()));
    let match_name_mut = match_name_mut
        .unwrap_or_else(|| Ident::new(&format!("{}MatchModelMut", base.ident), Span::call_site()));
    let serde_rename_all = base
        .attrs
        .iter()
        .filter(|attr| attr.path.is_ident("serde"))
        .filter_map(|attr| attr.parse_args::<MetaNameValue>().ok())
        .filter(|nv| nv.path.is_ident("rename_all"))
        .find_map(|nv| match nv.lit {
            Lit::Str(s) => Some(s.value()),
            _ => None,
        });
    if let Some(untagged) = base
        .attrs
        .iter()
        .filter(|attr| attr.path.is_ident("serde"))
        .filter_map(|attr| attr.parse_args::<MetaNameValue>().ok())
        .find(|nv| nv.path.is_ident("untagged"))
    {
        return Error::new(untagged.span(), "Cannot derive HasModel for untagged enum")
            .into_compile_error();
    }
    let mut serde_tag: BTreeMap<&'static str, Lit> = base
        .attrs
        .iter()
        .filter(|attr| attr.path.is_ident("serde"))
        .filter_map(|attr| {
            attr.parse_args_with(|s: ParseStream| {
                Punctuated::<MetaNameValue, Comma>::parse_terminated(s)
            })
            .ok()
        })
        .flatten()
        .filter_map(|nv| {
            if nv.path.is_ident("tag") {
                Some(("tag", nv.lit))
            } else if nv.path.is_ident("content") {
                Some(("content", nv.lit))
            } else {
                None
            }
        })
        .collect();
    let mut model_variants = TokenStream::new();
    let mut mut_model_variants = TokenStream::new();
    let (impl_new, impl_new_mut) = if let Some(Lit::Str(tag)) = serde_tag.remove("tag") {
        if let Some(Lit::Str(content)) = serde_tag.remove("content") {
            let mut tag_variants = TokenStream::new();
            let mut tag_variants_mut = TokenStream::new();
            for variant in &ast.variants {
                let variant_name = &variant.ident;
                let variant_accessor =
                    get_accessor(&serde_rename_all, &variant.attrs, variant_name);
                tag_variants.extend(quote_spanned! { variant_name.span() =>
                    Some(#variant_accessor) => #match_name::#variant_name(self.transmute(|v| {
                        #[allow(unused_imports)]
                        use patch_db::value::index::Index;
                        #content.index_into_owned(v).unwrap_or_default()
                    })),
                });
                tag_variants_mut.extend(quote_spanned! { variant_name.span() =>
                    Some(#variant_accessor) => #match_name_mut::#variant_name(self.transmute(|v| {
                        #[allow(unused_imports)]
                        use patch_db::value::index::Index;
                        #content.index_or_insert(v)
                    })),
                });
            }
            (
                quote! {
                    match self[#tag].as_str() {
                        #tag_variants
                        _ => #match_name::Error(self.into_inner()),
                    }
                },
                quote! {
                    match self[#tag].as_str() {
                        #tag_variants_mut
                        _ => #match_name_mut::Error(&mut *self),
                    }
                },
            )
        } else {
            let mut tag_variants = TokenStream::new();
            let mut tag_variants_mut = TokenStream::new();
            for variant in &ast.variants {
                let variant_name = &variant.ident;
                let variant_accessor =
                    get_accessor(&serde_rename_all, &variant.attrs, variant_name);
                tag_variants.extend(quote_spanned! { variant_name.span() =>
                    Some(#variant_accessor) => #match_name::#variant_name(self.transmute(|v| {
                        #[allow(unused_imports)]
                        use patch_db::value::index::Index;
                        #variant_accessor.index_into_owned(v).unwrap_or_default()
                    })),
                });
                tag_variants_mut.extend(quote_spanned! { variant_name.span() =>
                    Some(#variant_accessor) => #match_name_mut::#variant_name(self.transmute(|v| {
                        #[allow(unused_imports)]
                        use patch_db::value::index::Index;
                        #variant_accessor.index_or_insert(v)
                    })),
                });
            }
            (
                quote! {
                    match self[#tag].as_str() {
                        #tag_variants
                        _ => #match_name::Error(self.into_inner()),
                    }
                },
                quote! {
                    match self[#tag].as_str() {
                        #tag_variants_mut
                        _ => #match_name_mut::Error(&mut *self),
                    }
                },
            )
        }
    } else {
        let mut tag_variants = TokenStream::new();
        let mut tag_variants_mut = TokenStream::new();
        for variant in &ast.variants {
            let variant_name = &variant.ident;
            let variant_accessor = get_accessor(&serde_rename_all, &variant.attrs, variant_name);
            tag_variants.extend(quote_spanned! { variant_name.span() =>
                if value.as_object().map(|o| o.contains_key(#variant_accessor)).unwrap_or(false) {
                    #match_name::#variant_name(self.transmute(|v| {
                        #[allow(unused_imports)]
                        use patch_db::value::index::Index;
                        #variant_accessor.index_into_owned(v).unwrap_or_default()
                    }))
                } else
            });
            tag_variants_mut.extend(quote_spanned! { variant_name.span() =>
                if value.as_object().map(|o| o.contains_key(#variant_accessor)).unwrap_or(false) {
                    #match_name_mut::#variant_name(self.transmute(|v| {
                        #[allow(unused_imports)]
                        use patch_db::value::index::Index;
                        #variant_accessor.index_or_insert(v)
                    }))
                } else
            });
        }
        (
            quote! {
                #tag_variants {
                    #match_name::Error(self.into_inner()),
                }
            },
            quote! {
                #tag_variants_mut {
                    #match_name_mut::Error(&mut *self),
                }
            },
        )
    };
    for variant in &ast.variants {
        let name = &variant.ident;
        let ty = match &variant.fields {
            Fields::Unnamed(a) if a.unnamed.len() == 1 => &a.unnamed.first().unwrap().ty,
            a => {
                return Error::new(
                    a.span(),
                    "Can only derive HasModel for enums with newtype variants",
                )
                .into_compile_error()
            }
        };
        model_variants.extend(quote_spanned! { name.span() =>
            #name(patch_db::Model<#ty>),
        });
        mut_model_variants.extend(quote_spanned! { name.span() =>
            #name(&'a mut patch_db::Model<#ty>),
        });
    }
    let name = &base.ident;
    let vis = &base.vis;
    quote! {
        impl patch_db::HasModel for #name {}

        impl patch_db::Model<#name> {
            #vis fn matchable(&self) -> #match_name {
                #impl_new
            }
        }

        #vis enum #match_name {
            #model_variants
            Error(patch_db::Value),
        }

        impl patch_db::Model<#name> {
            #vis fn matchable<'a>(&'a self) -> #match_name_mut<'a> {
                #impl_new_mut
            }
        }

        #vis enum #match_name_mut<'a> {
            #mut_model_variants
            Error(&'a mut patch_db::Value),
        }
    }
}
