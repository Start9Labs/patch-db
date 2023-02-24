use std::collections::BTreeMap;

use heck::*;
use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::parse::ParseStream;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::token::{Comma, Paren, Pub};
use syn::{
    Attribute, Data, DataEnum, DataStruct, DeriveInput, Error, Fields, GenericArgument, Ident, Lit,
    LitInt, LitStr, MetaNameValue, Path, PathArguments, Type, VisPublic, VisRestricted, Visibility,
};

pub fn build_model(item: &DeriveInput) -> TokenStream {
    let mut model_name = None;
    for arg in item
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
            } if path.is_ident("name") => model_name = Some(s.parse().unwrap()),
            _ => (),
        }
    }
    let res = match &item.data {
        Data::Struct(struct_ast) => build_model_struct(item, struct_ast, model_name),
        Data::Enum(enum_ast) => build_model_enum(item, enum_ast, model_name),
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
    has_model: bool,
}
impl ChildInfo {
    fn from_fields(serde_rename_all: &Option<String>, fields: &Fields) -> Vec<Self> {
        let mut children = Vec::new();
        match fields {
            Fields::Named(f) => {
                for field in &f.named {
                    let ident = field.ident.clone().unwrap();
                    let ty = field.ty.clone();
                    let has_model = field.attrs.iter().any(|attr| attr.path.is_ident("model"));
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
                        has_model,
                    })
                }
            }
            Fields::Unnamed(f) => {
                for (i, field) in f.unnamed.iter().enumerate() {
                    let ident = Ident::new(&format!("idx_{i}"), field.span());
                    let ty = field.ty.clone();
                    let has_model = field.attrs.iter().any(|attr| attr.path.is_ident("model"));
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
                        has_model,
                    })
                }
            }
            Fields::Unit => (),
        }
        children
    }
}

fn separate_option(ty: &Type) -> (bool, &Type) {
    match ty {
        Type::Path(p) => {
            if let Some(s) = p.path.segments.first() {
                if s.ident == "Option" {
                    if let PathArguments::AngleBracketed(a) = &s.arguments {
                        if a.args.len() == 1 {
                            if let GenericArgument::Type(a) = &a.args[0] {
                                return (true, a);
                            }
                        }
                    }
                }
            }
        }
        _ => (),
    }

    (false, ty)
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
        has_model,
    } in children
    {
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
        let (optional, ty) = separate_option(ty);
        let (model_ty, model_mut_ty) = if *has_model {
            (
                quote_spanned! { name.span() =>
                    <#ty as patch_db::HasModel>::Model
                },
                quote_spanned! { name.span() =>
                    <<#ty as patch_db::HasModel>::Model as patch_db::Model>::Mut<'a>
                },
            )
        } else {
            (
                quote_spanned! { name.span() =>
                    patch_db::GenericModel::<#ty>
                },
                quote_spanned! { name.span() =>
                    patch_db::GenericModelMut::<'a, #ty>
                },
            )
        };
        let accessor = if let Some(accessor) = accessor {
            quote! { [#accessor] }
        } else {
            quote! {}
        };
        if optional {
            impl_fns.extend(quote_spanned! { name.span() =>
                #vis fn #name (&self) -> patch_db::OptionModel<#model_ty> {
                    <patch_db::OptionModel::<#model_ty> as patch_db::Model>::new((&**self) #accessor .clone())
                }
            });
            impl_mut_fns.extend(quote_spanned! { name.span() =>
                #vis fn #name (&self) -> patch_db::OptionModelMut<'a, #model_mut_ty> {
                    <patch_db::OptionModelMut::<'a, #model_mut_ty> as patch_db::ModelMut<'a>>::new(&mut (&mut **self) #accessor)
                }
            });
        } else {
            impl_fns.extend(quote_spanned! { name.span() =>
                #vis fn #name (&self) -> #model_ty {
                    <#model_ty as patch_db::Model>::new((&**self) #accessor .clone())
                }
            });
            impl_mut_fns.extend(quote_spanned! { name.span() =>
                #vis fn #name (&mut self) -> #model_mut_ty {
                    <#model_mut_ty as patch_db::ModelMut<'a>>::new(&mut (&mut **self) #accessor)
                }
            });
        }
    }
    Fns {
        impl_fns,
        impl_mut_fns,
    }
}

fn build_model_struct(
    base: &DeriveInput,
    ast: &DataStruct,
    module_name: Option<Ident>,
) -> TokenStream {
    let module_name = module_name.unwrap_or_else(|| {
        Ident::new(
            &format!("{}_model", heck::AsSnakeCase(base.ident.to_string())),
            proc_macro2::Span::call_site(),
        )
    });
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
    let vis = &base.vis;
    let Fns {
        impl_fns,
        impl_mut_fns,
    } = impl_fns(&children);
    quote! {
        impl patch_db::HasModel for #name {
            type Model = #module_name::Model;
        }
        #vis mod #module_name {
            use super::*;

            #[derive(Debug)]
            pub struct Model(patch_db::Value);
            impl patch_db::Model for Model {
                type T = #name;
                type Mut<'a> = ModelMut<'a>;
                fn new(value: patch_db::Value) -> Self {
                    Self(value)
                }
                fn into_inner(self) -> patch_db::Value {
                    self.0
                }
            }
            impl ::core::ops::Deref for Model {
                type Target = patch_db::Value;
                fn deref(&self) -> &Self::Target {
                    &self.0
                }
            }
            impl ::core::ops::DerefMut for Model {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    &mut self.0
                }
            }
            impl Model {
                #impl_fns
            }

            #[derive(Debug)]
            pub struct ModelMut<'a>(&'a mut patch_db::Value);
            impl<'a> patch_db::ModelMut<'a> for ModelMut<'a> {
                type T = #name;
                fn new(value: &'a mut patch_db::Value) -> Self {
                    Self(value)
                }
                fn into_inner(self) -> &'a mut patch_db::Value {
                    self.0
                }
            }
            impl<'a> ::core::ops::Deref for ModelMut<'a> {
                type Target = patch_db::Value;
                fn deref(&self) -> &Self::Target {
                    &*self.0
                }
            }
            impl<'a> ::core::ops::DerefMut for ModelMut<'a> {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    self.0
                }
            }
            impl<'a> ModelMut<'a> {
                #impl_mut_fns
            }
        }
    }
}

fn build_model_enum(base: &DeriveInput, ast: &DataEnum, module_name: Option<Ident>) -> TokenStream {
    let module_name = module_name.unwrap_or_else(|| {
        Ident::new(
            &format!("{}_model", heck::AsSnakeCase(base.ident.to_string())),
            proc_macro2::Span::call_site(),
        )
    });
    let mut children = Vec::new();
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
    let mut decl_model_variants = TokenStream::new();
    let mut into_inner = TokenStream::new();
    let mut deref = TokenStream::new();
    let mut deref_mut = TokenStream::new();
    let mut mut_into_inner = TokenStream::new();
    let mut mut_deref = TokenStream::new();
    let mut mut_deref_mut = TokenStream::new();
    let (impl_new, impl_new_mut) = if let Some(Lit::Str(tag)) = serde_tag.remove("tag") {
        children.push(ChildInfo {
            vis: Visibility::Public(VisPublic {
                pub_token: Pub::default(),
            }),
            name: Ident::new("tag", tag.span()),
            accessor: Some(Lit::Str(tag.clone())),
            ty: Type::Path(syn::TypePath {
                qself: None,
                path: Path::from(Ident::new("String", tag.span())),
            }),
            has_model: false,
        });
        if let Some(Lit::Str(content)) = serde_tag.remove("content") {
            let mut tag_variants = TokenStream::new();
            let mut tag_variants_mut = TokenStream::new();
            for variant in &ast.variants {
                let variant_name = &variant.ident;
                let variant_model =
                    Ident::new(&format!("{}Model", variant_name), variant_name.span());
                let variant_model_mut =
                    Ident::new(&format!("{}ModelMut", variant_name), variant_name.span());
                let variant_accessor =
                    get_accessor(&serde_rename_all, &variant.attrs, variant_name);
                tag_variants.extend(quote_spanned! { variant_name.span() =>
                    Some(#variant_accessor) => Model::#variant_name(#variant_model(value[#content].clone())),
                });
                tag_variants_mut.extend(quote_spanned! { variant_name.span() =>
                    Some(#variant_accessor) => ModelMut::#variant_name(#variant_model_mut(&mut value[#content])),
                });
            }
            (
                quote! {
                    match value[#tag].as_str() {
                        #tag_variants
                        _ => Model::Error(value),
                    }
                },
                quote! {
                    match value[#tag].as_str() {
                        #tag_variants_mut
                        _ => ModelMut::Error(value),
                    }
                },
            )
        } else {
            let mut tag_variants = TokenStream::new();
            let mut tag_variants_mut = TokenStream::new();
            for variant in &ast.variants {
                let variant_name = &variant.ident;
                let variant_model =
                    Ident::new(&format!("{}Model", variant_name), variant_name.span());
                let variant_model_mut =
                    Ident::new(&format!("{}ModelMut", variant_name), variant_name.span());
                let variant_accessor =
                    get_accessor(&serde_rename_all, &variant.attrs, variant_name);
                tag_variants.extend(quote_spanned! { variant_name.span() =>
                    Some(#variant_accessor) => Model::#variant_name(#variant_model(value)),
                });
                tag_variants_mut.extend(quote_spanned! { variant_name.span() =>
                    Some(#variant_accessor) => ModelMut::#variant_name(#variant_model_mut(value)),
                });
            }
            (
                quote! {
                    match value[#tag].as_str() {
                        #tag_variants
                        _ => Model::Error(value),
                    }
                },
                quote! {
                    match value[#tag].as_str() {
                        #tag_variants_mut
                        _ => ModelMut::Error(value),
                    }
                },
            )
        }
    } else {
        let mut tag_variants = TokenStream::new();
        let mut tag_variants_mut = TokenStream::new();
        for variant in &ast.variants {
            let variant_name = &variant.ident;
            let variant_model = Ident::new(&format!("{}Model", variant_name), variant_name.span());
            let variant_model_mut =
                Ident::new(&format!("{}ModelMut", variant_name), variant_name.span());
            let variant_accessor = get_accessor(&serde_rename_all, &variant.attrs, variant_name);
            tag_variants.extend(quote_spanned! { variant_name.span() =>
                if value.as_object().map(|o| o.contains_key(#variant_accessor)).unwrap_or(false) {
                    Model::#variant_name(#variant_model(value[#variant_accessor].clone()))
                } else
            });
            tag_variants_mut.extend(quote_spanned! { variant_name.span() =>
                if value.as_object().map(|o| o.contains_key(#variant_accessor)).unwrap_or(false) {
                    ModelMut::#variant_name(#variant_model_mut(&mut value[#variant_accessor]))
                } else
            });
        }
        (
            quote! {
                #tag_variants {
                    Model::Error(value),
                }
            },
            quote! {
                #tag_variants_mut {
                    ModelMut::Error(value),
                }
            },
        )
    };
    for variant in &ast.variants {
        let name = &variant.ident;
        let model_name = Ident::new(&format!("{}Model", variant.ident), variant.ident.span());
        let model_name_mut =
            Ident::new(&format!("{}ModelMut", variant.ident), variant.ident.span());
        let serde_rename_all = variant
            .attrs
            .iter()
            .filter(|attr| attr.path.is_ident("serde"))
            .filter_map(|attr| attr.parse_args::<MetaNameValue>().ok())
            .filter(|nv| nv.path.is_ident("rename_all"))
            .find_map(|nv| match nv.lit {
                Lit::Str(s) => Some(s.value()),
                _ => None,
            });
        model_variants.extend(quote_spanned! { name.span() =>
            #name(#model_name),
        });
        mut_model_variants.extend(quote_spanned! { name.span() =>
            #name(#model_name_mut<'a>),
        });
        let children: Vec<_> = ChildInfo::from_fields(&serde_rename_all, &variant.fields)
            .into_iter()
            .map(|c| ChildInfo {
                vis: Visibility::Public(VisPublic {
                    pub_token: Pub::default(),
                }),
                ..c
            })
            .collect();
        let Fns {
            impl_fns,
            impl_mut_fns,
        } = impl_fns(&children);
        decl_model_variants.extend(quote_spanned! { name.span() =>
            #[derive(Debug)]
            pub struct #model_name(patch_db::Value);
            impl ::core::ops::Deref for #model_name {
                type Target = patch_db::Value;
                fn deref(&self) -> &Self::Target {
                    &self.0
                }
            }
            impl ::core::ops::DerefMut for #model_name {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    &mut self.0
                }
            }
            impl #model_name {
                #impl_fns
            }

            #[derive(Debug)]
            pub struct #model_name_mut<'a>(&'a mut patch_db::Value);
            impl<'a> ::core::ops::Deref for #model_name_mut<'a> {
                type Target = patch_db::Value;
                fn deref(&self) -> &Self::Target {
                    &*self.0
                }
            }
            impl<'a> ::core::ops::DerefMut for #model_name_mut<'a> {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    self.0
                }
            }
            impl<'a> #model_name_mut<'a> {
                #impl_mut_fns
            }
        });
        into_inner.extend(quote_spanned! { name.span() =>
            Model::#name(a) => a.0,
        });
        deref.extend(quote_spanned! { name.span() =>
            Model::#name(a) => &a.0,
        });
        deref_mut.extend(quote_spanned! { name.span() =>
            Model::#name(a) => &mut a.0,
        });
        mut_into_inner.extend(quote_spanned! { name.span() =>
            ModelMut::#name(a) => a.0,
        });
        mut_deref.extend(quote_spanned! { name.span() =>
            ModelMut::#name(a) => &*a,
        });
        mut_deref_mut.extend(quote_spanned! { name.span() =>
            ModelMut::#name(a) => &mut *a,
        });
    }
    let name = &base.ident;
    let vis = &base.vis;
    let Fns {
        impl_fns,
        impl_mut_fns,
    } = impl_fns(&children);
    quote! {
        impl patch_db::HasModel for #name {
            type Model = #module_name::Model;
        }
        #vis mod #module_name {
            use super::*;

            #[derive(Debug)]
            pub enum Model {
                #model_variants
                Error(patch_db::Value),
            }
            impl patch_db::Model for Model {
                type T = #name;
                type Mut<'a> = ModelMut<'a>;
                fn new(value: patch_db::Value) -> Self {
                    #impl_new
                }
                fn into_inner(self) -> patch_db::Value {
                    match self {
                        #into_inner
                        Model::Error(a) => a,
                    }
                }
            }
            impl ::core::ops::Deref for Model {
                type Target = patch_db::Value;
                fn deref(&self) -> &Self::Target {
                    match self {
                        #deref
                        Model::Error(a) => &a,
                    }
                }
            }
            impl ::core::ops::DerefMut for Model {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    match self {
                        #deref_mut
                        Model::Error(a) => &mut a,
                    }
                }
            }
            impl Model {
                #impl_fns
            }

            #[derive(Debug)]
            pub enum ModelMut<'a> {
                #mut_model_variants
                Error(&'a mut patch_db::Value),
            }
            impl<'a> patch_db::ModelMut<'a> for ModelMut<'a> {
                type T = #name;
                fn new(value: &'a mut patch_db::Value) -> Self {
                    #impl_new_mut
                }
                fn into_inner(self) -> &'a mut patch_db::Value {
                    match self {
                        #mut_into_inner
                        ModelMut::Error(a) => a,
                    }
                }
            }
            impl<'a> ::core::ops::Deref for ModelMut<'a> {
                type Target = patch_db::Value;
                fn deref(&self) -> &Self::Target {
                    match self {
                        #mut_deref
                        ModelMut::Error(a) => &*a,
                    }
                }
            }
            impl<'a> ::core::ops::DerefMut for ModelMut<'a> {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    match self {
                        #mut_deref_mut
                        ModelMut::Error(a) => a,
                    }
                }
            }
            impl<'a> ModelMut<'a> {
                #impl_mut_fns
            }

            #decl_model_variants
        }
    }
}
