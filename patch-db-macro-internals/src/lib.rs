use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Data, DataEnum, DataStruct, DeriveInput, Fields, Ident, Lit, LitStr, MetaNameValue, Path, Type,
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
    match &item.data {
        Data::Struct(struct_ast) => build_model_struct(item, struct_ast, model_name),
        Data::Enum(enum_ast) => build_model_enum(item, enum_ast, model_name),
        _ => panic!("Models can only be created for Structs and Enums"),
    }
}

fn build_model_struct(
    base: &DeriveInput,
    ast: &DataStruct,
    model_name: Option<Ident>,
) -> TokenStream {
    let model_name = model_name.unwrap_or_else(|| {
        Ident::new(
            &format!("{}Model", base.ident),
            proc_macro2::Span::call_site(),
        )
    });
    let base_name = &base.ident;
    let model_vis = &base.vis;
    let mut child_fn_name: Vec<Ident> = Vec::new();
    let mut child_model: Vec<Type> = Vec::new();
    let mut child_path: Vec<Option<LitStr>> = Vec::new();
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
    match &ast.fields {
        Fields::Named(f) => {
            for field in &f.named {
                let ident = field.ident.clone().unwrap();
                child_fn_name.push(ident.clone());
                let ty = &field.ty;
                if let Some(child_model_name) = field
                    .attrs
                    .iter()
                    .filter(|attr| attr.path.is_ident("model"))
                    .filter_map(|attr| attr.parse_args::<MetaNameValue>().ok())
                    .filter(|nv| nv.path.is_ident("name"))
                    .find_map(|nv| match nv.lit {
                        Lit::Str(s) => Some(s),
                        _ => None,
                    })
                {
                    let child_model_ty =
                        Ident::new(&child_model_name.value(), child_model_name.span());
                    child_model
                        .push(syn::parse2(quote! { #child_model_ty }).expect("invalid model name"));
                } else if field.attrs.iter().any(|attr| attr.path.is_ident("model")) {
                    child_model
                        .push(syn::parse2(quote! { <#ty as patch_db::HasModel>::Model }).unwrap());
                } else {
                    child_model.push(syn::parse2(quote! { patch_db::Model<#ty> }).unwrap());
                }
                if field
                    .attrs
                    .iter()
                    .filter(|attr| attr.path.is_ident("serde"))
                    .filter_map(|attr| syn::parse2::<Path>(attr.tokens.clone()).ok())
                    .any(|path| path.is_ident("flatten"))
                {
                    child_path.push(None);
                } else {
                    let serde_rename = field
                        .attrs
                        .iter()
                        .filter(|attr| attr.path.is_ident("serde"))
                        .filter_map(|attr| syn::parse2::<MetaNameValue>(attr.tokens.clone()).ok())
                        .filter(|nv| nv.path.is_ident("rename"))
                        .find_map(|nv| match nv.lit {
                            Lit::Str(s) => Some(s),
                            _ => None,
                        });

                    child_path.push(Some(
                        match (serde_rename, serde_rename_all.as_ref().map(|s| s.as_str())) {
                            (Some(a), _) => a,
                            (None, Some("lowercase")) => LitStr::new(
                                &heck::CamelCase::to_camel_case(ident.to_string().as_str())
                                    .to_lowercase(),
                                ident.span(),
                            ),
                            (None, Some("UPPERCASE")) => LitStr::new(
                                &heck::CamelCase::to_camel_case(ident.to_string().as_str())
                                    .to_uppercase(),
                                ident.span(),
                            ),
                            (None, Some("PascalCase")) => LitStr::new(
                                &heck::CamelCase::to_camel_case(ident.to_string().as_str()),
                                ident.span(),
                            ),
                            (None, Some("camelCase")) => LitStr::new(
                                &heck::MixedCase::to_mixed_case(ident.to_string().as_str()),
                                ident.span(),
                            ),
                            (None, Some("SCREAMING_SNAKE_CASE")) => LitStr::new(
                                &heck::ShoutySnakeCase::to_shouty_snake_case(
                                    ident.to_string().as_str(),
                                ),
                                ident.span(),
                            ),
                            (None, Some("kebab-case")) => LitStr::new(
                                &heck::KebabCase::to_kebab_case(ident.to_string().as_str()),
                                ident.span(),
                            ),
                            (None, Some("SCREAMING-KEBAB-CASE")) => LitStr::new(
                                &heck::ShoutyKebabCase::to_shouty_kebab_case(
                                    ident.to_string().as_str(),
                                ),
                                ident.span(),
                            ),
                            _ => LitStr::new(&ident.to_string(), ident.span()),
                        },
                    ));
                }
            }
        }
        Fields::Unnamed(f) => {
            if f.unnamed.len() == 1 {
                // newtype wrapper
                let field = &f.unnamed[0];
                let ty = &field.ty;
                let inner_model: Type = if let Some(child_model_name) = field
                    .attrs
                    .iter()
                    .filter(|attr| attr.path.is_ident("model"))
                    .filter_map(|attr| Some(attr.parse_args::<MetaNameValue>().unwrap()))
                    .filter(|nv| nv.path.is_ident("name"))
                    .find_map(|nv| match nv.lit {
                        Lit::Str(s) => Some(s),
                        _ => None,
                    }) {
                    let child_model_ty =
                        Ident::new(&child_model_name.value(), child_model_name.span());
                    syn::parse2(quote! { #child_model_ty }).unwrap()
                } else if field.attrs.iter().any(|attr| attr.path.is_ident("model")) {
                    syn::parse2(quote! { <#ty as patch_db::HasModel>::Model }).unwrap()
                } else {
                    syn::parse2(quote! { patch_db::Model::<#ty> }).unwrap()
                };
                return quote! {
                    #[derive(Debug)]
                    #model_vis struct #model_name(#inner_model);
                    impl std::clone::Clone for #model_name {
                        fn clone(&self) -> Self {
                            #model_name(self.0.clone())
                        }
                    }
                    impl core::ops::Deref for #model_name {
                        type Target = #inner_model;
                        fn deref(&self) -> &Self::Target {
                            &self.0
                        }
                    }
                    impl core::ops::DerefMut for #model_name {
                        fn deref_mut(&mut self) -> &mut Self::Target {
                            &mut self.0
                        }
                    }
                    impl From<patch_db::json_ptr::JsonPointer> for #model_name {
                        fn from(ptr: patch_db::json_ptr::JsonPointer) -> Self {
                            #model_name(#inner_model::from(ptr))
                        }
                    }
                    impl From<#model_name> for patch_db::json_ptr::JsonPointer {
                        fn from(model: #model_name) -> Self {
                            model.0.into()
                        }
                    }
                    impl AsRef<patch_db::json_ptr::JsonPointer> for #model_name {
                        fn as_ref(&self) -> &patch_db::json_ptr::JsonPointer {
                            self.0.as_ref()
                        }
                    }
                    impl From<patch_db::Model<#base_name>> for #model_name {
                        fn from(model: patch_db::Model<#base_name>) -> Self {
                            #model_name(#inner_model::from(patch_db::json_ptr::JsonPointer::from(model)))
                        }
                    }
                    impl From<#inner_model> for #model_name {
                        fn from(model: #inner_model) -> Self {
                            #model_name(model)
                        }
                    }
                    impl patch_db::HasModel for #base_name {
                        type Model = #model_name;
                    }
                };
            } else if f.unnamed.len() > 1 {
                for (i, field) in f.unnamed.iter().enumerate() {
                    child_fn_name.push(Ident::new(
                        &format!("idx_{}", i),
                        proc_macro2::Span::call_site(),
                    ));
                    let ty = &field.ty;
                    if let Some(child_model_name) = field
                        .attrs
                        .iter()
                        .filter(|attr| attr.path.is_ident("model"))
                        .filter_map(|attr| Some(attr.parse_args::<MetaNameValue>().unwrap()))
                        .filter(|nv| nv.path.is_ident("name"))
                        .find_map(|nv| match nv.lit {
                            Lit::Str(s) => Some(s),
                            _ => None,
                        })
                    {
                        let child_model_ty =
                            Ident::new(&child_model_name.value(), child_model_name.span());
                        child_model.push(
                            syn::parse2(quote! { #child_model_ty }).expect("invalid model name"),
                        );
                    } else if field.attrs.iter().any(|attr| attr.path.is_ident("model")) {
                        child_model.push(
                            syn::parse2(quote! { <#ty as patch_db::HasModel>::Model }).unwrap(),
                        );
                    } else {
                        child_model.push(syn::parse2(quote! { patch_db::Model<#ty> }).unwrap());
                    }
                    // TODO: serde rename for tuple structs?
                    // TODO: serde flatten for tuple structs?
                    child_path.push(Some(LitStr::new(
                        &format!("{}", i),
                        proc_macro2::Span::call_site(),
                    )));
                }
            }
        }
        Fields::Unit => (),
    }
    let child_path_expr = child_path.iter().map(|child_path| {
        if let Some(child_path) = child_path {
            quote! {
                self.0.child(#child_path).into()
            }
        } else {
            quote! {
                self.0.into()
            }
        }
    });
    quote! {
        #[derive(Debug)]
        #model_vis struct #model_name(patch_db::Model<#base_name>);
        impl std::clone::Clone for #model_name {
            fn clone(&self) -> Self {
                #model_name(self.0.clone())
            }
        }
        impl core::ops::Deref for #model_name {
            type Target = patch_db::Model<#base_name>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl core::ops::DerefMut for #model_name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
        impl #model_name {
            #(
                pub fn #child_fn_name(self) -> #child_model {
                    #child_path_expr
                }
            )*
        }
        impl From<patch_db::json_ptr::JsonPointer> for #model_name {
            fn from(ptr: patch_db::json_ptr::JsonPointer) -> Self {
                #model_name(From::from(ptr))
            }
        }
        impl From<#model_name> for patch_db::json_ptr::JsonPointer {
            fn from(model: #model_name) -> Self {
                model.0.into()
            }
        }
        impl AsRef<patch_db::json_ptr::JsonPointer> for #model_name {
            fn as_ref(&self) -> &patch_db::json_ptr::JsonPointer {
                self.0.as_ref()
            }
        }
        impl From<patch_db::Model<#base_name>> for #model_name {
            fn from(model: patch_db::Model<#base_name>) -> Self {
                #model_name(model)
            }
        }
        impl patch_db::HasModel for #base_name {
            type Model = #model_name;
        }
    }
}

fn build_model_enum(base: &DeriveInput, _: &DataEnum, model_name: Option<Ident>) -> TokenStream {
    let model_name = model_name.unwrap_or_else(|| {
        Ident::new(
            &format!("{}Model", base.ident),
            proc_macro2::Span::call_site(),
        )
    });
    let base_name = &base.ident;
    let model_vis = &base.vis;
    quote! {
        #[derive(Debug, Clone)]
        #model_vis struct #model_name(patch_db::Model<#base_name>);
        impl core::ops::Deref for #model_name {
            type Target = patch_db::Model<#base_name>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl core::ops::DerefMut for #model_name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
        impl From<patch_db::json_ptr::JsonPointer> for #model_name {
            fn from(ptr: patch_db::json_ptr::JsonPointer) -> Self {
                #model_name(From::from(ptr))
            }
        }
        impl From<#model_name> for patch_db::json_ptr::JsonPointer {
            fn from(model: #model_name) -> Self {
                model.0.into()
            }
        }
        impl AsRef<patch_db::json_ptr::JsonPointer> for #model_name {
            fn as_ref(&self) -> &patch_db::json_ptr::JsonPointer {
                self.0.as_ref()
            }
        }
        impl From<patch_db::Model<#base_name>> for #model_name {
            fn from(model: patch_db::Model<#base_name>) -> Self {
                #model_name(model)
            }
        }
        impl patch_db::HasModel for #base_name {
            type Model = #model_name;
        }
    }
}
