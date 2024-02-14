use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
    punctuated::Punctuated,
    Expr, GenericArgument, Generics, Ident, ImplItem, ImplItemFn, Item,
    ItemImpl, Pat, PatType, PathArguments, PathSegment, Receiver, Result, Stmt,
    Token, Type,
};

const FIELD_TYPES: [&str; 8] =
    ["bool", "i8", "i16", "i32", "i64", "f32", "f64", "String"];

struct SqlFn {
    name: Ident,
    generics: Generics,
    args: Punctuated<PatType, Token![,]>,
    output: syn::ReturnType,
}

impl Parse for SqlFn {
    fn parse(input: ParseStream) -> Result<Self> {
        let name: Ident = input.parse()?;
        let generics: Generics = input.parse()?;

        let content;
        let _ = parenthesized!(content in input);

        let args = content.parse_terminated(PatType::parse, Token![,])?;

        let output: syn::ReturnType = input.parse()?;

        let _: Token![;] = input.parse()?;

        Ok(SqlFn {
            name,
            generics,
            args,
            output,
        })
    }
}

struct Database {
    functions: Vec<SqlFn>,
}

impl Parse for Database {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut functions: Vec<SqlFn> = Vec::new();

        while !input.is_empty() {
            functions.push(input.parse()?);
        }

        Ok(Database { functions })
    }
}

fn extract_generic_arg(segment: &PathSegment) -> &Type {
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        panic!("Expected angle bracketed arguments");
    };

    let GenericArgument::Type(ty) = arguments.args.first().unwrap() else {
        panic!("Expected a type");
    };

    ty
}

enum ReturnType<'a> {
    Default,
    Field(&'a Type),
    Row(&'a Type),
    Rows(&'a Type),
    Optional(&'a Type),
    Stream(&'a Type),
}

impl<'a> ReturnType<'a> {
    fn as_type(&self) -> Type {
        match *self {
            Self::Default => parse_quote! { ::sqlx::Result<()> },
            Self::Field(ty) => parse_quote! { ::sqlx::Result<#ty> },
            Self::Optional(ty) => parse_quote! {
                ::sqlx::Result<::std::option::Option<#ty>>
            },
            Self::Stream(ty) => parse_quote! {
                ::futures::stream::BoxStream<::sqlx::Result<#ty>>
            },
            Self::Rows(ty) => parse_quote! {
                ::sqlx::Result<::std::vec::Vec<#ty>>
            },
            Self::Row(ty) => parse_quote! { ::sqlx::Result<#ty> },
        }
    }
}

impl<'a> From<&'a syn::ReturnType> for ReturnType<'a> {
    fn from(value: &'a syn::ReturnType) -> Self {
        match value {
            syn::ReturnType::Default => Self::Default,
            syn::ReturnType::Type(_, ty) => match ty.as_ref() {
                Type::Path(type_path) => {
                    let segment = type_path.path.segments.first().unwrap();

                    match segment.ident.to_string().as_str() {
                        "Option" => {
                            Self::Optional(extract_generic_arg(segment))
                        }
                        "Stream" => Self::Stream(extract_generic_arg(segment)),
                        "Vec" => Self::Rows(extract_generic_arg(segment)),
                        ident if FIELD_TYPES.contains(&ident) => {
                            Self::Field(ty)
                        }
                        _ => Self::Row(ty),
                    }
                }
                _ => Self::Row(ty),
            },
        }
    }
}

fn query_for_fn(name: &str, args: usize) -> String {
    let mut query = format!("SELECT * FROM {name}(");

    for i in 1..=args {
        query.push_str(&format!("${i}"));

        if i < args {
            query.push(',');
        }
    }

    query.push(')');

    query
}

fn make_fn(sql_fn: SqlFn, is_mut: bool, executor: &Expr) -> ImplItemFn {
    let name = &sql_fn.name;
    let generics = &sql_fn.generics;
    let args = &sql_fn.args;
    let return_type = ReturnType::from(&sql_fn.output);
    let result = return_type.as_type();
    let receiver: Receiver = if is_mut {
        parse_quote! { &mut self }
    } else {
        parse_quote! { &self }
    };

    let mut function: ImplItemFn = parse_quote! {
        pub async fn #name #generics(#receiver, #args) -> #result {}
    };

    if let ReturnType::Stream(_) = &return_type {
        function.sig.asyncness = None;
    }

    let query_string =
        query_for_fn(&sql_fn.name.to_string(), sql_fn.args.len());

    let query: Ident = {
        let query = match &return_type {
            ReturnType::Default => "query",
            _ => "query_as",
        };

        Ident::new(query, proc_macro2::Span::call_site())
    };

    let stmts = &mut function.block.stmts;

    stmts.push(parse_quote! {
        let mut query = ::sqlx::#query(#query_string);
    });

    for arg in args {
        let var = &match arg.pat.as_ref() {
            Pat::Ident(ident) => ident,
            _ => panic!("Only identifier patterns are supported for arguments"),
        }
        .ident;

        stmts.push(parse_quote! {
            let mut query = query.bind(#var);
        });
    }

    let last: Expr = match return_type {
        ReturnType::Default => {
            stmts.push(parse_quote! {
                query.fetch_one(#executor).await?;
            });
            parse_quote! { Ok(()) }
        }
        ReturnType::Field(ty) => {
            stmts.push(parse_quote! {
                let row: (#ty,) = query.fetch_one(#executor).await?;
            });
            parse_quote! { Ok(row.0) }
        }
        ReturnType::Row(_) => parse_quote! {
            query.fetch_one(#executor).await
        },
        ReturnType::Rows(_) => parse_quote! {
            query.fetch_all(#executor).await
        },
        ReturnType::Optional(_) => parse_quote! {
            query.fetch_optional(#executor).await
        },
        ReturnType::Stream(_) => parse_quote! {
            query.fetch(#executor)
        },
    };

    stmts.push(Stmt::Expr(last, None));

    parse_quote! { #function }
}

#[proc_macro]
pub fn database(input: TokenStream) -> TokenStream {
    let db = parse_macro_input!(input as Database);

    let decl: Item = Item::Struct(parse_quote! {
        pub struct Database {
            pool: ::sqlx::postgres::PgPool,
        }
    });

    let mut imp: ItemImpl = parse_quote! {
        impl Database {
            pub fn new(pool: ::sqlx::postgres::PgPool) -> Self {
                Self { pool }
            }

            pub async fn close(&self) {
                self.pool.close().await
            }
        }
    };

    let executor: Expr = parse_quote! { &self.pool };

    for function in db.functions {
        imp.items
            .push(ImplItem::Fn(make_fn(function, false, &executor)));
    }

    let output = quote! {
        #decl
        #imp
    };

    output.into()
}

#[proc_macro]
pub fn transaction(input: TokenStream) -> TokenStream {
    let tx = parse_macro_input!(input as Database);

    let decl: Item = Item::Struct(parse_quote! {
        pub struct Transaction {
            inner: ::sqlx::Transaction<'static, ::sqlx::postgres::Postgres>,
        }
    });

    let begin: ItemImpl = parse_quote! {
        impl Database {
            pub async fn begin(&self) -> ::sqlx::Result<Transaction> {
                Ok(Transaction { inner: self.pool.begin().await? })
            }

        }
    };

    let mut imp: ItemImpl = parse_quote! {
        impl Transaction {
            pub async fn commit(self) -> ::sqlx::Result<()> {
                self.inner.commit().await
            }

            pub async fn rollback(self) -> ::sqlx::Result<()> {
                self.inner.rollback().await
            }
        }
    };

    let executor: Expr = parse_quote! { &mut *self.inner };

    for function in tx.functions {
        imp.items
            .push(ImplItem::Fn(make_fn(function, true, &executor)))
    }

    let output = quote! {
        #decl
        #begin
        #imp
    };

    output.into()
}
