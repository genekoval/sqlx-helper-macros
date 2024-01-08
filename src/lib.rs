use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
    punctuated::Punctuated,
    Generics, Ident, ImplItem, ImplItemFn, Item, ItemImpl, Pat, PatType,
    Result, ReturnType, Stmt, Token, Type,
};

struct SqlFn {
    name: Ident,
    generics: Generics,
    args: Punctuated<PatType, Token![,]>,
    output: ReturnType,
}

impl Parse for SqlFn {
    fn parse(input: ParseStream) -> Result<Self> {
        let name: Ident = input.parse()?;
        let generics: Generics = input.parse()?;

        let content;
        let _ = parenthesized!(content in input);

        let args = content.parse_terminated(PatType::parse, Token![,])?;

        let output: ReturnType = input.parse()?;

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

fn make_fn(sql_fn: SqlFn) -> ImplItemFn {
    let name = &sql_fn.name;
    let generics = &sql_fn.generics;
    let args = &sql_fn.args;
    let return_type: Option<Box<Type>> = {
        match sql_fn.output {
            ReturnType::Default => None,
            ReturnType::Type(_, ty) => Some(ty.clone()),
        }
    };

    let result: Type = match return_type {
        Some(ref ty) => parse_quote! { Result<#ty, sqlx::Error> },
        None => parse_quote! { Result<(), sqlx::Error> },
    };

    let mut function: ImplItemFn = parse_quote! {
        pub async fn #name #generics(&self, #args) -> #result {}
    };

    let mut query_string = format!("SELECT * FROM {}(", sql_fn.name);
    for i in 1..=sql_fn.args.len() {
        query_string.push_str(&format!("${}", i));

        if i < sql_fn.args.len() {
            query_string.push(',');
        }
    }
    query_string.push(')');

    let query: Ident = {
        let query = match return_type {
            Some(_) => "query_as",
            None => "query",
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
            _ => panic!("Only identifier patterns are supported"),
        }
        .ident;

        stmts.push(parse_quote! {
            let mut query = query.bind(#var);
        });
    }

    let fetch = {
        let fetch = match return_type {
            Some(ref ty) => match ty.as_ref() {
                Type::Path(path) => {
                    let segments = &path.path.segments;
                    let segstring = quote!(#segments).to_string();
                    let key = match segstring.find(' ') {
                        Some(i) => &segstring[..i],
                        None => &segstring,
                    };
                    match key {
                        "Option" => "fetch_optional",
                        "std::option::Option" => "fetch_optional",
                        "Vec" => "fetch_all",
                        "std::vec::Vec" => "fetch_all",
                        _ => "fetch_one",
                    }
                }
                _ => "fetch_one",
            },
            None => "execute",
        };

        Ident::new(fetch, proc_macro2::Span::call_site())
    };

    match return_type {
        Some(_) => {
            stmts.push(Stmt::Expr(
                parse_quote! {
                    query.#fetch(&self.pool).await
                },
                None,
            ));
        }
        None => {
            stmts.push(parse_quote! {
                let _ = query.#fetch(&self.pool).await?;
            });
            stmts.push(Stmt::Expr(
                parse_quote! {
                    Ok(())
                },
                None,
            ));
        }
    }

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
        impl Database {}
    };

    imp.items.push(ImplItem::Fn(parse_quote! {
        pub fn new(pool: ::sqlx::postgres::PgPool) -> Database {
            Database { pool }
        }
    }));

    for function in db.functions {
        imp.items.push(ImplItem::Fn(make_fn(function)));
    }

    let output = quote! {
        #decl
        #imp
    };

    output.into()
}