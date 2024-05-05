extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput};

#[proc_macro_derive(VariantNames)]
pub fn enum_variant_names_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let variants = if let Data::Enum(ref data_enum) = input.data {
        data_enum
            .variants
            .iter()
            .map(|v| &v.ident)
            .collect::<Vec<_>>()
    } else {
        panic!("VariantNames can only be derived for enums");
    };

    let generated = quote! {
        impl #name {
            pub fn all_variants() -> &'static [&'static str] {
                &[
                    #(stringify!(#variants)),*
                ]
            }
        }
    };

    TokenStream::from(generated)
}
