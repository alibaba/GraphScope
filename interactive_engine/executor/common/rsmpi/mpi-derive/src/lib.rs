#![recursion_limit = "256"]

type TokenStream1 = proc_macro::TokenStream;
type TokenStream2 = proc_macro2::TokenStream;

use quote::quote;
use syn::{Fields, Type};

#[proc_macro_derive(Equivalence)]
pub fn create_user_datatype(input: TokenStream1) -> TokenStream1 {
    let ast: syn::DeriveInput = syn::parse(input).expect("Couldn't parse struct");
    let result = match ast.data {
        syn::Data::Enum(_) => panic!("#[derive(Equivalence)] is not compatible with enums"),
        syn::Data::Union(_) => panic!("#[derive(Equivalence)] is not compatible with unions"),
        syn::Data::Struct(ref s) => equivalence_for_struct(&ast, &s.fields),
    };
    result.into()
}

fn equivalence_for_tuple_field(type_tuple: &syn::TypeTuple) -> TokenStream2 {
    let field_blocklengths = type_tuple.elems.iter().map(|_| 1);

    let fields = type_tuple
        .elems
        .iter()
        .enumerate()
        .map(|(i, _)| syn::Index::from(i));

    let field_datatypes = type_tuple
        .elems
        .iter()
        .map(|elem| equivalence_for_type(&elem));

    quote! {
        &::mpi::datatype::UncommittedUserDatatype::structured(
            &[#(#field_blocklengths as ::mpi::Count),*],
            &[#(::mpi::internal::memoffset::offset_of_tuple!(#type_tuple, #fields) as ::mpi::Address),*],
            &[#(::mpi::datatype::UncommittedDatatypeRef::from(#field_datatypes)),*],
        )
    }
}

fn equivalence_for_array_field(type_array: &syn::TypeArray) -> TokenStream2 {
    let ty = equivalence_for_type(&type_array.elem);
    let len = &type_array.len;
    // We use the len block to ensure that len is of type `usize` and not type
    // {integer}. We know that `#len` should be of type `usize` because it is an
    // array size.
    quote! { &::mpi::datatype::UncommittedUserDatatype::contiguous(
        {let len: usize = #len; len}.try_into().expect("rsmpi derive: Array size is to large for MPI_Datatype i32"), &#ty)
    }
}

fn equivalence_for_type(ty: &syn::Type) -> TokenStream2 {
    match ty {
        Type::Path(ref type_path) => quote!(
                <#type_path as ::mpi::datatype::Equivalence>::equivalent_datatype()),
        Type::Tuple(ref type_tuple) => equivalence_for_tuple_field(&type_tuple),
        Type::Array(ref type_array) => equivalence_for_array_field(&type_array),
        _ => panic!("Unsupported type!"),
    }
}

fn equivalence_for_struct(ast: &syn::DeriveInput, fields: &Fields) -> TokenStream2 {
    let ident = &ast.ident;

    let field_blocklengths = fields.iter().map(|_| 1);

    let field_names = fields
        .iter()
        .enumerate()
        .map(|(i, field)| -> Box<dyn quote::ToTokens> {
            if let Some(ident) = field.ident.as_ref() {
                // named struct fields
                Box::new(ident)
            } else {
                // tuple struct fields
                Box::new(syn::Index::from(i))
            }
        });

    let field_datatypes = fields.iter().map(|field| equivalence_for_type(&field.ty));

    let ident_str = ident.to_string();

    // TODO and NOTE: Technically this code can race with MPI init and finalize, as can any other
    // code in rsmpi that interacts with the MPI library without taking a handle to `Universe`.
    // This requires larger attention, and so currently this is not addressed.
    quote! {
        unsafe impl ::mpi::datatype::Equivalence for #ident {
            type Out = ::mpi::datatype::DatatypeRef<'static>;
            fn equivalent_datatype() -> Self::Out {
                use ::mpi::internal::once_cell::sync::Lazy;
                use ::std::convert::TryInto;

                static DATATYPE: Lazy<::mpi::datatype::UserDatatype> = Lazy::new(|| {
                    ::mpi::datatype::internal::check_derive_equivalence_universe_state(#ident_str);

                    ::mpi::datatype::UserDatatype::structured::<
                        ::mpi::datatype::UncommittedDatatypeRef,
                    >(
                        &[#(#field_blocklengths as ::mpi::Count),*],
                        &[#(::mpi::internal::memoffset::offset_of!(#ident, #field_names) as ::mpi::Address),*],
                        &[#(::mpi::datatype::UncommittedDatatypeRef::from(#field_datatypes)),*],
                    )
                });

                DATATYPE.as_ref()
            }
        }
    }
}
