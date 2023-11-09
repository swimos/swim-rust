use swim_form_derive::*;

fn main() {
    #[allow(non_snake_case)]
    #[derive(Form)]
    #[form_root(::swim_form)]
    enum Duplicates {
        First {
            #[form(convention = "camel")]
            first_field: i32,
            firstField: i32,
        },
        #[form(fields_convention = "camel")]
        Second {
            first_field: i32,
            #[form(name = "firstField")]
            second_field: i32,
        }
    }
}