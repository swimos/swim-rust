use swim_form_derive::*;

fn main() {
    #[derive(Form)]
    #[form_root(::swim_form)]
    #[allow(non_snake_case)]
    struct Duplicates {
        #[form(convention = "camel")]
        second_field: i32,
        secondField: String,
    }
}