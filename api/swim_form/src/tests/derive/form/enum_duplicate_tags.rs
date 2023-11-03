use swim_form_derive::*;

fn main() {
    #[allow(non_snake_case)]
    #[derive(Form)]
    #[form_root(::swim_form)]
    enum Duplicates {
        First {
            first_field: i32,
            second_Field: i32,
        },
        #[form(tag = "First")]
        Second,
    }
}