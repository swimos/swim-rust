use swimos_form_derive::*;

fn main() {
    #[derive(Form)]
    #[allow(non_snake_case)]
    struct Duplicates {
        #[form(convention = "camel")]
        second_field: i32,
        secondField: String,
    }
}
