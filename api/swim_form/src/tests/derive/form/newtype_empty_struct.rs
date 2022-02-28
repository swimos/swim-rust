use swim_form_derive::*;

fn main() {
    #[derive(Form)]
    #[form(newtype)]
    struct A{}
}
