use swim_form_derive::*;

fn main() {
    #[derive(Form)]
    #[form_root(::swim_form)]
    #[form(newtype)]
    struct A{}
}
