use swim_form_derive::*;

fn main() {
    #[derive(Form)]
    #[form_root(::swim_form)]
    #[form(newtype)]
    enum A {
        B,
        C,
    }
}
