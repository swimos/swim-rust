use swim_form_derive::*;

fn main() {
    #[derive(Form)]
    #[form_root(::swim_form)]
    enum A {
        #[form(newtype)]
        B(i32),
        C,
    }
}
