use swim_form_derive::*;

fn main() {
    #[derive(Form)]
    #[form(newtype)]
    struct A{
        a: i32,
        b: i32,
        #[form(skip)]
        c: i32,
    }
}
