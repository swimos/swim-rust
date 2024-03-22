use swimos_form_derive::*;

fn main() {
    #[derive(Form)]
    enum A {
        #[form(newtype)]
        B(i32),
        C,
    }
}
