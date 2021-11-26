use crate::Form;

mod swim_form {
    pub use crate::*;
}

#[test]
fn test_foo() {
    //Todo dm

    #[derive(Form)]
    #[form(newtype)]
    struct S(i32, #[form(skip)] u32);

    let s = S(665, 11);

    eprintln!("s = {:#?}", s.as_value().to_string());

    // assert_eq!(s.as_value(), rec);
    // assert_eq!(S::try_from_value(&rec), Ok(s.clone()));
    // assert_eq!(S::try_convert(rec.clone()), Ok(s.clone()));
    // assert_eq!(s.into_value(), rec);
}