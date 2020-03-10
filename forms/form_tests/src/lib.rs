use form_derive::Form;
use form_model::Form;

#[derive(Form)]
struct Parent {
    a: i32,
    b: i32,
    c: Child,
}

#[derive(Form)]
struct Child {
    d: Grandchild
}

struct Grandchild {
    e: i32,
}

#[test]
fn test_parent() {
    let parent = Parent {
        a: 1,
        b: 2,
        c: Child {
            d: Grandchild {
                e: 1
            }
        },
    };

    parent.__assert_receiver_is_total_form();
    // let _r = parent.try_into_value();
}
