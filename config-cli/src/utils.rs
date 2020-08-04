use crate::{flush, get_input, show_help};

pub(crate) fn number_menu(
    name: &str,
    info: &str,
    default: &str,
    finite: bool,
    non_zero: bool,
    duration: bool,
) {
    let value = if non_zero {
        if finite {
            "Positive integer"
        } else {
            "Positive integer or \"indefinite\""
        }
    } else {
        if finite {
            "Non-negative integer"
        } else {
            "Non-negative integer or \"indefinite\""
        }
    };

    loop {
        if duration {
            print!(
                "Enter a value for {}, in seconds, `h` for help or `b` to go back: ",
                name
            );
        } else {
            print!(
                "Enter a value for {}, `h` for help or `b` to go back: ",
                name
            );
        }

        flush();

        let input = get_input();

        match input.as_str() {
            "h" => show_help(info, value, default),
            "b" => break,
            _ => {
                if input == "indefinite" && !finite {
                    //Todo
                    break;
                } else {
                    match input.parse::<usize>() {
                        //Todo
                        Ok(value) if value > 0 || !non_zero => break,
                        _ => println!("Invalid value \"{}\" for {}!", input, name),
                    }
                }
            }
        }
    }
}
