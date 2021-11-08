/// Macro for safely creating NonZeroUsize
#[macro_export]
macro_rules! non_zero_usize {
    (0) => {
        compile_error!("Must be non-zero")
    };
    ($n:expr) => {
        unsafe { std::num::NonZeroUsize::new_unchecked($n) }
    };
}
