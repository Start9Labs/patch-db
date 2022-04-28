use std::marker::PhantomData;

use crate::JsonGlob;

/// Used to create a proof that will be consumed later to verify the amount of arguments needed to get a path.
/// One of the places that it is used is when creating a lock target
#[derive(Clone, Debug, Copy)]
pub struct UnsaturatedArgs<A>(PhantomData<A>);

pub trait AsUnsaturatedArgs<SB> {
    fn as_unsaturated_args(&self) -> UnsaturatedArgs<SB>;
}

impl AsUnsaturatedArgs<()> for JsonGlob {
    fn as_unsaturated_args(&self) -> UnsaturatedArgs<()> {
        let count = match self {
            JsonGlob::PathWithStar(path_with_star) => path_with_star.count(),
            JsonGlob::Path(_) => 0,
        };
        if count != 0 {
            #[cfg(feature = "tracing")]
            tracing::error!("By counts={}, this phantom type = () is not valid", count);
            #[cfg(test)]
            panic!("By counts={}, this phantom type = () is not valid", count);
        }
        UnsaturatedArgs(PhantomData)
    }
}
impl AsUnsaturatedArgs<String> for JsonGlob {
    fn as_unsaturated_args(&self) -> UnsaturatedArgs<String> {
        let count = match self {
            JsonGlob::PathWithStar(path_with_star) => path_with_star.count(),
            JsonGlob::Path(_) => 0,
        };
        if count != 1 {
            #[cfg(feature = "tracing")]
            tracing::error!(
                "By counts={}, this phantom type = String is not valid",
                count
            );
            #[cfg(test)]
            panic!(
                "By counts={}, this phantom type = String is not valid",
                count
            );
        }
        UnsaturatedArgs(PhantomData)
    }
}
impl AsUnsaturatedArgs<(String, String)> for JsonGlob {
    fn as_unsaturated_args(&self) -> UnsaturatedArgs<(String, String)> {
        let count = match self {
            JsonGlob::PathWithStar(path_with_star) => path_with_star.count(),
            JsonGlob::Path(_) => 0,
        };
        if count != 2 {
            #[cfg(feature = "tracing")]
            tracing::error!(
                "By counts={}, this phantom type = (String, String) is not valid",
                count
            );
            #[cfg(test)]
            panic!(
                "By counts={}, this phantom type = (String, String) is not valid",
                count
            );
        }
        UnsaturatedArgs(PhantomData)
    }
}
