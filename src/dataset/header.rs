/// Fields in a dataset's Geographical Header.
///
/// In general, `enum` types make the best `Field`s because they are easy to implement and the
/// compiler has an easy time checking for exhaustiveness of a `match` and such things.
///
/// Importantly, `Field`s must be possible to distinguish from each other, easy to copy, able to be
/// debugged, and they should also be sized.
pub trait Field:
	Sized + core::fmt::Debug + Copy + Clone + PartialEq + Eq + core::hash::Hash
{
}

#[cfg(test)]
mod tests {}
