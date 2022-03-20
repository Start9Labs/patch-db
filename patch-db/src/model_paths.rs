use std::{marker::PhantomData, str::FromStr};

use json_ptr::JsonPointer;

/// Used in the locking of a model where we have an all, a predicate to filter children.
/// This is split because we know the path or we have predicate filters
/// We split once we got the all, so we could go into the models and lock all of services.name for example
/// without locking all of them.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum JsonGlobSegment {
    /// Used to be just be a regular json path
    Path(String),
    /// Indicating that we are going to be using some part of all of this Vec, Map, etc.
    Star,
}
impl std::fmt::Display for JsonGlobSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonGlobSegment::Path(x) => {
                write!(f, "{}", x)?;
            }
            JsonGlobSegment::Star => {
                write!(f, "*")?;
            }
        }
        Ok(())
    }
}

/// Use in the model to point from root down a specific path
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum JsonGlob {
    /// This was the default
    Path(JsonPointer),
    /// Once we add an All, our predicate, we don't know the possible paths could be in the maps so we are filling
    /// in binds for the possible paths to take.
    PathWithStar(PathWithStar),
}

/// Path including the glob
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct PathWithStar {
    segments: Vec<JsonGlobSegment>,
    count: usize,
}

impl PathWithStar {
    pub fn segments(&self) -> &[JsonGlobSegment] {
        &self.segments
    }
    pub fn count(&self) -> usize {
        self.count
    }
}

impl Default for JsonGlob {
    fn default() -> Self {
        Self::Path(Default::default())
    }
}

impl From<JsonPointer> for JsonGlob {
    fn from(pointer: JsonPointer) -> Self {
        Self::Path(pointer)
    }
}

impl std::fmt::Display for JsonGlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonGlob::Path(x) => {
                write!(f, "{}", x)?;
            }
            JsonGlob::PathWithStar(PathWithStar {
                segments: path,
                count: _,
            }) => {
                for path in path.iter() {
                    write!(f, "/")?;
                    write!(f, "{}", path)?;
                }
            }
        }
        Ok(())
    }
}

impl FromStr for JsonGlob {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let split = s.split('/').filter(|x| !x.is_empty());
        if !s.contains('*') {
            return Ok(JsonGlob::Path(split.fold(
                JsonPointer::default(),
                |mut pointer, s| {
                    pointer.push_end(s);
                    pointer
                },
            )));
        }
        let segments: Vec<JsonGlobSegment> = split
            .map(|x| match x {
                "*" => JsonGlobSegment::Star,
                x => JsonGlobSegment::Path(x.to_string()),
            })
            .collect();
        let segments = segments;
        let count = segments
            .iter()
            .filter(|x| matches!(x, JsonGlobSegment::Star))
            .count();
        Ok(JsonGlob::PathWithStar(PathWithStar { segments, count }))
    }
}

impl JsonGlob {
    pub fn append(self, path: JsonGlob) -> Self {
        fn append_stars(
            PathWithStar {
                segments: mut left_segments,
                count: left_count,
            }: PathWithStar,
            PathWithStar {
                segments: mut right_segments,
                count: right_count,
            }: PathWithStar,
        ) -> PathWithStar {
            left_segments.append(&mut right_segments);

            PathWithStar {
                segments: left_segments,
                count: left_count + right_count,
            }
        }

        fn point_as_path_with_star(pointer: JsonPointer) -> PathWithStar {
            PathWithStar {
                segments: pointer.into_iter().map(JsonGlobSegment::Path).collect(),
                count: 0,
            }
        }

        match (self, path) {
            (JsonGlob::Path(mut paths), JsonGlob::Path(right_paths)) => {
                paths.append(&right_paths);
                JsonGlob::Path(paths)
            }
            (JsonGlob::Path(left), JsonGlob::PathWithStar(right)) => {
                JsonGlob::PathWithStar(append_stars(point_as_path_with_star(left), right))
            }
            (JsonGlob::PathWithStar(left), JsonGlob::Path(right)) => {
                JsonGlob::PathWithStar(append_stars(left, point_as_path_with_star(right)))
            }
            (JsonGlob::PathWithStar(left), JsonGlob::PathWithStar(right)) => {
                JsonGlob::PathWithStar(append_stars(left, right))
            }
        }
    }

    /// Used during the creation of star paths
    pub fn star() -> Self {
        JsonGlob::PathWithStar(PathWithStar {
            segments: vec![JsonGlobSegment::Star],
            count: 1,
        })
    }

    /// There are points that we use the JsonPointer starts_with, and we need to be able to
    /// utilize that and to be able to deal with the star paths
    pub fn starts_with(&self, other: &JsonGlob) -> bool {
        fn starts_with_<'a>(left: &Vec<JsonGlobSegment>, right: &Vec<JsonGlobSegment>) -> bool {
            let mut left_paths = left.iter();
            let mut right_paths = right.iter();
            loop {
                match (left_paths.next(), right_paths.next()) {
                    (Some(JsonGlobSegment::Path(x)), Some(JsonGlobSegment::Path(y))) => {
                        if x != y {
                            return false;
                        }
                    }
                    (Some(JsonGlobSegment::Star), Some(JsonGlobSegment::Star)) => {}
                    (Some(JsonGlobSegment::Star), Some(JsonGlobSegment::Path(_))) => {}
                    (Some(JsonGlobSegment::Path(_)), Some(JsonGlobSegment::Star)) => {}
                    (None, None) => return true,
                    (None, _) => return false,
                    (_, None) => return true,
                }
            }
        }
        match (self, other) {
            (JsonGlob::Path(x), JsonGlob::Path(y)) => x.starts_with(y),
            (
                JsonGlob::Path(x),
                JsonGlob::PathWithStar(PathWithStar {
                    segments: path,
                    count: _,
                }),
            ) => starts_with_(
                &x.iter()
                    .map(|x| JsonGlobSegment::Path(x.to_string()))
                    .collect(),
                path,
            ),
            (
                JsonGlob::PathWithStar(PathWithStar {
                    segments: path,
                    count: _,
                }),
                JsonGlob::Path(y),
            ) => starts_with_(
                path,
                &y.iter()
                    .map(|x| JsonGlobSegment::Path(x.to_string()))
                    .collect(),
            ),
            (
                JsonGlob::PathWithStar(PathWithStar {
                    segments: path,
                    count: _,
                }),
                JsonGlob::PathWithStar(PathWithStar {
                    segments: path_other,
                    count: _,
                }),
            ) => starts_with_(path, path_other),
        }
    }
    /// When we need to convert back into a usuable pointer string that is used for the paths of the
    /// get and set of the db.
    pub fn as_pointer(&self, binds: &[&str]) -> JsonPointer {
        match self {
            JsonGlob::Path(json_pointer) => json_pointer.clone(),
            JsonGlob::PathWithStar(PathWithStar {
                segments: path,
                count: _,
            }) => {
                let mut json_pointer: JsonPointer = Default::default();
                let mut binds = binds.iter();
                for path in (*path).iter() {
                    match path {
                        JsonGlobSegment::Path(path) => json_pointer.push_end(&path),
                        JsonGlobSegment::Star => {
                            if let Some(path) = binds.next() {
                                json_pointer.push_end(path)
                            }
                        }
                    }
                }
                json_pointer
            }
        }
    }

    pub fn star_count(&self) -> usize {
        match self {
            JsonGlob::Path(_) => 0,
            JsonGlob::PathWithStar(PathWithStar { count, .. }) => *count,
        }
    }
}

/// Used to determine the phantom type for bindings, () for no binds, String for one, (String, String) for two, ...
/// Also, there are message to indicate that this is not valid
pub trait AsPhantom<SB> {
    fn as_phantom_binds(&self) -> PhantomData<SB>;
}

impl AsPhantom<()> for JsonGlob {
    fn as_phantom_binds(&self) -> PhantomData<()> {
        let count = match self {
            JsonGlob::PathWithStar(PathWithStar { count, .. }) => *count,
            JsonGlob::Path(_) => 0,
        };
        if count != 0 {
            #[cfg(feature = "tracing")]
            tracing::error!("By counts={}, this phantom type = () is not valid", count);
            #[cfg(feature = "unstable")]
            panic!("By counts={}, this phantom type = () is not valid", count);
        }
        PhantomData
    }
}
impl AsPhantom<String> for JsonGlob {
    fn as_phantom_binds(&self) -> PhantomData<String> {
        let count = match self {
            JsonGlob::PathWithStar(PathWithStar { count, .. }) => *count,
            JsonGlob::Path(_) => 0,
        };
        if count != 1 {
            #[cfg(feature = "tracing")]
            tracing::error!(
                "By counts={}, this phantom type = String is not valid",
                count
            );
            #[cfg(feature = "unstable")]
            panic!(
                "By counts={}, this phantom type = String is not valid",
                count
            );
        }
        PhantomData
    }
}
impl AsPhantom<(String, String)> for JsonGlob {
    fn as_phantom_binds(&self) -> PhantomData<(String, String)> {
        let count = match self {
            JsonGlob::PathWithStar(PathWithStar { count, .. }) => *count,
            JsonGlob::Path(_) => 0,
        };
        if count != 2 {
            #[cfg(feature = "tracing")]
            tracing::error!(
                "By counts={}, this phantom type = (String, String) is not valid",
                count
            );
            #[cfg(feature = "unstable")]
            panic!(
                "By counts={}, this phantom type = (String, String) is not valid",
                count
            );
        }
        PhantomData
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use proptest::prelude::*;

    #[test]
    fn model_paths_parse_simple() {
        let path = "/a/b/c";
        let model_paths = JsonGlob::from_str(path).unwrap();
        assert_eq!(
            model_paths.as_pointer(&[]),
            JsonPointer::from_str(path).unwrap()
        );
    }
    #[test]
    fn model_paths_parse_star() {
        let path = "/a/b/c/*/e";
        let model_paths = JsonGlob::from_str(path).unwrap();
        assert_eq!(
            model_paths.as_pointer(&["d"]),
            JsonPointer::from_str("/a/b/c/d/e").unwrap()
        );
    }

    #[test]
    fn append() {
        let path = "/a/b/";
        let model_paths = JsonGlob::from_str(path)
            .unwrap()
            .append("c".parse().unwrap());
        assert_eq!(
            model_paths.as_pointer(&[]),
            JsonPointer::from_str("/a/b/c").unwrap()
        );
    }
    #[test]
    fn append_star() {
        let path = "/a/b/";
        let model_paths = JsonGlob::from_str(path)
            .unwrap()
            .append("*".parse().unwrap());
        assert_eq!(
            model_paths.as_pointer(&["c"]),
            JsonPointer::from_str("/a/b/c").unwrap()
        );
    }
    #[test]
    fn star_append() {
        let path = "/a/*/";
        let model_paths = JsonGlob::from_str(path)
            .unwrap()
            .append("c".parse().unwrap());
        assert_eq!(
            model_paths.as_pointer(&["b"]),
            JsonPointer::from_str("/a/b/c").unwrap()
        );
    }
    #[test]
    fn star_append_star() {
        let path = "/a/*/";
        let model_paths = JsonGlob::from_str(path)
            .unwrap()
            .append("*".parse().unwrap());
        assert_eq!(
            model_paths.as_pointer(&["b", "c"]),
            JsonPointer::from_str("/a/b/c").unwrap()
        );
    }
    #[test]
    fn starts_with_paths() {
        let path: JsonGlob = "/a/b".parse().unwrap();
        let path_b: JsonGlob = "/a".parse().unwrap();
        let path_c: JsonGlob = "/a/b/c".parse().unwrap();
        assert!(path.starts_with(&path_b));
        assert!(!path.starts_with(&path_c));
        assert!(path_c.starts_with(&path));
        assert!(!path_b.starts_with(&path));
    }
    #[test]
    fn starts_with_star_left() {
        let path: JsonGlob = "/a/*/c".parse().unwrap();
        let path_a: JsonGlob = "/a".parse().unwrap();
        let path_b: JsonGlob = "/b".parse().unwrap();
        let path_full_c: JsonGlob = "/a/b/c".parse().unwrap();
        let path_full_c_d: JsonGlob = "/a/b/c/d".parse().unwrap();
        let path_full_d_other: JsonGlob = "/a/b/d".parse().unwrap();
        assert!(path.starts_with(&path_a));
        assert!(path.starts_with(&path));
        assert!(!path.starts_with(&path_b));
        assert!(path.starts_with(&path_full_c));
        assert!(!path.starts_with(&path_full_c_d));
        assert!(!path.starts_with(&path_full_d_other));

        // Others start with
        assert!(!path_a.starts_with(&path));
        assert!(!path_b.starts_with(&path));
        assert!(path_full_c.starts_with(&path));
        assert!(path_full_c_d.starts_with(&path));
        assert!(!path_full_d_other.starts_with(&path));
    }

    /// A valid star path is something like `/a/*/c`
    /// A path may start with a letter, then any letter/ dash/ number
    /// A star path may only be a star
    pub fn arb_path_str() -> impl Strategy<Value = String> {
        // Funny enough we can't test the max size, running out of memory, funny that
        proptest::collection::vec("([a-z][a-z\\-0-9]*|\\*)", 0..100).prop_map(|a_s| {
            a_s.into_iter().fold(String::new(), |mut s, x| {
                s.push('/');
                s.push_str(&x);
                s
            })
        })
    }

    mod star_counts {
        use super::*;
        #[test]
        fn base_have_valid_star_count() {
            let path = "/a/*/c";
            let glob = JsonGlob::from_str(&path).unwrap();
            assert_eq!(
                glob.star_count(),
                1,
                "Star count should be the total number of star paths for path {}",
                path
            );
        }
        proptest! {
            #[test]
            fn all_valid_paths_have_valid_star_count(path in arb_path_str()) {
                let glob = JsonGlob::from_str(&path).unwrap();
                prop_assert_eq!(glob.star_count(), path.matches('*').count(), "Star count should be the total number of star paths for path {}", path);
            }
        }
    }

    proptest! {
        #[test]
        fn inductive_append_as_monoid(left in arb_path_str(), right in arb_path_str()) {
            let left_glob = JsonGlob::from_str(&left).unwrap();
            let right_glob = JsonGlob::from_str(&right).unwrap();
            let expected_join = format!("{}{}", left, right);
            let expected = JsonGlob::from_str(&expected_join).unwrap();
            let answer = left_glob.append(right_glob);
            prop_assert_eq!(answer, expected, "Appending another path should be the same as joining them as a string first for path {}", expected_join);
        }

        #[test]
        fn all_globs_parse_display_isomorphism(path in arb_path_str()) {
            let glob = JsonGlob::from_str(&path).unwrap();
            let other_glob = JsonGlob::from_str(&glob.to_string()).unwrap();
            prop_assert_eq!(other_glob, glob);
        }
    }
}
