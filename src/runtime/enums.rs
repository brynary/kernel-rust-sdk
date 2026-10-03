//! Support for string enums that stay compatible with new server values.

/// Define a string enum with an `Unknown(String)` fallback variant.
///
/// Unknown values from the server decode into `Unknown` instead of failing,
/// so new enum values do not break existing clients.
#[doc(hidden)]
#[macro_export]
macro_rules! string_enum {
    (
        $(#[$meta:meta])*
        $name:ident {
            $( $(#[$vmeta:meta])* $variant:ident = $value:literal, )*
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum $name {
            $( $(#[$vmeta])* $variant, )*
            /// A value not known to this version of the SDK.
            Unknown(String),
        }

        impl $name {
            /// The wire value.
            pub fn as_str(&self) -> &str {
                match self {
                    $( Self::$variant => $value, )*
                    Self::Unknown(s) => s.as_str(),
                }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::std::convert::From<&str> for $name {
            fn from(s: &str) -> Self {
                match s {
                    $( $value => Self::$variant, )*
                    other => Self::Unknown(other.to_owned()),
                }
            }
        }

        impl ::std::convert::From<String> for $name {
            fn from(s: String) -> Self {
                Self::from(s.as_str())
            }
        }

        impl ::serde::Serialize for $name {
            fn serialize<S: ::serde::Serializer>(&self, s: S) -> ::std::result::Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> ::std::result::Result<Self, D::Error> {
                let s = <::std::string::String as ::serde::Deserialize>::deserialize(d)?;
                Ok(Self::from(s))
            }
        }
    };
}
