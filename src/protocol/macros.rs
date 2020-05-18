macro_rules! get_type {
    ( { fn $fn:ident() -> $tp:ty } ) => ( $tp );
    ([$t:ident $body:tt] ) => (Vec<get_type!($t)>);
    ($t:ident $body:tt) => ($t);
    ($t:ident) => ($t);
    ([$t:ident]) => (Vec<$t>);
}

macro_rules! request {
    ($id:ident) => {};
    ( [$id:ident] ) => {};

    // Array of complex type
    ( [$sname:ident $tp:tt]) => {request!($sname $tp);};

    ($sname:ident $(, $response:ident)* { $($f:ident : $tp:tt),* } ) => {
        #[derive(Debug)]
        pub struct $sname {
            $(pub $f : get_type!($tp) ),*
        }

        impl ToKafka for $sname {
            fn to_kafka(&self, _buff: &mut BytesMut) {
                $(self.$f.to_kafka(_buff);)*
            }
        }

        $(impl Request for $sname {
            type Response = $response;
        })*

        $(request!($tp);)*
    };
}

macro_rules! response {
    // TODO: `fn` is redundant, avoid it
    ( { fn $fn:ident() -> $tp:ty} ) => {};
    ($id:ident) => {};
    ( [$id:ident] ) => {};
    ([Result<$tp:ty>]) => {};

    // Array of complex type
    ( [ $sname:ident $tp:tt ] ) => (response!($sname $tp););

    ([Result<$sname:ident> $def:tt ] ) => {
        response!($sname $def);
    };

    // Complex object
    ($sname:ident { $($f:tt : $tp:tt),* }) => {
        #[derive(Debug)]
        pub struct $sname {
            $(pub $f: get_type!($tp) ),*
        }

        impl FromKafka for $sname {
            fn from_kafka(_buff: &mut impl Buf) -> Result<$sname> {
                Ok($sname { $($f: <get_type!($tp)>::from_kafka(_buff)?),* })
            }
        }

        // Recursively generate complex types
        $( response!($tp); )*
    };

    (Result<$sname:ident> $def:tt) => {
        response!($sname $def);
    };
}
