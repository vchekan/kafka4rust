macro_rules! get_type {
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
            fn to_kafka(&self, buff: &mut BufMut) {
                $(self.$f.to_kafka(buff);)*
            }
        }

        $(impl Request for $sname {
            type Response = $response;
        })*

        $(request!($tp);)*
    };
}

macro_rules! response {
    ($id:ident) => {};
    ( [$id:ident] ) => {};

    // Array of complex type
    ( [ $sname:ident $tp:tt ] ) => (response!($sname $tp););


    ($sname:ident { $($f:ident : $tp:tt),* }) => {
        #[derive(Debug)]
        pub struct $sname {
            $(pub $f: get_type!($tp) ),*
        }

        impl FromKafka for $sname {
            fn from_kafka(buff: &mut Buf) -> $sname {
                $sname { $($f: <get_type!($tp)>::from_kafka(buff)),* }
            }
        }

        $( response!($tp); )*
    };
}