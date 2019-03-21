macro_rules! dissect_field {
    ($tree:ident, $f:ident, i32) => {
        proto_tree_add_item($tree, stringify!($f));
    };
    ($tree:ident, $f:ident, $tp:tt) => {

    };
}

macro_rules! request {
    ($sname:ident => { $($f:ident : $tp:tt),* } ) => {
        struct $sname {}

        impl $sname {
            fn dissect(tvb: *mut tvbuff_t, pinfo: *mut packet_info, tree: *mut proto_tree, mut offset: i32, api_version: i16) {
                $(dissect_field!(tree, $f, $tp);)*
            }
        }
    };
}
