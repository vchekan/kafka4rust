use crate::bindings::*;
use crate::fields::*;
use crate::plugin::{ETT_KAFKA, KAFKA_PORT, PROTO_KAFKA};
use crate::utils::i8_str;
use std::os::raw::{c_char, c_int, c_uint, c_void};

/// Kafka message can be split into more then one Tcp packets.
/// Assemble it back using wireshar's `tcp_dissect_pdus`.
pub(crate) extern "C" fn dissect_kafka_tcp(
    tvb: *mut tvbuff_t,
    pinfo: *mut packet_info,
    tree: *mut proto_tree,
    data: *mut c_void,
) -> c_int {
    unsafe {
        tcp_dissect_pdus(
            tvb,
            pinfo,
            tree,
            1,
            4,
            Some(get_kafka_pdu_len),
            Some(dissect_kafka),
            data,
        );
        tvb_captured_length(tvb) as c_int
    }
}

// TODO: looks like bindgen fantisized about field names in `tcp_dissect_pdus:get_pdu_len`
extern "C" fn get_kafka_pdu_len(
    pinfo: *mut packet_info,
    tvb: *mut tvbuff_t,
    offset: c_int,
    data: *mut c_void,
) -> guint {
    unsafe { 4 + tvb_get_ntohl(tvb, offset) }
}

extern "C" fn dissect_kafka(
    tvb: *mut tvbuff_t,
    pinfo: *mut packet_info,
    tree: *mut proto_tree,
    data: *mut c_void,
) -> c_int {
    let mut offset = 0;
    unsafe {
        col_set_str((*pinfo).cinfo, COL_PROTOCOL as c_int, i8_str("Kafka\0"));
        col_clear((*pinfo).cinfo, COL_INFO as c_int);

        let root_ti = proto_tree_add_item(tree, *PROTO_KAFKA.lock(), tvb, 0, -1, ENC_NA);
        let kafka_tree = proto_item_add_subtree(root_ti, *ETT_KAFKA.lock());
        proto_tree_add_item(kafka_tree, hf_kafka_len, tvb, offset, 4, ENC_BIG_ENDIAN);
        offset += 4;

        let is_request = KAFKA_PORT == (*pinfo).destport;
        if is_request {
            let api_key = tvb_get_ntohs(tvb, offset);
            let api_version = tvb_get_ntohs(tvb, offset + 2);
            col_add_fstr(
                (*pinfo).cinfo,
                COL_INFO as i32,
                i8_str("Kafka %s v%d request\0"),
                api_key_to_str(api_key).as_ptr(),
                api_version as c_uint,
            );
            // Add to proto root
            proto_item_append_text(
                root_ti,
                i8_str(" (%s v%d Request)\0"),
                api_key_to_str(api_key).as_ptr(),
                api_version as c_uint,
            );

            let ti = proto_tree_add_item(
                kafka_tree,
                hf_kafka_request_api_key,
                tvb,
                offset,
                2,
                ENC_BIG_ENDIAN,
            );
            offset += 2;
            // TODO:
            //kafka_check_supported_api_key(pinfo, ti, matcher);

            let ti = proto_tree_add_item(
                kafka_tree,
                hf_kafka_request_api_version,
                tvb,
                offset,
                2,
                ENC_BIG_ENDIAN,
            );
            offset += 2;
            //kafka_check_supported_api_version(pinfo, ti, matcher);

            proto_tree_add_item(
                kafka_tree,
                hf_kafka_correlation_id,
                tvb,
                offset,
                4,
                ENC_BIG_ENDIAN,
            );
            offset += 4;

            offset = dissect_kafka_string(kafka_tree, hf_kafka_client_id, tvb, pinfo, offset);
        } else {
            //
            // Response
            //
            proto_tree_add_item(
                kafka_tree,
                hf_kafka_correlation_id,
                tvb,
                offset,
                4,
                ENC_BIG_ENDIAN,
            );
            offset += 4;

            // In order to decode response we need to know what type of request was sent for given
            // correlation Id, because responses do not have Api Key or Api Version marker.
        }

        tvb_captured_length(tvb) as c_int
    }
}

fn dissect_kafka_string(
    tree: *mut proto_tree,
    hf_item: i32,
    tvb: *mut tvbuff_t,
    pinfo: *mut packet_info,
    mut offset: i32,
) -> i32 {
    unsafe {
        let len = tvb_get_ntohs(tvb, offset) as gint16;
        let pi = proto_tree_add_item(tree, hf_kafka_string_len, tvb, offset, 2, ENC_BIG_ENDIAN);
        offset += 2;

        if len < -1 {
            // TODO
            //expert_add_info(pinfo, pi, &ei_kafka_bad_string_length);
        } else {
            // TODO: do not show string len as separate item, show Client Id as root
            // with len and string underneath.
            if len == -1 {
                proto_tree_add_string(tree, hf_item, tvb, offset, 0, 0 as *const c_char);
            } else {
                proto_tree_add_item(tree, hf_item, tvb, offset, len as i32, ENC_NA | ENC_ASCII);
                offset += len as i32;
            }
        }
    }

    offset
}

fn api_key_to_str(api_key: u16) -> &'static str {
    if (api_key as usize) < api_keys.len() {
        api_keys[api_key as usize].1
    } else {
        "???"
    }
}
