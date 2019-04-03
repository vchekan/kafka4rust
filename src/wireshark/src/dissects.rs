use crate::bindings::*;
use crate::fields::*;
use crate::plugin::*;
use crate::utils::i8_str;
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::sync::{MutexGuard, Mutex};
use lazy_static::lazy_static;
use std::collections::HashMap;
use crate::protocol;

struct Correlation {
    api_key: u16,
    api_version: i16,
}
lazy_static!{
    // TODO: make it per-conversation
    // TODO: clear when new session starts. How? Is it wmem_file_scope?
    static ref correlation_map : Mutex<HashMap<i32,Correlation>> = Mutex::new(HashMap::new());
}

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

// TODO: looks like bindgen fantasized about field names in `tcp_dissect_pdus:get_pdu_len`
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

        let root_ti = proto_tree_add_item(tree, *PROTO_KAFKA.lock().unwrap(), tvb, 0, -1, ENC_NA);
        let kafka_tree = proto_item_add_subtree(root_ti, *ETT_KAFKA.lock().unwrap());
        proto_tree_add_item(kafka_tree, hf_kafka_len, tvb, offset, 4, ENC_BIG_ENDIAN);
        offset += 4;

        let is_request = KAFKA_PORT == (*pinfo).destport;
        if is_request {
            let api_key = tvb_get_ntohs(tvb, offset);
            let api_version = tvb_get_ntohs(tvb, offset + 2) as i16;
            let correlationId = tvb_get_ntohl(tvb, offset + 4) as i32;

            if (*(*pinfo).fd).flags.visited() == 0 {
                correlation_map.lock().unwrap().insert(correlationId, Correlation { api_key, api_version });
            }

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

            proto_tree_add_item(
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

            // ClientId
            offset = dissect_kafka_string(kafka_tree, hf_kafka_client_id, *ETT_CLIENT_ID.lock().unwrap(), tvb, pinfo, offset);

            match api_key {
                // ProduceRequest
                0 => {
                    protocol::ProduceRequest::dissect(tvb, pinfo, kafka_tree, offset, api_version);
                }
                3 => {
                    //dissect_kafka_metadata_request(tvb, pinfo, kafka_tree, offset, api_version);
                },
                18 => {
                    if api_version > 2 {
                        println!("Unknown ApiVersion request version: {}", api_version);
                    }
                },
                _ => {
                    println!("Dissection not implemented for api_key: {}", api_key);
                    
                }
            }
        } else {
            //
            // Response
            //
            let correlationId = tvb_get_ntohl(tvb, offset) as i32;
            proto_tree_add_item(
                kafka_tree,
                hf_kafka_correlation_id,
                tvb,
                offset,
                4,
                ENC_BIG_ENDIAN,
            );
            offset += 4;

            match correlation_map.lock().unwrap().get(&correlationId) {
                None => println!("Can not find matching request for response (correlationId={})", correlationId),
                Some(correlation) => {
                    col_add_fstr(
                        (*pinfo).cinfo,
                        COL_INFO as i32,
                        i8_str("Kafka %s v%d response\0"),
                        api_key_to_str(correlation.api_key).as_ptr(),
                        correlation.api_version as c_uint,
                    );
                    // Add to proto root
                    proto_item_append_text(
                        root_ti,
                        i8_str(" (%s v%d Response)\0"),
                        api_key_to_str(correlation.api_key).as_ptr(),
                        correlation.api_version as c_uint,
                    );

                    match correlation.api_key {
                        3 => {
                            //dissect_kafka_metadata_response(tvb, pinfo, kafka_tree, offset, correlation.api_version);
                            protocol::metadata_response::dissect(tvb, pinfo, kafka_tree, offset, correlation.api_version);
                        },
                        18 => {
                            //dissect_kafka_api_version_response(tvb, pinfo, kafka_tree, offset, correlation.api_version);
                        },
                        _ => {println!("Unknown api_key: {}", correlation.api_key)}
                    }
                }
            }
        }

        tvb_captured_length(tvb) as c_int
    }
}

pub(crate) fn dissect_kafka_array(tvb: *mut tvbuff_t, pinfo: *mut packet_info, tree: *mut proto_tree, mut offset: i32,
   api_version: i16,
   dissector: (fn(*mut tvbuff_t, *mut packet_info, *mut proto_tree, i32, i16) -> i32)
) -> i32 {
    unsafe {
        let count = tvb_get_ntohl(tvb, offset);
        proto_tree_add_item(tree, hf_kafka_array_count, tvb, offset, 4, ENC_BIG_ENDIAN);
        offset += 4;
        for _ in 0..count {
            offset = dissector(tvb, pinfo, tree, offset, api_version);
        }
    }
    offset
}

pub(crate) fn dissect_kafka_string(
    tree: *mut proto_tree,
    hf_item: i32,
    ett: i32,
    tvb: *mut tvbuff_t,
    pinfo: *mut packet_info,
    mut offset: i32,
) -> i32 {
    unsafe {
        let len = tvb_get_ntohs(tvb, offset) as gint16;
        offset += 2;

        if len == -1 {
            proto_tree_add_string(tree, hf_item, tvb, offset-2, 2, 0 as *const c_char);
        } else {
            let ti = proto_tree_add_item(tree, hf_item, tvb, offset, len as i32, ENC_NA | ENC_UTF_8);
            let subtree = proto_item_add_subtree(ti, ett);
            proto_tree_add_item(subtree, hf_kafka_string_len, tvb, offset - 2, 2, ENC_BIG_ENDIAN);
            proto_tree_add_item(subtree, hf_kafka_string, tvb, offset, len as i32, ENC_NA | ENC_UTF_8);
            offset += len as i32;
        }
    }

    offset
}

pub(crate) fn dissect_record_batch(
    tree: *mut proto_tree,
    tvb: *mut tvbuff_t,
    pinfo: *mut packet_info,
    mut offset: i32,
) -> i32 {
    unsafe {
        // TODO: validate segment size boundaries
        proto_tree_add_item(tree, hf_kafka_recordbatch_segment_size, tvb, offset, 4, ENC_BIG_ENDIAN);
        offset += 4;

        let magic = tvb_get_guint8(tvb, offset + 8 + 4 + 4);

        if magic == 1 {
            offset = dissect_message_set_v1(tree, tvb, pinfo, offset);
        }
        else if magic == 2 {
            proto_tree_add_item(tree, hf_kafka_recordbatch_baseoffset, tvb, offset, 8, ENC_BIG_ENDIAN);
            offset += 8;

            proto_tree_add_item(tree, hf_kafka_recordbatch_batchlength, tvb, offset, 4, ENC_BIG_ENDIAN);
            offset += 4;

            proto_tree_add_item(tree, hf_kafka_recordbatch_partition_leader_epoch, tvb, offset, 4, ENC_BIG_ENDIAN);
            offset += 4;

            proto_tree_add_item(tree, hf_kafka_recordbatch_magic, tvb, offset, 1, ENC_NA);
            offset += 1;

            // TODO: check crc and show error
            proto_tree_add_item(tree, hf_kafka_recordbatch_crc, tvb, offset, 4, ENC_BIG_ENDIAN);
            offset += 4;

            proto_tree_add_bitmask(tree, tvb, offset as u32, hf_kafka_recordbatch_attributes,
                               *ETT_BATCH_ATTRIBUTES.lock().unwrap(),
                               kafka_batch_attributes.as_mut_ptr(), ENC_BIG_ENDIAN);
            offset += 2;

            proto_tree_add_item(tree, hf_kafka_recordbatch_lastoffsetdelta, tvb, offset, 4, ENC_BIG_ENDIAN);
            offset += 4;

            proto_tree_add_item(tree, hf_kafka_recordbatch_firsttimestamp, tvb, offset, 8, ENC_BIG_ENDIAN);
            offset += 8;

            proto_tree_add_item(tree, hf_kafka_recordbatch_maxtimestamp, tvb, offset, 8, ENC_BIG_ENDIAN);
            offset += 8;

            proto_tree_add_item(tree, hf_kafka_recordbatch_producer_id, tvb, offset, 8, ENC_BIG_ENDIAN);
            offset += 8;

            proto_tree_add_item(tree, hf_kafka_recordbatch_producer_epoch, tvb, offset, 2, ENC_BIG_ENDIAN);
            offset += 2;

            proto_tree_add_item(tree, hf_kafka_recordbatch_base_sequence, tvb, offset, 4, ENC_BIG_ENDIAN);
            offset += 4;

            // [Records]
        } else {
            // TODO: unknown magic
            println!("Unknown magic: {}", magic);
        }

        offset += 10_000;

    }
    offset
}

fn dissect_message_set_v1(tree: *mut proto_tree, tvb: *mut tvbuff_t, pinfo: *mut packet_info, mut offset: i32) -> i32 {
    unsafe {
        proto_tree_add_item(tree, hf_kafka_messageset_offset, tvb, offset, 8, ENC_BIG_ENDIAN);
        offset += 8;

        proto_tree_add_item(tree, hf_kafka_messageset_msg_size, tvb, offset, 4, ENC_BIG_ENDIAN);
        offset += 4;

        // TODO: check crc and show error
        proto_tree_add_item(tree, hf_kafka_recordbatch_crc, tvb, offset, 4, ENC_BIG_ENDIAN);
        offset += 4;

        proto_tree_add_item(tree, hf_kafka_recordbatch_magic, tvb, offset, 1, ENC_NA);
        offset += 1;

        proto_tree_add_bitmask(tree, tvb, offset as u32, hf_kafka_messageset_attributes,
                   *ETT_BATCH_ATTRIBUTES.lock().unwrap(),
                   kafka_messageset_attributes.as_mut_ptr(), ENC_BIG_ENDIAN);
        offset += 1;

        proto_tree_add_item(tree, hf_kafka_recordbatch_timestamp, tvb, offset, 8, ENC_TIME_MSECS|ENC_BIG_ENDIAN);
        offset += 8;

        // Key
        // TODO: show array len
        let key_len = tvb_get_ntohl(tvb, offset) as i32;
        offset += 4;
        if key_len == -1 {
            proto_tree_add_item(tree, hf_kafka_recordbatch_key, tvb, offset, 0, ENC_NA);
        } else {
            proto_tree_add_item(tree, hf_kafka_recordbatch_key, tvb, offset, key_len, ENC_NA);
            offset += key_len;
        }

        // Value
        // TODO: show array len
        let value_len = tvb_get_ntohl(tvb, offset) as i32;
        offset += 4;
        if value_len == -1 {
            proto_tree_add_item(tree, hf_kafka_recordbatch_value, tvb, offset, 0, ENC_NA);
        } else {
            proto_tree_add_item(tree, hf_kafka_recordbatch_value, tvb, offset, value_len, ENC_NA);
            offset += value_len;
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
