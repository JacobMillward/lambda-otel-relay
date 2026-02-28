use std::collections::VecDeque;

use bytes::Bytes;
use prost::Message;

use super::*;
use crate::proto::opentelemetry::proto::{
    common::v1::{AnyValue, KeyValue, any_value},
    trace::v1::ScopeSpans,
};

fn kv(key: &str, val: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(val.to_owned())),
        }),
    }
}

fn resource(attrs: Vec<KeyValue>) -> Option<Resource> {
    Some(Resource {
        attributes: attrs,
        dropped_attributes_count: 0,
        entity_refs: vec![],
    })
}

fn trace_request(entries: Vec<ResourceSpans>) -> Bytes {
    let req = ExportTraceServiceRequest {
        resource_spans: entries,
    };
    Bytes::from(req.encode_to_vec())
}

fn resource_spans(res: Option<Resource>, num_scopes: usize) -> ResourceSpans {
    ResourceSpans {
        resource: res,
        scope_spans: (0..num_scopes)
            .map(|_| ScopeSpans {
                scope: None,
                spans: vec![],
                schema_url: String::new(),
            })
            .collect(),
        schema_url: String::new(),
    }
}

#[test]
fn same_resource_merges_scope_entries() {
    let r = resource(vec![kv("service.name", "my-svc")]);
    let payload1 = trace_request(vec![resource_spans(r.clone(), 1)]);
    let payload2 = trace_request(vec![resource_spans(r, 2)]);

    let mut queue = VecDeque::new();
    queue.push_back(payload1);
    queue.push_back(payload2);

    let merged = merge_traces(&queue);
    assert_eq!(
        merged.resource_spans.len(),
        1,
        "should merge into 1 resource"
    );
    assert_eq!(
        merged.resource_spans[0].scope_spans.len(),
        3,
        "should have 1+2=3 scope spans"
    );
}

#[test]
fn different_resources_stay_separate() {
    let r1 = resource(vec![kv("service.name", "svc-a")]);
    let r2 = resource(vec![kv("service.name", "svc-b")]);
    let payload = trace_request(vec![resource_spans(r1, 1), resource_spans(r2, 1)]);

    let mut queue = VecDeque::new();
    queue.push_back(payload);

    let merged = merge_traces(&queue);
    assert_eq!(merged.resource_spans.len(), 2);
}

#[test]
fn attribute_order_does_not_affect_identity() {
    let r1 = resource(vec![kv("a", "1"), kv("b", "2")]);
    let r2 = resource(vec![kv("b", "2"), kv("a", "1")]);
    let payload1 = trace_request(vec![resource_spans(r1, 1)]);
    let payload2 = trace_request(vec![resource_spans(r2, 1)]);

    let mut queue = VecDeque::new();
    queue.push_back(payload1);
    queue.push_back(payload2);

    let merged = merge_traces(&queue);
    assert_eq!(
        merged.resource_spans.len(),
        1,
        "[a,b] and [b,a] should merge"
    );
    assert_eq!(merged.resource_spans[0].scope_spans.len(), 2);
}

#[test]
fn none_resources_merge_together() {
    let payload1 = trace_request(vec![resource_spans(None, 1)]);
    let payload2 = trace_request(vec![resource_spans(None, 2)]);

    let mut queue = VecDeque::new();
    queue.push_back(payload1);
    queue.push_back(payload2);

    let merged = merge_traces(&queue);
    assert_eq!(merged.resource_spans.len(), 1);
    assert_eq!(merged.resource_spans[0].scope_spans.len(), 3);
}

#[test]
fn malformed_payload_skipped() {
    let valid = trace_request(vec![resource_spans(None, 1)]);

    let mut queue = VecDeque::new();
    queue.push_back(Bytes::from(vec![0xFF, 0xFF, 0xFF]));
    queue.push_back(valid);

    let merged = merge_traces(&queue);
    assert_eq!(merged.resource_spans.len(), 1);
}

#[test]
fn empty_attributes_and_none_resource_merge_together() {
    let empty_attrs = resource(vec![]);
    let payload1 = trace_request(vec![resource_spans(empty_attrs, 1)]);
    let payload2 = trace_request(vec![resource_spans(None, 2)]);

    let mut queue = VecDeque::new();
    queue.push_back(payload1);
    queue.push_back(payload2);

    let merged = merge_traces(&queue);
    assert_eq!(
        merged.resource_spans.len(),
        1,
        "empty-attributes and None resource should produce the same identity"
    );
    assert_eq!(merged.resource_spans[0].scope_spans.len(), 3);
}

#[test]
fn empty_payloads_produce_empty_request() {
    let queue = VecDeque::new();
    let merged = merge_traces(&queue);
    assert!(merged.resource_spans.is_empty());
}

#[test]
fn different_schema_urls_stay_separate() {
    let r = resource(vec![kv("service.name", "my-svc")]);
    let mut rs1 = resource_spans(r.clone(), 1);
    rs1.schema_url = "https://example.com/schema/v1".to_owned();
    let mut rs2 = resource_spans(r, 1);
    rs2.schema_url = "https://example.com/schema/v2".to_owned();

    let payload = trace_request(vec![rs1, rs2]);

    let mut queue = VecDeque::new();
    queue.push_back(payload);

    let merged = merge_traces(&queue);
    assert_eq!(
        merged.resource_spans.len(),
        2,
        "same attributes but different schema_url should stay separate"
    );
}
