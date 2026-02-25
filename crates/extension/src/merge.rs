use std::collections::{HashMap, VecDeque, hash_map::Entry};

use bytes::Bytes;
use prost::Message;
use tracing::warn;

use crate::proto::opentelemetry::proto::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    logs::v1::ResourceLogs,
    metrics::v1::ResourceMetrics,
    resource::v1::Resource,
    trace::v1::ResourceSpans,
};

/// Canonical identity for a Resource, derived from its sorted attributes.
/// Two resources with the same set of key-value attributes (regardless of
/// original order) produce the same identity.
#[derive(Clone, PartialEq, Eq, Hash)]
struct ResourceIdentity(Vec<u8>);

impl ResourceIdentity {
    fn from_resource(resource: Option<&Resource>) -> Self {
        let Some(resource) = resource else {
            return Self(Vec::new());
        };
        // Sort by full encoded KeyValue (covers both key and value) so that
        // attribute order doesn't affect identity and duplicate keys with
        // different values don't collide.
        let mut encoded: Vec<Vec<u8>> = resource
            .attributes
            .iter()
            .map(|kv| kv.encode_to_vec())
            .collect();
        encoded.sort_unstable();

        let mut buf = Vec::with_capacity(encoded.iter().map(|v| v.len()).sum());
        for e in encoded {
            buf.extend(e);
        }
        Self(buf)
    }
}

pub fn merge_traces(payloads: &VecDeque<Bytes>) -> ExportTraceServiceRequest {
    let mut groups: HashMap<ResourceIdentity, ResourceSpans> = HashMap::new();
    let mut order: Vec<ResourceIdentity> = Vec::new();

    for payload in payloads {
        let req = match ExportTraceServiceRequest::decode(payload.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "skipping malformed trace payload");
                continue;
            }
        };
        for rs in req.resource_spans {
            let id = ResourceIdentity::from_resource(rs.resource.as_ref());
            match groups.entry(id) {
                Entry::Occupied(mut e) => {
                    e.get_mut().scope_spans.extend(rs.scope_spans);
                }
                Entry::Vacant(e) => {
                    order.push(e.key().clone());
                    e.insert(rs);
                }
            }
        }
    }

    ExportTraceServiceRequest {
        resource_spans: order
            .into_iter()
            .filter_map(|id| groups.remove(&id))
            .collect(),
    }
}

pub fn merge_metrics(payloads: &VecDeque<Bytes>) -> ExportMetricsServiceRequest {
    let mut groups: HashMap<ResourceIdentity, ResourceMetrics> = HashMap::new();
    let mut order: Vec<ResourceIdentity> = Vec::new();

    for payload in payloads {
        let req = match ExportMetricsServiceRequest::decode(payload.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "skipping malformed metrics payload");
                continue;
            }
        };
        for rm in req.resource_metrics {
            let id = ResourceIdentity::from_resource(rm.resource.as_ref());
            match groups.entry(id) {
                Entry::Occupied(mut e) => {
                    e.get_mut().scope_metrics.extend(rm.scope_metrics);
                }
                Entry::Vacant(e) => {
                    order.push(e.key().clone());
                    e.insert(rm);
                }
            }
        }
    }

    ExportMetricsServiceRequest {
        resource_metrics: order
            .into_iter()
            .filter_map(|id| groups.remove(&id))
            .collect(),
    }
}

pub fn merge_logs(payloads: &VecDeque<Bytes>) -> ExportLogsServiceRequest {
    let mut groups: HashMap<ResourceIdentity, ResourceLogs> = HashMap::new();
    let mut order: Vec<ResourceIdentity> = Vec::new();

    for payload in payloads {
        let req = match ExportLogsServiceRequest::decode(payload.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "skipping malformed logs payload");
                continue;
            }
        };
        for rl in req.resource_logs {
            let id = ResourceIdentity::from_resource(rl.resource.as_ref());
            match groups.entry(id) {
                Entry::Occupied(mut e) => {
                    e.get_mut().scope_logs.extend(rl.scope_logs);
                }
                Entry::Vacant(e) => {
                    order.push(e.key().clone());
                    e.insert(rl);
                }
            }
        }
    }

    ExportLogsServiceRequest {
        resource_logs: order
            .into_iter()
            .filter_map(|id| groups.remove(&id))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
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
    fn empty_payloads_produce_empty_request() {
        let queue = VecDeque::new();
        let merged = merge_traces(&queue);
        assert!(merged.resource_spans.is_empty());
    }
}
