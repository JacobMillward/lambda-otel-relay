use std::collections::{BTreeMap, HashMap, VecDeque, hash_map::Entry};

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

/// Canonical identity for a Resource, derived from its sorted attributes and
/// the top-level `schema_url`. Two resources with the same set of key-value
/// attributes (regardless of original order) **and** the same schema URL
/// produce the same identity.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
struct ResourceIdentity {
    attributes: Vec<u8>,
    schema_url: String,
}

impl ResourceIdentity {
    fn new(resource: Option<&Resource>, schema_url: &str) -> Self {
        let attributes = resource.map_or_else(Vec::new, |r| {
            // BTreeMap deduplicates by key; per the OTLP spec, attribute keys MUST
            // be unique, so this is safe. Non-conformant duplicates use last-write-wins.
            let sorted: BTreeMap<&str, Vec<u8>> = r
                .attributes
                .iter()
                .map(|kv| (&kv.key[..], kv.encode_to_vec()))
                .collect();
            sorted.into_values().flatten().collect()
        });
        Self {
            attributes,
            schema_url: schema_url.to_owned(),
        }
    }
}

/// Abstracts the per-signal differences so a single generic [`merge`]
/// function can handle traces, metrics, and logs.
trait MergeableRequest: Message + Default {
    type Item;

    fn signal_name() -> &'static str;
    fn into_items(self) -> Vec<Self::Item>;
    fn from_items(items: Vec<Self::Item>) -> Self;
    fn identity(item: &Self::Item) -> ResourceIdentity;
    fn extend_scopes(existing: &mut Self::Item, incoming: Self::Item);
}

impl MergeableRequest for ExportTraceServiceRequest {
    type Item = ResourceSpans;

    fn signal_name() -> &'static str {
        "trace"
    }
    fn into_items(self) -> Vec<ResourceSpans> {
        self.resource_spans
    }
    fn from_items(items: Vec<ResourceSpans>) -> Self {
        Self {
            resource_spans: items,
        }
    }
    fn identity(item: &ResourceSpans) -> ResourceIdentity {
        ResourceIdentity::new(item.resource.as_ref(), &item.schema_url)
    }
    fn extend_scopes(existing: &mut ResourceSpans, incoming: ResourceSpans) {
        existing.scope_spans.extend(incoming.scope_spans);
    }
}

impl MergeableRequest for ExportMetricsServiceRequest {
    type Item = ResourceMetrics;

    fn signal_name() -> &'static str {
        "metrics"
    }
    fn into_items(self) -> Vec<ResourceMetrics> {
        self.resource_metrics
    }
    fn from_items(items: Vec<ResourceMetrics>) -> Self {
        Self {
            resource_metrics: items,
        }
    }
    fn identity(item: &ResourceMetrics) -> ResourceIdentity {
        ResourceIdentity::new(item.resource.as_ref(), &item.schema_url)
    }
    fn extend_scopes(existing: &mut ResourceMetrics, incoming: ResourceMetrics) {
        existing.scope_metrics.extend(incoming.scope_metrics);
    }
}

impl MergeableRequest for ExportLogsServiceRequest {
    type Item = ResourceLogs;

    fn signal_name() -> &'static str {
        "logs"
    }
    fn into_items(self) -> Vec<ResourceLogs> {
        self.resource_logs
    }
    fn from_items(items: Vec<ResourceLogs>) -> Self {
        Self {
            resource_logs: items,
        }
    }
    fn identity(item: &ResourceLogs) -> ResourceIdentity {
        ResourceIdentity::new(item.resource.as_ref(), &item.schema_url)
    }
    fn extend_scopes(existing: &mut ResourceLogs, incoming: ResourceLogs) {
        existing.scope_logs.extend(incoming.scope_logs);
    }
}

/// Decode, deduplicate by resource identity, and merge scope entries.
fn merge<M: MergeableRequest>(payloads: &VecDeque<Bytes>) -> M {
    let capacity = payloads.len().min(8);
    let mut groups: HashMap<ResourceIdentity, M::Item> = HashMap::with_capacity(capacity);
    let mut order: Vec<ResourceIdentity> = Vec::with_capacity(capacity);

    for payload in payloads {
        let req = match M::decode(payload.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "skipping malformed {} payload", M::signal_name());
                continue;
            }
        };
        for item in req.into_items() {
            let id = M::identity(&item);
            match groups.entry(id) {
                Entry::Occupied(mut e) => {
                    M::extend_scopes(e.get_mut(), item);
                }
                Entry::Vacant(e) => {
                    order.push(e.key().clone());
                    e.insert(item);
                }
            }
        }
    }

    M::from_items(
        order
            .into_iter()
            .filter_map(|id| groups.remove(&id))
            .collect(),
    )
}

pub fn merge_traces(payloads: &VecDeque<Bytes>) -> ExportTraceServiceRequest {
    merge(payloads)
}

pub fn merge_metrics(payloads: &VecDeque<Bytes>) -> ExportMetricsServiceRequest {
    merge(payloads)
}

pub fn merge_logs(payloads: &VecDeque<Bytes>) -> ExportLogsServiceRequest {
    merge(payloads)
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
}
