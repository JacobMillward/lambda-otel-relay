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
mod tests;
