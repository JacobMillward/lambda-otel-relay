/// Whether the extension is running in a standard Lambda environment or
/// on Lambda Managed Instances (concurrent invocations, no INVOKE events).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeMode {
    Standard,
    ManagedInstances,
}

impl RuntimeMode {
    /// Detect the runtime mode from `AWS_LAMBDA_INITIALIZATION_TYPE`.
    pub fn detect() -> Self {
        match std::env::var("AWS_LAMBDA_INITIALIZATION_TYPE").as_deref() {
            Ok("lambda-managed-instances") => Self::ManagedInstances,
            _ => Self::Standard,
        }
    }

    pub fn is_managed_instances(self) -> bool {
        self == Self::ManagedInstances
    }
}
