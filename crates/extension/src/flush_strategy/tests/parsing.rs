use std::time::Duration;

use super::super::*;

#[test]
fn parse_end() {
    let strategy = FlushStrategy::parse("end").unwrap();
    assert!(matches!(strategy, FlushStrategy::End));
}

#[test]
fn parse_empty_defaults_to_end() {
    let strategy = FlushStrategy::parse("").unwrap();
    assert!(matches!(strategy, FlushStrategy::End));
}

#[test]
fn parse_unknown_strategy_errors() {
    let err = FlushStrategy::parse("bogus").unwrap_err();
    assert!(matches!(err, FlushStrategyError::UnknownStrategy(_)));
}

#[test]
fn parse_periodically() {
    let strategy = FlushStrategy::parse("periodically,60000").unwrap();
    assert!(matches!(
        strategy,
        FlushStrategy::Periodically { interval } if interval == Duration::from_millis(60000)
    ));
}

#[test]
fn parse_periodically_missing_param() {
    let err = FlushStrategy::parse("periodically").unwrap_err();
    assert!(matches!(err, FlushStrategyError::InvalidParameter { .. }));
}

#[test]
fn parse_periodically_zero() {
    let err = FlushStrategy::parse("periodically,0").unwrap_err();
    assert!(matches!(err, FlushStrategyError::InvalidParameter { .. }));
}

#[test]
fn parse_periodically_non_numeric() {
    let err = FlushStrategy::parse("periodically,abc").unwrap_err();
    assert!(matches!(err, FlushStrategyError::InvalidParameter { .. }));
}
