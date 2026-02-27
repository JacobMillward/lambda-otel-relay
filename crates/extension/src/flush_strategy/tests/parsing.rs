use std::time::Duration;

use super::super::*;

#[test]
fn parse_end() {
    let strategy = "end".parse().unwrap();
    assert!(matches!(strategy, FlushStrategy::End));
}

#[test]
fn parse_empty_defaults_to_default() {
    let strategy = "".parse().unwrap();
    assert!(matches!(strategy, FlushStrategy::Default));
}

#[test]
fn parse_default() {
    let strategy = "default".parse().unwrap();
    assert!(matches!(strategy, FlushStrategy::Default));
}

#[test]
fn parse_unknown_strategy_errors() {
    let err = "bogus".parse::<FlushStrategy>().unwrap_err();
    assert!(matches!(err, FlushStrategyError::UnknownStrategy(_)));
}

#[test]
fn parse_strategy_prefix_without_comma_is_unknown() {
    // "periodicallyX" should be UnknownStrategy, not InvalidParameter
    let err = "periodicallyX".parse::<FlushStrategy>().unwrap_err();
    assert!(matches!(err, FlushStrategyError::UnknownStrategy(_)));

    let err = "continuouslyFoo".parse::<FlushStrategy>().unwrap_err();
    assert!(matches!(err, FlushStrategyError::UnknownStrategy(_)));
}

#[test]
fn parse_periodically() {
    let strategy = "periodically,60000".parse().unwrap();
    assert!(matches!(
        strategy,
        FlushStrategy::Periodically { interval } if interval == Duration::from_millis(60000)
    ));
}

#[test]
fn parse_periodically_missing_param() {
    let err = "periodically".parse::<FlushStrategy>().unwrap_err();
    assert!(matches!(err, FlushStrategyError::InvalidParameter { .. }));
}

#[test]
fn parse_periodically_zero() {
    let err = "periodically,0".parse::<FlushStrategy>().unwrap_err();
    assert!(matches!(err, FlushStrategyError::InvalidParameter { .. }));
}

#[test]
fn parse_periodically_non_numeric() {
    let err = "periodically,abc".parse::<FlushStrategy>().unwrap_err();
    assert!(matches!(err, FlushStrategyError::InvalidParameter { .. }));
}

#[test]
fn parse_end_periodically() {
    let strategy = "end,30000".parse().unwrap();
    assert!(matches!(
        strategy,
        FlushStrategy::EndPeriodically { interval } if interval == Duration::from_millis(30000)
    ));
}

#[test]
fn parse_end_periodically_zero() {
    let err = "end,0".parse::<FlushStrategy>().unwrap_err();
    assert!(matches!(err, FlushStrategyError::InvalidParameter { .. }));
}

#[test]
fn parse_continuously() {
    let strategy = "continuously,60000".parse().unwrap();
    assert!(matches!(
        strategy,
        FlushStrategy::Continuously { interval } if interval == Duration::from_millis(60000)
    ));
}

#[test]
fn parse_continuously_missing_param() {
    let err = "continuously".parse::<FlushStrategy>().unwrap_err();
    assert!(matches!(err, FlushStrategyError::InvalidParameter { .. }));
}

#[test]
fn parse_continuously_zero() {
    let err = "continuously,0".parse::<FlushStrategy>().unwrap_err();
    assert!(matches!(err, FlushStrategyError::InvalidParameter { .. }));
}

#[test]
fn display_roundtrips_through_parse() {
    let cases: &[&str] = &[
        "default",
        "end",
        "end,30000",
        "periodically,60000",
        "continuously,5000",
    ];
    for &input in cases {
        let strategy = input.parse::<FlushStrategy>().unwrap();
        let displayed = strategy.to_string();
        let reparsed = &displayed.parse::<FlushStrategy>().unwrap();
        assert_eq!(
            displayed,
            reparsed.to_string(),
            "roundtrip failed for {input}"
        );
    }
}

#[test]
fn parse_negative_interval_errors() {
    let err = "periodically,-5".parse::<FlushStrategy>().unwrap_err();
    assert!(matches!(err, FlushStrategyError::InvalidParameter { .. }));

    let err = "end,-100".parse::<FlushStrategy>().unwrap_err();
    assert!(matches!(err, FlushStrategyError::InvalidParameter { .. }));

    let err = "continuously,-1".parse::<FlushStrategy>().unwrap_err();
    assert!(matches!(err, FlushStrategyError::InvalidParameter { .. }));
}
