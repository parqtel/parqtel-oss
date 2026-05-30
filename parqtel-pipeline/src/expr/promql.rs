use serde::{Deserialize, Serialize};

/// A parsed PromQL expression (stored as string, validated at load time).
/// Full PromQL evaluation is delegated to the query engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromQlExpr {
    pub raw: String,
}

impl PromQlExpr {
    /// Parse and validate a PromQL expression string.
    /// Basic validation: non-empty, balanced brackets/parens.
    pub fn parse(input: &str) -> crate::Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(crate::Error::Expression("Empty PromQL expression".into()));
        }
        // Basic bracket balance check
        let mut paren_depth: i32 = 0;
        let mut bracket_depth: i32 = 0;
        for ch in trimmed.chars() {
            match ch {
                '(' => paren_depth += 1,
                ')' => paren_depth -= 1,
                '[' => bracket_depth += 1,
                ']' => bracket_depth -= 1,
                _ => {}
            }
            if paren_depth < 0 || bracket_depth < 0 {
                return Err(crate::Error::Expression(
                    "Unbalanced brackets in PromQL expression".into(),
                ));
            }
        }
        if paren_depth != 0 || bracket_depth != 0 {
            return Err(crate::Error::Expression(
                "Unbalanced brackets in PromQL expression".into(),
            ));
        }
        Ok(Self {
            raw: trimmed.to_string(),
        })
    }
}
