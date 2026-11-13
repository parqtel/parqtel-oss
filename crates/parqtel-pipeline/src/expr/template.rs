use std::collections::BTreeMap;

/// Render a label template like "{{ labels.service }}" against a field map.
pub fn render_template(template: &str, fields: &BTreeMap<String, String>) -> String {
    let mut result = template.to_string();
    // Replace {{ labels.KEY }} and {{ KEY }} patterns
    let re = regex::Regex::new(r"\{\{\s*(?:labels\.)?(\w[\w.]*)\s*\}\}")
        .unwrap_or_else(|_| regex::Regex::new(r"$^").unwrap_or_else(|_| unreachable!()));
    result = re
        .replace_all(&result, |caps: &regex::Captures| {
            let key = &caps[1];
            fields.get(key).cloned().unwrap_or_default()
        })
        .to_string();
    result
}
