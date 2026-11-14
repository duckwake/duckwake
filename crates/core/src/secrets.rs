//! Secret references in stored definitions: a password is kept in the lake as `env:NAME` or `file:/path`,
//! never as itself, and resolved by the process that connects, when it connects. Every worker that may run
//! the view must have the variable or file.
use anyhow::{bail, Context, Result};

/// Whether `v` is a reference this module resolves.
pub fn is_reference(v: &str) -> bool {
    v.starts_with("env:") || v.starts_with("file:")
}

/// Refuses a literal secret: `what` names the setting in the message.
pub fn check_reference(what: &str, v: &str) -> Result<()> {
    if !is_reference(v) || v == "env:" || v == "file:" {
        bail!("{what} must be a reference, env:NAME or file:/path, so the secret is never stored in the lake");
    }
    Ok(())
}

/// The secret behind `env:NAME` (that environment variable) or `file:/path` (the file's contents, without a
/// trailing newline).
pub fn resolve(what: &str, v: &str) -> Result<String> {
    if let Some(name) = v.strip_prefix("env:") {
        return std::env::var(name).with_context(|| format!("{what}: environment variable {name} is not set"));
    }
    if let Some(path) = v.strip_prefix("file:") {
        let s = std::fs::read_to_string(path).with_context(|| format!("{what}: reading {path}"))?;
        return Ok(s.trim_end_matches(['\n', '\r']).to_string());
    }
    bail!("{what} must be env:NAME or file:/path")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references() {
        assert!(check_reference("p", "hunter2").is_err());
        assert!(check_reference("p", "env:").is_err());
        assert!(check_reference("p", "env:KAFKA_PASSWORD").is_ok());
        let f = std::env::temp_dir().join(format!("duckwake-secret-{}", std::process::id()));
        std::fs::write(&f, "s3cret\n").unwrap();
        assert_eq!(resolve("p", &format!("file:{}", f.display())).unwrap(), "s3cret");
        std::fs::remove_file(&f).unwrap();
        assert!(resolve("p", "env:DUCKWAKE_SURELY_UNSET_VAR").is_err());
    }
}
