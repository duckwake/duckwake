use super::{Circuit, Operator};
use std::fmt::{self, Write};

impl fmt::Display for Circuit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (n, node) in self.nodes.iter().enumerate() {
            let inputs: Vec<String> = node
                .inputs
                .iter()
                .map(|p| if self.nodes[p.node].op.outputs() > 1 { format!("n{}.{}", p.node, p.port) } else { format!("n{}", p.node) })
                .collect();
            let mut line = format!("  n{n:<2} {}", node.op.name());
            if !inputs.is_empty() {
                let pad = 48usize.saturating_sub(line.chars().count()).max(1);
                let _ = write!(line, "{}← {}", " ".repeat(pad), inputs.join(", "));
            }
            let pad = 62usize.saturating_sub(line.chars().count()).max(2);
            let _ = write!(line, "{}{}", " ".repeat(pad), if node.props.append_only { "append-only" } else { "retracts" });
            writeln!(f, "{line}")?;
        }
        Ok(())
    }
}
