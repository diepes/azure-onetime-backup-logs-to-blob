use std::process::Command;
use std::time::Instant;

pub fn run_command(command: &str) -> Result<(String, String, f64), String> {
    let start_time = Instant::now();
    let output = Command::new("sh")
        .arg("-c")
        .arg(command)
        .output()
        .map_err(|e| format!("Failed to execute command: {}", e))?;
    let end_time = Instant::now();
    let duration = end_time.duration_since(start_time).as_secs_f64();

    let stdout = String::from_utf8(output.stdout)
        .map_err(|e| format!("Invalid UTF-8 sequence: {}", e))?;
    let stderr = String::from_utf8(output.stderr)
        .map_err(|e| format!("Invalid UTF-8 sequence: {}", e))?;

    Ok((stdout, stderr, duration))
}
