use std::env;
use std::fs;
use std::process::{self, Command};
use std::thread;
use std::time::Duration;

fn argument(name: &str) -> Option<String> {
    let mut args = env::args().skip(1);
    while let Some(value) = args.next() {
        if value == name {
            return args.next();
        }
    }
    None
}

fn main() {
    match argument("--mode").as_deref() {
        Some("exit") => process::exit(
            argument("--code")
                .and_then(|value| value.parse().ok())
                .unwrap_or(0),
        ),
        Some("sleep") => thread::sleep(Duration::from_millis(
            argument("--duration-ms")
                .and_then(|value| value.parse().ok())
                .unwrap_or(30_000),
        )),
        Some("tree-fail") => {
            let child_pid_path = argument("--child-pid-file").expect("--child-pid-file");
            let child = Command::new(env::current_exe().expect("current executable"))
                .args(["--mode", "sleep", "--duration-ms", "30000"])
                .spawn()
                .expect("spawn child");
            fs::write(child_pid_path, child.id().to_string()).expect("write child pid");
            process::exit(7);
        }
        Some(other) => panic!("unknown fixture mode {other}"),
        None => panic!("--mode is required"),
    }
}
