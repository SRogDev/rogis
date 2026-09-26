//! rogis: a Redis-compatible cache server.
//!
//! Usage: rogis [--port 6379] [--dir ./data] [--save 60] [--appendonly yes|no]

use rogis::persist::{self, Aof, PersistCfg, PersistCtl};
use rogis::server::{self, PubHub};
use rogis::store::Store;
use std::path::PathBuf;
use std::sync::Arc;

fn print_usage() {
    println!("usage: rogis [--port 6379] [--dir ./data] [--save 60] [--appendonly yes|no]");
    println!();
    println!("  --port N        TCP port to listen on (default 6379)");
    println!("  --dir PATH      persistence directory (default ./data)");
    println!("  --save SECS     snapshot to disk every SECS when dirty; 0 disables (default 60)");
    println!("  --appendonly    yes|no: log every write to appendonly.rogb (default no)");
}

fn die(msg: &str) -> ! {
    eprintln!("rogis: {msg}");
    print_usage();
    std::process::exit(2);
}

fn get_arg(args: &[String], i: usize, flag: &str) -> String {
    args.get(i)
        .cloned()
        .unwrap_or_else(|| die(&format!("{flag} needs a value")))
}

fn parse_arg<T>(args: &[String], i: usize, flag: &str) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    let s = get_arg(args, i, flag);
    s.parse()
        .unwrap_or_else(|e| die(&format!("bad {flag} value '{s}': {e:?}")))
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut port: u16 = 6379;
    let mut dir = PathBuf::from("./data");
    let mut save_secs: u64 = 60;
    let mut appendonly = false;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--help" | "-h" => {
                print_usage();
                return;
            }
            "--port" => {
                i += 1;
                port = parse_arg(&args, i, "--port");
            }
            "--dir" => {
                i += 1;
                dir = PathBuf::from(get_arg(&args, i, "--dir"));
            }
            "--save" => {
                i += 1;
                save_secs = parse_arg(&args, i, "--save");
            }
            "--appendonly" => {
                i += 1;
                appendonly = match get_arg(&args, i, "--appendonly")
                    .to_ascii_lowercase()
                    .as_str()
                {
                    "yes" | "true" | "1" => true,
                    "no" | "false" | "0" => false,
                    other => die(&format!(
                        "bad --appendonly value '{other}' (expected yes|no)"
                    )),
                };
            }
            other => die(&format!("unknown argument: '{other}'")),
        }
        i += 1;
    }

    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("rogis: cannot create data dir {}: {e}", dir.display());
        std::process::exit(1);
    }
    let store = Store::new();
    if let Err(e) = persist::load(&store, &dir) {
        eprintln!(
            "rogis: failed to load persistence from {}: {e}",
            dir.display()
        );
        std::process::exit(1);
    }
    let aof = match Aof::open(&PersistCfg {
        dir: dir.clone(),
        save_secs,
        appendonly,
    }) {
        Ok(aof) => aof,
        Err(e) => {
            eprintln!("rogis: cannot open AOF: {e}");
            std::process::exit(1);
        }
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|e| {
            eprintln!("rogis: cannot start async runtime: {e}");
            std::process::exit(1);
        });
    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
            .await
            .unwrap_or_else(|e| {
                eprintln!("rogis: cannot bind 0.0.0.0:{port}: {e}");
                std::process::exit(1);
            });
        println!("rogis listening on 0.0.0.0:{port}");
        server::serve(
            listener,
            Arc::new(store),
            Arc::new(PubHub::new()),
            Arc::new(aof),
            PersistCtl { save_secs },
        )
        .await;
    });
}
