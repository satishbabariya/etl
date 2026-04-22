use clap::Parser;

#[derive(Parser)]
#[command(name = "platform", version, about = "ETL platform CLI")]
struct Cli {}

fn main() -> anyhow::Result<()> {
    let _cli = Cli::parse();
    println!("platform CLI (stub)");
    Ok(())
}
