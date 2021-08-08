use mqs_cli::{parse_os_args, run_command, show_help, show_subcommand_help, ParsedArgs};
use std::process::exit;
use tokio::runtime::Builder;

fn main() {
    let code = match parse_os_args() {
        ParsedArgs::ShowHelp(error) => {
            show_help(error);

            1
        },
        ParsedArgs::ShowCommandHelp(error, cmd) => {
            show_subcommand_help(error, &cmd);

            1
        },
        ParsedArgs::RunCommand(host, port, trace_id, cmd) => {
            let rt = Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to create async runtime");
            rt.block_on(run_command(&host, port, trace_id, cmd))
        },
    };

    exit(code);
}
