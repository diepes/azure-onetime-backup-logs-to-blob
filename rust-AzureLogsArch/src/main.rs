mod mod_run;
//#fn run() { mod_run::run_command }

fn main() {
    let command = "ls -l";
    match mod_run::run_command(command) {
        Ok((output, error, duration)) => {
            println!("##Output:");
            println!("{}", output);
            println!("##Error:");
            println!("{}", error);
            println!("##Command took {:.2} seconds to execute", duration);
        },
        Err(e) => println!("Error: {}", e),
    }
}
