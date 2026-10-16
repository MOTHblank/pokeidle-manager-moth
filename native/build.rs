fn main() {
    slint_build::compile("ui/main.slint")
        .expect("Unable to compile the Pokeidle Manager Slint interface");
}
