// Large React-parity Slint layouts are recursive to compile. The default
// Windows build-script main thread has a small stack; compile on a dedicated
// 128 MiB worker instead of overflowing the build-script process.
fn main() {
    std::thread::Builder::new()
        .name("native-slint-compiler".into())
        .stack_size(128 * 1024 * 1024)
        .spawn(|| {
            slint_build::compile("ui/main.slint")
                .expect("Unable to compile the Pokeidle Manager Slint interface");
        })
        .expect("Unable to start the native Slint compiler")
        .join()
        .expect("Native Slint compilation thread panicked");
}
