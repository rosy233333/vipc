fn main() {
    #[cfg(feature = "vdso")]
    {
        use build_vdso::*;

        let mut config = BuildConfig::new("../vqueue", "vqueue");
        config.out_dir = String::from("output");
        config.verbose = 2;
        build_vdso(&config);
    }
}
