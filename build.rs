fn main() {
    #[cfg(feature = "vdso")]
    {
        use build_vdso::*;

        println!("cargo:rerun-if-changed=../vqueue");

        let mut config = BuildConfig::new("../vqueue", "vqueue");
        config.out_dir = String::from("output");
        config.verbose = 2;
        build_vdso(&config);
    }
}
