use config::Config;

pub(crate) fn log_init(config: &Config) {
    if let Ok(log_path) = config.get_string("log_path") {
        log4rs::init_file(log_path, Default::default()).unwrap();
    } else {
        let stderr = log4rs::append::console::ConsoleAppender::builder()
            .target(log4rs::append::console::Target::Stderr)
            .encoder(Box::new(log4rs::encode::pattern::PatternEncoder::new("[{h({l})}]\t{m}{n}")))
            .build();

        let verbose = config.get_int("verbose").unwrap_or(2);
        let level = match verbose {
            0 => log::LevelFilter::Off,
            1 => log::LevelFilter::Warn,
            2 => log::LevelFilter::Info,
            3 => log::LevelFilter::Debug,
            _ => log::LevelFilter::Trace,
        };

        let config = log4rs::Config::builder()
            .appender(log4rs::config::Appender::builder().build("stderr", Box::new(stderr)))
            .build(log4rs::config::Root::builder().appender("stderr").build(level))
            .unwrap();

        let _ = log4rs::init_config(config).unwrap();
    }
}
