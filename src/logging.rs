use crate::CliArgs;

pub(crate) fn log_init(elargs: &CliArgs) {
    if let Some(ref log_path) = elargs.log_path {
        log4rs::init_file(log_path, Default::default()).unwrap();
    } else {
        let stderr = log4rs::append::console::ConsoleAppender::builder()
            .target(log4rs::append::console::Target::Stderr)
            .encoder(Box::new(log4rs::encode::pattern::PatternEncoder::new("[{h({l})}]\t{m}{n}")))
            .build();

        let level = match elargs.verbose {
            0 => log::LevelFilter::Off,
            1 => log::LevelFilter::Warn,
            2 => log::LevelFilter::Info,
            3 => log::LevelFilter::Debug,
            _ => log::LevelFilter::Trace,
        };

        let config = log4rs::Config::builder()
            .appender(log4rs::config::Appender::builder().build("stderr", Box::new(stderr)))
            .build(log4rs::config::Root::builder().appender("stderr").build(level))
            .unwrap(); // TODO FIXME

        let _ = log4rs::init_config(config).unwrap();
    }
}
