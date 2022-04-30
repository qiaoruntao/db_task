#[cfg(test)]
pub mod tests {
    use tracing::subscriber::set_global_default;
    use tracing_log::LogTracer;
    use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, Registry};
    use tracing_subscriber::fmt::time::LocalTime;

    pub(crate) fn init_logger() {
        if let Err(e) = LogTracer::init() {
            dbg!(e);
            return;
        }

        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("error,db_task=debug"));
        let fmt_layer = fmt::Layer::default()
            .with_timer(LocalTime::rfc_3339())
            .with_writer(std::io::stdout);
        let subscriber = Registry::default()
            .with(env_filter)
            .with(fmt_layer);
        set_global_default(subscriber).expect("Failed to set subscriber");
    }
}
