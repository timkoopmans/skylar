use crate::app::App;
use scylla::Metrics;

impl App {
    pub fn update_metrics(&mut self, metrics: &Metrics) {
        let queries_num_rate = metrics.get_queries_num() - self.queries_num_prev;
        let queries_iter_num_rate = metrics.get_queries_iter_num() - self.queries_iter_num_prev;
        let errors_num_rate = metrics.get_errors_num() - self.errors_num_prev;
        let errors_iter_num_rate = metrics.get_errors_iter_num() - self.errors_iter_num_prev;

        self.queries_num_prev = metrics.get_queries_num();
        self.queries_iter_num_prev = metrics.get_queries_iter_num();
        self.errors_num_prev = metrics.get_errors_num();
        self.errors_iter_num_prev = metrics.get_errors_iter_num();

        self.queries_num.push(queries_num_rate);
        self.queries_iter_num.push(queries_iter_num_rate);
        self.errors_num.push(errors_num_rate);
        self.errors_iter_num.push(errors_iter_num_rate);
        self.latency_avg_ms
            .push(metrics.get_latency_avg_ms().unwrap_or(0));
        self.latency_percentile_ms
            .push(metrics.get_latency_percentile_ms(99.9).unwrap_or(0));

        self.trim_metrics();
    }

    fn trim_metrics(&mut self) {
        if self.queries_num.len() > 100 {
            self.queries_num.remove(0);
        }
        if self.queries_iter_num.len() > 100 {
            self.queries_iter_num.remove(0);
        }
        if self.errors_num.len() > 100 {
            self.errors_num.remove(0);
        }
        if self.errors_iter_num.len() > 100 {
            self.errors_iter_num.remove(0);
        }
        if self.latency_avg_ms.len() > 100 {
            self.latency_avg_ms.remove(0);
        }
        if self.latency_percentile_ms.len() > 100 {
            self.latency_percentile_ms.remove(0);
        }
    }
}
