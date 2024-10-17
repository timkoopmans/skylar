use crate::app::App;
use std::sync::{Arc, Mutex};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

pub fn initialize_system() -> Arc<Mutex<System>> {
    Arc::new(Mutex::new(System::new_with_specifics(
        RefreshKind::new()
            .with_cpu(CpuRefreshKind::new())
            .with_memory(MemoryRefreshKind::new()),
    )))
}

impl App {
    pub fn update_system(&mut self) {
        let mut system = self.system.lock().unwrap();
        system.refresh_cpu_all();
        system.refresh_memory();
        self.cpu_usage = system.global_cpu_usage();
        self.memory_usage = system.used_memory() as f32 / system.total_memory() as f32 * 100.0;
    }
}
