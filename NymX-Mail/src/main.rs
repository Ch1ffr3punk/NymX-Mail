#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
mod config;
mod get;
mod send;
mod ssh;

use eframe::egui;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

enum TaskMessage {
    Log(String),
    SendDone,
    FetchDone,
}

struct NymXApp {
    nymx_config: config::NymXConfig,
    sorted_nymx_aliases: Vec<String>,
    selected_nymx_alias: String,
    message_text: String,
    log: String,
    is_sending: bool,
    is_fetching: bool,
    ssh_password: String,
    save_path: PathBuf,
    rx: Arc<Mutex<mpsc::UnboundedReceiver<TaskMessage>>>,
    tx: mpsc::UnboundedSender<TaskMessage>,
    runtime: Arc<tokio::runtime::Runtime>,
    dark_mode: bool,
    show_add_contact: bool,
    show_about: bool,
    show_delete_confirmation: bool,
    new_alias: String,
    new_address: String,
    contact_to_delete: String,
}

impl NymXApp {
    fn new() -> Self {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let (tx, rx) = mpsc::unbounded_channel();
        let nymx_config = config::NymXConfig::load();
        let mut sorted_nymx_aliases: Vec<String> = nymx_config.aliases.keys().cloned().collect();
        sorted_nymx_aliases.sort();
        let selected_nymx_alias = sorted_nymx_aliases.first().cloned().unwrap_or_default();
        let save_path = std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("inbox");
            
        Self {
            nymx_config,
            sorted_nymx_aliases,
            selected_nymx_alias,
            message_text: String::new(),
            log: String::from("NymX Mail ready.\n"),
            is_sending: false,
            is_fetching: false,
            ssh_password: String::new(),
            save_path,
            rx: Arc::new(Mutex::new(rx)),
            tx,
            runtime,
            dark_mode: true,
            show_add_contact: false,
            show_about: false,
            show_delete_confirmation: false,
            new_alias: String::new(),
            new_address: String::new(),
            contact_to_delete: String::new(),
        }
    }
}

fn load_icon_data() -> egui::IconData {
    let app_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."));
    let icon_paths = vec![
        app_dir.join("assets/icon.ico"),
        app_dir.join("icon.ico"),
        PathBuf::from("assets/icon.ico"),
    ];
    for icon_path in icon_paths {
        if let Ok(data) = std::fs::read(&icon_path) {
            if let Ok(img) = image::load_from_memory(&data) {
                let rgba = img.to_rgba8();
                let (width, height) = rgba.dimensions();
                return egui::IconData {
                    rgba: rgba.into_raw(),
                    width,
                    height,
                };
            }
        }
    }
    let data = include_bytes!("../assets/icon.png");
    let img = image::load_from_memory(data).expect("Failed to load icon");
    let rgba = img.to_rgba8();
    let (width, height) = rgba.dimensions();
    egui::IconData {
        rgba: rgba.into_raw(),
        width,
        height,
    }
}

impl eframe::App for NymXApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if let Ok(mut rx) = self.rx.lock() {
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    TaskMessage::Log(text) => {
                        self.log.push_str(&text);
                        self.log.push('\n');
                    }
                    TaskMessage::SendDone => {
                        self.is_sending = false;
                        self.log.push_str("[INFO] Send task finished.\n");
                    }
                    TaskMessage::FetchDone => {
                        self.is_fetching = false;
                        self.log.push_str("[INFO] Fetch task finished.\n");
                    }
                }
            }
        }

        if self.dark_mode {
            ctx.set_visuals(egui::Visuals::dark());
        } else {
            ctx.set_visuals(egui::Visuals::light());
        }

        let screen_rect = ctx.screen_rect();
        let center = screen_rect.center();

        if self.show_add_contact {
            egui::Window::new("Add Contact")
                .collapsible(false)
                .resizable(false)
                .fixed_size([420.0, 280.0])
                .fixed_pos([center.x - 210.0, center.y - 140.0])
                .show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(10.0);
                        ui.label(egui::RichText::new("Add a new contact").size(16.0).strong());
                        ui.add_space(15.0);
                        ui.horizontal(|ui| {
                            ui.label("Alias:");
                            ui.add(egui::TextEdit::singleline(&mut self.new_alias).desired_width(280.0));
                        });
                        ui.add_space(8.0);
                        ui.horizontal(|ui| {
                            ui.label("Nym Address:");
                            ui.add(egui::TextEdit::singleline(&mut self.new_address).desired_width(280.0).hint_text("Enter Nym address..."));
                        });
                        ui.add_space(15.0);
                        ui.horizontal_centered(|ui| {
                            let add_button = egui::Button::new(
                                egui::RichText::new("Add Contact").color(egui::Color32::WHITE)
                            ).fill(egui::Color32::from_rgb(122, 110, 243));
                            if ui.add(add_button).clicked() {
                                if !self.new_alias.trim().is_empty() && !self.new_address.trim().is_empty() {
                                    self.nymx_config.aliases.insert(self.new_alias.clone(), self.new_address.clone());
                                    if let Err(e) = self.nymx_config.save() {
                                        self.log.push_str(&format!("[ERROR] Failed to save config: {}\n", e));
                                    } else {
                                        self.log.push_str("[INFO] Config saved successfully.\n");
                                    }
                                    self.sorted_nymx_aliases = self.nymx_config.aliases.keys().cloned().collect();
                                    self.sorted_nymx_aliases.sort();
                                    self.selected_nymx_alias = self.new_alias.clone();
                                    self.log.push_str(&format!("[INFO] Added contact: {} -> {}\n", self.new_alias, self.new_address));
                                    self.new_alias.clear();
                                    self.new_address.clear();
                                    self.show_add_contact = false;
                                }
                            }
                            ui.add_space(20.0);
                            if ui.button("Cancel").clicked() {
                                self.new_alias.clear();
                                self.new_address.clear();
                                self.show_add_contact = false;
                            }
                        });
                    });
                });
        }

        if self.show_delete_confirmation && !self.contact_to_delete.is_empty() {
            egui::Window::new("Delete Contact")
                .collapsible(false)
                .resizable(false)
                .fixed_size([350.0, 160.0])
                .fixed_pos([center.x - 175.0, center.y - 80.0])
                .show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(15.0);
                        ui.label(egui::RichText::new(format!("Delete '{}'?", self.contact_to_delete)).size(16.0));
                        ui.add_space(8.0);
                        ui.label(egui::RichText::new("This action cannot be undone.").color(egui::Color32::from_rgb(200, 200, 200)));
                        ui.add_space(15.0);
                        ui.horizontal_centered(|ui| {
                            let delete_button = egui::Button::new(
                                egui::RichText::new("Delete Contact").color(egui::Color32::WHITE)
                            ).fill(egui::Color32::from_rgb(122, 110, 243));
                            if ui.add(delete_button).clicked() {
                                if self.nymx_config.aliases.remove(&self.contact_to_delete).is_some() {
                                    if let Err(e) = self.nymx_config.save() {
                                        self.log.push_str(&format!("[ERROR] Failed to save config: {}\n", e));
                                    } else {
                                        self.log.push_str("[INFO] Config saved successfully.\n");
                                    }
                                    self.sorted_nymx_aliases = self.nymx_config.aliases.keys().cloned().collect();
                                    self.sorted_nymx_aliases.sort();
                                    if self.selected_nymx_alias == self.contact_to_delete {
                                        self.selected_nymx_alias = self.sorted_nymx_aliases.first().cloned().unwrap_or_default();
                                    }
                                    self.log.push_str(&format!("[INFO] Deleted contact: {}\n", self.contact_to_delete));
                                }
                                self.contact_to_delete.clear();
                                self.show_delete_confirmation = false;
                            }
                            ui.add_space(20.0);
                            if ui.button("Cancel").clicked() {
                                self.contact_to_delete.clear();
                                self.show_delete_confirmation = false;
                            }
                        });
                    });
                });
        }

        if self.show_about {
            egui::Window::new("About NymX Mail")
                .collapsible(false)
                .resizable(false)
                .fixed_size([450.0, 320.0])
                .fixed_pos([center.x - 225.0, center.y - 160.0])
                .show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(15.0);
                        ui.label(egui::RichText::new("NymX Mail").size(28.0).strong());
                        ui.add_space(5.0);
                        ui.label(egui::RichText::new("A privacy-focused email client").size(16.0));
                        ui.add_space(15.0);
                        ui.label(egui::RichText::new("NymX Mail - Anonymous emails leveraging SURBs").size(14.0));
                        ui.label(egui::RichText::new("via the Nym Mixnet.").size(14.0));
                        ui.add_space(8.0);
                        ui.label(egui::RichText::new("v0.1.0 (c) 2026 Ch1ffr3punk").size(13.0));
                        ui.add_space(8.0);
                        ui.label(egui::RichText::new("Released under the Apache 2.0 License").size(13.0));
                        ui.add_space(5.0);
                        ui.label(" ");
                            let url = "https://github.com/Ch1ffr3punk/NymX-Mail";
                            if ui.hyperlink(url).clicked() {
                                let _ = open::that(url);
                            }
                        ui.horizontal_centered(|_ui| {
                        });
                            let close_button = egui::Button::new(
                            egui::RichText::new("Close").color(egui::Color32::WHITE)
                        ).fill(egui::Color32::from_rgb(122, 110, 243));
                        if ui.add(close_button).clicked() {
                            self.show_about = false;
                        }
                    });
                });
        }

        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(5.0);
                ui.horizontal(|ui| {
                    ui.label("To:");
                    egui::ComboBox::from_id_source("nymx_alias_combo")
                        .selected_text(if self.selected_nymx_alias.is_empty() { "Select alias..." } else { &self.selected_nymx_alias })
                        .show_ui(ui, |ui| {
                            for alias in &self.sorted_nymx_aliases {
                                ui.selectable_value(&mut self.selected_nymx_alias, alias.clone(), alias);
                            }
                        });
                    ui.add_space(8.0);
                    let add_button = egui::Button::new(
                        egui::RichText::new("Add Contact").color(egui::Color32::WHITE)
                    ).fill(egui::Color32::from_rgb(122, 110, 243));
                    if ui.add(add_button).clicked() {
                        self.show_add_contact = true;
                    }
                    ui.add_space(5.0);
                    let delete_enabled = !self.selected_nymx_alias.is_empty();
                    let delete_button = egui::Button::new(
                        egui::RichText::new("Delete Contact").color(egui::Color32::WHITE)
                    ).fill(egui::Color32::from_rgb(122, 110, 243));
                    if ui.add_enabled(delete_enabled, delete_button).clicked() {
                        self.contact_to_delete = self.selected_nymx_alias.clone();
                        self.show_delete_confirmation = true;
                    }
                    ui.add_space(15.0);
                    let about_button = egui::Button::new(
                        egui::RichText::new("About").color(egui::Color32::WHITE)
                    ).fill(egui::Color32::from_rgb(122, 110, 243));
                    if ui.add(about_button).clicked() {
                        self.show_about = true;
                    }
                    ui.add_space(5.0);
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let theme_text = if self.dark_mode { "Light Theme" } else { "Dark Theme" };
                        let theme_button = egui::Button::new(
                            egui::RichText::new(theme_text).color(egui::Color32::WHITE)
                        ).fill(egui::Color32::from_rgb(122, 110, 243));
                        if ui.add(theme_button).clicked() {
                            self.dark_mode = !self.dark_mode;
                        }
                    });
                });
                ui.add_space(5.0);
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.vertical(|ui| {
                ui.horizontal(|ui| {
                    let send_enabled = !self.is_sending 
                        && !self.selected_nymx_alias.is_empty() 
                        && !self.message_text.trim().is_empty();
                    let send_button = egui::Button::new(
                        egui::RichText::new("Send")
                            .color(egui::Color32::WHITE)
                    )
                    .fill(egui::Color32::from_rgb(122, 110, 243));
                    if ui.add_enabled(send_enabled, send_button).clicked() {
                        self.is_sending = true;
                        self.log.push_str("\n[INFO] Starting send process...\n");
                        let tx = self.tx.clone();
                        let nymx_address = self.nymx_config.resolve(&self.selected_nymx_alias).unwrap_or(self.selected_nymx_alias.clone());
                        let message = self.message_text.clone();
                        
                        self.runtime.spawn(async move {
                            if let Err(e) = send::send_text_message(nymx_address, message, tx.clone()).await {
                                let _ = tx.send(TaskMessage::Log(format!("[ERROR] Send failed: {}", e)));
                            }
                            let _ = tx.send(TaskMessage::SendDone);
                        });
                    }
                    let clear_button = egui::Button::new(
                        egui::RichText::new("Clear")
                            .color(egui::Color32::WHITE)
                    )
                    .fill(egui::Color32::from_rgb(122, 110, 243));
                    if ui.add_enabled(!self.is_sending, clear_button).clicked() {
                        self.message_text.clear();
                        self.ssh_password.clear();
                        self.log = String::from("NymX Mail ready.\n");
                        self.log.push_str("[INFO] All fields cleared.\n");
                    }
                });
                ui.add_space(10.0);
                let available_height = ui.available_height() - 230.0;
                let text_height = available_height.max(130.0);
                egui::ScrollArea::both()
                    .max_height(text_height)
                    .auto_shrink([false; 2])
                    .show(ui, |ui| {
                        ui.set_min_width(f32::INFINITY);
                        ui.add(
                            egui::TextEdit::multiline(&mut self.message_text)
                                .font(egui::TextStyle::Monospace)
                                .desired_width(f32::INFINITY)
                                .desired_rows(30)
                        );
                     });
                ui.add_space(10.0);
                ui.group(|ui| {
                    ui.label("Fetch Messages");
                    ui.horizontal(|ui| {
                        ui.label("Password:");
                        ui.add(egui::TextEdit::singleline(&mut self.ssh_password).password(true));
                    });
                    ui.add_space(5.0);
                    ui.horizontal(|ui| {
                        ui.label("Save to:");
                        let path_str = self.save_path.to_string_lossy().to_string();
                        ui.label(egui::RichText::new(&path_str).monospace());
                        if ui.button("Browse...").clicked() {
                            if let Some(folder) = rfd::FileDialog::new()
                                .set_title("Select folder to save received files")
                                .pick_folder()
                            {
                                self.save_path = folder.join("inbox");
                                self.log.push_str(&format!("[INFO] Save path changed to: {}\n", self.save_path.display()));
                            }
                        }
                    });
                    ui.add_space(5.0);
                    let fetch_button = egui::Button::new(
                        egui::RichText::new("Fetch Messages")
                            .color(egui::Color32::WHITE)
                    )
                    .fill(egui::Color32::from_rgb(122, 110, 243));
                    if ui.add_enabled(!self.is_fetching, fetch_button).clicked() {
                        self.is_fetching = true;
                        self.log.push_str("\n[INFO] Starting fetch process...\n");
                        let tx = self.tx.clone();
                        let password = self.ssh_password.clone();
                        let save_path = self.save_path.clone();
                        self.runtime.spawn(async move {
                            if let Err(e) = get::run_get_mode(&password, save_path, tx.clone()).await {
                                let _ = tx.send(TaskMessage::Log(format!("[ERROR] Fetch failed: {}", e)));
                            }
                            let _ = tx.send(TaskMessage::FetchDone);
                        });
                    }
                });
                ui.add_space(8.0);
                ui.group(|ui| {
                    ui.label("Log");
                    let log_height = (ui.available_height() - 80.0).max(110.0);
                    egui::ScrollArea::vertical()
                        .max_height(log_height)
                        .auto_shrink([false; 2])
                        .stick_to_bottom(true)
                        .show(ui, |ui| {
                            ui.label(&self.log);
                        });
                });
                ui.add_space(12.0);
                ui.vertical_centered(|ui| {
                    ui.label(
                        egui::RichText::new("")
                            .size(13.0)
                            .color(egui::Color32::from_gray(160))
                            .weak()
                    );
                });
                ui.add_space(8.0);
            });
        });
    }
}

#[cfg(target_os = "linux")]
fn apply_wsl_workarounds() {
    if let Ok(ver) = std::fs::read_to_string("/proc/version") {
        if ver.to_ascii_lowercase().contains("microsoft") {
            #[allow(unsafe_code)]
            unsafe {
                std::env::remove_var("WAYLAND_DISPLAY");
            }
        }
    }
}

fn main() -> Result<(), eframe::Error> {
    // WSL-Workaround anwenden (tut nichts auf Nicht-Linux-Systemen)
    #[cfg(target_os = "linux")]
    apply_wsl_workarounds();

    let icon_data = load_icon_data();
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([720.0, 720.0])
            .with_title("NymX Mail")
            .with_resizable(true)
            .with_icon(Arc::new(icon_data)),
        ..Default::default()
    };
    eframe::run_native(
        "NymX",
        options,
        Box::new(|_cc| Ok(Box::new(NymXApp::new()))),
    )
}
