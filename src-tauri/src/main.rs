// Release 版本使用 Windows GUI 子系统，避免启动桌面应用时附带控制台黑框。
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    ai_gateway_lib::run();
}
