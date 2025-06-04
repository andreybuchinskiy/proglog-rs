use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

#[cfg(target_os = "linux")]
pub fn get_file_path(file: &File) -> io::Result<PathBuf> {
    let fd = file.as_raw_fd();
    let path = format!("/proc/self/fd/{}", fd);
    let path_str = std::fs::read_link(&path)?;
    Ok(path_str)
}

#[cfg(not(target_os = "linux"))]
pub fn get_file_path(_file: &File) -> io::Result<PathBuf> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Not supported on this platform",
    ))
}
