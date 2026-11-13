//! Snapshot memory served through userfaultfd, with the restore's working set prefaulted.
//!
//! A freshly restored microVM spends most of its first second faulting pages in from the
//! snapshot's memory file one at a time. Instead, Firecracker hands its guest memory to a
//! handler here (`backend_type: "Uffd"`), which copies in the pages restores of this snapshot
//! are known to touch before the VM resumes, and serves any other page on demand.
//!
//! The working set is recorded by the first restore of each snapshot: every page it faults in
//! during [`RECORD_FOR`] is saved next to the snapshot, so it covers the guest agent, the
//! executor and the workload's first steps after a restore.

use std::{
    collections::BTreeSet,
    fs::File,
    io::IoSliceMut,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    sync::{OnceLock, mpsc},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// File, next to a snapshot, listing the memory-file offsets its restores touch.
pub const WORKING_SET: &str = "working-set.json";
/// How long the recording restore keeps collecting touched pages.
const RECORD_FOR: Duration = Duration::from_secs(15);
const PAGE: u64 = 4096;
const UFFDIO_COPY: libc::c_ulong = 0xC028_AA03;
const UFFD_EVENT_PAGEFAULT: u8 = 0x12;

/// Whether restores use this handler. Firecracker (running as this user) needs
/// `/dev/userfaultfd`; without it, or with `SCRAPERACK_NODE_PREFAULT=0`, restores map the
/// memory file directly, as before.
pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let wanted = std::env::var("SCRAPERACK_NODE_PREFAULT").map_or(true, |value| value != "0");
        let usable = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/userfaultfd")
            .is_ok();
        if wanted && !usable {
            tracing::warn!("/dev/userfaultfd is not usable; restoring without memory prefault");
        }
        wanted && usable
    })
}

#[derive(Deserialize)]
struct Region {
    base_host_virt_addr: u64,
    size: u64,
    offset: u64,
}

#[repr(C)]
struct UffdioCopy {
    dst: u64,
    src: u64,
    len: u64,
    mode: u64,
    copy: i64,
}

/// Outcome of the prefault, reported once the VM may be resumed.
pub struct Prefaulted {
    pub pages: usize,
    pub elapsed: Duration,
    pub recording: bool,
}

/// Start serving a VM's memory. Returns the socket path to give Firecracker as
/// `backend_path`, and a channel that reports when the working set is in.
pub fn serve(
    directory: &Path,
    snapshot_dir: &Path,
    memory_file: &Path,
    firecracker_pid: u32,
) -> Result<(PathBuf, mpsc::Receiver<Result<Prefaulted>>)> {
    let socket = directory.join("uffd.sock");
    let listener = UnixListener::bind(&socket).context("bind userfaultfd socket")?;
    listener.set_nonblocking(true)?;
    let memory = File::open(memory_file).context("open snapshot memory")?;
    let working_set = snapshot_dir.join(WORKING_SET);
    let (ready, report) = mpsc::channel();
    thread::Builder::new().name("uffd".into()).spawn(move || {
        let handler = match Handler::accept(listener, memory, working_set) {
            Ok(handler) => handler,
            Err(error) => {
                let _ = ready.send(Err(error));
                return;
            }
        };
        handler.run(ready, firecracker_pid);
    })?;
    Ok((socket, report))
}

struct Handler {
    uffd: OwnedFd,
    regions: Vec<Region>,
    source: *const u8,
    source_len: usize,
    working_set: PathBuf,
}

// The source mapping is read-only and lives as long as the handler's thread.
unsafe impl Send for Handler {}

impl Handler {
    fn accept(listener: UnixListener, memory: File, working_set: PathBuf) -> Result<Self> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() > deadline {
                        bail!("firecracker did not connect to the userfaultfd socket");
                    }
                    thread::sleep(Duration::from_millis(2));
                }
                Err(error) => return Err(error.into()),
            }
        };
        stream.set_nonblocking(false)?;
        let (body, uffd) = receive_fd(&stream)?;
        let regions: Vec<Region> =
            serde_json::from_slice(&body).context("invalid guest memory regions")?;
        let source_len = memory.metadata()?.len() as usize;
        // SAFETY: a fresh read-only private mapping of the whole snapshot memory file.
        let source = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                source_len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                memory.as_raw_fd(),
                0,
            )
        };
        if source == libc::MAP_FAILED {
            bail!("map snapshot memory: {}", std::io::Error::last_os_error());
        }
        Ok(Self {
            uffd,
            regions,
            source: source.cast(),
            source_len,
            working_set,
        })
    }

    fn run(self, ready: mpsc::Sender<Result<Prefaulted>>, firecracker_pid: u32) {
        let started = Instant::now();
        let recorded = std::fs::read(&self.working_set)
            .ok()
            .and_then(|data| serde_json::from_slice::<Vec<u64>>(&data).ok());
        let recording = recorded.is_none();
        let pages = recorded
            .as_ref()
            .map_or(0, |offsets| self.prefault(offsets));
        let _ = ready.send(Ok(Prefaulted {
            pages,
            elapsed: started.elapsed(),
            recording,
        }));
        let mut touched = BTreeSet::new();
        let record_until = Instant::now() + RECORD_FOR;
        let mut saved = !recording;
        loop {
            if !saved && Instant::now() > record_until {
                self.save(&touched);
                saved = true;
            }
            let mut poll = libc::pollfd {
                fd: self.uffd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one valid pollfd.
            let ready = unsafe { libc::poll(&mut poll, 1, 500) };
            if ready == 0 {
                // SAFETY: signal 0 only checks that the process exists.
                if unsafe { libc::kill(firecracker_pid as i32, 0) } != 0 {
                    break;
                }
                continue;
            }
            if ready < 0 || poll.revents & (libc::POLLERR | libc::POLLHUP) != 0 {
                break;
            }
            let mut message = [0u8; 32];
            // SAFETY: reading one uffd_msg into a buffer of its size.
            let read = unsafe {
                libc::read(
                    self.uffd.as_raw_fd(),
                    message.as_mut_ptr().cast(),
                    message.len(),
                )
            };
            if read < 0 {
                if std::io::Error::last_os_error().raw_os_error() == Some(libc::EAGAIN) {
                    continue;
                }
                break;
            }
            if read != 32 {
                break;
            }
            if message[0] != UFFD_EVENT_PAGEFAULT {
                continue;
            }
            let address =
                u64::from_ne_bytes(message[16..24].try_into().expect("8 bytes")) & !(PAGE - 1);
            if let Some(offset) = self.file_offset(address) {
                if self.copy(address, offset, PAGE).is_err() {
                    break;
                }
                if !saved {
                    touched.insert(offset);
                }
            }
        }
        if !saved {
            self.save(&touched);
        }
    }

    /// Copy the recorded pages in, merged into contiguous runs.
    fn prefault(&self, offsets: &[u64]) -> usize {
        let mut copied = 0;
        let mut index = 0;
        while index < offsets.len() {
            let start = offsets[index];
            let mut end = start + PAGE;
            index += 1;
            while index < offsets.len() && offsets[index] == end {
                end += PAGE;
                index += 1;
            }
            let Some(address) = self.guest_address(start) else {
                continue;
            };
            if self.guest_address(end - PAGE) != Some(address + (end - start - PAGE)) {
                // The run crosses a region boundary: copy it page by page.
                for offset in (start..end).step_by(PAGE as usize) {
                    if let Some(address) = self.guest_address(offset) {
                        copied += usize::from(self.copy(address, offset, PAGE).is_ok());
                    }
                }
                continue;
            }
            if self.copy(address, start, end - start).is_ok() {
                copied += ((end - start) / PAGE) as usize;
            }
        }
        copied
    }

    fn copy(&self, address: u64, offset: u64, len: u64) -> Result<()> {
        if offset + len > self.source_len as u64 {
            bail!("page outside the snapshot memory");
        }
        let mut request = UffdioCopy {
            dst: address,
            src: self.source as u64 + offset,
            len,
            mode: 0,
            copy: 0,
        };
        loop {
            // SAFETY: UFFDIO_COPY with a valid argument; the source is our read-only mapping.
            let result = unsafe { libc::ioctl(self.uffd.as_raw_fd(), UFFDIO_COPY, &mut request) };
            if result == 0 {
                return Ok(());
            }
            let error = std::io::Error::last_os_error();
            match error.raw_os_error() {
                // Some pages were already in: copy the rest one by one.
                Some(libc::EEXIST) if len > PAGE => {
                    for step in (0..len).step_by(PAGE as usize) {
                        let _ = self.copy(address + step, offset + step, PAGE);
                    }
                    return Ok(());
                }
                Some(libc::EEXIST) => return Ok(()),
                // The address space changed under us; try again.
                Some(libc::EAGAIN) => {
                    request.copy = 0;
                    continue;
                }
                _ => return Err(error.into()),
            }
        }
    }

    fn file_offset(&self, address: u64) -> Option<u64> {
        self.regions.iter().find_map(|region| {
            (region.base_host_virt_addr..region.base_host_virt_addr + region.size)
                .contains(&address)
                .then(|| region.offset + (address - region.base_host_virt_addr))
        })
    }

    fn guest_address(&self, offset: u64) -> Option<u64> {
        self.regions.iter().find_map(|region| {
            (region.offset..region.offset + region.size)
                .contains(&offset)
                .then(|| region.base_host_virt_addr + (offset - region.offset))
        })
    }

    fn save(&self, touched: &BTreeSet<u64>) {
        if touched.is_empty() || self.working_set.exists() {
            return;
        }
        let temporary = self.working_set.with_extension("tmp");
        let offsets = touched.iter().copied().collect::<Vec<_>>();
        if serde_json::to_vec(&offsets)
            .ok()
            .and_then(|data| std::fs::write(&temporary, data).ok())
            .is_some()
        {
            let _ = std::fs::rename(&temporary, &self.working_set);
            tracing::info!(pages = offsets.len(), "recorded snapshot working set");
        }
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        // SAFETY: unmapping the mapping created in `accept`.
        unsafe { libc::munmap(self.source.cast_mut().cast(), self.source_len) };
    }
}

/// Receive Firecracker's message: the region list as JSON and the userfaultfd.
fn receive_fd(stream: &UnixStream) -> Result<(Vec<u8>, OwnedFd)> {
    let mut body = vec![0u8; 64 * 1024];
    let mut control = [0u8; 64];
    let mut iov = [IoSliceMut::new(&mut body)];
    // SAFETY: msghdr points at our buffers for the duration of recvmsg.
    let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
    header.msg_iov = iov.as_mut_ptr().cast();
    header.msg_iovlen = 1;
    header.msg_control = control.as_mut_ptr().cast();
    header.msg_controllen = control.len() as _;
    // SAFETY: valid socket and header.
    let received = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut header, 0) };
    if received <= 0 {
        bail!("no message from firecracker on the userfaultfd socket");
    }
    // SAFETY: walking the control messages recvmsg filled in.
    let fd = unsafe {
        let message = libc::CMSG_FIRSTHDR(&header);
        if message.is_null()
            || (*message).cmsg_level != libc::SOL_SOCKET
            || (*message).cmsg_type != libc::SCM_RIGHTS
        {
            bail!("firecracker did not send a userfaultfd");
        }
        std::ptr::read_unaligned(libc::CMSG_DATA(message).cast::<RawFd>())
    };
    body.truncate(received as usize);
    // SAFETY: the kernel just gave us this descriptor.
    Ok((body, unsafe { OwnedFd::from_raw_fd(fd) }))
}
