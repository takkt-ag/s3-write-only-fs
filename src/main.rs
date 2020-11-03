// Copyright 2025 TAKKT Industrial & Packaging GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    env,
    ffi::OsStr,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};
use libc::{EACCES, ENOENT};
use rusoto_core::Region;
use rusoto_s3::{PutObjectRequest, S3Client, S3};
use tokio::runtime::Runtime;

const GENERATION: u64 = 0;
const TTL: Duration = Duration::from_secs(0);

const ROOT_DIRECTORY_INODE: u64 = 1;
const ROOT_DIRECTORY_TTL: Duration = Duration::from_secs(60);

struct IdGenerator(AtomicU64);

impl IdGenerator {
    fn new(start: u64) -> Self {
        IdGenerator(AtomicU64::new(start))
    }

    fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst)
    }
}

struct Node {
    filename: String,
    file_attr: FileAttr,
    contents: Mutex<Vec<u8>>,
}

struct S3WriteOnlyFilesystem {
    uid: u32,
    gid: u32,
    time_mounted: SystemTime,

    id_generator: Arc<IdGenerator>,
    nodes: Arc<Mutex<HashMap<u64, Node>>>,

    s3: S3Client,
    runtime: Runtime,
}

impl S3WriteOnlyFilesystem {
    fn new(s3: S3Client) -> S3WriteOnlyFilesystem {
        S3WriteOnlyFilesystem {
            uid: 0,
            gid: 0,
            time_mounted: SystemTime::now(),
            id_generator: Arc::new(IdGenerator::new(10)),
            nodes: Arc::new(Mutex::new(HashMap::new())),
            s3,
            runtime: Runtime::new().expect("failed to create runtime"),
        }
    }

    fn root_directory_attributes(&self) -> FileAttr {
        FileAttr {
            ino: ROOT_DIRECTORY_INODE,
            size: 0,
            blocks: 0,
            atime: self.time_mounted,
            mtime: self.time_mounted,
            ctime: self.time_mounted,
            crtime: self.time_mounted,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
        }
    }
}

impl Filesystem for S3WriteOnlyFilesystem {
    fn lookup(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEntry) {
        eprintln!("lookup(parent={}, name={:?})", _parent, _name);
        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        match ino {
            ROOT_DIRECTORY_INODE => {
                reply.attr(&ROOT_DIRECTORY_TTL, &self.root_directory_attributes())
            }
            _ => {
                eprintln!("getattr(ino={})", ino);
                let nodes = self.nodes.lock().expect("failed to get nodes");
                if let Some(node) = nodes.get(&ino) {
                    reply.attr(&TTL, &node.file_attr);
                } else {
                    reply.error(ENOENT);
                }
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<SystemTime>,
        _mtime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let nodes = self.nodes.lock().expect("failed to get nodes");
        match nodes.get(&ino) {
            None => {
                reply.error(ENOENT);
            }
            Some(node) => {
                reply.attr(&TTL, &node.file_attr);
            }
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        reply: ReplyEntry,
    ) {
        reply.error(EACCES);
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: u32, reply: ReplyOpen) {
        let nodes = self.nodes.lock().expect("failed to get nodes");
        match nodes.get(&ino) {
            None => {
                reply.error(ENOENT);
            }
            Some(_) => {
                reply.opened(ino, 0);
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        eprintln!(
            "write(ino={}, fh={}, offset={}, len(data)={}, flags={})",
            ino,
            _fh,
            _offset,
            data.len(),
            _flags
        );
        let nodes = self.nodes.lock().expect("failed to get nodes");
        match nodes.get(&ino) {
            None => {
                reply.error(ENOENT);
            }
            Some(node) => {
                node.contents
                    .lock()
                    .expect("failed to get node contents")
                    .extend_from_slice(data);
                reply.written(data.len() as u32);
            }
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        eprintln!("flush(ino={}, fh={}, lock_owner={})", ino, _fh, _lock_owner,);
        reply.ok();
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        eprintln!(
            "release(ino={}, fh={}, flags={}, lock_owner={}, flush={})",
            ino, _fh, _flags, _lock_owner, _flush
        );
        let mut nodes = self.nodes.lock().expect("failed to get nodes");
        match nodes.remove(&ino) {
            None => {
                reply.error(ENOENT);
            }
            Some(node) => {
                let contents = node.contents.lock().expect("failed to get node contents");
                self.runtime
                    .block_on(self.s3.put_object(PutObjectRequest {
                        bucket: "example".to_string(),
                        key: node.filename.clone(),
                        body: Some((*contents).clone().into()),
                        ..Default::default()
                    }))
                    .expect("failed to upload file");
                reply.ok();
            }
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: u32, reply: ReplyOpen) {
        if ino == ROOT_DIRECTORY_INODE {
            reply.opened(ROOT_DIRECTORY_INODE, 0);
        } else {
            reply.error(EACCES);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != ROOT_DIRECTORY_INODE {
            reply.error(ENOENT);
            return;
        }

        if offset == 0 {
            reply.add(ROOT_DIRECTORY_INODE, 0, FileType::Directory, ".");
            reply.add(ROOT_DIRECTORY_INODE, 1, FileType::Directory, "..");
        }
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _flags: u32,
        reply: ReplyCreate,
    ) {
        if parent != ROOT_DIRECTORY_INODE {
            reply.error(ENOENT);
            return;
        }
        eprintln!("create(name={:?})", name);

        let filename = name.to_string_lossy().into_owned();

        let id = self.id_generator.next();
        let now = SystemTime::now();
        let node = Node {
            filename,
            file_attr: FileAttr {
                ino: id,
                size: 0,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: FileType::RegularFile,
                perm: 0o220,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
            },
            contents: Mutex::new(vec![]),
        };
        let mut x = self.nodes.lock().expect("failed to get nodes");
        reply.created(&TTL, &node.file_attr, GENERATION, id, 0);
        x.insert(id, node);
    }
}

fn main() {
    let s3 = S3Client::new(Region::EuCentral1);

    env_logger::init();
    let mountpoint = env::args_os().nth(1).unwrap();
    let options = ["-o", "fsname=hello", "-o", "uid=66671"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();
    let s3_write_only_filesystem = S3WriteOnlyFilesystem::new(s3);
    fuse::mount(s3_write_only_filesystem, mountpoint, &options).unwrap();
}
