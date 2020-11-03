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

use crate::{id_generator::IdGenerator, upload::Upload};
use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};
use libc::{EACCES, ENOENT};
use rusoto_s3::S3Client;
use std::{
    collections::HashMap,
    ffi::OsStr,
    ops::DerefMut,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::runtime::Runtime;

const GENERATION: u64 = 0;
const TTL: Duration = Duration::from_secs(0);

const ROOT_DIRECTORY_INODE: u64 = 1;
const ROOT_DIRECTORY_TTL: Duration = Duration::from_secs(60);

struct Node {
    file_attr: FileAttr,
    upload: Mutex<Upload>,
}

impl Node {
    fn new(id: u64, bucket: &str, key: &str) -> Node {
        let now = SystemTime::now();
        Node {
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
            upload: Mutex::new(Upload::new(bucket, key)),
        }
    }
}

pub(crate) struct S3WriteOnlyFilesystem {
    root_directory_fileattr: FileAttr,

    id_generator: Arc<IdGenerator>,
    nodes: Arc<Mutex<HashMap<u64, Node>>>,

    s3: S3Client,
    s3_bucket: String,
    runtime: Runtime,
}

impl S3WriteOnlyFilesystem {
    pub(crate) fn new(s3: S3Client, s3_bucket: String) -> S3WriteOnlyFilesystem {
        let now = SystemTime::now();
        let root_directory_fileattr = FileAttr {
            ino: ROOT_DIRECTORY_INODE,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
        };

        let id_generator = Arc::new(IdGenerator::new(10));
        let nodes = Arc::new(Mutex::new(HashMap::new()));
        let runtime = Runtime::new().expect("failed to create runtime");

        S3WriteOnlyFilesystem {
            root_directory_fileattr,
            id_generator,
            nodes,
            s3,
            s3_bucket,
            runtime,
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
            ROOT_DIRECTORY_INODE => reply.attr(&ROOT_DIRECTORY_TTL, &self.root_directory_fileattr),
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
        if let Some(node) = nodes.get(&ino) {
            reply.attr(&TTL, &node.file_attr);
        } else {
            reply.error(ENOENT);
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
        if let Some(_) = nodes.get(&ino) {
            reply.opened(ino, 0);
        } else {
            reply.error(ENOENT);
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
        // eprintln!(
        //     "write(ino={}, fh={}, offset={}, len(data)={}, flags={})",
        //     ino,
        //     _fh,
        //     _offset,
        //     data.len(),
        //     _flags
        // );
        let mut nodes = self.nodes.lock().expect("failed to get nodes");
        if let Some(node) = nodes.deref_mut().get_mut(&ino) {
            let mut upload = std::mem::take(&mut node.upload)
                .into_inner()
                .expect("failed to get node upload");
            upload = upload.write(&mut self.runtime, &self.s3, data);
            let _ = std::mem::replace(&mut node.upload, Mutex::new(upload));
            reply.written(data.len() as u32);
        } else {
            reply.error(ENOENT);
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
        if let Some(mut node) = nodes.remove(&ino) {
            let upload = std::mem::take(&mut node.upload)
                .into_inner()
                .expect("failed to get node upload");
            upload.finish(&mut self.runtime, &self.s3);
            reply.ok();
        } else {
            reply.error(ENOENT);
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

        let id = self.id_generator.next();
        let filename = name.to_string_lossy().into_owned();
        let node = Node::new(id, &self.s3_bucket, &filename);

        reply.created(&TTL, &node.file_attr, GENERATION, id, 0);

        let mut nodes = self.nodes.lock().expect("failed to get nodes");
        nodes.insert(id, node);
    }
}
