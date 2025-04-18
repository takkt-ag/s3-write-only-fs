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

use crate::{
    id_generator::IdGenerator,
    upload::Upload,
};
use anyhow::{
    Context,
    Result,
};
use fuse::{
    FileAttr,
    FileType,
    Filesystem,
    ReplyAttr,
    ReplyCreate,
    ReplyData,
    ReplyDirectory,
    ReplyEmpty,
    ReplyEntry,
    ReplyOpen,
    ReplyWrite,
    Request,
};
use libc::{
    EACCES,
    EIO,
    ENOENT,
};
use rusoto_s3::S3Client;
use slog_scope::{
    debug,
    error,
    info,
    trace,
};
use std::{
    collections::HashMap,
    ffi::OsStr,
    ops::DerefMut,
    str::FromStr,
    sync::{
        Arc,
        Mutex,
    },
    time::{
        Duration,
        SystemTime,
    },
};
use tokio::runtime::Runtime;

const GENERATION: u64 = 0;
const TTL: Duration = Duration::from_secs(0);

const ROOT_DIRECTORY_INODE: u64 = 1;
const ROOT_DIRECTORY_TTL: Duration = Duration::from_secs(60);

const HELP_EN_INODE: u64 = 2;
const HELP_EN_NAME: &str = "_Uploaded files will not be visible.txt";
const HELP_EN_CONTENTS: &str = include_str!("../resources/help_en.txt");
const HELP_EN_FILEATTR: FileAttr = FileAttr {
    ino: HELP_EN_INODE,
    size: HELP_EN_CONTENTS.len() as u64,
    blocks: 1,
    atime: SystemTime::UNIX_EPOCH,
    mtime: SystemTime::UNIX_EPOCH,
    ctime: SystemTime::UNIX_EPOCH,
    crtime: SystemTime::UNIX_EPOCH,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 0,
    gid: 0,
    rdev: 0,
    flags: 0,
};
const HELP_DE_INODE: u64 = 3;
const HELP_DE_NAME: &str = "_Hochgeladene Dateien werden nicht sichtbar sein.txt";
const HELP_DE_CONTENTS: &str = include_str!("../resources/help_de.txt");
const HELP_DE_FILEATTR: FileAttr = FileAttr {
    ino: HELP_DE_INODE,
    size: HELP_DE_CONTENTS.len() as u64,
    blocks: 1,
    atime: SystemTime::UNIX_EPOCH,
    mtime: SystemTime::UNIX_EPOCH,
    ctime: SystemTime::UNIX_EPOCH,
    crtime: SystemTime::UNIX_EPOCH,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 0,
    gid: 0,
    rdev: 0,
    flags: 0,
};

const STATIC_INODES: &[u64] = &[ROOT_DIRECTORY_INODE, HELP_EN_INODE, HELP_DE_INODE];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketAndPrefix {
    pub s3_bucket_name: String,
    pub prefix_path: Option<String>,
}

impl FromStr for BucketAndPrefix {
    type Err = anyhow::Error;

    fn from_str(device: &str) -> Result<Self, Self::Err> {
        if let Some(index) = device.find(':') {
            let prefix_path = device[index + 1..]
                .trim_start_matches('/')
                .trim_end_matches('/');
            let prefix_path = if prefix_path.is_empty() {
                None
            } else {
                Some(prefix_path.to_owned())
            };
            Ok(BucketAndPrefix {
                s3_bucket_name: device[..index].to_owned(),
                prefix_path,
            })
        } else {
            Ok(BucketAndPrefix {
                s3_bucket_name: device.to_owned(),
                prefix_path: None,
            })
        }
    }
}

#[test]
fn bucket_and_prefix_fromstr() {
    assert_eq!(
        "my-bucket".parse::<BucketAndPrefix>().unwrap(),
        BucketAndPrefix {
            s3_bucket_name: "my-bucket".to_owned(),
            prefix_path: None
        }
    );
    assert_eq!(
        "my-bucket:".parse::<BucketAndPrefix>().unwrap(),
        BucketAndPrefix {
            s3_bucket_name: "my-bucket".to_owned(),
            prefix_path: None,
        }
    );
    assert_eq!(
        "my-bucket:/".parse::<BucketAndPrefix>().unwrap(),
        BucketAndPrefix {
            s3_bucket_name: "my-bucket".to_owned(),
            prefix_path: None,
        }
    );
    assert_eq!(
        "my-bucket://".parse::<BucketAndPrefix>().unwrap(),
        BucketAndPrefix {
            s3_bucket_name: "my-bucket".to_owned(),
            prefix_path: None,
        }
    );
    assert_eq!(
        "my-bucket:/single-prefix"
            .parse::<BucketAndPrefix>()
            .unwrap(),
        BucketAndPrefix {
            s3_bucket_name: "my-bucket".to_owned(),
            prefix_path: Some("single-prefix".to_owned()),
        }
    );
    assert_eq!(
        "my-bucket://single-prefix/"
            .parse::<BucketAndPrefix>()
            .unwrap(),
        BucketAndPrefix {
            s3_bucket_name: "my-bucket".to_owned(),
            prefix_path: Some("single-prefix".to_owned()),
        }
    );
    assert_eq!(
        "my-bucket:/multi/prefix"
            .parse::<BucketAndPrefix>()
            .unwrap(),
        BucketAndPrefix {
            s3_bucket_name: "my-bucket".to_owned(),
            prefix_path: Some("multi/prefix".to_owned()),
        }
    );
    assert_eq!(
        "my-bucket:/multi//prefix/"
            .parse::<BucketAndPrefix>()
            .unwrap(),
        BucketAndPrefix {
            s3_bucket_name: "my-bucket".to_owned(),
            prefix_path: Some("multi//prefix".to_owned()),
        }
    );
}

struct Node {
    key: String,
    file_attr: FileAttr,
    upload: Mutex<Upload>,
}

impl Node {
    fn new(id: u64, bucket: &str, key: &str) -> Node {
        let now = SystemTime::now();
        Node {
            key: key.to_owned(),
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

    fn write(&mut self, runtime: &mut Runtime, s3: &S3Client, data: &[u8]) -> Result<()> {
        let mut upload = std::mem::take(&mut self.upload)
            .into_inner()
            .context("failed to lock node.upload")?;
        upload = upload.write(runtime, s3, data)?;
        let _ = std::mem::replace(&mut self.upload, Mutex::new(upload));

        Ok(())
    }

    fn finish(&mut self, runtime: &mut Runtime, s3: &S3Client) -> Result<()> {
        let upload = std::mem::take(&mut self.upload)
            .into_inner()
            .context("failed to lock node.upload")?;
        upload.finish(runtime, s3)?;

        Ok(())
    }

    fn destroy(&mut self, runtime: &mut Runtime, s3: &S3Client) -> Result<()> {
        let upload = std::mem::take(&mut self.upload)
            .into_inner()
            .context("failed to lock node.upload")?;
        upload.destroy(runtime, s3)?;

        Ok(())
    }
}

pub(crate) struct S3WriteOnlyFilesystem {
    root_directory_fileattr: FileAttr,

    id_generator: Arc<IdGenerator>,
    nodes: Arc<Mutex<HashMap<u64, Node>>>,

    s3: S3Client,
    s3_bucket: String,
    s3_prefix_path: Option<String>,
    runtime: Runtime,
}

impl S3WriteOnlyFilesystem {
    pub(crate) fn new(
        s3: S3Client,
        bucket_and_prefix: BucketAndPrefix,
    ) -> Result<S3WriteOnlyFilesystem> {
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
        let runtime = Runtime::new()?;

        Ok(S3WriteOnlyFilesystem {
            root_directory_fileattr,
            id_generator,
            nodes,
            s3,
            s3_bucket: bucket_and_prefix.s3_bucket_name,
            s3_prefix_path: bucket_and_prefix.prefix_path,
            runtime,
        })
    }
}

impl Drop for S3WriteOnlyFilesystem {
    fn drop(&mut self) {
        trace!("S3WriteOnlyFilesystem::drop()");
        match self.nodes.lock() {
            Ok(mut nodes) => {
                for node in nodes.values_mut() {
                    if let Err(error) = node.destroy(&mut self.runtime, &self.s3) {
                        error!("Failed to destroy node '{}'", node.key; "error" => %error);
                    }
                }
            }
            Err(error) => {
                error!("failed to acquire lock on filesystem nodes"; "error" => %error);
            }
        }
    }
}

impl Filesystem for S3WriteOnlyFilesystem {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        trace!("lookup(parent={}, name={:?})", parent, name);
        if parent != ROOT_DIRECTORY_INODE {
            reply.error(ENOENT);
            return;
        }

        if name == HELP_EN_NAME {
            reply.entry(&TTL, &HELP_EN_FILEATTR, GENERATION);
        } else if name == HELP_DE_NAME {
            reply.entry(&TTL, &HELP_DE_FILEATTR, GENERATION);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        trace!("getattr(ino={})", ino);
        match ino {
            ROOT_DIRECTORY_INODE => reply.attr(&ROOT_DIRECTORY_TTL, &self.root_directory_fileattr),
            HELP_EN_INODE => reply.attr(&ROOT_DIRECTORY_TTL, &HELP_EN_FILEATTR),
            HELP_DE_INODE => reply.attr(&ROOT_DIRECTORY_TTL, &HELP_DE_FILEATTR),
            _ => {
                match self.nodes.lock() {
                    Ok(nodes) => {
                        if let Some(node) = nodes.get(&ino) {
                            reply.attr(&TTL, &node.file_attr);
                            return;
                        }
                    }
                    Err(error) => {
                        error!("failed to acquire lock on filesystem nodes"; "error" => %error);
                    }
                }
                reply.error(ENOENT);
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
        trace!(
            "setattr(ino={}, mode={:?}, uid={:?}, gid={:?}, size={:?}, atime={:?}, mtime={:?}, fh={:?}, crtime={:?}, chgtime={:?}, bkuptime={:?}, flags={:?})",
            ino, _mode, _uid, _gid, _size, _atime, _mtime, _fh, _crtime, _chgtime, _bkuptime, _flags,
        );

        match self.nodes.lock() {
            Ok(nodes) => {
                if let Some(node) = nodes.get(&ino) {
                    reply.attr(&TTL, &node.file_attr);
                    return;
                }
            }
            Err(error) => {
                error!("failed to acquire lock on filesystem nodes"; "error" => %error);
            }
        }

        reply.error(ENOENT);
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        reply: ReplyEntry,
    ) {
        trace!(
            "mkdir(parent={}, name={:?}, mode={})",
            _parent,
            _name,
            _mode
        );
        reply.error(EACCES);
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: u32, reply: ReplyOpen) {
        trace!("open(ino={}, flags={})", ino, _flags);

        if ino == ROOT_DIRECTORY_INODE {
            reply.error(ENOENT);
            return;
        }

        // Open static file if requested
        if STATIC_INODES.contains(&ino) {
            reply.opened(ino, 0);
            return;
        }

        match self.nodes.lock() {
            Ok(nodes) => {
                if nodes.get(&ino).is_some() {
                    reply.opened(ino, 0);
                    return;
                }
            }
            Err(error) => {
                error!("failed to acquire lock on filesystem nodes"; "error" => %error);
            }
        }

        reply.error(ENOENT);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        let offset = offset as usize;
        let size = size as usize;

        trace!(
            "read(ino={}, fh={}, offset={}, size={})",
            ino,
            _fh,
            offset,
            size
        );
        let contents = match ino {
            HELP_EN_INODE => HELP_EN_CONTENTS,
            HELP_DE_INODE => HELP_DE_CONTENTS,
            _ => {
                reply.error(ENOENT);
                return;
            }
        }
        .as_bytes();

        // If we offset past the end of our contents, return no more data.
        if offset >= contents.len() {
            reply.data(&[]);
            return;
        }
        // Determine the end-offset such that we don't slice past the end of our content, i.e. don't
        // allow more data to be requested than is available.
        let end = std::cmp::min(contents.len(), offset + size);

        reply.data(&contents[offset..end]);
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
        trace!(
            "write(ino={}, fh={}, offset={}, len(data)={}, flags={})",
            ino,
            _fh,
            _offset,
            data.len(),
            _flags,
        );

        match self.nodes.lock() {
            Ok(mut nodes) => {
                if let Some(node) = nodes.deref_mut().get_mut(&ino) {
                    match node.write(&mut self.runtime, &self.s3, data) {
                        Ok(_) => {
                            trace!("written {} bytes to node for '{}'", data.len(), node.key);
                            reply.written(data.len() as u32);
                        }
                        Err(error) => {
                            error!("failed to write data to node"; "error" => %error);
                            reply.error(EIO);
                        }
                    }
                    return;
                }
            }
            Err(error) => {
                error!("failed to acquire lock on filesystem nodes"; "error" => %error);
            }
        }

        reply.error(ENOENT);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        trace!("flush(ino={}, fh={}, lock_owner={})", ino, _fh, _lock_owner);
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
        trace!(
            "release(ino={}, fh={}, flags={}, lock_owner={}, flush={})",
            ino,
            _fh,
            _flags,
            _lock_owner,
            _flush
        );

        if STATIC_INODES.contains(&ino) {
            reply.ok();
            return;
        }

        match self.nodes.lock() {
            Ok(mut nodes) => {
                if let Some(mut node) = nodes.remove(&ino) {
                    match node.finish(&mut self.runtime, &self.s3) {
                        Ok(_) => {
                            info!("Uploaded new file: {}", node.key);
                            reply.ok();
                        }
                        Err(error) => {
                            error!("failed to finalize node"; "error" => %error);
                            reply.error(EIO);
                        }
                    }
                    return;
                }
            }
            Err(error) => {
                error!("failed to acquire lock on filesystem nodes"; "error" => %error);
            }
        }

        reply.error(ENOENT);
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: u32, reply: ReplyOpen) {
        trace!("opendir(ino={}, flags={})", ino, _flags);

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
        trace!("readdir(ino={}, fh={}, offset={})", ino, _fh, offset);

        if ino != ROOT_DIRECTORY_INODE {
            reply.error(ENOENT);
            return;
        }

        if offset == 0 {
            reply.add(ROOT_DIRECTORY_INODE, 0, FileType::Directory, ".");
            reply.add(ROOT_DIRECTORY_INODE, 1, FileType::Directory, "..");
            reply.add(HELP_EN_INODE, 2, FileType::RegularFile, HELP_EN_NAME);
            reply.add(HELP_DE_INODE, 3, FileType::RegularFile, HELP_DE_NAME);
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
        trace!(
            "create(parent={}, name={:?}, mode={}, flags={})",
            parent,
            name,
            _mode,
            _flags
        );

        if parent != ROOT_DIRECTORY_INODE {
            reply.error(ENOENT);
            return;
        }

        match self.nodes.lock() {
            Ok(mut nodes) => {
                let id = self.id_generator.next();
                let mut filename = name.to_string_lossy().into_owned();
                if let Some(s3_prefix) = &self.s3_prefix_path {
                    filename = [s3_prefix, &*filename].join("/")
                };
                let node = Node::new(id, &self.s3_bucket, &filename);
                reply.created(&TTL, &node.file_attr, GENERATION, id, 0);

                debug!("Started new upload for file: {}", node.key);
                nodes.insert(id, node);
            }
            Err(error) => {
                error!("failed to acquire lock on filesystem nodes"; "error" => %error);
                reply.error(EACCES);
            }
        }
    }
}
