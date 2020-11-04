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

#![deny(unused_must_use)]

mod id_generator;
mod s3_write_only_filesystem;
mod upload;

use crate::s3_write_only_filesystem::S3WriteOnlyFilesystem;
use anyhow::Result;
use rusoto_core::Region;
use rusoto_s3::S3Client;
use std::{env, ffi::OsStr};

fn main() -> Result<()> {
    let s3 = S3Client::new(Region::EuCentral1);

    env_logger::init();
    let mountpoint = env::args_os().nth(1).unwrap();
    let s3_bucket = env::args_os().nth(2).unwrap();
    let options = ["-o", "fsname=hello", "-o", "uid=66671"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();
    let s3_write_only_filesystem = S3WriteOnlyFilesystem::new(s3, s3_bucket.into_string().unwrap())?;
    fuse::mount(s3_write_only_filesystem, mountpoint, &options).unwrap();

    Ok(())
}
