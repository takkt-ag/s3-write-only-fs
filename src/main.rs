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
use slog::{o, Drain};
use slog_scope::{debug, info};
use std::{env, ffi::OsStr, sync::Mutex};

fn main() -> Result<()> {
    // Setup logging
    // Create a JSON-drain, i.e. a drain that will print out the structured log-message as a JSON-object.
    let json_drain = Mutex::new(slog_json::Json::default(std::io::stdout())).map(slog::Fuse);
    // Create the root slog-logger.
    let logger = slog::Logger::root(json_drain, o!());
    // Setup bridge between `log` and `slog`.
    slog_stdlog::init_with_level(log::Level::Info).expect("failed to setup logging");
    // Apply the root logger to the global scope.
    let _global_logger_guard = slog_scope::set_global_logger(logger.clone());

    info!("Starting application";
          "version" => env!("CARGO_PKG_VERSION"));

    debug!("Creating S3 client");
    let s3 = S3Client::new(Region::EuCentral1);

    let mountpoint = env::args_os().nth(1).unwrap();
    let s3_bucket = env::args_os().nth(2).unwrap();
    let options = ["-o", "fsname=hello", "-o", "uid=66671"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();

    debug!("Creating S3 write-only filesystem");
    let s3_write_only_filesystem = S3WriteOnlyFilesystem::new(s3, s3_bucket.into_string().unwrap())?;
    fuse::mount(s3_write_only_filesystem, mountpoint, &options).unwrap();

    Ok(())
}
