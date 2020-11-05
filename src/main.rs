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
use clap::{crate_authors, crate_description, crate_version, Clap};
use rusoto_core::Region;
use rusoto_s3::S3Client;
use slog::{o, Drain};
use slog_scope::{debug, error, info};
use std::{env, ffi::OsString};

#[derive(Debug, Clap)]
#[clap(
    author = crate_authors!(),
    version = crate_version!(),
    about = crate_description!(),
)]
struct Opts {
    /// S3 bucket name to mount the write-only filesystem against.
    s3_bucket_name: String,
    /// Mountpoint to mount the filesystem to.
    mountpoint: OsString,
    /// Don't daemonize, i.e. continue to run in the foreground
    #[clap(long = "foreground")]
    foreground: bool,
    /// Tolerate sloppy mount options, i.e. do not fail if unknown options were passed.
    #[clap(hidden = true, short = 's')]
    tolerate_sloppy_mount_options: bool,
    /// Don't actually mount the filesystem.
    #[clap(hidden = true, short = 'f')]
    fake: bool,
    /// Don't update /etc/mtab.
    #[clap(hidden = true, short = 'n')]
    dont_write_mtab: bool,
    /// Enable verbose output
    #[clap(hidden = true, short = 'v')]
    verbose: bool,
    /// The filesystem type to mount.
    #[clap(hidden = true, short = 't')]
    filesystem_type: Option<OsString>,
    /// Filesystem options, comma-separated.
    #[clap(short = 'o', value_delimiter = ",", use_delimiter = true)]
    options: Vec<OsString>,
}

fn main() -> Result<()> {
    // Parse command-line arguments
    let opts = Opts::parse();

    // Setup logging
    // Setup terminal logger
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    // Create the root slog-logger.
    let logger = slog::Logger::root(drain, o!());
    // Setup bridge between `log` and `slog`.
    slog_stdlog::init_with_level(log::Level::Info).expect("failed to setup logging");
    // Apply the root logger to the global scope.
    let _global_logger_guard = slog_scope::set_global_logger(logger.clone());

    info!("Starting application";
          "version" => env!("CARGO_PKG_VERSION"));

    debug!("Creating S3 client");
    let s3 = S3Client::new(Region::EuCentral1);

    let options = mount_options(&opts);
    let options_ref = options.iter().map(OsString::as_ref).collect::<Vec<_>>();

    let s3_bucket = opts.s3_bucket_name;
    let mountpoint = opts.mountpoint;

    if opts.foreground {
        debug!("Staying in foreground");
        debug!("Creating S3 write-only filesystem");
        let s3_write_only_filesystem = S3WriteOnlyFilesystem::new(s3, s3_bucket)?;
        fuse::mount(s3_write_only_filesystem, mountpoint, &options_ref).unwrap();
    } else {
        info!(
            "Foreground execution not requested, this process will daemonize now! This means that \
             it will continue to run in the background, serving the write-only filesystem under \
             the requested mountpoint."
        );
        match daemonize::Daemonize::new()
            .working_directory(std::env::current_dir()?)
            .start()
        {
            Ok(_) => {
                // Reconfigure logging to use journald
                let logger = slog::Logger::root(slog_journald::JournaldDrain.ignore_res(), o!());
                // Apply the root logger to the global scope.
                let _global_logger_guard = slog_scope::set_global_logger(logger.clone());

                debug!("Daemonized into background successfully");
                debug!("Creating S3 write-only filesystem");
                let s3_write_only_filesystem = S3WriteOnlyFilesystem::new(s3, s3_bucket)?;
                fuse::mount(s3_write_only_filesystem, mountpoint, &options_ref).unwrap();
            }
            Err(error) => {
                error!("Failed to daemonize, the filesystem will not be available";
                       "error" => %error);
            }
        }
    }

    Ok(())
}

fn mount_options(opts: &Opts) -> Vec<OsString> {
    let mut options: Vec<OsString> = vec![];
    if opts.tolerate_sloppy_mount_options {
        options.push("-s".into());
    }
    if opts.fake {
        options.push("-m".into());
    }
    if opts.dont_write_mtab {
        options.push("-n".into());
    }
    if opts.verbose {
        options.push("-v".into());
    }
    options.extend_from_slice(&[
        "-o".into(),
        format!("fsname={}", opts.s3_bucket_name).into(),
        "-o".into(),
        "subtype=s3wofs".into(),
    ]);
    for option in &opts.options {
        options.extend_from_slice(&["-o".into(), option.to_owned()]);
    }

    options
}
