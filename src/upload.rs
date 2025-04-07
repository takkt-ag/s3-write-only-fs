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

use crate::id_generator::IdGenerator;
use anyhow::{
    anyhow,
    Result,
};
use rusoto_s3::{
    AbortMultipartUploadRequest,
    CompleteMultipartUploadRequest,
    CompletedMultipartUpload,
    CompletedPart,
    CreateMultipartUploadRequest,
    PutObjectRequest,
    S3Client,
    UploadPartRequest,
    S3,
};
use slog_scope::debug;
use std::sync::Arc;
use tokio::runtime::Runtime;

const MULTIPART_MINIMUM_PART_SIZE: usize = 5 * 1024 * 1024;

pub(crate) enum Upload {
    Empty,
    Regular {
        bucket: String,
        key: String,
        current_buffer: Vec<u8>,
    },
    Multipart {
        bucket: String,
        key: String,
        multipart_upload_id: String,
        multipart_part_number_generator: Arc<IdGenerator>,
        current_buffer: Vec<u8>,
        parts: Vec<CompletedPart>,
    },
}

impl Default for Upload {
    fn default() -> Self {
        Self::Empty
    }
}

impl Upload {
    pub(crate) fn new(bucket: &str, key: &str) -> Self {
        Upload::Regular {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            current_buffer: vec![],
        }
    }

    fn create_multipart_upload(
        runtime: &mut Runtime,
        s3: &S3Client,
        bucket: &str,
        key: &str,
    ) -> Result<String> {
        runtime
            .block_on(s3.create_multipart_upload(CreateMultipartUploadRequest {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                ..Default::default()
            }))?
            .upload_id
            .ok_or_else(|| anyhow!("upload id was unset after multipart upload was created"))
    }

    fn upload_part(
        runtime: &mut Runtime,
        s3: &S3Client,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
        body: Vec<u8>,
    ) -> Result<CompletedPart> {
        let e_tag = runtime
            .block_on(s3.upload_part(UploadPartRequest {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                upload_id: upload_id.to_owned(),
                body: Some(body.into()),
                part_number,
                ..Default::default()
            }))?
            .e_tag
            .ok_or_else(|| anyhow!("uploaded multipart did not return e-tag"))?;
        debug!("Uploaded multipart {} for '{}'", part_number, key);

        Ok(CompletedPart {
            e_tag: Some(e_tag),
            part_number: Some(part_number),
        })
    }

    pub(crate) fn write(self, runtime: &mut Runtime, s3: &S3Client, data: &[u8]) -> Result<Upload> {
        Ok(match self {
            Self::Regular {
                bucket,
                key,
                mut current_buffer,
            } => {
                current_buffer.extend_from_slice(data);
                if current_buffer.len() >= MULTIPART_MINIMUM_PART_SIZE {
                    debug!(
                        "Switching to multipart-upload for '{}', more than {} bytes written",
                        key, MULTIPART_MINIMUM_PART_SIZE
                    );
                    let multipart_part_number_generator = Arc::new(IdGenerator::new(1));
                    let multipart_upload_id: String =
                        Self::create_multipart_upload(runtime, s3, &bucket, &key)?;
                    let completed_part: CompletedPart = Self::upload_part(
                        runtime,
                        s3,
                        &bucket,
                        &key,
                        &multipart_upload_id,
                        multipart_part_number_generator.next() as i64,
                        current_buffer,
                    )?;
                    Self::Multipart {
                        bucket,
                        key,
                        multipart_upload_id,
                        multipart_part_number_generator,
                        current_buffer: vec![],
                        parts: vec![completed_part],
                    }
                } else {
                    Self::Regular {
                        bucket,
                        key,
                        current_buffer,
                    }
                }
            }
            Self::Multipart {
                bucket,
                key,
                multipart_upload_id,
                multipart_part_number_generator,
                mut current_buffer,
                mut parts,
            } => {
                current_buffer.extend_from_slice(data);
                if current_buffer.len() >= MULTIPART_MINIMUM_PART_SIZE {
                    let completed_part: CompletedPart = Self::upload_part(
                        runtime,
                        s3,
                        &bucket,
                        &key,
                        &multipart_upload_id,
                        multipart_part_number_generator.next() as i64,
                        current_buffer,
                    )?;
                    parts.push(completed_part);
                    current_buffer = vec![];
                }
                Self::Multipart {
                    bucket,
                    key,
                    multipart_upload_id,
                    multipart_part_number_generator,
                    current_buffer,
                    parts,
                }
            }
            any => any,
        })
    }

    pub(crate) fn finish(self, runtime: &mut Runtime, s3: &S3Client) -> Result<()> {
        match self {
            Self::Empty => return Err(anyhow!("Upload is in invalid state, cannot finish")),
            Self::Regular {
                bucket,
                key,
                current_buffer,
            } => {
                runtime.block_on(s3.put_object(PutObjectRequest {
                    bucket,
                    key: key.clone(),
                    body: Some(current_buffer.into()),
                    ..Default::default()
                }))?;
                debug!("Finished regular upload for '{}'", key);
            }
            Self::Multipart {
                bucket,
                key,
                multipart_upload_id,
                multipart_part_number_generator,
                current_buffer,
                mut parts,
            } => {
                if !current_buffer.is_empty() {
                    let completed_part: CompletedPart = Self::upload_part(
                        runtime,
                        s3,
                        &bucket,
                        &key,
                        &multipart_upload_id,
                        multipart_part_number_generator.next() as i64,
                        current_buffer,
                    )?;
                    parts.push(completed_part);
                }
                runtime.block_on(
                    s3.complete_multipart_upload(CompleteMultipartUploadRequest {
                        bucket,
                        key: key.clone(),
                        upload_id: multipart_upload_id,
                        multipart_upload: Some(CompletedMultipartUpload { parts: Some(parts) }),
                        ..Default::default()
                    }),
                )?;
                debug!("Finished multipart upload for '{}'", key);
            }
        }

        Ok(())
    }

    pub(crate) fn destroy(self, runtime: &mut Runtime, s3: &S3Client) -> Result<()> {
        match self {
            Self::Empty => {}
            Self::Regular { .. } => {}
            Self::Multipart {
                bucket,
                key,
                multipart_upload_id,
                ..
            } => {
                runtime.block_on(s3.abort_multipart_upload(AbortMultipartUploadRequest {
                    bucket,
                    key: key.clone(),
                    upload_id: multipart_upload_id,
                    ..Default::default()
                }))?;
                debug!("Successfully aborted multipart upload for '{}'", key);
            }
        }
        Ok(())
    }
}
