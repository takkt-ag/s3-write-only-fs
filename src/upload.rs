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
use rusoto_s3::{
    CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadRequest, PutObjectRequest, S3Client, UploadPartRequest, S3,
};
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
    ) -> String {
        runtime
            .block_on(s3.create_multipart_upload(CreateMultipartUploadRequest {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                ..Default::default()
            }))
            .expect("failed to create multipart upload")
            .upload_id
            .expect("upload id was unset")
    }

    fn upload_part(
        runtime: &mut Runtime,
        s3: &S3Client,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
        body: Vec<u8>,
    ) -> CompletedPart {
        let e_tag = runtime
            .block_on(s3.upload_part(UploadPartRequest {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                upload_id: upload_id.to_owned(),
                body: Some(body.into()),
                part_number,
                ..Default::default()
            }))
            .expect("failed to upload multipart")
            .e_tag
            .expect("uploaded multipart did not return e-tag");

        CompletedPart {
            e_tag: Some(e_tag),
            part_number: Some(part_number),
        }
    }

    pub(crate) fn write(self, runtime: &mut Runtime, s3: &S3Client, data: &[u8]) -> Upload {
        match self {
            Self::Regular {
                bucket,
                key,
                mut current_buffer,
            } => {
                current_buffer.extend_from_slice(data);
                if current_buffer.len() >= MULTIPART_MINIMUM_PART_SIZE {
                    let multipart_part_number_generator = Arc::new(IdGenerator::new(1));
                    let multipart_upload_id: String =
                        Self::create_multipart_upload(runtime, s3, &bucket, &key);
                    let completed_part: CompletedPart = Self::upload_part(
                        runtime,
                        s3,
                        &bucket,
                        &key,
                        &multipart_upload_id,
                        multipart_part_number_generator.next() as i64,
                        current_buffer,
                    );
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
                    );
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
        }
    }

    pub(crate) fn finish(self, runtime: &mut Runtime, s3: &S3Client) {
        match self {
            Self::Empty => {}
            Self::Regular {
                bucket,
                key,
                current_buffer,
            } => {
                runtime
                    .block_on(s3.put_object(PutObjectRequest {
                        bucket,
                        key,
                        body: Some(current_buffer.into()),
                        ..Default::default()
                    }))
                    .expect("failed to put object");
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
                    );
                    parts.push(completed_part);
                }
                runtime
                    .block_on(
                        s3.complete_multipart_upload(CompleteMultipartUploadRequest {
                            bucket,
                            key,
                            upload_id: multipart_upload_id,
                            multipart_upload: Some(CompletedMultipartUpload { parts: Some(parts) }),
                            ..Default::default()
                        }),
                    )
                    .expect("failed to complete multipart upload");
            }
        }
    }
}
