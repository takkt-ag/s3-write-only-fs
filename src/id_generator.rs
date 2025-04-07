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

use std::sync::atomic::{
    AtomicU64,
    Ordering,
};

pub(crate) struct IdGenerator(AtomicU64);

impl IdGenerator {
    pub(crate) fn new(start: u64) -> Self {
        IdGenerator(AtomicU64::new(start))
    }

    pub(crate) fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst)
    }
}
