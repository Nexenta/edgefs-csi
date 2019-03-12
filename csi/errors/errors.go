/*
 * Copyright (c) 2015-2019 Nexenta Systems, Inc.
 *
 * This file is part of EdgeFS Project
 * (see https://github.com/Nexenta/edgefs).
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package errors

const (
	EdgefsVolumeNotExistsErrorCode      = 1
	EdgefsVolumeNotFoundErrorCode       = 2
	EdgefsServiceNotExistsErrorCode     = 3
	EdgefsNoServiceAvailableErrorCode   = 4
	EdgefsUnknownBackendTypeErrorCode   = 5
	EdgefsSnapshotNotExistErrorCode     = 6
	EdgefsSnapshotAlreadyExistErrorCode = 7
)

type EdgefsError struct {
	Msg  string
	Code int
}

func (e *EdgefsError) Error() string {
	return e.Msg
}

func GetEdgefsErrorCode(err error) int {
	if edgefsErr, ok := err.(*EdgefsError); ok {
		return edgefsErr.Code
	}
	return 0
}

func IsEdgefsVolumeNotExistsError(err error) bool {
	return GetEdgefsErrorCode(err) == EdgefsVolumeNotExistsErrorCode
}

func IsEdgefsServiceNotExistsError(err error) bool {
	return GetEdgefsErrorCode(err) == EdgefsServiceNotExistsErrorCode
}

func IsEdgefsNoServiceAvailableError(err error) bool {
	return GetEdgefsErrorCode(err) == EdgefsNoServiceAvailableErrorCode
}

func IsEdgefsUnknownBackendTypeError(err error) bool {
	return GetEdgefsErrorCode(err) == EdgefsUnknownBackendTypeErrorCode
}

func IsEdgefsSnapshotNotExistTypeError(err error) bool {
	return GetEdgefsErrorCode(err) == EdgefsSnapshotNotExistErrorCode
}

func IsEdgefsSnapshotAlreadyExistTypeError(err error) bool {
	return GetEdgefsErrorCode(err) == EdgefsSnapshotAlreadyExistErrorCode
}
