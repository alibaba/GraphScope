/** Copyright 2020 Alibaba Group Holding Limited.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/**
 @file error.h
 @brief Define the error code related APIs
*/

#include "grin/src/predefine.h"

#include "grin/include/common/error.h"

__thread GRIN_ERROR_CODE grin_error_code = GRIN_ERROR_CODE::NO_ERROR;

GRIN_ERROR_CODE grin_get_last_error_code() { return grin_error_code; }