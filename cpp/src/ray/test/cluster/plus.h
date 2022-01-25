// Copyright 2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <ray/api.h>

/// general function of user code
int Return1();
int Plus1(int x);
int Plus(int x, int y);
void ThrowTask();
std::string GetVal(ray::ObjectRef<std::string> obj);
int Add(ray::ObjectRef<int> obj1, ray::ObjectRef<int> obj2);
int GetList(std::vector<ray::ObjectRef<int>> list);
std::array<int, 100000> ReturnLargeArray(std::array<int, 100000> x);
