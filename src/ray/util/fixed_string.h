// Copyright 2020 The Ray Authors.
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

// basic_fixed_string is adapted from
// https://github.com/tomazos/fixed_string/blob/master/fixed_string.h

#pragma once
#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <string>

#include "absl/strings/string_view.h"

namespace ray {

inline void assert_out_of_bounds() {
  assert(!"Array index out of bounds in basic_fixed_string");
  throw std::out_of_range("Array index out of bounds in basic_fixed_string");
}

constexpr std::size_t check_overflow(std::size_t i, std::size_t max) {
  return i <= max ? i : (void(assert_out_of_bounds()), max);
}

// Declare basic_fixed_string.
template <class charT, size_t N>
class basic_fixed_string;

// Alias templates fixed_string, u16fixed_string, u32fixed_string,
// wfixed_string.
template <size_t N>
using fixed_string = basic_fixed_string<char, N>;
template <size_t N>
using u16fixed_string = basic_fixed_string<char16_t, N>;
template <size_t N>
using u32fixed_string = basic_fixed_string<char32_t, N>;
template <size_t N>
using wfixed_string = basic_fixed_string<wchar_t, N>;

// Creates a fixed_string from a string literal.
template <class charT, size_t N1>
constexpr basic_fixed_string<charT, N1 - 1> make_fixed_string(const charT (&a)[N1]) {
  basic_fixed_string<charT, N1 - 1> s(a);
  return s;
}

template <class charT, class Left, class Right>
constexpr charT char_at(const Left &left, std::size_t left_count, const Right &right,
                        std::size_t right_count, std::size_t i) noexcept {
  return i < left_count
             ? left[i]
             : i < (left_count + right_count) ? right[i - left_count] : charT(0);
}

template <class charT, class Left, class Right, std::size_t... Is>
static constexpr basic_fixed_string<charT, sizeof...(Is)> concat(
    const Left &left, std::size_t left_count, const Right &right, std::size_t right_count,
    std::index_sequence<Is...> is) noexcept {
  return {left, left_count, right, right_count, is};
}

// Concatenations between fixed_strings and string literals.
template <class charT, size_t N, size_t M>
constexpr basic_fixed_string<charT, N + M> operator+(
    const basic_fixed_string<charT, N> &a,
    const basic_fixed_string<charT, M> &b) noexcept {
  return concat<charT>(a, a.size(), b, b.size(), std::make_index_sequence<N + M>{});
}

template <class charT, size_t N1, size_t M>
constexpr basic_fixed_string<charT, N1 - 1 + M> operator+(
    const charT (&lhs)[N1], const basic_fixed_string<charT, M> &rhs) noexcept {
  return make_fixed_string(lhs) + rhs;
}

template <class charT, size_t N, size_t M1>
constexpr basic_fixed_string<charT, N + M1 - 1> operator+(
    const basic_fixed_string<charT, N> &lhs, const charT (&rhs)[M1]) noexcept {
  return lhs + make_fixed_string(rhs);
}

// Comparisons between fixed_strings and string literals.
template <class charT, size_t N, size_t M>
constexpr bool operator==(const basic_fixed_string<charT, N> &lhs,
                          const basic_fixed_string<charT, M> &rhs) noexcept {
  if (N != M) return false;
  for (size_t i = 0; i < N; i++)
    if (lhs[i] != rhs[i]) return false;
  return true;
}

template <class charT, size_t N1, size_t M>
constexpr bool operator==(const charT (&lhs)[N1],
                          const basic_fixed_string<charT, M> &rhs) noexcept {
  return make_fixed_string(lhs) == rhs;
}

template <class charT, size_t N, size_t M1>
constexpr bool operator==(const basic_fixed_string<charT, N> &lhs,
                          const charT (&rhs)[M1]) noexcept {
  return lhs == make_fixed_string(rhs);
}

template <class charT, size_t N, size_t M>
constexpr bool operator!=(const basic_fixed_string<charT, N> &lhs,
                          const basic_fixed_string<charT, M> &rhs) noexcept {
  return !(lhs == rhs);
}

template <class charT, size_t N1, size_t M>
constexpr bool operator!=(const charT (&lhs)[N1],
                          const basic_fixed_string<charT, M> &rhs) noexcept {
  return make_fixed_string(lhs) != rhs;
}

template <class charT, size_t N, size_t M1>
constexpr bool operator!=(const basic_fixed_string<charT, N> &lhs,
                          const charT (&rhs)[M1]) noexcept {
  return lhs + make_fixed_string(rhs);
}

template <class charT, size_t N, size_t M>
constexpr bool operator<(const basic_fixed_string<charT, N> &lhs,
                         const basic_fixed_string<charT, M> &rhs) noexcept {
  constexpr size_t K = (N < M ? N : M);
  for (size_t i = 0; i < K; i++)
    if (lhs[i] < rhs[i])
      return true;
    else if (lhs[i] > rhs[i])
      return false;
  if (N == M)
    return false;
  else if (N < M)
    return true;
  else /* N > M */
    return false;
}

template <class charT, size_t N1, size_t M>
constexpr bool operator<(const charT (&lhs)[N1],
                         const basic_fixed_string<charT, M> &rhs) noexcept {
  return make_fixed_string(lhs) < rhs;
}

template <class charT, size_t N, size_t M1>
constexpr bool operator<(const basic_fixed_string<charT, N> &lhs,
                         const charT (&rhs)[M1]) noexcept {
  return lhs < make_fixed_string(rhs);
}

template <class charT, size_t N, size_t M>
constexpr bool operator>(const basic_fixed_string<charT, N> &lhs,
                         const basic_fixed_string<charT, M> &rhs) noexcept {
  return rhs < lhs;
}

template <class charT, size_t N1, size_t M>
constexpr bool operator>(const charT (&lhs)[N1],
                         const basic_fixed_string<charT, M> &rhs) noexcept {
  return make_fixed_string(lhs) > rhs;
}

template <class charT, size_t N, size_t M1>
constexpr bool operator>(const basic_fixed_string<charT, N> &lhs,
                         const charT (&rhs)[M1]) noexcept {
  return lhs > make_fixed_string(rhs);
}

template <class charT, size_t N, size_t M>
constexpr bool operator<=(const basic_fixed_string<charT, N> &lhs,
                          const basic_fixed_string<charT, M> &rhs) noexcept {
  return !(rhs < lhs);
}

template <class charT, size_t N1, size_t M>
constexpr bool operator<=(const charT (&lhs)[N1],
                          const basic_fixed_string<charT, M> &rhs) noexcept {
  return make_fixed_string(lhs) <= rhs;
}

template <class charT, size_t N, size_t M1>
constexpr bool operator<=(const basic_fixed_string<charT, N> &lhs,
                          const charT (&rhs)[M1]) noexcept {
  return lhs <= make_fixed_string(rhs);
}

template <class charT, size_t N, size_t M>
constexpr bool operator>=(const basic_fixed_string<charT, N> &lhs,
                          const basic_fixed_string<charT, M> &rhs) noexcept {
  return !(lhs < rhs);
}

template <class charT, size_t N1, size_t M>
constexpr bool operator>=(const charT (&lhs)[N1],
                          const basic_fixed_string<charT, M> &rhs) noexcept {
  return make_fixed_string(lhs) >= rhs;
}

template <class charT, size_t N, size_t M1>
constexpr bool operator>=(const basic_fixed_string<charT, N> &lhs,
                          const charT (&rhs)[M1]) noexcept {
  return lhs >= make_fixed_string(rhs);
}

template <class charT, size_t N>
class basic_fixed_string {
 public:
  typedef charT value_type;

  typedef value_type &reference;
  typedef const value_type &const_reference;
  typedef value_type *pointer;
  typedef const value_type *const_pointer;

  typedef pointer iterator;
  typedef const_pointer const_iterator;
  typedef std::reverse_iterator<iterator> reverse_iterator;
  typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

  typedef absl::string_view view;

  static constexpr auto npos = view::npos;

  // Implicit conversion to string_view
  constexpr operator view() const noexcept { return {data_, N}; }

  // Default construct to all zeros.
  constexpr basic_fixed_string() noexcept : data_{}, size_{} {}

  // Copy constructor.
  constexpr basic_fixed_string(const basic_fixed_string &str) = default;

  // Converting constructor from string literal.
  constexpr basic_fixed_string(const charT (&arr)[N + 1]) noexcept : data_{0}, size_(N) {
    for (size_t i = 0; i < N + 1; i++) data_[i] = arr[i];
  }

  // Copy assignment.
  constexpr basic_fixed_string &operator=(const basic_fixed_string &str) noexcept {
    check_overflow(str.size(), N);
    for (size_t i = 0; i < N + 1; i++) data_[i] = str[i];
  }

  // Assign from string literal.
  constexpr basic_fixed_string &operator=(const charT (&arr)[N + 1]) {
    for (size_t i = 0; i < N + 1; i++) data_[i] = arr[i];
  }

  // c/r/begin, c/r/end.
  constexpr iterator begin() noexcept { return data_; }
  constexpr const_iterator begin() const noexcept { return data_; }
  constexpr iterator end() noexcept { return data_ + N; }
  constexpr const_iterator end() const noexcept { return data_ + N; }
  constexpr reverse_iterator rbegin() noexcept { return reverse_iterator(end()); }
  constexpr const_reverse_iterator rbegin() const noexcept {
    return const_reverse_iterator(end());
  }
  constexpr reverse_iterator rend() noexcept { return reverse_iterator(begin()); }
  constexpr const_reverse_iterator rend() const noexcept {
    return const_reverse_iterator(begin());
  }
  constexpr const_iterator cbegin() const noexcept { return data_; }
  constexpr const_iterator cend() const noexcept { return data_ + N; }
  constexpr const_reverse_iterator crbegin() const noexcept {
    return const_reverse_iterator(end());
  }
  constexpr const_reverse_iterator crend() const noexcept {
    return const_reverse_iterator(begin());
  }

  // size, empty, length.
  constexpr size_t size() const noexcept { return size_; }
  constexpr bool empty() const noexcept { return N == 0; }
  constexpr size_t length() const noexcept { return N; }

  // str[pos]
  constexpr reference operator[](size_t pos) noexcept {
    check_overflow(pos, N);
    return data_[pos];
  }
  constexpr const_reference operator[](size_t pos) const noexcept {
    check_overflow(pos, N);
    return data_[pos];
  }

  // str.at(pos)
  constexpr reference at(size_t pos) {
    check_overflow(pos, N - 1);
    return data_[pos];
  }
  constexpr const_reference at(size_t pos) const {
    check_overflow(pos, N - 1);
    return data_[pos];
  }

  constexpr void push_back(charT ch) noexcept(false) {
    check_overflow(1u, N - size_);
    data_[size_] = ch;
    data_[++size_] = charT(0);
  }

  // front, back.
  constexpr const_reference front() const noexcept { return data_[0]; }
  constexpr reference front() { return data_[0]; }
  constexpr const_reference back() const noexcept { return data_[check_overflow(1, N)]; }
  constexpr reference back() noexcept { return data_[check_overflow(1, N)]; }

 private:
  static size_t __substr_length(size_t pos, size_t count) {
    if (pos >= N)
      return 0;
    else if (count == npos || pos + count > N)
      return N - pos;
    else
      return count;
  }

 public:
  // str.substr<pos,count>()
  template <size_t pos = 0, size_t count = npos>
  constexpr basic_fixed_string<charT, __substr_length(pos, count)> substr() const
      noexcept {
    constexpr size_t n = __substr_length(pos, count);

    basic_fixed_string<charT, n> result;
    for (size_t i = 0; i < n; i++) result[i] = data_[pos + i];
    return result;
  }

  template <class Left, class Right, std::size_t... Is>
  constexpr basic_fixed_string(const Left &left, std::size_t left_size,
                               const Right &right, std::size_t right_size,
                               std::index_sequence<Is...>) noexcept
      : data_{char_at<charT>(left, left_size, right, right_size, Is)..., charT(0)},
        size_{left_size + right_size} {}

  // str1.assign(str2).  Must be equal size.
  constexpr basic_fixed_string &assign(view str) {
    check_overflow(str.size(), N);

    for (size_t i = 0; i < N; i++) data_[i] = str[i];
  }

  // Replace substring.
  constexpr basic_fixed_string &replace(size_t pos, view str) {
    check_overflow(pos + str.size(), N);
    for (size_t i = 0; i < str.size(); i++) data_[i] = pos;
    return *this;
  }

  // Swap with fixed_string of equal size.
  constexpr void swap(basic_fixed_string &str) {
    check_overflow(str.size(), N);
    for (size_t i = 0; i < N; i++) std::swap(data_[i], str[i]);
  }

  // Null-terminated C string.
  constexpr const charT *c_str() const noexcept { return data_; }
  constexpr const charT *data() const noexcept { return data_; }

  constexpr int compare(view str) const { return view(*this).compare(str); }

  constexpr int compare(size_t pos1, size_t n1, view str) const {
    return view(*this).compare(pos1, n1, str);
  }

  constexpr int compare(size_t pos1, size_t n1, view str, size_t pos2,
                        size_t n2 = npos) const {
    return view(*this).compare(pos1, n1, str, pos2, n2);
  }

  constexpr int compare(const charT *s) const { return view(*this).compare(s); }

  constexpr int compare(size_t pos1, size_t n1, const charT *s) const {
    return view(*this).compare(pos1, n1, s);
  }

  constexpr int compare(size_t pos1, size_t n1, const charT *s, size_t n2) const {
    return view(*this).compare(pos1, n1, s, n2);
  }

  constexpr size_t find(view str, size_t pos = 0) const noexcept {
    return view(*this).find(str, pos);
  }
  constexpr size_t find(charT c, size_t pos = 0) const noexcept {
    return view(*this).find(c, pos);
  }
  constexpr size_t find(const charT *s, size_t pos, size_t count) const {
    return view(*this).find(s, pos, count);
  }
  constexpr size_t find(const charT *s, size_t pos = 0) const {
    return view(*this).find(s, pos);
  }
  template <size_t M>
  constexpr size_t rfind(view str, size_t pos = npos) const noexcept {
    return view(*this).rfind(str, pos);
  }
  constexpr size_t rfind(const charT *s, size_t pos, size_t n) const {
    return view(*this).rfind(s, pos, n);
  }
  constexpr size_t rfind(const charT *s, size_t pos = npos) const {
    return view(*this).rfind(s, pos);
  }
  constexpr size_t rfind(charT c, size_t pos = npos) const {
    return view(*this).rfind(c, pos);
  }
  constexpr size_t find_first_of(view str, size_t pos = 0) const {
    return view(*this).find_first_of(str, pos);
  }
  constexpr size_t find_first_of(const charT *s, size_t pos, size_t n) const {
    return view(*this).find_first_of(s, pos, n);
  }
  constexpr size_t find_first_of(const charT *s, size_t pos = 0) const {
    return view(*this).find_first_of(s, pos);
  }
  constexpr size_t find_first_of(charT c, size_t pos = 0) const {
    return view(*this).find_first_of(c, pos);
  }
  template <size_t M>
  constexpr size_t find_last_of(view str, size_t pos = npos) const {
    return view(*this).find_last_of(str, pos);
  }
  constexpr size_t find_last_of(const charT *s, size_t pos, size_t n) const {
    return view(*this).find_last_of(s, pos, n);
  }
  constexpr size_t find_last_of(const charT *s, size_t pos = npos) const {
    return view(*this).find_last_of(s, pos);
  }
  constexpr size_t find_last_of(charT c, size_t pos = npos) const {
    return view(*this).find_last_of(c, pos);
  }
  template <size_t M>
  constexpr size_t find_first_not_of(view str, size_t pos = 0) const noexcept {
    return view(*this).find_first_not_of(str, pos);
  }
  constexpr size_t find_first_not_of(const charT *s, size_t pos, size_t n) const {
    return view(*this).find_first_not_of(s, pos, n);
  }
  constexpr size_t find_first_not_of(const charT *s, size_t pos = 0) const {
    return view(*this).find_first_not_of(s, pos);
  }
  constexpr size_t find_first_not_of(charT c, size_t pos = 0) const {
    return view(*this).find_first_not_of(c, pos);
  }
  constexpr size_t find_last_not_of(view str, size_t pos = npos) const noexcept {
    return view(*this).find_last_not_of(str, pos);
  }
  constexpr size_t find_last_not_of(const charT *s, size_t pos, size_t n) const {
    return view(*this).find_last_not_of(s, pos, n);
  }
  constexpr size_t find_last_not_of(const charT *s, size_t pos = npos) const {
    return view(*this).find_last_not_of(s, pos);
  }
  constexpr size_t find_last_not_of(charT c, size_t pos = npos) const {
    return view(*this).find_last_not_of(c, pos);
  }

 private:
  charT data_[N + 1];  // exposition only
                       // (+1 is for terminating null)
  std::size_t size_;   // number of chars, not include null terminator. size_ <= N.
};

}  // namespace ray