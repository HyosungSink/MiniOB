/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/tokenizer/jieba_tokenizer.h"
#include "common/utils/private_accessor.h"

#include <algorithm>
#include <cstdlib>
#include <unistd.h>

IMPLEMENT_GET_PRIVATE_VAR(KeywordExtractor, cppjieba::KeywordExtractor, stopWords_, std::unordered_set<std::string>)

namespace {

struct JiebaPaths
{
  std::string dict_path;
  std::string model_path;
  std::string user_dict_path;
  std::string idf_path;
  std::string stop_word_path;
};

std::string path_join(const std::string &dir, const std::string &file)
{
  if (dir.empty()) {
    return file;
  }
  char last_char = dir[dir.size() - 1];
  if (last_char == '/' || last_char == '\\') {
    return dir + file;
  }
  return dir + "/" + file;
}

bool readable_file(const std::string &path)
{
  return access(path.c_str(), R_OK) == 0;
}

bool usable_dict_dir(const std::string &dir)
{
  return readable_file(path_join(dir, "jieba.dict.utf8")) && readable_file(path_join(dir, "hmm_model.utf8")) &&
         readable_file(path_join(dir, "idf.utf8")) && readable_file(path_join(dir, "stop_words.utf8"));
}

void append_source_tree_candidates(std::vector<std::string> &dirs)
{
  std::string dir = __FILE__;
  size_t pos = dir.find_last_of("/\\");
  if (pos == std::string::npos) {
    return;
  }
  dir.resize(pos);

  for (int i = 0; i < 8 && !dir.empty(); i++) {
    dirs.emplace_back(path_join(dir, "deps/3rd/usr/local/dict"));
    dirs.emplace_back(path_join(dir, "deps/3rd/cppjieba/dict"));

    pos = dir.find_last_of("/\\");
    if (pos == std::string::npos) {
      break;
    }
    dir.resize(pos);
  }
}

std::string find_jieba_dict_dir()
{
  std::vector<std::string> dirs;
  if (const char *env_dir = std::getenv("MINIOB_JIEBA_DICT_DIR"); env_dir != nullptr && env_dir[0] != '\0') {
    dirs.emplace_back(env_dir);
  }
  dirs.emplace_back("/usr/local/dict");
  dirs.emplace_back("/usr/local/share/cppjieba/dict");
  dirs.emplace_back("dict");
  dirs.emplace_back("deps/3rd/usr/local/dict");
  dirs.emplace_back("deps/3rd/cppjieba/dict");
  append_source_tree_candidates(dirs);

  for (const std::string &dir : dirs) {
    if (usable_dict_dir(dir)) {
      return dir;
    }
  }

  return "/usr/local/dict";
}

JiebaPaths make_jieba_paths()
{
  std::string dir = find_jieba_dict_dir();
  return {path_join(dir, "jieba.dict.utf8"),
      path_join(dir, "hmm_model.utf8"),
      path_join(dir, "user.dict.utf8"),
      path_join(dir, "idf.utf8"),
      path_join(dir, "stop_words.utf8")};
}

const JiebaPaths &jieba_paths()
{
  static JiebaPaths paths = make_jieba_paths();
  return paths;
}

}  // namespace

JiebaTokenizer::JiebaTokenizer()
    : jieba(jieba_paths().dict_path,
          jieba_paths().model_path,
          jieba_paths().user_dict_path,
          jieba_paths().idf_path,
          jieba_paths().stop_word_path)
{}

RC JiebaTokenizer::cut(std::string &text, std::vector<std::string> &tokens)
{
    // Implement Jieba tokenizer logic here
    jieba.Cut(text, tokens);
    // Remove stop words
    auto &stopWords_ = *GET_PRIVATE(cppjieba::KeywordExtractor, &jieba.extractor, KeywordExtractor, stopWords_);
    tokens.erase(std::remove_if(tokens.begin(),
                        tokens.end(),
                        [&stopWords_](const std::string &word) { return stopWords_.find(word) != stopWords_.end(); }),
        tokens.end());
    return RC::SUCCESS;
}
