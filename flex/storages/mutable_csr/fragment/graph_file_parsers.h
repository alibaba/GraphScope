#ifndef GRAPHSCOPE_FRAGMENT_GRAPH_FILE_PARSERS_H_
#define GRAPHSCOPE_FRAGMENT_GRAPH_FILE_PARSERS_H_

#include "flex/storages/mutable_csr/graph/id_indexer.h"
#include "flex/storages/mutable_csr/property/table.h"
#include "flex/storages/mutable_csr/types.h"
#include "flex/storages/mutable_csr/property/column.h"
#include "flex/utils/mmap_array.h"

namespace gs {
// id|creationDate|locationIP|browserUsed|content|length
// 4947802324993|2011-08-17T14:26:59.961+0000|103.246.108.57|Safari|yes|3
inline void process_line_comment(FILE* fin, IdIndexer<oid_t, vid_t>& indexer, Table& table) {
  mmap_array<Date>& creationDate_col = std::dynamic_pointer_cast<DateColumn>(table.get_column("creationDate"))->buffer();
  mmap_array<std::string_view>& content_col = std::dynamic_pointer_cast<StringColumn>(table.get_column("content"))->buffer();

  oid_t id;
  vid_t vid;
  Date d;

  char line_buf[4096];
  while (fgets(line_buf, 4096, fin) != NULL) {
    char* ptr = line_buf; // id
    char* next = strchr(ptr, '|');
    *next = '\0';

#ifdef __APPLE__
    sscanf(ptr, "%lld", &id);
#else
    sscanf(ptr, "%" SCNd64, &id);
#endif

    indexer.add(id, vid);

    ptr = next + 1; // creationDate
    next = strchr(ptr, '|');

    d.reset(ptr);
    creationDate_col.insert(vid, d);

    ptr = next + 1; // locationIp
    next = strchr(ptr, '|');

    ptr = next + 1; // browserUsed
    next = strchr(ptr, '|');

    ptr = next + 1; // content
    next = strchr(ptr, '|');
    content_col.insert(vid, std::string_view(ptr, next - ptr));
  }
}

// id|imageFile|creationDate|locationIP|browserUsed|language|content|length
// 4947802324992||2011-08-17T06:05:40.595+0000|49.246.218.237|Firefox|uz|About Rupert Murdoch, t newer electronic publishing technoAbout George Frideric Handel,  concertos. Handel was born in 1685,About Kurt Vonne|140
inline void process_line_post(FILE* fin, IdIndexer<oid_t, vid_t>& indexer, Table& table) {
  mmap_array<std::string_view>& imageFile_col = std::dynamic_pointer_cast<StringColumn>(table.get_column("imageFile"))->buffer();
  mmap_array<Date>& creationDate_col = std::dynamic_pointer_cast<DateColumn>(table.get_column("creationDate"))->buffer();
  mmap_array<std::string_view>& content_col = std::dynamic_pointer_cast<StringColumn>(table.get_column("content"))->buffer();
  mmap_array<int>& length_col = std::dynamic_pointer_cast<IntColumn>(table.get_column("length"))->buffer();

  oid_t id;
  vid_t vid;
  Date d;
  int l;

  char line_buf[4096];
  while (fgets(line_buf, 4096, fin) != NULL) {
    char* ptr = line_buf; // id
    char* next = strchr(ptr, '|');
    *next = '\0';

#ifdef __APPLE__
    sscanf(ptr, "%lld", &id);
#else
    sscanf(ptr, "%" SCNd64, &id);
#endif

    indexer.add(id, vid);

    ptr = next + 1; // imageFile
    next = strchr(ptr, '|');
    imageFile_col.insert(vid, std::string_view(ptr, next - ptr));

    ptr = next + 1; // creationDate
    next = strchr(ptr, '|');

    d.reset(ptr);
    creationDate_col.insert(vid, d);

    ptr = next + 1; // locationIp
    next = strchr(ptr, '|');

    ptr = next + 1; // browserUsed
    next = strchr(ptr, '|');

    ptr = next + 1; // language
    next = strchr(ptr, '|');

    ptr = next + 1; // content
    next = strchr(ptr, '|');
    content_col.insert(vid, std::string_view(ptr, next - ptr));

    ptr = next + 1; //length
    sscanf(ptr, "%d", &l);
    length_col.insert(vid, l);
  }
}

// id|title|creationDate
// 0|Wall of Mahinda Perera|2010-02-14T15:32:20.447+0000
inline void process_line_forum(FILE* fin, IdIndexer<oid_t, vid_t>& indexer, Table& table) {
  mmap_array<std::string_view>& title_col = std::dynamic_pointer_cast<StringColumn>(table.get_column("title"))->buffer();

  oid_t id;
  vid_t vid;

  char line_buf[4096];
  while (fgets(line_buf, 4096, fin) != NULL) {
    char* ptr = line_buf; // id
    char* next = strchr(ptr, '|');
    *next = '\0';

#ifdef __APPLE__
    sscanf(ptr, "%lld", &id);
#else
    sscanf(ptr, "%" SCNd64, &id);
#endif

    indexer.add(id, vid);

    ptr = next + 1; // title
    next = strchr(ptr, '|');
    title_col.insert(vid, std::string_view(ptr, next - ptr));
  }
}

// id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|language|email
// 933|Mahinda|Perera|male|1989-12-03|2010-02-14T15:32:10.447+0000|119.235.7.103|Firefox|si;en|Mahinda933@boarderzone.com;Mahinda933@hotmail.com;Mahinda933@yahoo.com;Mahinda933@zoho.com
inline void process_line_person(FILE* fin, IdIndexer<oid_t, vid_t>& indexer, Table& table) {
  mmap_array<std::string_view>& firstName_col = std::dynamic_pointer_cast<StringColumn>(table.get_column("firstName"))->buffer();
  mmap_array<std::string_view>& lastName_col = std::dynamic_pointer_cast<StringColumn>(table.get_column("lastName"))->buffer();
  mmap_array<Gender>& gender_col = std::dynamic_pointer_cast<GenderColumn >(table.get_column("gender"))->buffer();
  mmap_array<Date>& birthday_col = std::dynamic_pointer_cast<DateColumn>(table.get_column("birthday"))->buffer();
  mmap_array<Date>& creationDate_col = std::dynamic_pointer_cast<DateColumn>(table.get_column("creationDate"))->buffer();
  mmap_array<IpAddr>& locationIP_col = std::dynamic_pointer_cast<IpAddrColumn>(table.get_column("locationIP"))->buffer();
  mmap_array<Browser>& browserUsed_col = std::dynamic_pointer_cast<BrowserColumn>(table.get_column("browserUsed"))->buffer();
  mmap_array<std::string_view>& language_col = std::dynamic_pointer_cast<StringColumn>(table.get_column("language"))->buffer();
  mmap_array<std::string_view>& email_col = std::dynamic_pointer_cast<StringColumn>(table.get_column("email"))->buffer();

  oid_t id;
  vid_t vid;
  Date b, c;
  IpAddr ip;

  char line_buf[4096];
  while (fgets(line_buf, 4096, fin) != NULL) {
    char* ptr = line_buf; // id
    char* next = strchr(ptr, '|');
    *next = '\0';

#ifdef __APPLE__
    sscanf(ptr, "%lld", &id);
#else
    sscanf(ptr, "%" SCNd64, &id);
#endif

    indexer.add(id, vid);

    ptr = next + 1; // firstName
    next = strchr(ptr, '|');
    firstName_col.insert(vid, std::string_view(ptr, next - ptr));

    ptr = next + 1; // lastName
    next = strchr(ptr, '|');
    lastName_col.insert(vid, std::string_view(ptr, next - ptr));

    ptr = next + 1; // gender
    next = strchr(ptr, '|');
    gender_col.insert(vid, *ptr == 'm' ? Gender::kMale : Gender::kFemale);

    ptr = next + 1; // birthday
    next = strchr(ptr, '|');
    b.reset(ptr);
    birthday_col.insert(vid, b);

    ptr = next + 1; // creationDate
    next = strchr(ptr, '|');
    c.reset(ptr);
    creationDate_col.insert(vid, c);

    ptr = next + 1; // locationIP
    next = strchr(ptr, '|');
    ip.from_str(ptr, next - ptr);
    locationIP_col.insert(vid, ip);

    ptr = next + 1; // browserUsed
    next = strchr(ptr, '|');
    if (*ptr == 'I') {
      browserUsed_col.insert(vid, Browser::kIE);
    } else if (*ptr == 'O') {
      browserUsed_col.insert(vid, Browser::kOpera);
    } else if (*ptr == 'F') {
      browserUsed_col.insert(vid, Browser::kFirefox);
    } else if (*ptr == 'C') {
      browserUsed_col.insert(vid, Browser::kChrome);
    } else {
      browserUsed_col.insert(vid, Browser::kSafari);
    }

    ptr = next + 1; // language
    next = strchr(ptr, '|');
    language_col.insert(vid, std::string_view(ptr, next - ptr));

    ptr = next + 1; // email
    std::string_view email = std::string_view(ptr);
    int length = email.size();
    if (email.back() == '\n') {
      --length;
    }
    
    email_col.insert(vid, std::string_view(ptr, length));
  }
}

}  // namespace gs

#endif  // GRAPHSCOPE_FRAGMENT_GRAPH_FILE_PARSERS_H_
