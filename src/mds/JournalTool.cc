// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab


#include <sstream>
#include <boost/system/error_code.hpp>
#include <boost/filesystem/convenience.hpp>
#include <boost/filesystem/fstream.hpp>

#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "osdc/Journaler.h"
#include "mds/mdstypes.h"
#include "mds/LogEvent.h"
#include "mds/Dumper.h"
#include "mds/Resetter.h"

// Hack, special case for getting metablob, replace with generic
#include "mds/events/EUpdate.h"

#include "JournalTool.h"

#define dout_subsys ceph_subsys_mds


const string JournalFilter::range_separator("..");


void JournalTool::usage()
{
  std::cout << "Usage: \n"
    << "  cephfs-journal-tool [options] journal [inspect|import|export]\n"
    << "  cephfs-journal-tool [options] header <get|set <field> <value>\n"
    << "  cephfs-journal-tool [options] event <selector> <effect> <output>\n"
    << "    <selector>:  [--by-type=<metablob|client|mds|...?>|--by-inode=<inode>|--by-path=<path>|by-tree=<path>|by-range=<N>..<M>|by-dirfrag-name=<dirfrag id>,<name>]\n"
    << "    <effect>: [get|splice]\n"
    << "    <output>: [summary|binary|json] [-o <path>] [--latest]\n"
    << "\n"
    << "Options:\n"
    << "  --rank=<int>  Journal rank (default 0)\n";

  generic_client_usage();
}

JournalTool::~JournalTool()
{
}


/**
 * Handle arguments and hand off to journal/header/event mode
 */
int JournalTool::main(std::vector<const char*> &argv)
{
  int r;

  dout(10) << "JournalTool::main " << dendl;
  // Common arg parsing
  // ==================
  if (argv.empty()) {
    usage();
    return -EINVAL;
  }

  std::vector<const char*>::iterator arg = argv.begin();
  std::string rank_str;
  if(ceph_argparse_witharg(argv, arg, &rank_str, "--rank", (char*)NULL)) {
    std::string rank_err;
    rank = strict_strtol(rank_str.c_str(), 10, &rank_err);
    if (!rank_err.empty()) {
        derr << "Bad rank '" << rank_str << "'" << dendl;
        usage();
    }
  }

  std::string mode;
  if (arg == argv.end()) {
    derr << "Missing mode [journal|header|event]" << dendl;
    return -EINVAL;
  }
  mode = std::string(*arg);
  arg = argv.erase(arg);

  // RADOS init
  // ==========
  r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    derr << "RADOS unavailable, cannot scan filesystem journal" << dendl;
    return r;
  }

  dout(4) << "JournalTool: connecting to RADOS..." << dendl;
  rados.connect();
 
  int const pool_id = mdsmap->get_metadata_pool();
  dout(4) << "JournalTool: resolving pool " << pool_id << dendl;
  std::string pool_name;
  r = rados.pool_reverse_lookup(pool_id, &pool_name);
  if (r < 0) {
    derr << "Pool " << pool_id << " named in MDS map not found in RADOS!" << dendl;
    return r;
  }

  dout(4) << "JournalTool: creating IoCtx.." << dendl;
  r = rados.ioctx_create(pool_name.c_str(), io);
  assert(r == 0);

  // Execution
  // =========
  dout(4) << "Executing for rank " << rank << dendl;
  if (mode == std::string("journal")) {
    return main_journal(argv);
  } else if (mode == std::string("header")) {
    return main_header(argv);
  } else if (mode == std::string("event")) {
    return main_event(argv);
  } else {
    derr << "Bad command '" << mode << "'" << dendl;
    usage();
    return -EINVAL;
  }
}


/**
 * Handle arguments for 'journal' mode
 *
 * This is for operations that act on the journal as a whole.
 */
int JournalTool::main_journal(std::vector<const char*> &argv)
{
  std::string command = argv[0];
  if (command == "inspect") {
    return journal_inspect();
  } else if (command == "export" || command == "import") {
    if (argv.size() >= 2) {
      std::string const path = argv[1];
      return journal_export(path, command == "import");
    } else {
      derr << "Missing path" << dendl;
      return -EINVAL;
    }
  } else if (command == "reset") {
      return journal_reset();
  } else {
    derr << "Bad journal command '" << command << "'" << dendl;
    return -EINVAL;
  }
}


/**
 * Parse arguments and execute for 'header' mode
 *
 * This is for operations that act on the header only.
 */
int JournalTool::main_header(std::vector<const char*> &argv)
{
  JournalFilter filter;
  JournalScanner js(io, rank, filter);
  int r = js.scan(false);
  if (r < 0) {
    derr << "Unable to scan journal" << dendl;
    return r;
  }

  if (!js.header_present) {
    derr << "Header object not found!" << dendl;
    return -ENOENT;
  } else if (!js.header_valid) {
    derr << "Header invalid!" << dendl;
    return -ENOENT;
  } else {
    assert(js.header != NULL);
  }

  if (argv.size() == 0) {
    derr << "Invalid header command, must be [get|set]" << dendl;
    return -EINVAL;
  }
  std::vector<const char *>::iterator arg = argv.begin();
  std::string const command = *arg;
  arg = argv.erase(arg);

  if (command == std::string("get")) {
    JSONFormatter jf(true);
    js.header->dump(&jf);
    jf.flush(std::cout);
  } else if (command == std::string("set")) {
    // Need two more args <key> <val>
    if (argv.size() != 2) {
      derr << "'set' requires two arguments <trimmed_pos|expire_pos|write_pos> <value>" << dendl;
      return -EINVAL;
    }

    std::string const field_name = *arg;
    arg = argv.erase(arg);

    std::string const value_str = *arg;
    arg = argv.erase(arg);
    assert(argv.empty());

    std::string parse_err;
    uint64_t new_val = strict_strtoll(value_str.c_str(), 0, &parse_err);
    if (!parse_err.empty()) {
      derr << "Invalid value '" << value_str << "': " << parse_err << dendl;
      return -EINVAL;
    }

    uint64_t *field = NULL;
    if (field_name == "trimmed_pos") {
      field = &(js.header->trimmed_pos);
    } else if (field_name == "expire_pos") {
      field = &(js.header->expire_pos);
    } else if (field_name == "write_pos") {
      field = &(js.header->write_pos);
    } else {
      derr << "Invalid field '" << field_name << "'" << dendl;
      return -EINVAL;
    }

    dout(4) << "Updating " << field_name << std::hex << " 0x" << *field << " -> 0x" << new_val << std::dec << dendl;
    *field = new_val;

    dout(4) << "Writing object..." << dendl;
    bufferlist header_bl;
    ::encode(*(js.header), header_bl);
    io.write_full(js.obj_name(0), header_bl);
    dout(4) << "Write complete." << dendl;
  } else {
    derr << "Bad header command '" << command << "'" << dendl;
    return -EINVAL;
  }

  return 0;
}


/**
 * Parse arguments and execute for 'event' mode
 *
 * This is for operations that act on LogEvents within the log
 */
int JournalTool::main_event(std::vector<const char*> &argv)
{
  int r;

  std::vector<const char*>::iterator arg = argv.begin();

  std::string command = *(arg++);
  if (arg == argv.end()) {
    derr << "Incomplete command line" << dendl;
    usage();
    return -EINVAL;
  }

  // Parse filter options
  // ====================
  JournalFilter filter;
  r = filter.parse_args(argv, arg);
  if (r) {
    return r;
  }

  // Parse output options
  // ====================
  if (arg == argv.end()) {
    derr << "Missing output command" << dendl;
    usage();
  }
  std::string output_style = *(arg++);
  std::string output_path = "dump";
  while(arg != argv.end()) {
    std::string arg_str;
    if (ceph_argparse_witharg(argv, arg, &arg_str, "--path", (char*)NULL)) {
      output_path = arg_str;
    } else {
      derr << "Unknown argument: '" << *arg << "'" << dendl;
      return -EINVAL;
    }
  }

  // Execute command
  // ===============
  JournalScanner js(io, rank, filter);
  if (command == "get") {
    r = js.scan();
    if (r) {
      derr << "Failed to scan journal (" << cpp_strerror(r) << ")" << dendl;
      return r;
    }
  } else if (command == "apply") {
    r = js.scan();
    if (r) {
      derr << "Failed to scan journal (" << cpp_strerror(r) << ")" << dendl;
      return r;
    }

    bool dry_run = false;
    if (arg != argv.end() && ceph_argparse_flag(argv, arg, "--dry_run", (char*)NULL)) {
      dry_run = true;
    }

    for (JournalScanner::EventMap::iterator i = js.events.begin(); i != js.events.end(); ++i) {
      LogEvent *le = i->second;
      EMetaBlob *mb = le->get_metablob();
      if (mb) {
        replay_offline(*mb, dry_run);
      }
    }
  } else {
    derr << "Bad journal command '" << command << "'" << dendl;
    return -EINVAL;
  }

  // Generate output
  // ===============
  EventOutputter output(js, output_path);
  if (output_style == "binary") {
      output.binary();
  } else if (output_style == "json") {
      output.json();
  } else if (output_style == "summary") {
      output.summary();
  } else if (output_style == "list") {
      output.list();
  } else {
    derr << "Bad output command '" << output_style << "'" << dendl;
    return -EINVAL;
  }

  return 0;
}

/**
 * Provide the user with information about the condition of the journal,
 * especially indicating what range of log events is available and where
 * any gaps or corruptions in the journal are.
 */
int JournalTool::journal_inspect()
{
  int r;

  JournalFilter filter;
  JournalScanner js(io, rank, filter);
  r = js.scan();
  if (r) {
    derr << "Failed to scan journal (" << cpp_strerror(r) << ")" << dendl;
    return r;
  }

  dout(1) << "Journal scanned, healthy=" << js.is_healthy() << dendl;

  return 0;
}


/**
 * Attempt to export a binary dump of the journal.
 *
 * This is allowed to fail if the header is malformed or there are
 * objects inaccessible, in which case the user would have to fall
 * back to manually listing RADOS objects and extracting them, which
 * they can do with the ``rados`` CLI.
 */
int JournalTool::journal_export(std::string const &path, bool import)
{
  int r = 0;
  Dumper dumper;
  JournalScanner js(io, rank);

  /*
   * Check that the header is valid and no objects are missing before
   * trying to dump
   */
  r = js.scan();
  if (r < 0) {
    derr << "Unable to scan journal, assuming badly damaged" << dendl;
    return r;
  }
  if (!js.is_readable()) {
    derr << "Journal not readable, attempt object-by-object dump with `rados`" << dendl;
    return -EIO;
  }

  /*
   * Assuming we can cleanly read the journal data, dump it out to a file
   */
  r = dumper.init(rank);
  if (r < 0) {
    derr << "dumper::init failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  if (import) {
    dumper.undump(path.c_str());
  } else {
    dumper.dump(path.c_str());
  }
  dumper.shutdown();

  return r;
}


/**
 * Truncate journal and insert EResetJournal
 */
int JournalTool::journal_reset()
{
  int r = 0;
  Resetter resetter;
  r = resetter.init(rank);
  if (r < 0) {
    derr << "resetter::init failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  resetter.reset();
  resetter.shutdown();

  return r;
}

std::string JournalScanner::obj_name(uint64_t offset) const
{
  char header_name[60];
  snprintf(header_name, sizeof(header_name), "%llx.%08llx",
      (unsigned long long)(MDS_INO_LOG_OFFSET + rank),
      (unsigned long long)offset);
  return std::string(header_name);
}

/**
 * Read journal header, followed by sequential scan through journal space.
 *
 * Return 0 on success, else error code.  Note that success has the special meaning
 * that we were able to apply our checks, it does *not* mean that the journal is
 * healthy.
 */
int JournalScanner::scan(bool const full)
{
  int r = 0;


  r = scan_header();
  if (r < 0) {
    return r;
  }
  if (full) {
    r = scan_events();
    if (r < 0) {
      return r;
    }
  }

  return 0;
}

int JournalScanner::scan_header()
{
  int r;

  bufferlist header_bl;
  std::string header_name = obj_name(0);
  dout(4) << "JournalScanner::scan: reading header object '" << header_name << "'" << dendl;
  r = io.read(header_name, header_bl, INT_MAX, 0);
  if (r < 0) {
    derr << "Header " << header_name << " is unreadable" << dendl;
    return 0;  // "Successfully" found an error
  } else {
    header_present = true;
  }

  bufferlist::iterator header_bl_i = header_bl.begin();
  header = new Journaler::Header();
  try
  {
    header->decode(header_bl_i);
  }
  catch (buffer::error e)
  {
    derr << "Header is corrupt (" << e.what() << ")" << dendl;
    return 0;  // "Successfully" found an error
  }

  if (header->magic != std::string(CEPH_FS_ONDISK_MAGIC)) {
    derr << "Header is corrupt (bad magic)" << dendl;
    return 0;  // "Successfully" found an error
  }
  if (!((header->trimmed_pos <= header->expire_pos) && (header->expire_pos <= header->write_pos))) {
    derr << "Header is corrupt (inconsistent offsets)" << dendl;
    return 0;  // "Successfully" found an error
  }
  header_valid = true;

  return 0;
}


/**
 * Call this when having read from read_buf we find that 
 * the contents are not the start of a valid entry: discard
 * the current position as part of a gap.
 */
void JournalScanner::gap_advance()
{

}


int JournalScanner::scan_events()
{
  int r;

  uint64_t object_size = g_conf->mds_log_segment_size;
  if (object_size == 0) {
    // Default layout object size
    object_size = g_default_file_layout.fl_object_size;
  }

  uint64_t read_offset = header->expire_pos;
  dout(10) << std::hex << "Header 0x"
    << header->trimmed_pos << " 0x"
    << header->expire_pos << " 0x"
    << header->write_pos << std::dec << dendl;
  dout(10) << "Starting journal scan from offset 0x" << std::hex << read_offset << std::dec << dendl;

  // TODO also check for extraneous objects before the trimmed pos or after the write pos,
  // which would indicate a bogus header.

  bufferlist read_buf;
  bool gap = false;
  uint64_t gap_start = -1;
  for (uint64_t obj_offset = (read_offset / object_size); ; obj_offset++) {
    // Read this journal segment
    bufferlist this_object;
    r = io.read(obj_name(obj_offset), this_object, INT_MAX, 0);
    this_object.copy(0, this_object.length(), read_buf);

    // Handle absent journal segments
    if (r < 0) {
      if (obj_offset > (header->write_pos / object_size)) {
        dout(4) << "Reached end of journal objects" << dendl;
        break;
      } else {
        derr << "Missing object " << obj_name(obj_offset) << dendl;
      }

      objects_missing.push_back(obj_offset);
      gap = true;
      gap_start = read_offset;
      continue;
    } else {
      objects_valid.push_back(obj_name(obj_offset));
    }

    if (gap) {
      // No valid data at the current read offset, scan forward until we find something valid looking
      // or have to drop out to load another object.
      dout(4) << "Searching for sentinel from 0x" << std::hex << read_buf.length() << std::dec << " bytes available" << dendl;

      do {
        bufferlist::iterator p;
        uint64_t candidate_sentinel;
        ::decode(candidate_sentinel, p);

        if (candidate_sentinel == JournalStream::sentinel) {
          ranges_invalid.push_back(Range(gap_start, -1));
        } else {
          // No sentinel, discard this byte
          read_buf.splice(0, 1);
          read_offset += 1;
        }
      } while (read_buf.length() >= sizeof(JournalStream::sentinel));
      
      assert(0);
    } else {
      dout(10) << "Parsing data, 0x" << std::hex << read_buf.length() << std::dec << " bytes available" << dendl;
      while(true) {
        // TODO: detect and handle legacy format journals: can do many things
        // on them but on read errors have to give up instead of searching
        // for sentinels.
        JournalStream journal_stream(JOURNAL_FORMAT_RESILIENT);
        bool readable = false;
        try {
          uint64_t need;
          readable = journal_stream.readable(read_buf, need);
        } catch (buffer::error e) {
          readable = false;
          dout(4) << "Invalid container encoding at 0x" << std::hex << read_offset << std::dec << dendl;
          gap = true;
          gap_start = read_offset;
          read_buf.splice(0, 1);
          read_offset += 1;
          break;
        }

        if (!readable) {
          // Out of data, continue to read next object
          break;
        }

        bufferlist le_bl;  //< Serialized LogEvent blob
        dout(10) << "Attempting decode at 0x" << std::hex << read_offset << std::dec << dendl;
        // This cannot fail to decode because we pre-checked that a serialized entry
        // blob would be readable.
        uint64_t start_ptr = 0;
        uint64_t consumed = journal_stream.read(read_buf, le_bl, start_ptr);
        if (start_ptr != read_offset) {
          derr << "Bad entry start ptr at 0x" << std::hex << start_ptr << std::dec << dendl;
          gap = true;
          gap_start = read_offset;
          break;
        }

        LogEvent *le = LogEvent::decode(le_bl);
        if (le) {
          dout(10) << "Valid entry at 0x" << std::hex << read_offset << std::dec << dendl;

          if (filter.apply(read_offset, *le)) {
            events[read_offset] = le;
          } else {
            delete le;
          }
          events_valid.push_back(read_offset);
          read_offset += consumed;
        } else {
          dout(10) << "Invalid entry at 0x" << std::hex << read_offset << std::dec << dendl;
          gap = true;
          gap_start = read_offset;
          read_buf.splice(0, 1);
          read_offset += 1;
        }
      }
    }
  }

  if (gap) {
    // Ended on a gap, assume it ran to end
    ranges_invalid.push_back(Range(gap_start, -1));
  }

  dout(4) << "Scanned objects, " << objects_missing.size() << " missing, " << objects_valid.size() << " valid" << dendl;
  dout(4) << "Events scanned, " << ranges_invalid.size() << " gaps" << dendl;
  dout(4) << "Found " << events_valid.size() << " valid events" << dendl;
  dout(4) << "Selected " << events.size() << " events for output" << dendl;

  return 0;
}

JournalScanner::~JournalScanner()
{
  if (header) {
    delete header;
    header = NULL;
  }
  dout(4) << events.size() << " events" << dendl;
  for (EventMap::iterator i = events.begin(); i != events.end(); ++i) {
    delete i->second;
  }
  events.clear();
}

/**
 * Whether the journal data looks valid and replayable
 */
bool JournalScanner::is_healthy() const
{
  return (header_present && header_valid && ranges_invalid.empty() && objects_missing.empty());
}

/**
 * Whether the journal data can be read from RADOS
 */
bool JournalScanner::is_readable() const
{
  return (header_present && header_valid && objects_missing.empty());
}

bool JournalFilter::apply(uint64_t pos, LogEvent &le) const
{
  /* Filtering by journal offset range */
  if (pos < range_start || pos >= range_end) {
    return false;
  }

  /* Filtering by file path */
  if (!path_expr.empty()) {
    EMetaBlob *metablob = le.get_metablob();
    if (metablob) {
      std::vector<std::string> paths;
      metablob->get_paths(paths);
      bool match_any = false;
      for (std::vector<std::string>::iterator p = paths.begin(); p != paths.end(); ++p) {
        if ((*p).find(path_expr) != std::string::npos) {
          match_any = true;
          break;
        }
      }
      if (!match_any) {
        return false;
      }
    } else {
      return false;
    }
  }

  /* Filtering by inode */
  if (inode) {
    EMetaBlob *metablob = le.get_metablob();
    if (metablob) {
      std::set<inodeno_t> inodes;
      metablob->get_inodes(inodes);
      bool match_any = false;
      for (std::set<inodeno_t>::iterator i = inodes.begin(); i != inodes.end(); ++i) {
        if (*i == inode) {
          match_any = true;
          break;
        }
      }
      if (!match_any) {
        return false;
      }
    } else {
      return false;
    }
  }

  return true;
}

void EventOutputter::binary() const
{
  // Binary output, files
  boost::filesystem::create_directories(boost::filesystem::path(path));
  for (JournalScanner::EventMap::const_iterator i = scan.events.begin(); i != scan.events.end(); ++i) {
    LogEvent *le = i->second;
    bufferlist le_bin;
    le->encode(le_bin);

    std::stringstream filename;
    filename << "0x" << std::hex << i->first << std::dec << "_" << le->get_type_str() << ".bin";
    std::string const file_path = path + std::string("/") + filename.str();
    boost::filesystem::ofstream bin_file(file_path, std::ofstream::out | std::ofstream::binary);
    le_bin.write_stream(bin_file);
    bin_file.close();
  }
  dout(1) << "Wrote output to binary files in directory '" << path << "'" << dendl;
}

void EventOutputter::json() const
{
  JSONFormatter jf(true);
  boost::filesystem::ofstream out_file(path, std::ofstream::out);
  jf.open_array_section("journal");
  {
    for (JournalScanner::EventMap::const_iterator i = scan.events.begin(); i != scan.events.end(); ++i) {
      LogEvent *le = i->second;
      jf.open_object_section("log_event");
      {
        le->dump(&jf);
      }
      jf.close_section();  // log_event
    }
  }
  jf.close_section();  // journal
  jf.flush(out_file);
  out_file.close();
  dout(1) << "Wrote output to JSON file '" << path << "'" << dendl;
}

void EventOutputter::list() const
{
  for (JournalScanner::EventMap::const_iterator i = scan.events.begin(); i != scan.events.end(); ++i) {
    std::vector<std::string> ev_paths;
    std::string detail;
    if (i->second->get_type() == EVENT_UPDATE) {
      EUpdate *eu = reinterpret_cast<EUpdate*>(i->second);
      eu->metablob.get_paths(ev_paths);
      detail = eu->type;
    }
    dout(1) << "0x"
      << std::hex << i->first << std::dec << " "
      << i->second->get_type_str() << ": "
      << " (" << detail << ")" << dendl;
    for (std::vector<std::string>::iterator i = ev_paths.begin(); i != ev_paths.end(); ++i) {
      dout(1) << "  " << *i << dendl;
    }
  }
}

void EventOutputter::summary() const
{
  std::map<std::string, int> type_count;
  for (JournalScanner::EventMap::const_iterator i = scan.events.begin(); i != scan.events.end(); ++i) {
    std::string const type = i->second->get_type_str();
    if (type_count.count(type) == 0) {
      type_count[type] = 0;
    }
    type_count[type] += 1;
  }

  dout(1) << "Events by type:" << dendl;
  for (std::map<std::string, int>::iterator i = type_count.begin(); i != type_count.end(); ++i) {
    dout(1) << "  " << i->first << ": " << i->second << dendl;
  }
}

int JournalFilter::parse_args(
  std::vector<const char*> &argv, 
  std::vector<const char*>::iterator &arg)
{
  while(arg != argv.end()) {
    std::string arg_str;
    if (ceph_argparse_witharg(argv, arg, &arg_str, "--range", (char*)NULL)) {
      size_t sep_loc = arg_str.find(JournalFilter::range_separator);
      if (sep_loc == std::string::npos || arg_str.size() <= JournalFilter::range_separator.size()) {
        derr << "Invalid range '" << arg_str << "'" << dendl;
        return -EINVAL;
      }

      // We have a lower bound
      if (sep_loc > 0) {
        std::string range_start_str = arg_str.substr(0, sep_loc); 
        std::string parse_err;
        range_start = strict_strtoll(range_start_str.c_str(), 0, &parse_err);
        if (!parse_err.empty()) {
          derr << "Invalid lower bound '" << range_start_str << "': " << parse_err << dendl;
          return -EINVAL;
        }
      }

      if (sep_loc < arg_str.size() - JournalFilter::range_separator.size()) {
        std::string range_end_str = arg_str.substr(sep_loc + range_separator.size()); 
        std::string parse_err;
        range_end = strict_strtoll(range_end_str.c_str(), 0, &parse_err);
        if (!parse_err.empty()) {
          derr << "Invalid upper bound '" << range_end_str << "': " << parse_err << dendl;
          return -EINVAL;
        }
      }
    } else if (ceph_argparse_witharg(argv, arg, &arg_str, "--path", (char*)NULL)) {
      dout(4) << "Filtering by path '" << arg_str << "'" << dendl;
      path_expr = arg_str;
    } else if (ceph_argparse_witharg(argv, arg, &arg_str, "--inode", (char*)NULL)) {
        dout(4) << "Filtering by inode '" << arg_str << "'" << dendl;
        std::string parse_err;
        inode = strict_strtoll(arg_str.c_str(), 0, &parse_err);
        if (!parse_err.empty()) {
          derr << "Invalid inode '" << arg_str << "': " << parse_err << dendl;
          return -EINVAL;
        }
    } else {
      // We're done with args the filter understands
      break;
    }
  }

  return 0;
}

int JournalTool::replay_offline(EMetaBlob &metablob, bool const dry_run)
{
  int r;

  // Replay roots
  for (list<ceph::shared_ptr<EMetaBlob::fullbit> >::iterator p = metablob.roots.begin(); p != metablob.roots.end(); ++p) {
    EMetaBlob::fullbit &fb = *(*p);
    inodeno_t ino = fb.inode.ino;
    dout(4) << __func__ << ": updating root 0x" << std::hex << ino << std::dec << dendl;

    object_t root_oid = InodeStore::get_object_name(ino, frag_t(), ".inode");
    dout(4) << __func__ << ": object id " << root_oid.name << dendl;

    bufferlist inode_bl;
    r = io.read(root_oid.name, inode_bl, (1<<22), 0);
    InodeStore inode;
    if (r == -ENOENT) {
      dout(4) << __func__ << ": root does not exist, will create" << dendl;
    } else {
      dout(4) << __func__ << ": root exists, will modify (" << inode_bl.length() << ")" << dendl;
      // TODO: add some kind of force option so that we can overwrite bad inodes
      // from the journal if needed
      bufferlist::iterator inode_bl_iter = inode_bl.begin(); 
      std::string magic;
      ::decode(magic, inode_bl_iter);
      if (magic == CEPH_FS_ONDISK_MAGIC) {
        dout(4) << "magic ok" << dendl;
      } else {
        dout(4) << "magic bad: '" << magic << "'" << dendl;
      }
      inode.decode(inode_bl_iter);
    }

    // This is a distant cousin of EMetaBlob::fullbit::update_inode, but for use
    // on an offline InodeStore instance.  It's way simpler, because we are just
    // uncritically hauling the data between structs.
    inode.inode = fb.inode;
    inode.xattrs = fb.xattrs;
    inode.dirfragtree = fb.dirfragtree;
    inode.snap_blob = fb.snapbl;
    inode.symlink = fb.symlink;
    inode.old_inodes = fb.old_inodes;

    inode_bl.clear();
    std::string magic = CEPH_FS_ONDISK_MAGIC;
    ::encode(magic, inode_bl);
    inode.encode(inode_bl);

    if (!dry_run) {
      r = io.write_full(root_oid.name, inode_bl);
      assert(r == 0);
    }
  }

  // TODO: respect metablob.renamed_dirino (cues us as to which dirlumps
  // indicate renamed directories)

  // Replay fullbits (dentry+inode)
  for (list<dirfrag_t>::iterator lp = metablob.lump_order.begin(); lp != metablob.lump_order.end(); ++lp) {
    dirfrag_t &frag = *lp;
    EMetaBlob::dirlump &lump = metablob.lump_map[frag];
    lump._decode_bits();
    object_t frag_object_id = InodeStore::get_object_name(frag.ino, frag.frag, "");

    // Check for presence of dirfrag object
    uint64_t psize;
    time_t pmtime;
    r = io.stat(frag_object_id.name, &psize, &pmtime);
    if (r == -ENOENT) {
      dout(4) << "Frag object " << frag_object_id.name << " did not exist, will create" << dendl;
    } else if (r != 0) {
      // FIXME: what else can happen here?  can I deal?
      assert(r == 0);
    } else {
      dout(4) << "Frag object " << frag_object_id.name << " exists, will modify" << dendl;
    }

    // Write fnode to omap header of dirfrag object
    bufferlist fnode_bl;
    lump.fnode.encode(fnode_bl);
    if (!dry_run) {
      r = io.omap_set_header(frag_object_id.name, fnode_bl);
      if (r != 0) {
        derr << "Failed to write fnode for frag object " << frag_object_id.name << dendl;
        return r;
      }
    }

    // Try to get the existing dentry
    list<ceph::shared_ptr<EMetaBlob::fullbit> > &fb_list = lump.get_dfull();
    for (list<ceph::shared_ptr<EMetaBlob::fullbit> >::iterator fbi = fb_list.begin(); fbi != fb_list.end(); ++fbi) {
      EMetaBlob::fullbit &fb = *(*fbi);

      // Get a key like "foobar_head"
      std::string key;
      dentry_key_t dn_key(fb.dnlast, fb.dn.c_str());
      dn_key.encode(key);

      // See if the dentry is present
      std::set<std::string> keys;
      keys.insert(key);
      std::map<std::string, bufferlist> vals;
      r = io.omap_get_vals_by_keys(frag_object_id.name, keys, &vals);
      assert (r == 0);  // I assume success because I checked object existed and absence of 
                        // dentry gives me empty map instead of failure
                        // FIXME handle failures so we can replay other events
                        // if this one is causing some unexpected issue
    
      if (vals.find(key) == vals.end()) {
        dout(4) << "dentry " << key << " does not exist, will be created" << dendl;
      } else {
        dout(4) << "dentry " << key << " existed already" << dendl;
        // TODO: read out existing dentry before adding new one so that
        // we can print a bit of info about what we're overwriting
      }
    
      bufferlist dentry_bl;
      ::encode(fb.dnfirst, dentry_bl);
      ::encode('I', dentry_bl);

      InodeStore inode;
      inode.inode = fb.inode;
      inode.xattrs = fb.xattrs;
      inode.dirfragtree = fb.dirfragtree;
      inode.snap_blob = fb.snapbl;
      inode.symlink = fb.symlink;
      inode.old_inodes = fb.old_inodes;
      inode.encode_bare(dentry_bl);
      
      vals[key] = dentry_bl;
      if (!dry_run) {
        r = io.omap_set(frag_object_id.name, vals);
        assert(r == 0);  // FIXME handle failures
      }
    }

    list<EMetaBlob::nullbit> &nb_list = lump.get_dnull();
    for (list<EMetaBlob::nullbit>::const_iterator
	iter = nb_list.begin(); iter != nb_list.end(); ++iter) {
      EMetaBlob::nullbit const &nb = *iter;

      // Get a key like "foobar_head"
      std::string key;
      dentry_key_t dn_key(nb.dnlast, nb.dn.c_str());
      dn_key.encode(key);

      // Remove it from the dirfrag
      dout(4) << "Removing dentry " << key << dendl;
      std::set<std::string> keys;
      keys.insert(key);
      if (!dry_run) {
        r = io.omap_rm_keys(frag_object_id.name, keys);
        assert(r == 0);
      }
    }
  }

  for (std::vector<inodeno_t>::iterator i = metablob.destroyed_inodes.begin();
       i != metablob.destroyed_inodes.end(); ++i) {
    dout(4) << "Destroyed inode: " << *i << dendl;
    // TODO: if it was a dir, then delete its dirfrag objects
  }

  return 0;
}

