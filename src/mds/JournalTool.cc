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

void JournalTool::init()
{
  MDSUtility::init();
}

void JournalTool::shutdown()
{
  MDSUtility::shutdown();
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
      derr << "Unknown argument: '" << *arg << dendl;
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
  for (
      uint64_t obj_offset = (read_offset / object_size);
      obj_offset <= (header->write_pos / object_size);
      obj_offset++) {

    // Read this journal segment
    bufferlist this_object;
    r = io.read(obj_name(obj_offset), this_object, INT_MAX, 0);
    this_object.copy(0, this_object.length(), read_buf);

    // Handle absent journal segments
    if (r < 0) {
      derr << "Missing object " << obj_name(obj_offset) << dendl;
      objects_missing.push_back(obj_offset);
      gap = true;
      gap_start = read_offset;
      continue;
    } else {
      objects_valid.push_back(obj_name(obj_offset));
    }

    // Consume available events
    if (gap) {
      // We're coming out the other side of a gap, search up to the next sentinel
      dout(4) << "Searching for sentinel from 0x" << std::hex << read_buf.length() << std::dec << " bytes available" << dendl;
      while(read_buf.length() >= sizeof(Journaler::sentinel)) {
      }
      // TODO
      // 1. Search forward for sentinel
      // 2. When you find sentinel, point read_offset at it and try reading an entry, especially validate that start_ptr is
      //    correct.
      assert(0);
    } else {
      dout(10) << "Parsing data, 0x" << std::hex << read_buf.length() << std::dec << " bytes available" << dendl;
      while(true) {
        uint32_t entry_size = 0;
        uint64_t start_ptr = 0;
        uint64_t entry_sentinel;
        bufferlist::iterator p = read_buf.begin();
        if (read_buf.length() >= sizeof(entry_sentinel) + sizeof(entry_size)) {
          ::decode(entry_sentinel, p);
          ::decode(entry_size, p);
        } else {
          // Out of data, continue to read next object
          break;
        }

        if (entry_sentinel != Journaler::sentinel) {
          dout(4) << "Invalid sentinel at 0x" << std::hex << read_offset << std::dec << dendl;
          gap = true;
          gap_start = read_offset;
          break;
        }

        if (read_buf.length() >= sizeof(entry_sentinel) + sizeof(entry_size) + entry_size + sizeof(start_ptr)) {
          dout(10) << "Attempting decode at 0x" << std::hex << read_offset << std::dec << dendl;
          bufferlist le_bl;
          read_buf.splice(0, sizeof(entry_sentinel));
          read_buf.splice(0, sizeof(entry_size));
          read_buf.splice(0, entry_size, &le_bl);
          read_buf.splice(0, sizeof(start_ptr));
          // TODO: read out start_ptr and ensure that it points back to read_offset
          // FIXME: LogEvent::decode_event contains an assertion that there is no trailing
          // data, but we would want it to return NULL or somesuch in that case so that
          // a corruption of length doesn't stop the scan.
          LogEvent *le = LogEvent::decode(le_bl);
          if (le) {
            dout(10) << "Valid entry at 0x" << std::hex << read_offset << std::dec << dendl;

            if (filter.apply(read_offset, *le)) {
              events[read_offset] = le;
            } else {
              delete le;
            }
            events_valid.push_back(read_offset);
            read_offset += sizeof(entry_sentinel) + sizeof(entry_size) + entry_size + sizeof(start_ptr);
          } else {
            dout(10) << "Invalid entry at 0x" << std::hex << read_offset << std::dec << dendl;
            // Ensure we don't hit this entry again when scanning for next sentinel
            read_offset += 1;
            gap = true;
            gap_start = read_offset;
          }
        } else {
          // Out of data, continue to read next object
          break;
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
#if 0
  dout(4) << events.size() << " events" << dendl;
  for (EventMap::iterator i = events.begin(); i != events.end(); ++i) {
    dout(4) << "Deleting " << i->second << dendl;
    delete i->second;
    dout(4) << "Deleted" << dendl;
  }
  events.clear();
#endif
}

bool JournalScanner::is_healthy() const
{
  return (header_present && header_valid && ranges_invalid.empty() && objects_missing.empty());
}

bool JournalFilter::apply(uint64_t pos, LogEvent &le) const
{
  if (pos < range_start || pos >= range_end) {
    return false;
  }

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
    } else {
      // We're done with args the filter understands
      break;
    }
  }

  return 0;
}

