#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define ARG_IDX_ID 2
#define ARG_IDX_TYPE 1
#define ARG_MIN 3
#define ARG_TYPE_MASTER "master"
#define ARG_TYPE_WORKER "worker"
#define DIRENTRY_DOT "."
#define DIRENTRY_DOTDOT ".."
#define EXIT_FAILED -1
#define FILENAME_INPUT "input"
#define FILENAME_INPUTTEMP "intemp"
#define FILENAME_LOCK ".lock"
#define FILENAME_OUTPUT "output"
#define FILENAME_OUTPUTTEMP "outtemp"
#define FILENAME_PROBLEM "problem"
#define FOLDER_STORE "store"
#define MAX_IDLE_SECONDS 10
#define MAX_LOCKFILE_AGE_SECONDS 20
#define N_PROBLEMS 10
#define SLEEP_SECONDS_IDLE 5
#define SLEEP_SECONDS_PRETEND_WORKING 3
#define TAKE_LOCKFILE_EXISTENCE_AS_LOCK false

void print_usage(char* name)
{
  std::cout << "Usage: " << name << " <master|worker> <id>" << std::endl;
}

inline bool file_exists(const std::string& path_to_file) {
  struct stat buffer;
  return(stat(path_to_file.c_str(), &buffer) == 0);
}

/*! Try to get lock. Return its file descriptor or -1 if failed.
 *
 *  @param path_to_lock_file Name of file used as lock (i.e. '/var/lock/myLock').
 *  @return File descriptor of lock file, or -1 if failed.
 */
int try_get_lock(const std::string& path_to_lock_file)
{
  mode_t m = umask(0);
  int fd = open(path_to_lock_file.c_str(), O_RDWR | O_CREAT, 0666);
  umask(m);
  if (fd >= 0 && flock(fd, LOCK_EX | LOCK_NB ) < 0) {
    close(fd);
    fd = -1;
  }
  return fd;
}

/*! Release the lock obtained with try_get_lock(path_to_lock_file).
 *
 *  @param fd File descriptor of lock returned by try_get_lock(path_to_lock_file).
 *  @param path_to_lock_file Name of file used as lock (i.e. '/var/lock/myLock').
 */
void release_lock(int fd, const std::string& path_to_lock_file)
{
    if (fd < 0)
      return;
    remove(path_to_lock_file.c_str());
    close(fd);
}

bool replace(std::string& str, const std::string& from, const std::string& to) {
    size_t start_pos = str.find(from);
    if (start_pos == std::string::npos)
        return false;
    str.replace(start_pos, from.length(), to);
    return true;
}

void worker_process_file(int worker_id, const std::string& path_to_file)
{
  std::cout << "Processing file " << path_to_file << std::endl;
  sleep(SLEEP_SECONDS_PRETEND_WORKING);
  std::string path_to_output_file = path_to_file;
  replace(path_to_output_file, FILENAME_INPUT, FILENAME_OUTPUT);
  std::cout << "Output file is " << path_to_output_file << std::endl;
  std::string path_to_temp_output_file = path_to_file;
  replace(path_to_temp_output_file, FILENAME_INPUT, FILENAME_OUTPUTTEMP);
  path_to_temp_output_file += "." + worker_id;
  char hostname[1024];
  gethostname(hostname, 1024);
  std::ostringstream filename_suffix;
  filename_suffix << ".worker" << worker_id << "." << hostname;
  path_to_temp_output_file += filename_suffix.str();
  std::cout << "Temp output file is " << path_to_temp_output_file << std::endl;
  std::ofstream wfile;
  wfile.open(path_to_temp_output_file.c_str());
  wfile << "Solution to problem in file " << path_to_file << std::endl;
  wfile << "Done by worker " << worker_id << " on host " << hostname << std::endl;
  wfile.close();
  std::cout << "Renaming to output file." << std::endl;
  rename(path_to_temp_output_file.c_str(), path_to_output_file.c_str());
  std::cout << "Done processing file " << path_to_file << std::endl;
}

bool worker_inspect_dir_entry(int worker_id, const std::string& path_to_file)
{
  bool retval = false;
  std::cout << "Inspecting " << path_to_file << std::endl;
  if ((path_to_file.find(FILENAME_INPUT) != std::string::npos) &&
      (path_to_file.find(FILENAME_LOCK) == std::string::npos)) {
    std::cout << "Seems to be an input file." << std::endl;
    std::string path_to_lock_file = path_to_file + FILENAME_LOCK;
    std::string path_to_output_file = path_to_file;
    replace(path_to_output_file, FILENAME_INPUT, FILENAME_OUTPUT);
    bool should_consider_file;
    if (TAKE_LOCKFILE_EXISTENCE_AS_LOCK)
      should_consider_file = file_exists(path_to_file) &&
          !file_exists(path_to_output_file) && !file_exists(path_to_lock_file);
    else
      should_consider_file = file_exists(path_to_file) && !file_exists(path_to_output_file);
    if (should_consider_file) {
      std::cout << "Seems available." << std::endl;
      if (file_exists(path_to_lock_file)) {
        std::cout << "I see a lock file present." << std::endl;
      }
      std::cout << "Attempting to lock file." << std::endl;
      int ld = try_get_lock(path_to_lock_file);
      if (ld > 0) {
        std::cout << "Locked file." << std::endl;
        worker_process_file(worker_id, path_to_file);
        release_lock(ld, path_to_lock_file);
        retval = true;
      } else {
        std::cout << "Failed to lock file." << std::endl;
      }
    } else {
      std::cout << "Lock file or output file exists. Skipping." << std::endl;
    }
    std::cout << std::endl;
  }
  return retval;
}

void worker(int worker_id)
{
  std::cout << "Launching worker " << worker_id << "..." << std::endl;
  time_t time_last_working, time_now;
  time(&time_last_working);
  time(&time_now);
  double time_idle_secs = difftime(time_now, time_last_working);
  std::string store_prefix = std::string(FOLDER_STORE) + "/";
  while (time_idle_secs < MAX_IDLE_SECONDS) {
    DIR *dir;
    struct dirent *ent;
    std::cout << "Scanning store" << std::endl;
    if ((dir = opendir(FOLDER_STORE)) != NULL) {
      /* Process all the files and directories within directory */
      while ((ent = readdir(dir)) != NULL) {
        std::string filename(ent->d_name);
        if ((filename.compare(DIRENTRY_DOT) != 0) &&
            (filename.compare(DIRENTRY_DOTDOT) != 0)) {
          std::string path_to_file = store_prefix + filename;
          bool did_work = worker_inspect_dir_entry(worker_id, path_to_file);
          if (did_work) {
            time(&time_last_working);
          }
        }
      }
      closedir(dir);
    } else {
      /* could not open directory */
      perror("Could not open directory");
    }

    time(&time_now);
    time_idle_secs = difftime(time_now, time_last_working);
    std::cout << "Idle for " << time_idle_secs << "s" << std::endl;
    std::cout << "Sleeping for " << SLEEP_SECONDS_IDLE << "s" << std::endl;
    sleep(SLEEP_SECONDS_IDLE);
    std::cout << std::endl;
  }
  std::cout << "Worker " << worker_id << " exiting." << std::endl;
}

void master()
{
  std::cout << "Launching master..." << std::endl;

  /* Creating input files */

  std::cout << "Creating input files..." << std::endl;

  for (int i = 0; i < N_PROBLEMS; ++i) {
    std::ostringstream path_to_temp_input_file, path_to_input_file;
    path_to_temp_input_file << FOLDER_STORE << "/" << FILENAME_PROBLEM << "-" << i << "-" << FILENAME_INPUTTEMP;
    path_to_input_file      << FOLDER_STORE << "/" << FILENAME_PROBLEM << "-" << i << "-" << FILENAME_INPUT;
    const std::string path_to_temp_input_file_str = path_to_temp_input_file.str();
    const std::string path_to_input_file_str = path_to_input_file.str();

    std::ofstream inputfile;
    inputfile.open(path_to_temp_input_file_str.c_str());
    inputfile << "Problem " << i << std::endl;
    inputfile.close();
  }
  for (int i = 0; i < N_PROBLEMS; ++i) {
    std::ostringstream path_to_temp_input_file, path_to_input_file;
    path_to_temp_input_file << FOLDER_STORE << "/" << FILENAME_PROBLEM << "-" << i << "-" << FILENAME_INPUTTEMP;
    path_to_input_file      << FOLDER_STORE << "/" << FILENAME_PROBLEM << "-" << i << "-" << FILENAME_INPUT;
    const std::string path_to_temp_input_file_str = path_to_temp_input_file.str();
    const std::string path_to_input_file_str = path_to_input_file.str();

    rename(path_to_temp_input_file_str.c_str(), path_to_input_file_str.c_str());
  }
  std::cout << "Input files created..." << std::endl;

  /* Monitoring for output files */

  //bool output_ready[N_PROBLEMS];
  bool is_all_processed = false;
  while (!is_all_processed) {
    std::cout << "Monitoring output files..." << std::endl;
    sleep(SLEEP_SECONDS_IDLE);
    //std::fill(output_ready, output_ready + N_PROBLEMS, false);
    is_all_processed = true;
    int num_processed = 0;
    for (int i = 0; i < N_PROBLEMS; ++i) {
      std::ostringstream path_to_output_file;
      path_to_output_file << FOLDER_STORE << "/" << FILENAME_PROBLEM << "-" << i << "-" << FILENAME_OUTPUT;
      const std::string path_to_output_file_str = path_to_output_file.str();
      if (file_exists(path_to_output_file_str))
        num_processed++;
      else
        is_all_processed = false;

      std::ostringstream path_to_lock_file;
      path_to_lock_file << FOLDER_STORE << "/" << FILENAME_PROBLEM << "-" << i << "-" << FILENAME_INPUT << FILENAME_LOCK;
      const std::string path_to_lock_file_str = path_to_lock_file.str();
      if (file_exists(path_to_lock_file_str)) {
        struct stat attrib; // create a file attribute structure
        time_t time_file_modified, time_now;
        stat(path_to_lock_file_str.c_str(), &attrib); // get the attributes of the file
        time_file_modified = attrib.st_mtime; // Get the last modified time
        time(&time_now);
        double time_age_secs = difftime(time_now, time_file_modified);
        if (time_age_secs > MAX_LOCKFILE_AGE_SECONDS) {
          std::cout << "Lock file " << path_to_lock_file_str << " is " <<
              time_age_secs << "s old.  Deleting file..." << std::endl;
          remove(path_to_lock_file_str.c_str());
        }
      }
    }
    std::cout << num_processed << " out of " << N_PROBLEMS << " output files ready." << std::endl;
  }

  /* Collecting output files */
  std::cout << "All output files are ready." << std::endl;
  std::cout << "Master exiting." << std::endl;
}

int main(int argc, char* argv[])
{
  if (argc < ARG_MIN) {
    print_usage(argv[0]);
    return EXIT_FAILED;
  }

  std::stringstream ss(argv[ARG_IDX_ID]);
  int id;
  if (!(ss >> id)) {
    std::cerr << "Invalid number " << argv[ARG_IDX_ID] << std::endl;
    print_usage(argv[0]);
    return EXIT_FAILED;
  }

  if (strcmp(argv[ARG_IDX_TYPE], ARG_TYPE_MASTER) == 0)
    master();
  else if (strcmp(argv[ARG_IDX_TYPE], ARG_TYPE_WORKER) == 0)
    worker(id);
  else {
    print_usage(argv[0]);
    return EXIT_FAILED;
  }
  return 0;
}
