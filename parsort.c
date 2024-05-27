
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Compare function for qsort. Compares two int64_t values.
// Parameters:
//  - left_: pointer to the left comparison value
//  - right_: pointer to the right comparison value
// Returns: -1 if left < right, 1 if left > right, 0 if they are equal
int compare_i64(const void *left_, const void *right_) {
  int64_t left = *(int64_t *)left_;
  int64_t right = *(int64_t *)right_;
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

// Sorts a segment of an array of int64_t in ascending order using qsort.
// Parameters:
//  - arr: pointer to the first element of the array
//  - begin: index of the first element to sort
//  - end: index one past the last element to sort
void seq_sort(int64_t *arr, size_t begin, size_t end) {
  size_t num_elements = end - begin;
  qsort(arr + begin, num_elements, sizeof(int64_t), compare_i64);
}


// Merges two sorted halves of an array into a single sorted segment.
// Parameters:
//  - arr: pointer to the first element of the array
//  - begin: start index of the first sorted segment
//  - mid: end index of the first sorted segment and start index of the second
//  - end: end index of the second sorted segment
//  - temparr: temporary array to hold the merged result
void merge(int64_t *arr, size_t begin, size_t mid, size_t end, int64_t *temparr) {
  int64_t *endl = arr + mid;
  int64_t *endr = arr + end;
  int64_t *left = arr + begin, *right = arr + mid, *dst = temparr;

  for (;;) {
    int at_end_l = left >= endl;
    int at_end_r = right >= endr;

    if (at_end_l && at_end_r) break;

    if (at_end_l)
      *dst++ = *right++;
    else if (at_end_r)
      *dst++ = *left++;
    else {
      int cmp = compare_i64(left, right);
      if (cmp <= 0)
        *dst++ = *left++;
      else
        *dst++ = *right++;
    }
  }
}

// Prints a fatal error message to stderr and exits.
// Parameters:
//  - msg: error message to print
void fatal(const char *msg) __attribute__ ((noreturn));
void fatal(const char *msg) {
  fprintf(stderr, "Error: %s\n", msg);
  exit(1);
}

void merge_sort(int64_t *arr, size_t begin, size_t end, size_t threshold);



//
// Waits for a sorting process to complete.
// Parameters:
//  - pid: PID of the child process to wait for.
void wait_for_sort_process(pid_t pid) {
  int status;
  if (waitpid(pid, &status, 0) == -1) {
    fatal("Error: waitpid failure");
  }
  if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
    fatal("Error: Child process exited with error");
  }
}


 // Creates a process to sort a portion of the array.
 // Parameters:
 //  - arr: Pointer to the array to sort.
 //  - begin: Index of the first element in the segment to sort.
 //  - end: Index one past the last element in the segment to sort.
 //  - threshold: Size below which sequential sort is used instead of merge sort.
 // Returns: PID of the child process created for sorting.
pid_t create_sort_process(int64_t *arr, size_t begin, size_t end, size_t threshold) {
  pid_t pid = fork();
  if (pid == -1) {
    fatal("Error: fork failed to start a new process");
  } else if (pid == 0) { // In child process
    merge_sort(arr, begin, end, threshold);
    exit(0);
  }
  return pid; // Parent process returns child PID
}


 // Performs a merge sort on an array of int64_t. If the size of the segment to be sorted is
 // less than or equal to a threshold, it uses sequential sort instead of continuing with merge sort.
 // Parameters:
 //  - arr: Pointer to the array to sort.
 //  - begin: Index of the first element to sort.
 //  - end: Index one past the last element to sort.
 //  - threshold: Size below which sequential sort is used instead of merge sort.
void merge_sort(int64_t *arr, size_t begin, size_t end, size_t threshold) {
  assert(end >= begin);
  size_t size = end - begin;

  if (size <= threshold) {
    seq_sort(arr, begin, end);
    return;
  }

  size_t mid = begin + size / 2;

  // Create and manage sorting processes for each half
  pid_t left_pid = create_sort_process(arr, begin, mid, threshold);
  pid_t right_pid = create_sort_process(arr, mid, end, threshold);

  // Wait for both processes to complete
  wait_for_sort_process(left_pid);
  wait_for_sort_process(right_pid);

  // Merge sorted halves
  int64_t *temp_arr = (int64_t *)malloc(size * sizeof(int64_t));
  if (temp_arr == NULL) {
    fatal("malloc() failed");
  }
  merge(arr, begin, mid, end, temp_arr);

  // Copy data back to main array and free temporary array
  for (size_t i = 0; i < size; i++) 
    arr[begin + i] = temp_arr[i];
  free(temp_arr);
}



int main(int argc, char **argv) {
  // check for correct number of command line arguments
  if (argc != 3) {
    fprintf(stderr, "Usage: %s <filename> <sequential threshold>\n", argv[0]);
    return 1;
  }

  // process command line arguments
  const char *filename = argv[1];
  char *end;
  size_t threshold = (size_t) strtoul(argv[2], &end, 10);
  if (end != argv[2] + strlen(argv[2])) {
    fprintf(stderr, "Error: threshold value is invalid\n");
  }

  int file = open(filename, O_RDWR);

  // Error Handling: Failed to open file
  if (file < 0) {
    fprintf(stderr, "Error: failed to open file\n");

    return 1;
  }

  struct stat stat_file;
  int rc = fstat(file, &stat_file);

  // Error Handling: Failed to get file stats
  if (rc != 0) {
    perror("Error: Failed to get file status\n");
    close(file);
    return 1;
  }

  size_t file_size = stat_file.st_size;
  int64_t *data = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, file, 0);

  // Error Handling: Failed to map file
  if (data == MAP_FAILED) {
    perror("Error: Failed to map the file\n");
    close(file);
    return 1;
  }

  merge_sort(data, 0, file_size / 8, threshold);

  // Clean up
  if (munmap(data, file_size) != 0) {
    fprintf(stderr, "Failed to unmap the file\n");
  }
  if (close(file) != 0) {
    fprintf(stderr, "Failed to close the file\n");
    return 1;
  }

  return 0;
}
