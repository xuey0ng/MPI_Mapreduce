/***************************************************** 
 * Note: this file is provided for the convenience of
 *       students. Students can choose to edit, delete
 *       or even leave the file as-is. This file will
 *       NOT be replaced during grading.
 ****************************************************/
#ifndef UTILS_H_

#define UTILS_H_

typedef struct _KeyValueArr {
    char key[8];
    int *val;
    int len;
    int max_size;
} KeyValueArr;

int partition(char *key, int num_partitions);
char *read_from_file(char const *file_path);
void write_to_file(const char* file_path, const char* data);
void append_to_file(const char* file_path, const char* data);

#endif 
