#include <mpi.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "tasks.h"
#include "utils.h"

long long wall_clock_time()
{
#ifdef __linux__
    struct timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    return (long long)(tp.tv_nsec + (long long)tp.tv_sec * 1000000000ll);
#else
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)(tv.tv_usec * 1000 + (long long)tv.tv_sec * 1000000000ll);
#endif
}

int main(int argc, char** argv)
{
    int start, end;
    start = wall_clock_time();

    MPI_Init(&argc, &argv);

    int world_size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Create MPI_Datatype for struct KeyValue */
    const int KV_COUNT = 2;
    const int KV_BLOCKLENGTHS[2] = {8, 1};
    const MPI_Aint KV_DISPLACEMENTS[2] = {offsetof(KeyValue, key), offsetof(KeyValue, val)};
    const MPI_Datatype KV_TYPES[2] = {MPI_CHAR, MPI_INT};
    MPI_Datatype MPI_KeyValue;

    MPI_Type_create_struct(KV_COUNT, KV_BLOCKLENGTHS, KV_DISPLACEMENTS, KV_TYPES, &MPI_KeyValue);
    MPI_Type_commit(&MPI_KeyValue);

    /* Get command-line params */
    char *input_files_dir = argv[1];
    int num_files = atoi(argv[2]);
    int num_map_workers = atoi(argv[3]);
    int num_reduce_workers = atoi(argv[4]);
    char *output_file_name = argv[5];
    int map_reduce_task_num = atoi(argv[6]);

    /* Create MPI intra_communicators for synchronisation */
	MPI_Group orig_group, new_group;
	MPI_Comm intra_comm;
    MPI_Request master_map_req;

    int ranks1[num_map_workers + 1];
    int ranks2[num_reduce_workers];

    for (int i = 0; i <= num_map_workers + num_reduce_workers; i++)
    {
        if (i <= num_map_workers)
        {
            ranks1[i] = i;
        }
        else
        {
            ranks2[i - num_map_workers - 1] = i;
        }
    }

	MPI_Comm_group(MPI_COMM_WORLD, &orig_group);

    if (rank <= num_map_workers)
    {
        MPI_Group_incl(orig_group, num_map_workers + 1, ranks1, &new_group);
    }
    else
    {
        MPI_Group_incl(orig_group, num_reduce_workers, ranks2, &new_group);
    }

    MPI_Comm_create(MPI_COMM_WORLD, new_group, &intra_comm);

    /* Tags i'm using for coomunication */
    const int READY_SIG = 1;
    const int REDUCE_MASTER = 11;

    /* Identify the specific map function to use */
    MapTaskOutput* (*map) (char*);
    switch(map_reduce_task_num){
        case 1:
            map = &map1;
            break;
        case 2:
            map = &map2;
            break;
        case 3:
            map = &map3;
            break;
    }

    /* Distinguish between master, map workers and reduce workers */
    if (rank == 0)
    {
        /* Initialise data structures to store input, and other relevant booking information. */
        char *file_path = (char *) malloc((strlen(input_files_dir) + 12));
        char **files = (char **) malloc(num_files * sizeof(char *));
        int message_length[num_files];

        /* 
           In order to maximise efficiency, master will read, and if there is a ready map worker, it will send.
           Otherwise, it will carry on reading.
        */
        int send_idx = 0;
        int is_ready;
        int recv_flag;

        MPI_Status send_stat;

        for (int i = 0; i < num_files; i++)
        {
            sprintf(file_path, "%s/%d.txt", input_files_dir, i);
            files[i] = read_from_file(file_path);
            message_length[i] = strlen(files[i]) + 1;
            MPI_Iprobe(MPI_ANY_SOURCE, READY_SIG, MPI_COMM_WORLD, &is_ready, &send_stat);

            /* Check if there are any map workers free to receive work. */
            if (is_ready)
            {
                /* Consume the request and send the file. */
                MPI_Recv(&recv_flag, 1, MPI_INT, READY_SIG, send_stat.MPI_TAG, MPI_COMM_WORLD, &send_stat);
                MPI_Send(files[send_idx], message_length[send_idx], MPI_CHAR, send_stat.MPI_SOURCE, send_stat.MPI_TAG, MPI_COMM_WORLD);
                send_idx++;
            }
            
        }

        /* There are still files yet to be sent, finish sending them */
        while (send_idx < num_files)
        {
            MPI_Iprobe(MPI_ANY_SOURCE, READY_SIG, MPI_COMM_WORLD, &is_ready, &send_stat);
            if (is_ready)
            {
                MPI_Recv(&recv_flag, 1, MPI_INT, READY_SIG, send_stat.MPI_TAG, MPI_COMM_WORLD, &send_stat);
                MPI_Send(files[send_idx], message_length[send_idx], MPI_CHAR, send_stat.MPI_SOURCE, send_stat.MPI_TAG, MPI_COMM_WORLD);
                send_idx++;
            }
        }

        /* We don't really care about the contents of the request here. Inform other processes that master is done sending. */
        MPI_Ibarrier(intra_comm, &master_map_req);
        MPI_Ibarrier(MPI_COMM_WORLD, &master_map_req);

        for (int i = 0; i < num_files; i++)
        {
            free(files[i]);
        }
        free(files);
        free(file_path);

        MPI_Status recv_stat;
        KeyValue *recv_buffer;

        int m_len;

        /* Since its painful to realloc a string all the time, we shall continuously write to the file.*/
        write_to_file(output_file_name, "");

        /* Int is at most 10 chars, key is 8, space and newline are 1 each.*/
        char write_buff[20];
        for (int i = 0; i < num_reduce_workers; i++)
        {
            /* Check the tag used between master and reduce for the header containing the count of KeyValue pairs.*/
            MPI_Probe(MPI_ANY_SOURCE, REDUCE_MASTER, MPI_COMM_WORLD, &recv_stat);
            MPI_Recv(&m_len, 1, MPI_INT, recv_stat.MPI_SOURCE, REDUCE_MASTER, MPI_COMM_WORLD, &recv_stat);

            /* If there are pairs to be had, receive them. */
            if (m_len)
            {
                recv_buffer = (KeyValue *) malloc(m_len * sizeof(KeyValue));
                MPI_Recv(recv_buffer, m_len, MPI_KeyValue, recv_stat.MPI_SOURCE, REDUCE_MASTER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                for (int j = 0; j < m_len; j++)
                {
                    sprintf(write_buff, "%s %d\n", recv_buffer[j].key, recv_buffer[j].val);
                    append_to_file(output_file_name, write_buff);
                }
                free(recv_buffer);                   
            }
        }
    }
    else if ((rank >= 1) && (rank <= num_map_workers))
    {
        /* files by worker keeps track of how many files a worker works on. */
        int files_complete;
        int has_file;
        int req_count = 0;
        int num_files_by_worker = 0;
        int message_length[num_files];

        MPI_Status status[num_files];
        MPI_Request files_request;

        char **files = (char **) malloc(num_files * sizeof(char *));

        MPI_Ibarrier(intra_comm, &files_request);

        /* Wait for files while master has files to send. */
        while (!files_complete || has_file)
        {
            /* Request for messasges from master. */
            MPI_Request_get_status(files_request, &files_complete, MPI_STATUS_IGNORE);

            /* Alright to oversend by a bit since they will be ignored. */
            if (req_count < 1)
            {
                MPI_Send(&READY_SIG, 1, MPI_INT, 0, rank, MPI_COMM_WORLD);
                req_count++;
            }
            
            /* Check for files to receive. */
            MPI_Iprobe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &has_file, &status[num_files_by_worker]);

            if (has_file)
            {
                req_count--;
                MPI_Get_count(&status[num_files_by_worker], MPI_CHAR, &message_length[num_files_by_worker]);

                files[num_files_by_worker] = (char *) malloc(message_length[num_files_by_worker]);
                if (!files[num_files_by_worker])
                {
                    exit(1);
                }
                
                MPI_Recv(files[num_files_by_worker], message_length[num_files_by_worker], MPI_CHAR, 0, rank,
                    MPI_COMM_WORLD, &status[num_files_by_worker]);

                MapTaskOutput *output = map(files[num_files_by_worker]);

                int pairs_per_worker[num_reduce_workers];
                int partition_arr[output->len];
                memset(pairs_per_worker, 0, num_reduce_workers * sizeof(int)); 

                /* Send output to reduce workers */
                for (int j = 0; j < output->len; j++)
                {
                    partition_arr[j] = partition(output->kvs[j].key, num_reduce_workers);
                    pairs_per_worker[partition_arr[j]]++;
                }

                for (int j = 0; j < num_reduce_workers; j++)
                {
                    MPI_Send(&pairs_per_worker[j], 1, MPI_INT, num_map_workers + j + 1, 3, MPI_COMM_WORLD);
                }

                for (int j = 0; j < output->len; j++)
                {
                    MPI_Send(&output->kvs[j], 1, MPI_KeyValue, partition_arr[j] + 1 + num_map_workers, 3, MPI_COMM_WORLD);
                }

                free_map_task_output(output);
                num_files_by_worker++;
            }

        }
        
        MPI_Ibarrier(MPI_COMM_WORLD, &master_map_req);

        for (int i = 0; i < num_files_by_worker; i++)
        {
            free(files[i]);
        }
        free(files);    
    }
    else
    {
        MPI_Status stat;
        MPI_Request personal_req[num_reduce_workers];

        int map_complete = 0;
        int has_message = 0;
                
        /* initial size and setup for storing key-value pairs. We malloc more at the start since its slow to realloc */
        int keyval_size = 1000000;
        int key_count = 0;
        KeyValueArr *kvm = (KeyValueArr *) malloc(keyval_size * sizeof(KeyValueArr));

        MPI_Ibarrier(MPI_COMM_WORLD, &personal_req[rank - num_reduce_workers]);

        /* 
           Similar to the communication between master and map worker, we poll for messages here, and check if there are
           no messages left.   
        */
        while (!map_complete || has_message)
        {
            MPI_Request_get_status(personal_req[rank - num_reduce_workers], &map_complete, &stat);
            MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &has_message, &stat);
            if (has_message)
            {
                int kv_count;

                MPI_Recv(&kv_count, 1, MPI_INT, stat.MPI_SOURCE, stat.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                KeyValue *kv_buffer = (KeyValue *) malloc(kv_count * sizeof(KeyValue));
                if (!kv_buffer)
                {
                    exit(1);
                }

                for (int i = 0; i < kv_count; i++)
                {
                    MPI_Recv(kv_buffer + i, 1, MPI_KeyValue, stat.MPI_SOURCE, stat.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                }

                /* Iterate through all key value pairs received. realloc if insufficient memory. Technically can be abstracted into a "vector". oop xd */
                for (int i = 0; i < kv_count; i++)
                {   
                    int inserted = 0;

                    /* Linear probe to determine whether the key exists. */
                    for (int j = 0; j < key_count; j++)
                    {
                        if (!strcmp(kv_buffer[i].key, kvm[j].key))
                        {
                            inserted = 1;
                            if (kvm[j].len < kvm[j].max_size)
                            {
                                kvm[j].val[kvm[j].len] = kv_buffer[i].val;
                                kvm[j].len++;
                            }
                            else
                            {
                                kvm[j].max_size *= 2;
                                kvm[j].val = (int*) realloc(kvm[j].val, kvm[j].max_size * sizeof(int));
                                if (!kvm[j].val)
                                {
                                    exit(1);
                                }
                                kvm[j].val[kvm[j].len] = kv_buffer[i].val;
                                kvm[j].len++;
                            }
                            break;
                        }
                    }
                    /* If it is the first time encountering a key, append it. */
                    if (!inserted)
                    {
                        if (key_count >= keyval_size)
                        {
                            keyval_size *= 2;
                            kvm = (KeyValueArr *) realloc(kvm, keyval_size * sizeof(KeyValueArr));
                            if (!kvm)
                            {
                                exit(1);
                            }
                        }
                        strcpy(kvm[key_count].key, kv_buffer[i].key);
                        kvm[key_count].max_size = 100000;
                        kvm[key_count].val = malloc(kvm[key_count].max_size * sizeof(int));
                        kvm[key_count].len = 1;
                        kvm[key_count].val[0] = kv_buffer[i].val;
                        key_count++;
                    }                    
                }
                free(kv_buffer);
            }
        }

        /* Reduce the data after receiving everything. */
        KeyValue *send_buffer = (KeyValue *) malloc(key_count * sizeof(KeyValue));
        for (int i = 0; i < key_count; i++)
        {
            send_buffer[i] = reduce(kvm[i].key, kvm[i].val, kvm[i].len);
        }

        free(kvm);

        /* Use a "header" to inform master of the number of KeyValue pairs to expect from the reduce worker. */
        MPI_Send(&key_count, 1, MPI_INT, 0, REDUCE_MASTER, MPI_COMM_WORLD);

        if (key_count)
        {
            MPI_Send(send_buffer, key_count, MPI_KeyValue, 0, REDUCE_MASTER, MPI_COMM_WORLD);
        }

        free(send_buffer);
    }


    //Clean up
    MPI_Finalize();

    end = wall_clock_time();
    fprintf(stderr, "rank %d took %1.2f seconds\n", rank, ((float)(end - start))/1000000000);

    return 0;
}
