#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <semaphore.h>
#include <string.h>
#include <math.h>

#define min(a, b) ((a) < (b) ? (a) : (b))
#define MAX_WORD_SIZE 64
#define MAX_INT 2147483647

typedef struct group {
    int id;
    FILE* fd;
} file_detail;

typedef struct word{
    int max_ids, crt_ids;
    int *file_list;
    char word[MAX_WORD_SIZE];
} word;

typedef struct m_ret {
    int max_size;
    int crt_size;
    word *wrd_list;
} mapper_ret;

typedef struct T {
    // int thread_id;
    int num_files;
    mapper_ret **ret_to;
    
    pthread_mutex_t *next_mutex;
    int *next_file;
    // int *next_list;

    pthread_barrier_t *barrier;

    file_detail *files;
} mapper_arg;

typedef struct Y {
    int num_lists, thread_id, all_words_size, num_reducers;
    
    int *next;
    pthread_mutex_t *mutex;
    int *alphabet;
    int *crt_letter;
    int *keep_sorting;

    mapper_ret **lists;

    int *next_batch;
    word *all_words;

    pthread_barrier_t *start, *internal;
} reducer_arg;


void sanitize_string(char *str) {
    for(int i = 0; str[i] != '\0'; i++) {
        if (str[i] > 64 && str[i] < 91) 
            str[i] += 32;
        if ((str[i] > 31 && str[i] < 97) || str[i] > 122 || str[i] < 0) {
            for(int j = i; str[j] != '\0'; j++)
                str[j] = str[j + 1];
            i--;
            }
        if(str[i] == '\n') str[i] = '\0';
    }
}

int in_list(mapper_ret *entry, char *to_find) {
    word *list = entry->wrd_list;
    for(int i = 0; i < entry->crt_size; i++) {
        if(strncmp(list[i].word, to_find, MAX_WORD_SIZE) == 0)
            return 1;
    }

    return 0;
}

int preliminary_sort(const void *a, const void * b) {
    word *first = (word *)a;
    word *second = (word *)b;
    int ret = strncmp(second->word, first->word, MAX_WORD_SIZE);
    
    if(ret) return -ret;
    return -second->file_list[0] + first->file_list[0];
}

void combine_dups(word *list, int start, int end, int N, pthread_barrier_t *barrier) {

    int last_word_chage = start;
    int i;
    for(i = start + 1; i < end; i++) {
        // if the next words is the same as current word, combine the lists of files they appear in
        if(!strcmp(list[i].word, list[last_word_chage].word)) {
            for(int j = 0; j < list[i].crt_ids; j++) {
                list[last_word_chage].file_list[list[last_word_chage].crt_ids++] = list[i].file_list[j];

                // make more space in the file list
                if (list[last_word_chage].crt_ids >= list[last_word_chage].max_ids) {
                    list[last_word_chage].max_ids *= 2;
                    int *tmp = malloc(list[last_word_chage].max_ids * sizeof(int));
                    memcpy(tmp, list[last_word_chage].file_list, list[last_word_chage].max_ids * sizeof(int) / 2);
                    free(list[last_word_chage].file_list);
                    list[last_word_chage].file_list = tmp;
                }
            }
            list[i].crt_ids = 0;
        } else last_word_chage = i;
    }

    pthread_barrier_wait(barrier);

    // after each thread did their part, check if the next thread processed the word
    // the previous thread had at the end
    // if yes, combine file lists
    if(i < N) {
        if(!strcmp(list[i].word, list[last_word_chage].word)) {
            for(int j = 0; j < list[i].crt_ids; j++) {
                list[last_word_chage].file_list[list[last_word_chage].crt_ids++] = list[i].file_list[j];

                if (list[last_word_chage].crt_ids >= list[last_word_chage].max_ids) {
                    list[last_word_chage].max_ids *= 2;
                    int *tmp = malloc(list[last_word_chage].max_ids * sizeof(int));
                    memcpy(tmp, list[last_word_chage].file_list, list[last_word_chage].max_ids * sizeof(int) / 2);
                    free(list[last_word_chage].file_list);
                    list[last_word_chage].file_list = tmp;
                }
            }
            list[i].crt_ids = 0;
        } 
    }
}

int compare_list_size(const void *a, const void * b) {
    word *first = (word *)a;
    word *second = (word *)b;
    return second->crt_ids - first->crt_ids;
}

void *write_in_files(reducer_arg *r_arg) {
    int my_letter;
    int next_letter;
    int size;
    char filename[] = "x.txt";
    FILE *out;
    while(1) {
        // get the next unprocessed letter
        pthread_mutex_lock(r_arg->mutex);
        while(*r_arg->crt_letter < 26 && r_arg->alphabet[*r_arg->crt_letter] == MAX_INT) {
            if (r_arg->thread_id == 0) {
                filename[0] = (*r_arg->crt_letter) + 97;
                out = fopen(filename, "w");
                fclose(out);
            }
            (*r_arg->crt_letter)++;
        }
        my_letter = (*r_arg->crt_letter)++;
        pthread_mutex_unlock(r_arg->mutex);

        if(my_letter >= 26) return NULL;

        // find how many words start with the letter curently processed
        for(next_letter = my_letter + 1; next_letter < 26 && r_arg->alphabet[next_letter] == MAX_INT; next_letter++);

        if(next_letter == 26) size = r_arg->all_words_size - r_arg->alphabet[my_letter];
        else size = r_arg->alphabet[next_letter] - r_arg->alphabet[my_letter];

        // sort only the words starting with the curently processed letter
        // by the number of files it appears in
        qsort(r_arg->all_words + r_arg->alphabet[my_letter], size, sizeof(word), compare_list_size);

        // write all words starting with the curently prcessed letter in the corrent output file
        filename[0] = (char)my_letter + 97;
        out = fopen(filename, "w");
        for(int i = r_arg->alphabet[my_letter]; i < r_arg->alphabet[my_letter] + size; i++) {
            if(r_arg->all_words[i].crt_ids > 0) {
                fprintf(out, "%s:[", r_arg->all_words[i].word);

                for(int j = 0; j < r_arg->all_words[i].crt_ids; j++) {
                    if(j != 0 && j != r_arg->all_words[i].crt_ids) fprintf(out, " ");
                    fprintf(out, "%d", r_arg->all_words[i].file_list[j]);
                }
                fprintf(out, "]\n");
            }
        }
        fclose(out);
    }
}

void *mapper_function(void *arg) {
    mapper_arg *m_arg = (mapper_arg *)arg;
    int crt;
    int file_id;
    int return_idx;
    char buffer[2 * MAX_WORD_SIZE];
    mapper_ret *ret;
    
    pthread_barrier_wait(m_arg->barrier);

    while(1) {
        pthread_mutex_lock(m_arg->next_mutex);
        crt = (*(m_arg->next_file))++; // get the next unprocessed file
        pthread_mutex_unlock(m_arg->next_mutex);
        if(crt >= m_arg->num_files) break;
        
        ret = calloc(1, sizeof(mapper_ret));
        ret->max_size = 128;
        ret->wrd_list = calloc(ret->max_size, sizeof(word));
        ret->crt_size = 0;

        file_id = m_arg->files[crt].id;

            while(fscanf(m_arg->files[crt].fd, "%s", buffer) > 0) {

                sanitize_string(buffer);

                if(in_list(ret, buffer) || buffer[0] == '\0') continue;
                
                // for each word, create a list of files it appears in
                ret->wrd_list[ret->crt_size].max_ids = 16;
                ret->wrd_list[ret->crt_size].file_list = calloc(16,  sizeof(int));
                ret->wrd_list[ret->crt_size].file_list[0] = file_id;
                ret->wrd_list[ret->crt_size].crt_ids = 1;
                strncpy(ret->wrd_list[ret->crt_size].word, buffer, MAX_WORD_SIZE);
                ret->crt_size++;

                // make more space in the current list
                if((ret->crt_size) >= ret->max_size) {
                    ret->max_size *= 2;
                    word *tmp = calloc(ret->max_size, sizeof(word));
                    memcpy(tmp, ret->wrd_list, ret->max_size * sizeof(word) / 2);
                    free(ret->wrd_list);
                    ret->wrd_list = tmp;
                }
            }
            fclose(m_arg->files[crt].fd);
            *(m_arg->ret_to + crt) = ret;
        }

        pthread_barrier_wait(m_arg->barrier);
        return NULL;
}

void *reducer_function(void *arg) {
    reducer_arg *r_arg = (reducer_arg *)arg;
    pthread_barrier_wait(r_arg->start);

    // merging of lists returned by mappers
    int crt;
    int write_at;
    while(1) {
        pthread_mutex_lock(r_arg->mutex);
        crt = (*(r_arg->next))++; // get the next unprocessed list from mappers
        write_at = (*(r_arg->next_batch)); 
        if(crt >= r_arg->num_lists) {
            pthread_mutex_unlock(r_arg->mutex);
            break;
        }
        // reserve space to write all the words in the currently processed list
        // in the list containing all words
        (*(r_arg->next_batch)) = write_at + (r_arg->lists[crt]->crt_size);
        pthread_mutex_unlock(r_arg->mutex);
        
        for(int i = 0; i < r_arg->lists[crt]->crt_size; i++)
            r_arg->all_words[write_at + i] = r_arg->lists[crt]->wrd_list[i];
    }

    pthread_barrier_wait(r_arg->internal);

    // sort all the words alphabetically, and by the file they apper in if words are equal
    int thread_id = r_arg->thread_id;
    int N = r_arg->all_words_size;
    int P = r_arg->num_reducers;
    int start = thread_id * ceil((double)N / P);
    int end = min((thread_id + 1) * ceil((double)N / P), N);

    if(thread_id == 0) qsort(r_arg->all_words, N, sizeof(word), preliminary_sort);

    pthread_barrier_wait(r_arg->internal);

    // find the indexes where words with a certain letter start
    int first_letter = start;
    for (int i = start; i < end; i++) {
        if(r_arg->all_words[first_letter].word[0] != r_arg->all_words[i].word[0] || i == end - 1) {
                pthread_mutex_lock(r_arg->mutex);
                if(r_arg->alphabet[r_arg->all_words[first_letter].word[0] - 97] > first_letter)
                    r_arg->alphabet[r_arg->all_words[first_letter].word[0] - 97] = first_letter;
                pthread_mutex_unlock(r_arg->mutex);
                first_letter = i;
            }
    }
    pthread_barrier_wait(r_arg->internal);

    // combine duplicate words to make a single list with all files it appears in
    combine_dups(r_arg->all_words, start, end, N, r_arg->internal);

    pthread_barrier_wait(r_arg->internal);

    // create and write the result in the correct files
    write_in_files(r_arg);

    return  NULL;
}

int main(int argc, char **argv)
{
    int M = atoi(argv[1]);
    int R = atoi(argv[2]);
    FILE* input = fopen(argv[3], "r");
    int num_files;
    char buffer[100];
    void *status;

    fscanf(input, "%d", &num_files);
    file_detail *files = malloc(num_files * sizeof(file_detail));

    // initialization of argument for mapper threads
    int next_file = 0;
    mapper_arg M_arg;
    M_arg.num_files = num_files;
    M_arg.files = files;
    M_arg.next_file = &next_file;
    pthread_mutex_t next_mutex;
    pthread_mutex_init(&next_mutex, NULL);
    M_arg.next_mutex = &next_mutex;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, 0, M + 1);
    M_arg.barrier = &barrier;
    mapper_ret **m_ret = calloc(num_files, sizeof(mapper_ret *));
    M_arg.ret_to = m_ret;

    // initializatopn of arguments for reducer threads 
    int next = 0;
    int batch = 0;
    int keep_sorting = 1;
    reducer_arg R_arg;
    pthread_barrier_t start;
    pthread_barrier_init(&start, 0, R + 1);
    R_arg.start = &start;
    R_arg.lists = m_ret;
    R_arg.num_lists = num_files;
    R_arg.next = &next;
    R_arg.next_batch = &batch;
    R_arg.num_reducers = R;
    R_arg.keep_sorting = &keep_sorting;
    pthread_mutex_t r_mutex;
    pthread_mutex_init(&r_mutex, NULL);
    R_arg.mutex = &r_mutex;
    pthread_barrier_t internal;
    pthread_barrier_init(&internal, 0, R);
    R_arg.internal = &internal;
    int alphabet[27]; // keep the index where words with a letter start in the final word set
    int current_letter = 0;
    for(int i = 0; i < 26; i++) alphabet[i] = MAX_INT;
    R_arg.alphabet = alphabet;
    R_arg.crt_letter = &current_letter;
    reducer_arg *r_arg = malloc(R * sizeof(reducer_arg));

    pthread_t *threads = malloc((M + R) * sizeof(pthread_t));

    for(int i = 0 ; i < M + R; i++) {
        if(i < R) {
            r_arg[i] = R_arg;
            r_arg[i].thread_id = i;
            pthread_create(&threads[i], NULL, reducer_function, r_arg + i);
        } else {
            pthread_create(&threads[i], NULL, mapper_function, &M_arg);
        }
    }

    for(int i = 0; fscanf(input, "%s", buffer) > 0; i++) {
        files[i].fd = fopen(buffer, "r");
        files[i].id = i + 1;
    }
    fclose(input);
    pthread_barrier_wait(M_arg.barrier);
    pthread_barrier_wait(M_arg.barrier);
    free(files);

    int num_all_words = 0;
    for(int i = 0; i < num_files; i++)
        num_all_words += m_ret[i]->crt_size;

    word *all_words = malloc(num_all_words * sizeof(word));

    // give the reducer threads the space to work with all words
    for(int i = 0; i < R; i++) {
        r_arg[i].all_words = all_words;
        r_arg[i].all_words_size = num_all_words;
        }
    pthread_barrier_wait(R_arg.start);

    for(int i = 0; i < M + R; i++) {
        pthread_join(threads[i], &status);
    }

    for(int i = 0; i < num_all_words; i++) {
        free(all_words[i].file_list);
    }
    for(int i = 0; i < num_files; i++) {
        free(m_ret[i]->wrd_list);
        free(m_ret[i]);
    }
    free(m_ret);
    free(all_words);
    free(r_arg);
    free(threads);

    return 0;
}
