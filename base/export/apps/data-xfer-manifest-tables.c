#include "jsoc_main.h"
#include "drms_types.h"

char *module_name = "data-xfer-manifest-tables";

typedef enum
{
   kMymodErr_Success,
   kMymodErr_Whatever
} MymodError_t;

#define ARG_SERIES "series"
#define ARG_OPERATION "operation"
#define OPTION_NUMBER "n"
#define OPTION_NUMBER_DEFAULT "4096"
#define OPTION_SEGMENTS "segments"
#define OPTION_SEGMENTS_DEFAULT "*"
#define OPTION_ID_VALUE "new_value"
#define OPTION_ID_VALUE_DEFAULT "Y"

#define OPERATION_CREATE "create"
#define OPERATION_DELETE "delete"
#define OPERATION_UPDATE "update"
#define OPERATION_FETCH "fetch"

struct segment_obj_struct
{
    char recnum[32];
    char segment[DRMS_MAXSEGNAMELEN];
};

typedef struct segment_obj_struct segment_obj_t;

ModuleArgs_t module_args[] =
{
    { ARG_STRING, ARG_SERIES, NULL, "data series for which the manifest table exists" },
    { ARG_STRING, ARG_OPERATION, NULL, "the manifest-table operation to perform (create, delete, update, fetch)" },
    { ARG_INT, OPTION_NUMBER, OPTION_NUMBER_DEFAULT, "when the operation is `fetch`, this option is the number of DRMS_IDs to fetch" },
    { ARG_STRINGS, OPTION_SEGMENTS, OPTION_SEGMENTS_DEFAULT, "when the operation is `fetch`, this option is a list of segments for which DRMS_IDs will be fetched; `*` implies all segments" },
    { ARG_STRING, OPTION_ID_VALUE, OPTION_ID_VALUE_DEFAULT, "when the operation is `update`, this option is the new value for the provided DRMS IDs" },
    { ARG_END }
};

static int validate_segments(DRMS_Env_t *env, LinkedList_t *list, const char *series, char **segments, int number_segments)
{
    DRMS_Record_t *template = NULL;
    int all_segments = -1;
    DRMS_Segment_t *segment = NULL;
    HIterator_t *iterator = NULL;
    int segment_index = -1;
    int drms_status = DRMS_SUCCESS;
    int error = 0;

    /* series exists, so this should not be NULL */
    XASSERT(list && series && segments);
    template = drms_template_record(env, series, &drms_status);
    XASSERT(template);

    if (drms_status == DRMS_SUCCESS)
    {
        if (number_segments == 1 && strcmp(segments[0], "*") == 0)
        {
            /* all segments - the command line contained `segments=*` (or no `segments` argument, which will
             * cause segments to default to '*') */

            /* do not follow links */
            while ((segment = drms_record_nextseg(template, &iterator, 0)) != NULL)
            {
                list_llinserttail(list, &segment->info + offsetof(DRMS_SegmentInfo_t, name));
            }

            if (iterator)
            {
                 hiter_destroy(&iterator);
            }
        }
        else
        {
            for (segment_index = 0; segment_index < number_segments; segment_index++)
            {
                if (hcon_member_lower(&template->segments, segments[segment_index]))
                {
                    list_llinserttail(list, &(segments[segment_index]));
                }
                else
                {
                    fprintf(stderr, "[ validate_segments ] WARNING: unknown segment `%s`; skipping\n", segments[segment_index]);
                }
            }
        }
    }
    else
    {
        error = 1;
        fprintf(stderr, "[ validate_segments ] failure obtaining template records for series `%s`\n", series);
    }

    return error;
}

static int create_manifest(DRMS_Env_t *env, const char *series)
{
    int error = 0;
    char command[256] = {0};
    DB_Text_Result_t *query_result = NULL;

    snprintf(command, sizeof(command), "SELECT drms.create_manifest('%s')", series);

    query_result = drms_query_txt(drms_env->session, command);
    if (query_result)
    {
        if (query_result->num_rows == 1 && query_result->num_cols == 1)
        {
            if (*query_result->field[0][0] != 't')
            {
                error = 1;
                fprintf(stderr, "[ create_manifest ] failure creating manifest table\n");
            }
        }
        else
        {
            error = 1;
            fprintf(stderr, "[ create_manifest ] unexpected number of rows or columns returned\n");
        }

        db_free_text_result(query_result);
        query_result = NULL;
    }
    else
    {
        error = 1;
        fprintf(stderr, "[ create_manifest ] invalid DB command `%s`\n", command);
    }

    return error;
}

static int delete_manifest(DRMS_Env_t *env, const char *series)
{
    int error = 0;
    char command[256] = {0};
    DB_Text_Result_t *query_result = NULL;

    snprintf(command, sizeof(command), "SELECT drms.delete_manifest('%s')", series);

    query_result = drms_query_txt(drms_env->session, command);
    if (query_result)
    {
        if (query_result->num_rows == 1 && query_result->num_cols == 1)
        {
            if (*query_result->field[0][0] != 't')
            {
                error = 1;
                fprintf(stderr, "[ delete_manifest ] failure deleting manifest table\n");
            }
        }
        else
        {
            error = 1;
            fprintf(stderr, "[ delete_manifest ] unexpected number of rows or columns returned\n");
        }

        db_free_text_result(query_result);
        query_result = NULL;
    }
    else
    {
        error = 1;
        fprintf(stderr, "[ delete_manifest ] invalid DB command `%s`\n", command);
    }

    return error;
}

static int fetch_ids(DRMS_Env_t *env, const char *series, LinkedList_t *segments, int number_ids, FILE *stream_out, LinkedList_t **segment_obj_list_out)
{
    int error = 0;
    char *segment_list = NULL;
    size_t sz_segment_list = 128;
    HIterator_t *iterator = NULL;
    char *segment = NULL;
    char command[256] = {0};
    char buffer[256] = {0};
    char *separator = NULL;
    char *ptr_recnum = NULL;
    char *ptr_segment = NULL;
    HContainer_t *segment_hash = NULL;
    char segment_id[64] = {0};
    int first = -1;
    segment_obj_t segment_obj;
    LinkedList_t *segment_obj_list = NULL;
    ListNode_t *node = NULL;

    DB_Text_Result_t *query_result = NULL;
    int row_index = -1;

    segment_list = calloc(sz_segment_list, sizeof(char));

    if (segment_list)
    {
        first = 1;
        list_llreset(segments);
        while ((node = list_llnext(segments)) != NULL)
        {
            segment = *((char **)node->data);

            if (!first)
            {
                segment_list = base_strcatalloc(segment_list, ", ", &sz_segment_list);
            }
            else
            {
                first = 0;
            }

            snprintf(buffer, sizeof(buffer), "'%s'", segment);
            segment_list = base_strcatalloc(segment_list, buffer, &sz_segment_list);
        }

        /* returns IDs ordered by ascending recnum */
        snprintf(command, sizeof(command), "SELECT drms_id FROM drms.get_n_drms_ids('%s', ARRAY[%s], %d)", series, segment_list, number_ids);

        fprintf(stderr, command);
        fprintf(stderr, "\n");

        free(segment_list);
        segment_list = NULL;

        query_result = drms_query_txt(drms_env->session, command);
        if (query_result)
        {
            if (query_result->num_cols == 1)
            {
                segment_hash = hcon_create(sizeof(char), 64, NULL, NULL, NULL, NULL, 0);

                /* print all to `stream_out` (`stream_out` should be fully buffered) */
                setvbuf(stream_out, NULL, _IOFBF, 0);
                for (row_index = 0, first = 1; row_index < query_result->num_rows; row_index++)
                {
                    /* stream to update code - must strip out recnums, removing duplicates*/
                    snprintf(buffer, sizeof(buffer), "%s", query_result->field[row_index][0]);

                    ptr_recnum = strchr(buffer, ':') + 1;
                    separator = strchr(ptr_recnum, ':');
                    *separator = '\0';
                    ptr_segment = separator + 1;

                    snprintf(segment_id, sizeof(segment_id), "%s:%s", ptr_recnum, ptr_segment);
                    if (!hcon_member(segment_hash, segment_id))
                    {
                        if (!segment_obj_list)
                        {
                            segment_obj_list = list_llcreate(sizeof(segment_obj_t), NULL);
                            first = 0;
                        }

                        snprintf(segment_obj.recnum, sizeof(segment_obj.recnum), "%s", ptr_recnum);
                        snprintf(segment_obj.segment, sizeof(segment_obj.segment), "%s", ptr_segment);
                        list_llinserttail(segment_obj_list, &segment_obj);

                        fprintf(stderr, "added %s to segment_obj_list\n", segment_id);
                        hcon_insert(segment_hash, segment_id, "T");
                    }

                    /* stream out to caller */
                    fprintf(stream_out, query_result->field[row_index][0]);
                    fprintf(stream_out, "\n");
                }

                hcon_destroy(&segment_hash);
                fflush(stream_out);
            }
            else
            {
                error = 1;
                fprintf(stderr, "[ fetch_ids ] unexpected number of columns returned\n");
            }

            db_free_text_result(query_result);
            query_result = NULL;
        }
        else
        {
            error = 1;
            fprintf(stderr, "[ fetch_ids ] invalid DB command `%s`\n", command);
        }
    }
    else
    {
        error = 1;
        fprintf(stderr, "[ fetch_ids ] out of memory\n");
    }

    if (!error)
    {
        *segment_obj_list_out = segment_obj_list;
    }

    return error;
}

static int update_manifest(DRMS_Env_t *env, const char *series, LinkedList_t *segments, const char *new_value, FILE *stream, const char *recnum_list_in)
{
    char *line = NULL;
    size_t buffer_length = 0;
    ssize_t number_chars = 0;
    size_t total_number_chars = 0;
    char *command = NULL;
    size_t sz_command = 512;
    DB_Text_Result_t *query_result = NULL;
    char drms_id[128];
    char *recnum_list = NULL;
    size_t sz_recnum_list = 1048576; /* write up to 1 M bytes each iteration */
    char *segment_list = NULL;
    size_t sz_segment_list = 1024;
    char *segment = NULL;
    int first = -1;
    char *ptr_newline = NULL;
    ListNode_t *node = NULL;
    char buffer[64];
    int error = 0;

    if (strcmp(new_value, OPTION_ID_VALUE_DEFAULT) != 0)
    {
        /* make comma-separated list of segments */
        segment_list = calloc(sz_segment_list, sizeof(char));
        if (segment_list)
        {
            first = 1;
            list_llreset(segments);
            while ((node = list_llnext(segments)) != NULL)
            {
                segment = *((char **)node->data);

                if (!first)
                {
                    segment_list = base_strcatalloc(segment_list, ", ", &sz_segment_list);
                }
                else
                {
                    first = 0;
                }

                snprintf(buffer, sizeof(buffer), "'%s'", segment);
                segment_list = base_strcatalloc(segment_list, buffer, &sz_segment_list);
            }
        }
        else
        {
            fprintf(stderr, "[ update_manifest ] out of memory 1\n");
            error == 1;
        }

        if (!error)
        {
            /* list of recnums is piped in to stdin */
            recnum_list = calloc(sz_recnum_list, sizeof(char));
            if (!recnum_list)
            {
                fprintf(stderr, "[ update_manifest ] out of memory 2\n");
                error == 1;
            }
        }

        if (!error)
        {
            total_number_chars = 0;
            while (1)
            {
                if (stream)
                {
                    number_chars = getline(&line, &buffer_length, stream);
                }

                if ((stream && (total_number_chars >= 104800 || number_chars == -1)) || recnum_list_in)
                {
                    if (recnum_list_in)
                    {
                        recnum_list = (char *)recnum_list_in;
                    }


                    command = calloc(sizeof(char), sz_command);

                    command = base_strcatalloc(command, "SELECT drms_id AS answer FROM drms.update_drms_ids('", &sz_command);
                    command = base_strcatalloc(command, series, &sz_command);
                    command = base_strcatalloc(command, "', ARRAY[", &sz_command);
                    command = base_strcatalloc(command, segment_list, &sz_command);
                    command = base_strcatalloc(command, "], ARRAY[", &sz_command);
                    command = base_strcatalloc(command, recnum_list, &sz_command);
                    command = base_strcatalloc(command, "], '", &sz_command);
                    command = base_strcatalloc(command, new_value, &sz_command);
                    command = base_strcatalloc(command, "')", &sz_command);

                    fprintf(stderr, command);
                    fprintf(stderr, "\n");

                    query_result = drms_query_txt(drms_env->session, command);

                    free(command);

                    if (query_result->num_rows == 1 && query_result->num_cols == 1)
                    {
                        if (*query_result->field[0][0] != 't')
                        {
                            error = 1;
                            fprintf(stderr, "[ update_manifest ] failure updating manifest table\n");
                        }
                    }
                    else
                    {
                        error = 1;
                        fprintf(stderr, "[ update_manifest ] unexpected number of rows or columns returned (rows=%d, cols=%d)\n", query_result->num_rows, query_result->num_cols);
                    }

                    if (recnum_list_in)
                    {
                        break;
                    }

                    if (recnum_list)
                    {
                        free(recnum_list);
                        recnum_list = NULL;
                    }

                    if (error)
                    {
                        break;
                    }

                    if (number_chars == -1)
                    {
                        break;
                    }

                    recnum_list = calloc(sz_recnum_list, sizeof(char));
                    total_number_chars = 0;
                }

                if (total_number_chars != 0)
                {
                    recnum_list = base_strcatalloc(recnum_list, ", ", &sz_recnum_list);
                }

                /* `line` has a trailing newline */
                if ((ptr_newline = strrchr(line, '\n')) !=NULL && ptr_newline == &line[strlen(line) - 1])
                {
                    *ptr_newline = '\0';
                }

                recnum_list = base_strcatalloc(recnum_list, line, &sz_recnum_list);
                total_number_chars += number_chars;
            }
        }

        if (recnum_list && !recnum_list_in)
        {
            free(recnum_list);
            recnum_list = NULL;
        }

        if (segment_list)
        {
            free(segment_list);
            segment_list = NULL;
        }
    }
    else
    {
        error = 1;
        fprintf(stderr, "[ update_manifest ] must specify a value for `new_value`\n");
    }

    return error;
}

static int update_one_manifest(DRMS_Env_t *env, const char *series, const char *recnum, const char *segment, const char *new_value)
{
    char *command = NULL;
    size_t sz_command = 512;
    DB_Text_Result_t *query_result = NULL;
    int error = 0;

    command = calloc(sizeof(char), sz_command);

    command = base_strcatalloc(command, "SELECT drms_id AS answer FROM drms.update_drms_ids('", &sz_command);
    command = base_strcatalloc(command, series, &sz_command);
    command = base_strcatalloc(command, "', ARRAY['", &sz_command);
    command = base_strcatalloc(command, segment, &sz_command);
    command = base_strcatalloc(command, "'], ARRAY[", &sz_command);
    command = base_strcatalloc(command, recnum, &sz_command);
    command = base_strcatalloc(command, "], '", &sz_command);
    command = base_strcatalloc(command, new_value, &sz_command);
    command = base_strcatalloc(command, "')", &sz_command);

    fprintf(stderr, command);
    fprintf(stderr, "\n");

    query_result = drms_query_txt(drms_env->session, command);

    free(command);

    if (query_result->num_rows == 1 && query_result->num_cols == 1)
    {
        if (*query_result->field[0][0] != 't')
        {
            error = 1;
            fprintf(stderr, "[ update_one_manifest ] failure updating manifest table\n");
        }
    }
    else
    {
        error = 1;
        fprintf(stderr, "[ update_one_manifest ] unexpected number of rows or columns returned (rows=%d, cols=%d)\n", query_result->num_rows, query_result->num_cols);
    }

    return error;
}

int DoIt(void)
{
    int drms_error = DRMS_SUCCESS;
    const char *series = NULL;
    const char *operation = NULL;
    int number_ids = -1;
    int number_segments = -1;
    int segment_index = -1;
    char **segments = NULL;
    const char *new_value = NULL;
    LinkedList_t *segment_list = NULL;
    int series_exists = -1;
    char series_lower[DRMS_MAXSERIESNAMELEN] = {0};
    char *manifest_table = NULL;
    LinkedList_t *segment_obj_list = NULL;
    ListNode_t *node = NULL;
    segment_obj_t *segment_obj = NULL;
    int error = 0;

    series = params_get_str(&cmdparams, ARG_SERIES);
    operation = params_get_str(&cmdparams, ARG_OPERATION);
    number_ids = params_get_int(&cmdparams, OPTION_NUMBER);
    number_segments = cmdparams_get_strarr(&cmdparams, OPTION_SEGMENTS, &segments, NULL);
    new_value = params_get_str(&cmdparams, OPTION_ID_VALUE);

    segment_list = list_llcreate(sizeof(char *), NULL);

    if (!segment_list)
    {
        fprintf(stderr, "out of memory 1\n");
        error = 1;
    }

    if (!error)
    {
        /* check for series existence */
        series_exists = drms_series_exists(drms_env, series, &drms_error);

        if (!drms_error)
        {
            if (series_exists)
            {
                error = validate_segments(drms_env, segment_list, series, segments, number_segments);

                if (!error)
                {
                    /* db functions check for existence of shadow table, manifest */
                    if (strcasecmp(operation, OPERATION_CREATE) == 0)
                    {
                        error = create_manifest(drms_env, series);
                    }
                    else if (strcasecmp(operation, OPERATION_DELETE) == 0)
                    {
                        error = delete_manifest(drms_env, series);
                    }
                    else if (strcasecmp(operation, OPERATION_UPDATE) == 0)
                    {
                        error = update_manifest(drms_env, series, segment_list, new_value, stdin, NULL);
                    }
                    else if (strcasecmp(operation, OPERATION_FETCH) == 0)
                    {
                        error = fetch_ids(drms_env, series, segment_list, number_ids, stdout, &segment_obj_list);
                        if (!error)
                        {
                            if (segment_obj_list)
                            {
                                list_llreset(segment_obj_list);
                                while ((node = list_llnext(segment_obj_list)) != NULL)
                                {
                                    segment_obj = (segment_obj_t *)node->data;

                                    /* there might have been no available DRMS_IDs, in which case the manifest need to be updated */
                                    error = update_one_manifest(drms_env, series, segment_obj->recnum, segment_obj->segment, "P");

                                    if (error)
                                    {
                                        break;
                                    }
                                }
                            }
                        }

                        list_llfree(&segment_obj_list);
                    }
                    else
                    {
                        error = 1;
                        fprintf(stderr, "invalid operation `%s`\n", operation);
                    }
                }
                else
                {
                    error = 1;
                    fprintf(stderr, "invalid `%s` argument\n", OPTION_SEGMENTS);
                }
            }
            else
            {
                /* series does not exist */
                error = 1;
                fprintf(stderr, "series `%s` does not exist\n", series);
            }
        }
        else
        {
            error = 1;
            fprintf(stderr, "failure checking for series existence\n");
        }
    }

    /* if the table */

    if (error)
    {

    }


    return error;
}
