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
#define OPTION_SEGMENTS "segments"
#define OPTION_ID_VALUE "new_value"
#define OPTION_ID_VALUE_DEFAULT "*"

#define OPERATION_CREATE "create"
#define OPERATION_DELETE "delete"
#define OPERATION_UPDATE "update"
#define OPERATION_FETCH "fetch"


ModuleArgs_t module_args[] =
{
    { ARG_STRING, ARG_SERIES, NULL, "data series for which the manifest table exists" },
    { ARG_STRING, ARG_OPERATION, NULL, "the manifest-table operation to perform (create, delete, update, fetch)" },
    { ARG_INT, OPTION_NUMBER, "4096", "when the operation is `fetch`, this option is the number of DRMS_IDs to fetch" },
    { ARG_STRINGS, OPTION_SEGMENTS, "*", "when the operation is `fetch`, this option is a list of segments for which DRMS_IDs will be fetched" },
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
            /* all segments */

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

static int fetch_ids(DRMS_Env_t *env, const char *series, LinkedList_t *segments, int number_ids, FILE *stream)
{
    int error = 0;
    char *segment_list = NULL;
    size_t sz_segment_list = 128;
    HIterator_t *iterator = NULL;
    char *segment = NULL;
    char command[256] = {0};
    char buffer[256] = {0};
    int first = -1;
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

        snprintf(command, sizeof(command), "SELECT drms_id FROM drms.get_n_drms_ids('%s', ARRAY[%s], %d)", series, segment_list, number_ids);

        free(segment_list);
        segment_list = NULL;

        query_result = drms_query_txt(drms_env->session, command);
        if (query_result)
        {
            if (query_result->num_cols == 1)
            {
                /* print all to `stream` (`stream` should be fully buffered) */
                setvbuf(stream, NULL, _IOFBF, 0);
                for (row_index = 0; row_index < query_result->num_rows; row_index++)
                {
                    fprintf(stream, query_result->field[row_index][0]);
                    fprintf(stream, "\n");
                }

                fflush(stream);
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

    return error;
}

static int update_manifest(DRMS_Env_t *env, const char *series, const char *new_value, FILE *stream)
{
    char *line = NULL;
    size_t buffer_length = 0;
    ssize_t number_chars = 0;
    size_t total_number_chars = 0;
    char command[256] = {0};
    DB_Text_Result_t *query_result = NULL;
    char drms_id[128] = {0};
    char *drms_ids = NULL;
    size_t sz_drms_ids = 1048576; /* write up to 1 M bytes of DRMS ID values each iteration */
    int error = 0;

    if (strcmp(new_value, OPTION_ID_VALUE_DEFAULT) != 0)
    {
        /* list of drms_ids is piped in to stdin */
        drms_ids = calloc(sz_drms_ids, sizeof(char));
        total_number_chars = 0;
        while (1)
        {
            number_chars = getline(&line, &buffer_length, stream);

            if (total_number_chars >= 104800 || number_chars == -1)
            {
                snprintf(command, sizeof(command), "SELECT drms_id AS answer FROM drms.update_drms_ids(ARRAY[%s], '%s')", drms_ids, new_value);
                query_result = drms_query_txt(drms_env->session, command);

                if (query_result->num_rows == 1 && query_result->num_cols == 1)
                {
                    if (*query_result->field[0][0] != 't')
                    {
                        error = 1;
                        fprintf(stderr, "[ update_manifest ] failure deleting manifest table\n");
                    }
                }
                else
                {
                    error = 1;
                    fprintf(stderr, "[ update_manifest ] unexpected number of rows or columns returned (rows=%d, cols=%d)\n", query_result->num_rows, query_result->num_cols);
                }

                if (drms_ids)
                {
                    free(drms_ids);
                }

                if (error)
                {
                    break;
                }

                if (number_chars == -1)
                {
                    break;
                }

                drms_ids = calloc(sz_drms_ids, sizeof(char));
                total_number_chars = 0;
            }

            if (total_number_chars != 0)
            {
                drms_ids = base_strcatalloc(drms_ids, ", ", &sz_drms_ids);
            }

            snprintf(drms_id, sizeof(drms_id), "'%s'", line);
            drms_ids = base_strcatalloc(drms_ids, drms_id, &sz_drms_ids);
            total_number_chars += number_chars;
        }
    }
    else
    {
        error = 1;
        fprintf(stderr, "[ update_manifest ] must specify a value for `new_value`\n");
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
                        error = update_manifest(drms_env, series, new_value, stdin);
                    }
                    else if (strcasecmp(operation, OPERATION_FETCH) == 0)
                    {
                        error = fetch_ids(drms_env, series, segment_list, number_ids, stdout);
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
